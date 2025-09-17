# src/loaders/clickhouse_loader.py
from typing import Callable, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from core.config import config
from core.logging import log_with_timestamp

def load_to_clickhouse(
    data: pd.DataFrame,
    table_name: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    batch_size: int = None,
    target_hour: Optional[datetime] = None
) -> bool:
    """
    Generic ClickHouse loader that works with pandas DataFrames.
    """
    try:
        from clickhouse_driver import Client

        clickhouse_config = config.get_clickhouse_config()
        client = Client(
            host=host or clickhouse_config['host'],
            port=port or clickhouse_config['port'],
            user=user or clickhouse_config['user'],
            password=password or clickhouse_config['password'],
            database=database or clickhouse_config['database']
        )

        if data.empty:
            log_with_timestamp(f"No data to load to ClickHouse table '{table_name}'", "ClickHouse Loader", "info")
            return True

        # Convert DataFrame to list of dictionaries for ClickHouse
        data_list = data.to_dict('records')
        
        # Handle NaN values and array data by converting to None
        string_columns = ['name', 'symbol', 'slug', 'tags', 'platform', 'quote_currency']
        numeric_columns = {
            'id','cmc_rank','num_market_pairs','circulating_supply','total_supply','max_supply','price',
            'volume_24h','volume_change_24h','percent_change_1h','percent_change_24h','percent_change_7d',
            'percent_change_30d','percent_change_60d','percent_change_90d','market_cap','market_cap_dominance',
            'fully_diluted_market_cap','self_reported_circulating_supply','self_reported_market_cap','tvl_ratio'
        }
        for record in data_list:
            for key, value in record.items():
                # Convert Decimal to float for ClickHouse Float/Decimal columns
                try:
                    from decimal import Decimal
                    if isinstance(value, Decimal):
                        record[key] = float(value)
                        value = record[key]
                except Exception:
                    pass
                # Normalize numpy scalar types
                try:
                    import numpy as _np
                    if isinstance(value, (_np.float32, _np.float64)):
                        record[key] = float(value)
                        value = record[key]
                    elif isinstance(value, (_np.int32, _np.int64)):
                        record[key] = int(value)
                        value = record[key]
                except Exception:
                    pass
                if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                    if key in string_columns:
                        record[key] = ''  # Empty string for string columns
                    else:
                        record[key] = None  # None for numeric columns
                # Coerce numeric columns from string when possible
                elif key in numeric_columns and isinstance(value, str):
                    try:
                        record[key] = float(value)
                    except Exception:
                        try:
                            record[key] = int(value)
                        except Exception:
                            record[key] = None
                # Ensure non-nullable numeric columns have safe defaults
                if key == 'price' and record.get(key) is None:
                    record[key] = 0.0
                elif isinstance(value, dict):
                    # Convert dict to JSON string
                    import json
                    try:
                        record[key] = json.dumps(value)
                    except Exception:
                        record[key] = str(value)
                elif isinstance(value, np.ndarray):
                    # Convert numpy arrays to Python lists
                    record[key] = value.tolist()
                elif hasattr(value, 'tolist'):
                    # Convert pandas arrays to Python lists
                    record[key] = value.tolist()
                elif isinstance(value, list):
                    # Handle lists properly
                    try:
                        if key == 'tags':
                            # Array(String) must be [], not NULL
                            record[key] = [str(item) for item in value if item is not None]
                        else:
                            record[key] = [str(item) if item is not None else None for item in value]
                    except Exception:
                        record[key] = [] if key == 'tags' else None
                elif isinstance(value, str) and value == '':
                    record[key] = ''  # Keep empty strings as empty strings

        batch_size = batch_size or config.get('BATCH_SIZE', 1000)
        
        log_with_timestamp(f"Loading {len(data_list)} records to ClickHouse table '{table_name}' in batches of {batch_size}...", "ClickHouse Loader")

        # Load data in batches
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            try:
                # Convert batch to list of tuples for ClickHouse compatibility
                batch_tuples = []
                for record in batch:
                    # Convert dict to tuple in the correct column order
                    tuple_record = tuple(record.get(col, None) for col in data.columns)
                    batch_tuples.append(tuple_record)
                
                columns_csv = ', '.join(list(data.columns))
                client.execute(f'INSERT INTO {table_name} ({columns_csv}) VALUES', batch_tuples)
                log_with_timestamp(f"Loaded batch {i//batch_size + 1} ({len(batch)} records) to {table_name}", "ClickHouse Loader")
            except Exception as e:
                log_with_timestamp(f"Failed to load batch {i//batch_size + 1} to {table_name}: {e}", "ClickHouse Loader", "error")
                return False

        log_with_timestamp(f"Successfully loaded {len(data_list)} records to ClickHouse table '{table_name}'", "ClickHouse Loader")
        return True

    except Exception as e:
        log_with_timestamp(f"ClickHouse load failed for table '{table_name}': {e}", "ClickHouse Loader", "error")
        return False

def upsert_to_clickhouse(
    data: pd.DataFrame,
    table_name: str,
    unique_key_columns: list,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    batch_size: int = None
) -> bool:
    """
    Upsert data to ClickHouse using pandas DataFrame.
    Implements proper idempotent upsert using ALTER TABLE UPDATE for existing records
    and INSERT for new records.
    """
    try:
        from clickhouse_driver import Client

        clickhouse_config = config.get_clickhouse_config()
        client = Client(
            host=host or clickhouse_config['host'],
            port=port or clickhouse_config['port'],
            user=user or clickhouse_config['user'],
            password=password or clickhouse_config['password'],
            database=database or clickhouse_config['database']
        )

        if data.empty:
            log_with_timestamp(f"No data to upsert to ClickHouse table '{table_name}'", "ClickHouse Loader", "info")
            return True

        # Convert DataFrame to list of dictionaries
        data_list = data.to_dict('records')
        
        # Handle NaN values and array data
        for record in data_list:
            for key, value in record.items():
                if pd.isna(value) or (isinstance(value, float) and np.isnan(value)):
                    record[key] = None
                elif isinstance(value, np.ndarray):
                    # Convert numpy arrays to Python lists
                    record[key] = value.tolist()
                elif hasattr(value, 'tolist'):
                    # Convert pandas arrays to Python lists
                    record[key] = value.tolist()
                elif isinstance(value, list):
                    # Handle lists properly - convert empty lists to None for ClickHouse
                    if len(value) == 0:
                        record[key] = None
                    else:
                        # Ensure all list elements are properly formatted
                        try:
                            record[key] = [str(item) if item is not None else None for item in value]
                        except Exception:
                            record[key] = None
                elif isinstance(value, str) and value == '':
                    # Handle empty strings
                    record[key] = None

        batch_size = batch_size or config.get('BATCH_SIZE', 1000)
        
        log_with_timestamp(f"Upserting {len(data_list)} records to ClickHouse table '{table_name}' (idempotent upsert) in batches of {batch_size}...", "ClickHouse Loader")

        # Process data in batches for idempotent upsert
        for i in range(0, len(data_list), batch_size):
            batch = data_list[i:i + batch_size]
            try:
                # For each record in the batch, check if it exists and update or insert accordingly
                for record in batch:
                    # Build WHERE clause for checking existing records
                    where_conditions = []
                    where_values = []
                    
                    for key_col in unique_key_columns:
                        if key_col in record and record[key_col] is not None:
                            where_conditions.append(f"{key_col} = %s")
                            where_values.append(record[key_col])
                    
                    if not where_conditions:
                        log_with_timestamp(f"No unique key columns found for record, skipping: {record}", "ClickHouse Loader", "warning")
                        continue
                    
                    # Check if record exists
                    # ClickHouse doesn't support parameterized queries with %s, so we need to format the query
                    formatted_where_conditions = []
                    for i, (col, val) in enumerate(zip(unique_key_columns, where_values)):
                        if val is None:
                            formatted_where_conditions.append(f"{col} IS NULL")
                        elif isinstance(val, str):
                            # Escape single quotes in strings
                            escaped_val = val.replace("'", "''")
                            formatted_where_conditions.append(f"{col} = '{escaped_val}'")
                        elif isinstance(val, datetime):
                            formatted_where_conditions.append(f"{col} = '{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                        elif isinstance(val, list):
                            # Handle arrays properly for ClickHouse - skip array columns in WHERE clause
                            # Arrays can cause comparison issues, so we'll use a different approach
                            continue
                        else:
                            formatted_where_conditions.append(f"{col} = {val}")
                    
                    # Skip if no valid WHERE conditions (e.g., only array columns)
                    if not formatted_where_conditions:
                        log_with_timestamp(f"No valid WHERE conditions for record, skipping: {record}", "ClickHouse Loader", "warning")
                        continue
                    
                    check_query = f"SELECT COUNT(*) FROM {table_name} WHERE {' AND '.join(formatted_where_conditions)}"
                    log_with_timestamp(f"Checking existence with query: {check_query}", "ClickHouse Loader", "debug")
                    result = client.execute(check_query)
                    exists = result[0][0] > 0 if result else False
                    log_with_timestamp(f"Record exists: {exists} (count: {result[0][0] if result else 0})", "ClickHouse Loader", "debug")
                    
                    if exists:
                        # Update existing record
                        update_columns = []
                        
                        # Define key columns that cannot be updated in ClickHouse
                        key_columns = ['id', 'symbol', 'last_updated']  # These are part of the primary key
                        
                        for col in data.columns:
                            if (col not in unique_key_columns and 
                                col not in key_columns and 
                                col in record and record[col] is not None):
                                val = record[col]
                                if isinstance(val, str):
                                    update_columns.append(f"{col} = '{val}'")
                                elif isinstance(val, datetime):
                                    update_columns.append(f"{col} = '{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                                else:
                                    update_columns.append(f"{col} = {val}")
                        
                        if update_columns:
                            # Add updated_at timestamp only if not already in update_columns
                            if not any("updated_at" in col for col in update_columns):
                                update_columns.append("updated_at = now()")
                            
                            update_query = f"""
                                ALTER TABLE {table_name} 
                                UPDATE {', '.join(update_columns)}
                                WHERE {' AND '.join(formatted_where_conditions)}
                            """
                            client.execute(update_query)
                            log_with_timestamp(f"Updated existing record for {unique_key_columns} = {[record.get(k) for k in unique_key_columns]}", "ClickHouse Loader", "debug")
                    else:
                        # Insert new record
                        insert_columns = list(data.columns)
                        insert_values = []
                        
                        for col in insert_columns:
                            val = record.get(col, None)
                            if val is None:
                                insert_values.append('NULL')
                            elif isinstance(val, str):
                                # Escape single quotes in strings and handle JSON
                                escaped_val = val.replace("'", "''")
                                # Check if it's a JSON string (starts with { or [)
                                if escaped_val.strip().startswith(('{', '[')):
                                    # For JSON strings, use proper escaping
                                    escaped_val = escaped_val.replace('\\', '\\\\')
                                insert_values.append(f"'{escaped_val}'")
                            elif isinstance(val, datetime):
                                insert_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            elif isinstance(val, (int, float)):
                                insert_values.append(str(val))
                            else:
                                insert_values.append(f"'{str(val)}'")
                        
                        # Add created_at and updated_at timestamps if not already present
                        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if 'created_at' not in insert_columns:
                            insert_columns.append('created_at')
                            insert_values.append(f"'{now_str}'")
                        if 'updated_at' not in insert_columns:
                            insert_columns.append('updated_at')
                            insert_values.append(f"'{now_str}'")
                        
                        insert_query = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) VALUES ({', '.join(insert_values)})"
                        client.execute(insert_query)
                        log_with_timestamp(f"Inserted new record for {unique_key_columns} = {[record.get(k) for k in unique_key_columns]}", "ClickHouse Loader", "debug")
                
                log_with_timestamp(f"Processed batch {i//batch_size + 1} ({len(batch)} records) for {table_name}", "ClickHouse Loader")
            except Exception as e:
                log_with_timestamp(f"Failed to upsert batch {i//batch_size + 1} to {table_name}: {e}", "ClickHouse Loader", "error")
                return False

        log_with_timestamp(f"Successfully upserted {len(data_list)} records to ClickHouse table '{table_name}' (idempotent)", "ClickHouse Loader")
        return True

    except Exception as e:
        log_with_timestamp(f"ClickHouse upsert failed for table '{table_name}': {e}", "ClickHouse Loader", "error")
        return False

def create_clickhouse_loader(
    table_name: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    batch_size: int = None,
    name: str = "ClickHouse Loader"
):
    """
    Factory function to create ClickHouse loader with pandas DataFrame support.
    """
    def loader(data: pd.DataFrame) -> bool:
        log_with_timestamp(f"Running {name} for table '{table_name}'", "ClickHouse Loader")
        return load_to_clickhouse(
            data=data,
            table_name=table_name,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            batch_size=batch_size
        )
    
    return loader

def create_clickhouse_upsert_loader(
    table_name: str,
    unique_key_columns: list,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    batch_size: int = None,
    name: str = "ClickHouse Upsert Loader"
):
    """
    Factory function to create ClickHouse upsert loader with pandas DataFrame support.
    """
    def loader(data: pd.DataFrame) -> bool:
        log_with_timestamp(f"Running {name} for table '{table_name}' (upsert)", "ClickHouse Loader")
        return upsert_to_clickhouse(
            data=data,
            table_name=table_name,
            unique_key_columns=unique_key_columns,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            batch_size=batch_size
        )
    
    return loader