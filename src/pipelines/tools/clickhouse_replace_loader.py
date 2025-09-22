# src/pipelines/tools/clickhouse_replace_loader.py
"""
ClickHouse loader with DELETE + INSERT pattern for ensuring no duplicates.

This module provides a generic loader that uses the DELETE + INSERT pattern
to ensure no duplicate records in ClickHouse tables.
"""

import pandas as pd
from typing import List, Optional, Dict, Any
from core.config import config
from core.logging import log_with_timestamp
from .data_utils import clean_data_for_clickhouse, deduplicate_data

async def load_to_clickhouse_with_replace(
    data: pd.DataFrame,
    table_name: str,
    key_columns: List[str],
    sort_column: str = 'updated_at_trade',
    string_columns: Optional[List[str]] = None,
    max_retries: int = 3,
    name: str = "ClickHouse Replace Loader"
) -> bool:
    """
    Load data to ClickHouse using DELETE + INSERT pattern to ensure no duplicates.
    
    Args:
        data: DataFrame to load
        table_name: Target ClickHouse table name
        key_columns: Columns to use for deduplication
        sort_column: Column to sort by for deduplication (highest value wins)
        string_columns: List of string columns to clean (optional)
        max_retries: Maximum number of retry attempts
        name: Name for logging purposes
        
    Returns:
        True if successful, False otherwise
    """
    if data.empty:
        log_with_timestamp("No data to load", name, "warning")
        return False

    log_with_timestamp(f"Loading {len(data)} records to ClickHouse", name)
    
    # Get all unique key values from the current batch
    key_values = data[key_columns[0]].dropna().unique().tolist()
    
    if not key_values:
        log_with_timestamp(f"No valid {key_columns[0]} found in data", name, "warning")
        return False
    
    # Deduplicate data at application level before inserting
    data_deduplicated = deduplicate_data(data, key_columns, sort_column)
    
    # Use direct insert approach with DELETE + INSERT pattern
    for attempt in range(max_retries):
        try:
            import clickhouse_connect
            clickhouse_config = config.get_clickhouse_config()
            client = clickhouse_connect.get_client(
                host=clickhouse_config['host'],
                port=clickhouse_config['port'],
                username=clickhouse_config['user'],
                password=clickhouse_config['password'],
                database=clickhouse_config['database']
            )
            
            # Clean data for ClickHouse insertion
            cleaned_data = clean_data_for_clickhouse(data_deduplicated, string_columns)
            
            # Log data quality information
            log_with_timestamp(f"Data quality check - Records: {len(cleaned_data)}, Unique keys: {len(key_values)}", name)
            
            # Log sample timestamp values for debugging
            if sort_column in cleaned_data.columns:
                sample_timestamps = cleaned_data[sort_column].dropna().head(3).tolist()
                log_with_timestamp(f"Sample {sort_column} timestamps: {sample_timestamps}", name)
            
            # Use REPLACE approach to ensure no duplicates
            # First, delete existing records for these key values
            key_values_list = "', '".join(key_values)
            delete_query = f"ALTER TABLE {table_name} DELETE WHERE {key_columns[0]} IN ('{key_values_list}')"
            client.command(delete_query)
            
            # Then insert the new data
            client.insert_df(table_name, cleaned_data)
            
            log_with_timestamp(f"Successfully loaded {len(data_deduplicated)} records to {table_name}", name)
            return True
            
        except Exception as e:
            log_with_timestamp(f"Attempt {attempt + 1}/{max_retries} failed to load data to ClickHouse: {e}", name, "error")
            
            # Log additional debugging information
            if hasattr(e, 'args') and e.args:
                log_with_timestamp(f"Error details: {e.args[0] if e.args else str(e)}", name, "error")
            
            # If this is the last attempt, return False
            if attempt == max_retries - 1:
                log_with_timestamp(f"All {max_retries} attempts failed to load data to ClickHouse", name, "error")
                return False
            
            # Wait before retrying
            import time
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return False

def create_clickhouse_replace_loader(
    table_name: str,
    key_columns: List[str],
    sort_column: str = 'updated_at_trade',
    string_columns: Optional[List[str]] = None,
    max_retries: int = 3,
    name: str = "ClickHouse Replace Loader"
) -> callable:
    """
    Create a ClickHouse replace loader function with pre-configured parameters.
    
    Args:
        table_name: Target ClickHouse table name
        key_columns: Columns to use for deduplication
        sort_column: Column to sort by for deduplication (highest value wins)
        string_columns: List of string columns to clean (optional)
        max_retries: Maximum number of retry attempts
        name: Name for logging purposes
        
    Returns:
        Async function that performs the replace load operation
        
    Example:
        >>> loader = create_clickhouse_replace_loader(
        ...     table_name="my_table",
        ...     key_columns=["id"],
        ...     sort_column="updated_at"
        ... )
        >>> result = await loader(data)
    """
    async def clickhouse_replace_loader_func(data: pd.DataFrame) -> bool:
        return await load_to_clickhouse_with_replace(
            data=data,
            table_name=table_name,
            key_columns=key_columns,
            sort_column=sort_column,
            string_columns=string_columns,
            max_retries=max_retries,
            name=name
        )
    
    return clickhouse_replace_loader_func

# Public API
__all__ = [
    'load_to_clickhouse_with_replace',
    'create_clickhouse_replace_loader',
]
