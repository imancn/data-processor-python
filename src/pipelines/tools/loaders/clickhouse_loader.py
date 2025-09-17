# src/loaders/clickhouse_loader.py
from typing import List, Dict, Any, Callable, Optional
from datetime import datetime
import pytz
from core.config import config
from core.logging import log_with_timestamp

def load_to_clickhouse(
    data: List[Dict[str, Any]],
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
    Generic ClickHouse loader.
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

        batch_size = batch_size or config.batch_size

        if not data:
            log_with_timestamp(f"No data to load to {table_name}", "ClickHouse Loader", "info")
            return True

        enriched_data = []
        for record in data:
            enriched_record = record.copy()
            enriched_record['_processed_at'] = datetime.now(pytz.timezone('Asia/Tehran'))
            if target_hour:
                enriched_record['_target_hour'] = target_hour
            enriched_data.append(enriched_record)

        if not enriched_data:
            log_with_timestamp(f"No enriched data to load to {table_name}", "ClickHouse Loader", "info")
            return True

        columns = list(enriched_data[0].keys())
        values_placeholder = ', '.join([f'%({col})s' for col in columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values_placeholder})"

        log_with_timestamp(f"Loading {len(enriched_data)} records to ClickHouse table '{table_name}' in batches of {batch_size}...", "ClickHouse Loader")

        for i in range(0, len(enriched_data), batch_size):
            batch = enriched_data[i:i + batch_size]
            client.execute(insert_query, batch)
            log_with_timestamp(f"Loaded batch {i // batch_size + 1} (records {i}-{min(i + batch_size, len(enriched_data))})", "ClickHouse Loader", "debug")

        log_with_timestamp(f"Successfully loaded {len(enriched_data)} records to '{table_name}'", "ClickHouse Loader")
        return True
    except ImportError:
        log_with_timestamp("ClickHouse driver not installed. Please install with 'pip install clickhouse-driver'", "ClickHouse Loader", "error")
        return False
    except Exception as e:
        log_with_timestamp(f"ClickHouse load failed for table '{table_name}': {e}", "ClickHouse Loader", "error")
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
) -> Callable[..., bool]:
    """Factory function to create a configured ClickHouse loader."""
    def loader_func(data: List[Dict[str, Any]], target_hour: Optional[datetime] = None) -> bool:
        log_with_timestamp(f"Running {name} for table '{table_name}'", name)
        return load_to_clickhouse(data, table_name, host, port, user, password, database, batch_size, target_hour)
    return loader_func

def create_clickhouse_upsert_loader(
    table_name: str,
    unique_key_columns: List[str],
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    batch_size: int = None,
    name: str = "ClickHouse Upsert Loader"
) -> Callable[..., bool]:
    """
    Factory function to create a ClickHouse loader that performs upserts
    (insert or replace) using ReplacingMergeTree engine.
    """
    def loader_func(data: List[Dict[str, Any]], target_hour: Optional[datetime] = None) -> bool:
        log_with_timestamp(f"Running {name} for table '{table_name}' (upsert)", name)
        return load_to_clickhouse(data, table_name, host, port, user, password, database, batch_size, target_hour)
    return loader_func
