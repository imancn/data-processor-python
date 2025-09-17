# src/extractors/clickhouse_extractor.py
from typing import List, Dict, Any, Callable, Optional
from core.config import config
from core.logging import log_with_timestamp

async def extract_from_clickhouse(
    query: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None
) -> List[Dict[str, Any]]:
    """
    Generic ClickHouse extractor.
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

        result = client.execute(query)

        if result and client.column_names:
            records = []
            for row in result:
                record = {}
                for i, col_name in enumerate(client.column_names):
                    record[col_name] = row[i]
                records.append(record)
            return records
        elif result:
            log_with_timestamp("ClickHouse client did not provide column names. Returning raw list of lists.", "ClickHouse Extractor", "warning")
            return result
        else:
            return []
    except ImportError:
        log_with_timestamp("ClickHouse driver not installed. Please install with 'pip install clickhouse-driver'", "ClickHouse Extractor", "error")
        return []
    except Exception as e:
        log_with_timestamp(f"ClickHouse extraction failed for query '{query[:100]}...': {e}", "ClickHouse Extractor", "error")
        return []

def create_clickhouse_extractor(
    query: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None,
    name: str = "ClickHouse Extractor"
) -> Callable[..., Any]:
    """Factory function to create a configured ClickHouse extractor."""
    async def extractor_func(*args, **kwargs):
        log_with_timestamp(f"Running {name} with query '{query[:100]}...'", name)
        return await extract_from_clickhouse(query, host, port, user, password, database)
    return extractor_func
