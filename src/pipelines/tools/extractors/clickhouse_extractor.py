# src/pipelines/tools/extractors/clickhouse_extractor.py
from typing import List, Dict, Any, Callable, Optional
import pandas as pd
from core.config import config
from core.logging import log_with_timestamp

async def extract_from_clickhouse(
    query: str,
    host: str = None,
    port: int = None,
    user: str = None,
    password: str = None,
    database: str = None
) -> pd.DataFrame:
    """
    Generic ClickHouse extractor.
    """
    try:
        import clickhouse_connect

        clickhouse_config = config.get_clickhouse_config()
        client = clickhouse_connect.get_client(
            host=host or clickhouse_config['host'],
            port=port or clickhouse_config['port'],
            username=user or clickhouse_config['user'],
            password=password or clickhouse_config['password'],
            database=database or clickhouse_config['database']
        )

        qr = client.query(query)
        rows = qr.result_rows if hasattr(qr, 'result_rows') else []
        cols = qr.column_names if hasattr(qr, 'column_names') else []

        if not rows:
            return pd.DataFrame()

        if cols:
            records = [dict(zip(cols, row)) for row in rows]
            return pd.DataFrame(records)
        else:
            # Fallback: generic columns
            records = []
            for row in rows:
                rec = {f"col_{i}": v for i, v in enumerate(row)}
                records.append(rec)
            return pd.DataFrame(records)
    except ImportError:
        log_with_timestamp("ClickHouse client not installed. Install with 'pip install clickhouse-connect'", "ClickHouse Extractor", "error")
        return pd.DataFrame()
    except Exception as e:
        log_with_timestamp(f"ClickHouse extraction failed for query '{query[:100]}...': {e}", "ClickHouse Extractor", "error")
        return pd.DataFrame()

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
