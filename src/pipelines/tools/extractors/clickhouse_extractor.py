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
        from clickhouse_driver import Client

        clickhouse_config = config.get_clickhouse_config()
        client = Client(
            host=host or clickhouse_config['host'],
            port=port or clickhouse_config['port'],
            user=user or clickhouse_config['user'],
            password=password or clickhouse_config['password'],
            database=database or clickhouse_config['database']
        )
        
        # Ensure we're using the correct database
        if database or clickhouse_config['database']:
            client.execute(f"USE {database or clickhouse_config['database']}")

        result = client.execute(query)

        if result:
            # Get column names by executing a simple query first
            try:
                # Get column names from the table structure
                table_name = query.split('FROM ')[1].split()[0] if 'FROM ' in query else 'cmc_latest_quotes'
                desc_result = client.execute(f"DESCRIBE TABLE {table_name}")
                col_names = [col[0] for col in desc_result] if desc_result else []
                
                if col_names and len(col_names) == len(result[0]):
                    records = []
                    for row in result:
                        record = {}
                        for i, col_name in enumerate(col_names):
                            # Convert data types properly
                            value = row[i]
                            if col_name in ['id', 'cmc_rank', 'num_market_pairs', 'is_active', 'is_fiat', 'infinite_supply']:
                                # Integer columns
                                record[col_name] = int(value) if value is not None else 0
                            elif col_name in ['circulating_supply', 'total_supply', 'max_supply', 'price', 'volume_24h', 'volume_change_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d', 'percent_change_30d', 'percent_change_60d', 'percent_change_90d', 'market_cap', 'market_cap_dominance', 'fully_diluted_market_cap', 'self_reported_circulating_supply', 'self_reported_market_cap', 'tvl_ratio']:
                                # Float columns
                                record[col_name] = float(value) if value is not None else None
                            elif col_name in ['last_updated', 'date_added', 'created_at', 'updated_at', 'timestamp']:
                                # DateTime columns
                                record[col_name] = value  # Keep as string, will be converted later
                            else:
                                # String columns
                                record[col_name] = str(value) if value is not None else None
                        records.append(record)
                    return pd.DataFrame(records)
                else:
                    # Fallback: use generic column names
                    records = []
                    for i, row in enumerate(result):
                        record = {}
                        for j, value in enumerate(row):
                            record[f"col_{j}"] = value
                        records.append(record)
                    return pd.DataFrame(records)
            except Exception as e:
                log_with_timestamp(f"Failed to get column names: {e}", "ClickHouse Extractor", "warning")
                # Fallback: use generic column names
                records = []
                for i, row in enumerate(result):
                    record = {}
                    for j, value in enumerate(row):
                        record[f"col_{j}"] = value
                    records.append(record)
                return pd.DataFrame(records)
        else:
            return pd.DataFrame()
    except ImportError:
        log_with_timestamp("ClickHouse driver not installed. Please install with 'pip install clickhouse-driver'", "ClickHouse Extractor", "error")
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
