# src/pipelines/cmc_pipeline.py
"""
Refactored CMC pipeline with new architecture:
- cmc_latest_quotes: Gets data from API (source of truth)
- All other time scopes: Transform data FROM cmc_latest_quotes (not from API)
"""
from typing import Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from core.config import config
from core.logging import log_with_timestamp

# Import generic components
from pipelines.tools.extractors.http_extractor import create_http_extractor
from pipelines.tools.extractors.clickhouse_extractor import create_clickhouse_extractor
from pipelines.tools.loaders.clickhouse_loader import create_clickhouse_loader, create_clickhouse_upsert_loader
from pipelines.tools.transformers.transformers import (
    create_lambda_transformer, create_type_converter, create_column_transformer
)
from pipelines.pipeline_factory import create_el_pipeline, create_etl_pipeline

# Pipeline registry
PIPELINE_REGISTRY = {}

def get_time_range_params(time_scope: str) -> dict:
    """Get time range parameters for different time scopes - all use current quotes."""
    now = datetime.now()
    
    # For all time scopes, we use current quotes but with different timestamps
    # to simulate the time intervals for data organization
    if time_scope == 'hourly':
        # Use current time rounded to the hour
        time_start = now.replace(minute=0, second=0, microsecond=0)
        time_end = time_start
        count = 1
    elif time_scope == 'daily':
        # Use current time rounded to the day
        time_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        time_end = time_start
        count = 1
    elif time_scope == 'weekly':
        # Use current time rounded to the start of week (Monday)
        days_since_monday = now.weekday()
        time_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
        time_end = time_start
        count = 1
    elif time_scope == 'monthly':
        # Use current time rounded to the start of month
        time_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        time_end = time_start
        count = 1
    elif time_scope == 'yearly':
        # Use current time rounded to the start of year
        time_start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        time_end = time_start
        count = 1
    else:
        # Default to latest data - use current time
        time_start = now
        time_end = now
        count = 1

    return {
        'time_start': time_start.strftime('%Y-%m-%d'),
        'time_end': time_end.strftime('%Y-%m-%d'),
        'count': count
    }

def transform_cmc_data_from_api(df: pd.DataFrame, time_scope: str) -> pd.DataFrame:
    """Transform CMC data from API for cmc_latest_quotes table."""
    if df.empty:
        return df
    
    try:
        # Create a copy to avoid modifying the original
        transformed_df = df.copy()
        
        # Extract quote data from nested structure
        if 'quote' in transformed_df.columns:
            # Flatten the quote data
            quote_data = pd.json_normalize(transformed_df['quote'].apply(lambda x: x.get('USD', {}) if isinstance(x, dict) else {}))
            if not quote_data.empty:
                # Add prefix to avoid column name conflicts
                quote_data.columns = [f'quote_{col}' for col in quote_data.columns]
                # Merge with main dataframe
                transformed_df = pd.concat([transformed_df, quote_data], axis=1)
                # Add quote_currency column
                transformed_df['quote_currency'] = 'USD'
        
        # Select and rename columns to match database schema
        column_mapping = {
            'id': 'id',
            'name': 'name',
            'symbol': 'symbol',
            'slug': 'slug',
            'cmc_rank': 'cmc_rank',
            'num_market_pairs': 'num_market_pairs',
            'circulating_supply': 'circulating_supply',
            'total_supply': 'total_supply',
            'max_supply': 'max_supply',
            'last_updated': 'last_updated',
            'date_added': 'date_added',
            'tags': 'tags',
            'platform': 'platform',
            'quote_price': 'price',
            'quote_volume_24h': 'volume_24h',
            'quote_volume_change_24h': 'volume_change_24h',
            'quote_percent_change_1h': 'percent_change_1h',
            'quote_percent_change_24h': 'percent_change_24h',
            'quote_percent_change_7d': 'percent_change_7d',
            'quote_percent_change_30d': 'percent_change_30d',
            'quote_percent_change_60d': 'percent_change_60d',
            'quote_percent_change_90d': 'percent_change_90d',
            'quote_market_cap': 'market_cap',
            'quote_market_cap_dominance': 'market_cap_dominance',
            'quote_fully_diluted_market_cap': 'fully_diluted_market_cap',
            'is_active': 'is_active',
            'is_fiat': 'is_fiat',
            'infinite_supply': 'infinite_supply',
            'self_reported_circulating_supply': 'self_reported_circulating_supply',
            'self_reported_market_cap': 'self_reported_market_cap',
            'tvl_ratio': 'tvl_ratio'
        }
        
        # Apply column mapping
        for source_col, target_col in column_mapping.items():
            if source_col in transformed_df.columns:
                if target_col == 'tags':
                    # Convert tags list to comma-separated string for latest table
                    def convert_tags_to_string(x):
                        try:
                            if pd.isna(x) or x == '':
                                return ''
                            elif isinstance(x, str):
                                return x
                            elif isinstance(x, list):
                                return ','.join(map(str, x)) if x else ''
                            else:
                                return str(x) if x is not None else ''
                        except Exception:
                            return ''
                    transformed_df[target_col] = transformed_df[source_col].apply(convert_tags_to_string)
                else:
                    transformed_df[target_col] = transformed_df[source_col]
        
        # Add missing columns with default values for cmc_latest_quotes
        required_columns = [
            'id', 'name', 'symbol', 'slug', 'cmc_rank', 'circulating_supply', 'total_supply', 'max_supply',
            'date_added', 'num_market_pairs', 'tags', 'platform', 'last_updated', 'quote_currency', 'price',
            'volume_24h', 'volume_change_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d',
            'percent_change_30d', 'percent_change_60d', 'percent_change_90d', 'market_cap', 'market_cap_dominance',
            'fully_diluted_market_cap', 'is_active', 'is_fiat', 'infinite_supply', 'self_reported_circulating_supply',
            'self_reported_market_cap', 'tvl_ratio', 'created_at', 'updated_at'
        ]
        
        for col in required_columns:
            if col not in transformed_df.columns:
                if col in ['created_at', 'updated_at']:
                    transformed_df[col] = datetime.now()
                elif col in ['is_active', 'is_fiat', 'infinite_supply']:
                    transformed_df[col] = 0
                elif col == 'tags':
                    # For latest table, tags is a String - convert list to comma-separated string
                    def safe_fill_tags(x):
                        try:
                            if pd.isna(x) or x == '':
                                return ''
                            elif isinstance(x, str):
                                return x
                            elif isinstance(x, list):
                                # Convert list to comma-separated string
                                return ','.join(map(str, x)) if x else ''
                            else:
                                return str(x) if x is not None else ''
                        except Exception:
                            return ''
                    transformed_df[col] = transformed_df[col].apply(safe_fill_tags)
                else:
                    transformed_df[col] = None
        
        # Add quote_currency column
        transformed_df['quote_currency'] = 'USD'
        
        # Select only the required columns
        transformed_df = transformed_df[required_columns]
        
        # Convert data types with proper NaN handling for historical tables
        type_mapping = {
            'id': 'int64',
            'cmc_rank': 'Int32',
            'num_market_pairs': 'Int32',
            'circulating_supply': 'float64',
            'total_supply': 'float64',
            'max_supply': 'float64',
            'price': 'float64',
            'volume_24h': 'float64',
            'volume_change_24h': 'float64',
            'percent_change_1h': 'float64',
            'percent_change_24h': 'float64',
            'percent_change_7d': 'float64',
            'percent_change_30d': 'float64',
            'percent_change_60d': 'float64',
            'percent_change_90d': 'float64',
            'market_cap': 'float64',
            'market_cap_dominance': 'float64',
            'fully_diluted_market_cap': 'float64',
            'is_active': 'Int8',
            'is_fiat': 'Int8',
            'infinite_supply': 'Int8',
            'self_reported_circulating_supply': 'float64',
            'self_reported_market_cap': 'float64',
            'tvl_ratio': 'float64',
            'last_updated': 'datetime64[ns]',
            'date_added': 'datetime64[ns]',
            'timestamp': 'datetime64[ns]'
        }
        
        for col, dtype in type_mapping.items():
            if col in transformed_df.columns:
                try:
                    if dtype == 'datetime64[ns]':
                        transformed_df[col] = pd.to_datetime(transformed_df[col])
                    elif dtype in ['Int32', 'Int8']:
                        # Fill NaN with 0 for integer columns
                        transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce').fillna(0).astype(dtype)
                    else:
                        # For float columns, convert Decimal to float first, then handle NaN
                        from decimal import Decimal
                        if transformed_df[col].dtype == 'object':
                            # Convert Decimal objects to float
                            transformed_df[col] = transformed_df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
                        transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce')
                        # Replace NaN with None for ClickHouse compatibility
                        transformed_df[col] = transformed_df[col].where(pd.notna(transformed_df[col]), None)
                except Exception as e:
                    log_with_timestamp(f"Failed to convert column {col} to {dtype}: {e}", "CMC Transformer", "warning")
        
        # Clean up any dictionary/object data that might cause parsing issues
        for col in transformed_df.columns:
            if transformed_df[col].dtype == 'object':
                # Convert any dictionary-like strings to None
                    transformed_df[col] = transformed_df[col].apply(
                    lambda x: None if isinstance(x, (dict, list)) or (isinstance(x, str) and x.startswith('{')) else x
                )
        
        # Convert data types with proper NaN handling
        type_mapping = {
            'id': 'int64',
            'cmc_rank': 'Int32',
            'num_market_pairs': 'Int32',
            'circulating_supply': 'float64',
            'total_supply': 'float64',
            'max_supply': 'float64',
            'price': 'float64',
            'volume_24h': 'float64',
            'volume_change_24h': 'float64',
            'percent_change_1h': 'float64',
            'percent_change_24h': 'float64',
            'percent_change_7d': 'float64',
            'percent_change_30d': 'float64',
            'percent_change_60d': 'float64',
            'percent_change_90d': 'float64',
            'market_cap': 'float64',
            'market_cap_dominance': 'float64',
            'fully_diluted_market_cap': 'float64',
            'is_active': 'Int8',
            'is_fiat': 'Int8',
            'infinite_supply': 'Int8',
            'self_reported_circulating_supply': 'float64',
            'self_reported_market_cap': 'float64',
            'tvl_ratio': 'float64',
            'last_updated': 'datetime64[ns]',
            'date_added': 'datetime64[ns]'
        }
        
        for col, dtype in type_mapping.items():
            if col in transformed_df.columns:
                try:
                    if dtype == 'datetime64[ns]':
                        transformed_df[col] = pd.to_datetime(transformed_df[col])
                    elif dtype in ['Int32', 'Int8']:
                        # Fill NaN with 0 for integer columns
                        transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce').fillna(0).astype(dtype)
                    else:
                        # For float columns, convert NaN to None for ClickHouse compatibility
                        transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce')
                        # Replace NaN with None for ClickHouse compatibility
                        transformed_df[col] = transformed_df[col].where(pd.notna(transformed_df[col]), None)
                except Exception as e:
                    log_with_timestamp(f"Failed to convert column {col} to {dtype}: {e}", "CMC Transformer", "warning")
        
        log_with_timestamp(f"Transformed {len(transformed_df)} records for latest quotes", "CMC Transformer")
        return transformed_df
        
    except Exception as e:
        log_with_timestamp(f"CMC transformation failed: {e}", "CMC Transformer", "error")
        return pd.DataFrame()

def transform_cmc_data_from_latest(df: pd.DataFrame, time_scope: str) -> pd.DataFrame:
    """Transform data from cmc_latest_quotes to historical time scope tables."""
    if df.empty:
        return df
    
    try:
        # Create a copy to avoid modifying the original
        transformed_df = df.copy()
        
        # If the dataframe has generic column names (col_0, col_1, etc.), map them to actual column names
        if 'col_0' in transformed_df.columns:
            # Map generic columns to actual column names based on the order from ClickHouse
            column_mapping = {
                'col_0': 'id', 'col_1': 'name', 'col_2': 'symbol', 'col_3': 'slug', 'col_4': 'cmc_rank',
                'col_5': 'circulating_supply', 'col_6': 'total_supply', 'col_7': 'max_supply', 'col_8': 'date_added',
                'col_9': 'num_market_pairs', 'col_10': 'tags', 'col_11': 'platform', 'col_12': 'last_updated',
                'col_13': 'quote_currency', 'col_14': 'price', 'col_15': 'volume_24h', 'col_16': 'volume_change_24h',
                'col_17': 'percent_change_1h', 'col_18': 'percent_change_24h', 'col_19': 'percent_change_7d',
                'col_20': 'percent_change_30d', 'col_21': 'percent_change_60d', 'col_22': 'percent_change_90d',
                'col_23': 'market_cap', 'col_24': 'market_cap_dominance', 'col_25': 'fully_diluted_market_cap',
                'col_26': 'is_active', 'col_27': 'is_fiat', 'col_28': 'infinite_supply', 'col_29': 'self_reported_circulating_supply',
                'col_30': 'self_reported_market_cap', 'col_31': 'tvl_ratio', 'col_32': 'created_at', 'col_33': 'updated_at'
            }
            
            # Rename columns
            transformed_df = transformed_df.rename(columns=column_mapping)
        
        # Add time_scope column
        transformed_df['time_scope'] = time_scope
        
        # Add timestamp based on time scope for proper data organization
        now = datetime.now()
        if time_scope == 'hourly':
            # Round to current hour
            timestamp = now.replace(minute=0, second=0, microsecond=0)
        elif time_scope == 'daily':
            # Round to current day
            timestamp = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif time_scope == 'weekly':
            # Round to start of current week (Monday)
            days_since_monday = now.weekday()
            timestamp = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
        elif time_scope == 'monthly':
            # Round to start of current month
            timestamp = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif time_scope == 'yearly':
            # Round to start of current year
            timestamp = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            # Use current time
            timestamp = now
        
        transformed_df['timestamp'] = timestamp
        
        # Convert tags from String to Array(String) for historical tables
        if 'tags' in transformed_df.columns:
            def convert_tags_to_array(x):
                try:
                    if pd.isna(x) or x == '':
                        return []
                    elif isinstance(x, str):
                        return [x] if x else []
                    elif isinstance(x, list):
                        return x
                    else:
                        return []
                except Exception:
                    return []
            transformed_df['tags'] = transformed_df['tags'].apply(convert_tags_to_array)
        
        # Add created_at and updated_at
        transformed_df['created_at'] = pd.Timestamp.now()
        transformed_df['updated_at'] = pd.Timestamp.now()

        # Ensure arrays are represented as ClickHouse array literals for loader
        if 'tags' in transformed_df.columns:
            def format_array(arr):
                if arr is None:
                    return None
                if isinstance(arr, list):
                    # produce a comma-separated string; loader will treat lists properly
                    return arr
                if isinstance(arr, str) and arr:
                    return [arr]
                return []
            transformed_df['tags'] = transformed_df['tags'].apply(format_array)
        
        # Ensure numerics are casted to float to satisfy Float64 schema
        numeric_cols = [
            'circulating_supply','total_supply','max_supply','price','volume_24h','volume_change_24h',
            'percent_change_1h','percent_change_24h','percent_change_7d','percent_change_30d','percent_change_60d',
            'percent_change_90d','market_cap','market_cap_dominance','fully_diluted_market_cap'
        ]
        for col in numeric_cols:
            if col in transformed_df.columns:
                from decimal import Decimal
                transformed_df[col] = transformed_df[col].apply(lambda v: float(v) if isinstance(v, Decimal) else v)
                transformed_df[col] = pd.to_numeric(transformed_df[col], errors='coerce')

        # Define required columns for historical tables
        required_columns = [
            'id', 'name', 'symbol', 'slug', 'cmc_rank', 'circulating_supply', 'total_supply', 'max_supply',
            'date_added', 'num_market_pairs', 'tags', 'platform', 'last_updated', 'quote_currency', 'price',
            'volume_24h', 'volume_change_24h', 'percent_change_1h', 'percent_change_24h', 'percent_change_7d',
            'percent_change_30d', 'percent_change_60d', 'percent_change_90d', 'market_cap', 'market_cap_dominance',
            'fully_diluted_market_cap', 'time_scope', 'timestamp', 'created_at', 'updated_at'
        ]
        
        # Select only the columns that exist in the dataframe
        available_columns = [col for col in required_columns if col in transformed_df.columns]
        transformed_df = transformed_df[available_columns]
        
        # Add missing columns with default values
        for col in required_columns:
            if col not in transformed_df.columns:
                if col in ['created_at', 'updated_at']:
                    transformed_df[col] = datetime.now()
                elif col == 'time_scope':
                    transformed_df[col] = time_scope
                elif col == 'timestamp':
                    # Add timestamp based on time scope
                    now = datetime.now()
                    if time_scope == 'hourly':
                        timestamp = now.replace(minute=0, second=0, microsecond=0)
                    elif time_scope == 'daily':
                        timestamp = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    elif time_scope == 'weekly':
                        days_since_monday = now.weekday()
                        timestamp = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
                    elif time_scope == 'monthly':
                        timestamp = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    elif time_scope == 'yearly':
                        timestamp = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    else:
                        timestamp = now
                    transformed_df[col] = timestamp
                else:
                    transformed_df[col] = None
        
        log_with_timestamp(f"Transformed {len(transformed_df)} records for {time_scope} from latest data", "CMC Transformer")
        return transformed_df
        
    except Exception as e:
        log_with_timestamp(f"CMC transformation from latest failed: {e}", "CMC Transformer", "error")
        return pd.DataFrame()

def create_cmc_latest_pipeline() -> dict:
    """Create the cmc_latest_quotes pipeline that gets data from API."""
    
    # Get CMC configuration
    cmc_config = {
        'base_url': 'https://pro-api.coinmarketcap.com/v1',
        'api_key': config.get('CMC_API_KEY', '')
    }
    
    if not cmc_config['api_key']:
        log_with_timestamp("CMC_API_KEY not found in configuration", "CMC Pipeline", "error")
        return None
    
    # Headers for CMC API
    headers = {
        'X-CMC_PRO_API_KEY': cmc_config['api_key'],
        'Accept': 'application/json'
    }
    
    # Get symbols from config
    symbols = config.get('CMC_SYMBOLS', 'BTC,ETH,SOL,DOGE,SHIB,PEPE,BABYDOGE,BBL,ADA,MATIC,AVAX,DOT,LINK,UNI,LTC')
    
    # Latest quotes pipeline - gets data from API
    cmc_extractor = create_http_extractor(
        url=f"{cmc_config['base_url']}/cryptocurrency/quotes/latest",
        headers=headers,
        params={'symbol': symbols, 'convert': 'USD'},
        name="CMC Latest Quotes Extractor"
    )
    
    cmc_transformer = create_lambda_transformer(
        lambda df: transform_cmc_data_from_api(df, 'latest'),
        name="CMC Latest Quotes Transformer"
    )
    
    cmc_loader = create_clickhouse_upsert_loader(
        table_name="cmc_latest_quotes",
        unique_key_columns=["symbol"],
        name="CMC Latest Quotes Loader"
    )
    
    pipeline = create_etl_pipeline(
        extractor=cmc_extractor,
        transformer=cmc_transformer,
        loader=cmc_loader,
        name="CMC Latest Quotes Pipeline"
    )
    
    return {
        'pipeline': pipeline,
        'description': 'CMC latest cryptocurrency data pipeline (source of truth)',
        'time_scope': 'latest',
        'table_name': 'cmc_latest_quotes'
    }

def create_cmc_historical_pipeline(time_scope: str) -> dict:
    """Create historical time scope pipeline that fetches from CMC API (no CH extraction)."""
    # CMC config
    cmc_config = {
        'base_url': 'https://pro-api.coinmarketcap.com/v1',
        'api_key': config.get('CMC_API_KEY', '')
    }

    if not cmc_config['api_key']:
        log_with_timestamp("CMC_API_KEY not found in configuration", "CMC Pipeline", "error")
        return None

    headers = {
        'X-CMC_PRO_API_KEY': cmc_config['api_key'],
        'Accept': 'application/json'
    }

    symbols = config.get('CMC_SYMBOLS', 'BTC,ETH,SOL,DOGE,SHIB,PEPE,BABYDOGE,BBL,ADA,MATIC,AVAX,DOT,LINK,UNI,LTC')

    # Extract directly from CMC quotes/latest
    http_extractor = create_http_extractor(
        url=f"{cmc_config['base_url']}/cryptocurrency/quotes/latest",
        headers=headers,
        params={'symbol': symbols, 'convert': 'USD'},
        name=f"CMC {time_scope.title()} Extractor (HTTP)"
    )

    # First transform API payload to latest schema, then add time_scope/timestamp and cast for historical tables
    def historical_transform(df: pd.DataFrame) -> pd.DataFrame:
        base = transform_cmc_data_from_api(df, 'latest')
        return transform_cmc_data_from_latest(base, time_scope)

    cmc_transformer = create_lambda_transformer(
        historical_transform,
        name=f"CMC {time_scope.title()} Transformer"
    )
        
    cmc_loader = create_clickhouse_loader(
        table_name=f"cmc_{time_scope}",
        name=f"CMC {time_scope.title()} Loader"
    )
        
    pipeline = create_etl_pipeline(
        extractor=http_extractor,
        transformer=cmc_transformer,
        loader=cmc_loader,
        name=f"CMC {time_scope.title()} Pipeline (HTTP Just-in-Time)"
    )
    
    return {
        'pipeline': pipeline,
        'description': f'CMC {time_scope} cryptocurrency data pipeline (HTTP just-in-time)',
        'time_scope': time_scope,
        'table_name': f"cmc_{time_scope}"
    }

def create_cmc_pipeline(time_scope: str) -> dict:
    """Create a CMC pipeline for a specific time scope."""
    
    if time_scope == 'latest':
        return create_cmc_latest_pipeline()
    else:
        return create_cmc_historical_pipeline(time_scope)

def register_cmc_pipelines():
    """Register all CMC pipelines for different time scopes."""
    log_with_timestamp("Registering CMC pipelines with new architecture...", "CMC")
    
    time_scopes = ['latest', 'hourly', 'daily', 'weekly', 'monthly', 'yearly']
    
    for time_scope in time_scopes:
        try:
            pipeline_data = create_cmc_pipeline(time_scope)
            if pipeline_data:
                pipeline_name = f"cmc_{time_scope}" if time_scope != 'latest' else "cmc_latest_quotes"
                PIPELINE_REGISTRY[pipeline_name] = pipeline_data
                log_with_timestamp(f"Registered CMC {time_scope} pipeline", "CMC")
            else:
                log_with_timestamp(f"Failed to create CMC {time_scope} pipeline", "CMC", "error")
        except Exception as e:
            log_with_timestamp(f"Error creating CMC {time_scope} pipeline: {e}", "CMC", "error")
    
    log_with_timestamp(f"Registered {len(PIPELINE_REGISTRY)} CMC pipelines", "CMC")

def get_pipeline_registry() -> dict:
    """Get the pipeline registry."""
    return PIPELINE_REGISTRY

# Register pipelines when module is imported
register_cmc_pipelines()