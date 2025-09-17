# src/pipelines/cmc_pipeline.py
"""
CoinMarketCap pipeline implementation.
"""
from typing import Dict, Any, List
from core.config import config
from core.logging import log_with_timestamp

# Import generic components
from pipelines.tools.extractors.http_extractor import create_http_extractor, create_paginated_http_extractor
from pipelines.tools.loaders.clickhouse_loader import create_clickhouse_upsert_loader
from pipelines.tools.transformers.transformers import create_lambda_transformer, add_timestamp_metadata, select_fields, rename_fields
from pipelines.pipeline_factory import create_el_pipeline, create_etl_pipeline

# Pipeline registry
_pipeline_registry = {}

def register_cmc_pipelines():
    """Register all CMC pipelines."""
    log_with_timestamp("Registering CMC pipelines...", "CMC")
    
    # CMC API configuration
    cmc_config = config.get_cmc_api_config()
    
    # Validate API key
    if not cmc_config.get('api_key'):
        log_with_timestamp("CMC API key not configured. CMC pipelines will not be registered.", "CMC", "warning")
        return
    
    headers = {
        'X-CMC_PRO_API_KEY': cmc_config['api_key'],
        'Accept': 'application/json'
    }
    
    # 1. CMC Latest Quotes Pipeline (EL)
    cmc_latest_extractor = create_http_extractor(
        url=f"{cmc_config['base_url']}/cryptocurrency/quotes/latest",
        headers=headers,
        params={'limit': 100, 'convert': 'USD'},
        name="CMC Latest Quotes Extractor"
    )
    
    cmc_latest_loader = create_clickhouse_upsert_loader(
        table_name="cmc_latest_quotes",
        unique_key_columns=["id", "symbol"],
        name="CMC Latest Quotes Loader"
    )
    
    cmc_latest_pipeline = create_el_pipeline(
        cmc_latest_extractor,
        cmc_latest_loader,
        "CMC Latest Quotes Pipeline"
    )
    
    _pipeline_registry["cmc_latest_quotes"] = cmc_latest_pipeline
    
    # 2. CMC Historical Data Pipeline (ETL)
    cmc_historical_extractor = create_http_extractor(
        url=f"{cmc_config['base_url']}/cryptocurrency/quotes/historical",
        headers=headers,
        params={'time_start': '2024-01-01', 'time_end': '2024-01-31', 'count': 30},
        name="CMC Historical Extractor"
    )
    
    def transform_cmc_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform CMC data for ClickHouse."""
        if 'data' in record and isinstance(record['data'], dict):
            data = record['data']
            return {
                'id': data.get('id'),
                'name': data.get('name'),
                'symbol': data.get('symbol'),
                'slug': data.get('slug'),
                'cmc_rank': data.get('cmc_rank'),
                'num_market_pairs': data.get('num_market_pairs'),
                'circulating_supply': data.get('circulating_supply'),
                'total_supply': data.get('total_supply'),
                'max_supply': data.get('max_supply'),
                'last_updated': data.get('last_updated'),
                'date_added': data.get('date_added'),
                'tags': str(data.get('tags', [])),
                'platform': str(data.get('platform', {})),
                'quote': str(data.get('quote', {}))
            }
        elif isinstance(record, dict):
            # Handle direct data records
            return {
                'id': record.get('id'),
                'name': record.get('name'),
                'symbol': record.get('symbol'),
                'slug': record.get('slug'),
                'cmc_rank': record.get('cmc_rank'),
                'num_market_pairs': record.get('num_market_pairs'),
                'circulating_supply': record.get('circulating_supply'),
                'total_supply': record.get('total_supply'),
                'max_supply': record.get('max_supply'),
                'last_updated': record.get('last_updated'),
                'date_added': record.get('date_added'),
                'tags': str(record.get('tags', [])),
                'platform': str(record.get('platform', {})),
                'quote': str(record.get('quote', {}))
            }
        else:
            log_with_timestamp(f"Unexpected record format in CMC transformer: {type(record)}", "CMC Transformer", "warning")
            return None
    
    cmc_historical_transformer = create_lambda_transformer(
        transform_cmc_data,
        "CMC Historical Transformer"
    )
    
    cmc_historical_loader = create_clickhouse_upsert_loader(
        table_name="cmc_historical_data",
        unique_key_columns=["id", "last_updated"],
        name="CMC Historical Loader"
    )
    
    cmc_historical_pipeline = create_etl_pipeline(
        cmc_historical_extractor,
        cmc_historical_transformer,
        cmc_historical_loader,
        "CMC Historical Data Pipeline"
    )
    
    _pipeline_registry["cmc_historical_data"] = cmc_historical_pipeline
    
    log_with_timestamp(f"Registered {len(_pipeline_registry)} CMC pipelines", "CMC")

def get_cmc_pipeline(pipeline_name: str):
    """Get a CMC pipeline by name."""
    return _pipeline_registry.get(pipeline_name)

def list_cmc_pipelines():
    """List all available CMC pipelines."""
    return list(_pipeline_registry.keys())