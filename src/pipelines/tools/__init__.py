# src/pipelines/tools/__init__.py
"""
Pipeline tools for data processing.

This package provides a comprehensive set of tools for building ETL pipelines,
including extractors for data retrieval, transformers for data processing,
and loaders for data storage.

Example:
    >>> from pipelines.tools import (
    ...     create_http_extractor,
    ...     create_lambda_transformer, 
    ...     create_clickhouse_loader
    ... )
    >>> 
    >>> # Create pipeline components
    >>> extractor = create_http_extractor("https://api.example.com/data")
    >>> transformer = create_lambda_transformer(lambda df: df.dropna())
    >>> loader = create_clickhouse_loader("my_table", ["id", "name"])
"""

# Import all tools for convenience
from .extractors import (
    create_http_extractor,
    create_clickhouse_extractor,
    create_metabase_extractor,
)

from .transformers import (
    create_lambda_transformer,
)

from .loaders import (
    create_clickhouse_loader,
    create_clickhouse_upsert_loader,
    create_console_loader,
)

# Public API - All pipeline tools
__all__ = [
    # Extractors
    'create_http_extractor',
    'create_clickhouse_extractor',
    'create_metabase_extractor',
    
    # Transformers
    'create_lambda_transformer',
    
    # Loaders
    'create_clickhouse_loader',
    'create_clickhouse_upsert_loader',
    'create_console_loader',
]
