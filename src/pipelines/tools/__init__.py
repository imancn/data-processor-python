# src/pipelines/tools/__init__.py
"""
Pipeline tools for data processing.

This package provides tools for building ETL pipelines,
including extractors for data retrieval, transformers for data processing,
and loaders for data storage.

Example:
    >>> from pipelines.tools import (
    ...     create_http_extractor,
    ...     create_metabase_extractor,
    ...     create_lambda_transformer, 
    ...     create_clickhouse_loader
    ... )
    >>> 
    >>> # Create pipeline components
    >>> http_extractor = create_http_extractor("https://api.example.com/data")
    >>> metabase_extractor = create_metabase_extractor(database_id=1, table_id=2)
    >>> transformer = create_lambda_transformer(lambda df: df.dropna())
    >>> loader = create_clickhouse_loader("my_table", ["id", "name"])
"""

# Import all tools for convenience
from .extractors import (
    create_http_extractor,
    create_metabase_extractor,
)

from .transformers import (
    create_lambda_transformer,
)

from .loaders import (
    create_clickhouse_loader,
    create_clickhouse_upsert_loader,
)

from .data_utils import (
    convert_to_timestamp,
    clean_data_for_clickhouse,
    deduplicate_data,
    add_merge_metadata,
    prepare_datetime_columns,
)

from .pagination_utils import (
    extract_with_pagination,
    create_paginated_extractor,
)

from .backfill_utils import (
    BackfillManager,
    backfill_manager,
    run_backfill,
)

from .clickhouse_replace_loader import (
    load_to_clickhouse_with_replace,
    create_clickhouse_replace_loader,
)

# Public API - All pipeline tools
__all__ = [
    # Extractors
    'create_http_extractor',
    'create_metabase_extractor',
    
    # Transformers
    'create_lambda_transformer',
    
    # Loaders
    'create_clickhouse_loader',
    'create_clickhouse_upsert_loader',
    'create_clickhouse_replace_loader',
    
    # Data Utilities
    'convert_to_timestamp',
    'clean_data_for_clickhouse',
    'deduplicate_data',
    'add_merge_metadata',
    'prepare_datetime_columns',
    
    # Pagination Utilities
    'extract_with_pagination',
    'create_paginated_extractor',
    
    # Backfill Utilities
    'BackfillManager',
    'backfill_manager',
    'run_backfill',
    
    # ClickHouse Replace Loader
    'load_to_clickhouse_with_replace',
]
