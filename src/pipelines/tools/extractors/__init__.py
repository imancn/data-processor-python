# src/pipelines/tools/extractors/__init__.py
"""
Generic extractors for various data sources.

This package provides extractors for retrieving data from different sources
including HTTP APIs, Metabase, and other external systems.

Available Extractors:
    - HTTP Extractor: Extract data from HTTP APIs
    - Metabase Extractor: Extract data from any table in any Metabase database

Example:
    >>> from pipelines.tools.extractors import create_http_extractor, create_metabase_extractor
    >>> 
    >>> # HTTP API extraction
    >>> http_extractor = create_http_extractor(
    ...     url="https://api.example.com/data",
    ...     headers={"Authorization": "Bearer token"}
    ... )
    >>> data = await http_extractor()
    >>> 
    >>> # Metabase extraction
    >>> metabase_extractor = create_metabase_extractor(
    ...     base_url="https://metabase.devinvex.com",
    ...     api_key="CHANGE_ME",
    ...     database_id=1,
    ...     table_id=2
    ... )
    >>> data = await metabase_extractor()
"""

from .http_extractor import create_http_extractor
from .metabase_extractor import create_metabase_extractor
from .metabase_paginated_extractor import create_metabase_paginated_extractor, MetabasePaginatedExtractor

# Public API
__all__ = [
    'create_http_extractor',
    'create_metabase_extractor',
    'create_metabase_paginated_extractor',
    'MetabasePaginatedExtractor',
]
