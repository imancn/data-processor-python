# src/pipelines/tools/extractors/__init__.py
"""
Generic extractors for various data sources.

This package provides extractors for retrieving data from different sources
including HTTP APIs, databases, files, and other external systems.

Example:
    >>> from pipelines.tools.extractors import create_http_extractor
    >>> extractor = create_http_extractor(
    ...     url="https://api.example.com/data",
    ...     headers={"Authorization": "Bearer token"}
    ... )
    >>> data = extractor()
"""

from .http_extractor import create_http_extractor
from .clickhouse_extractor import create_clickhouse_extractor

# Public API
__all__ = [
    'create_http_extractor',
    'create_clickhouse_extractor',
]
