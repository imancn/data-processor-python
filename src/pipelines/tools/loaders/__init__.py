# src/pipelines/tools/loaders/__init__.py
"""
Generic loaders for various data destinations.

This package provides loaders for storing data to different destinations
including databases and other external systems.

Example:
    >>> from pipelines.tools.loaders import create_clickhouse_loader
    >>> loader = create_clickhouse_loader(
    ...     table="my_table",
    ...     columns=["id", "name", "value"]
    ... )
    >>> loader(dataframe)
"""

from .clickhouse_loader import create_clickhouse_loader, create_clickhouse_upsert_loader

# Public API
__all__ = [
    'create_clickhouse_loader',
    'create_clickhouse_upsert_loader',
]
