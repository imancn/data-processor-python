# src/pipelines/tools/transformers/__init__.py
"""
Generic transformers for data processing.

This package provides transformers for processing and modifying data
between extraction and loading phases of ETL pipelines.

Example:
    >>> from pipelines.tools.transformers import create_lambda_transformer
    >>> transformer = create_lambda_transformer(
    ...     lambda df: df.dropna().reset_index(drop=True)
    ... )
    >>> clean_data = transformer(raw_data)
"""

from .transformers import create_lambda_transformer

# Public API
__all__ = [
    'create_lambda_transformer',
]
