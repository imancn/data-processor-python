# src/pipelines/tools/extractors/metabase_paginated_extractor.py
"""
Paginated Metabase extractor for large datasets.

This module provides a specialized Metabase extractor that handles
pagination automatically for large datasets.
"""

import pandas as pd
from typing import Optional, Dict, Any, List
from core.config import config
from core.logging import log_with_timestamp
from .metabase_extractor import create_metabase_extractor
from ..pagination_utils import extract_with_pagination


class MetabasePaginatedExtractor:
    """
    Paginated Metabase extractor for handling large datasets.
    
    This class provides a convenient interface for extracting data from
    Metabase with automatic pagination handling.
    """
    
    def __init__(self, database_id: int, batch_size: int = 2000):
        """
        Initialize the paginated extractor.
        
        Args:
            database_id: Metabase database ID
            batch_size: Number of records per batch
        """
        self.database_id = database_id
        self.batch_size = batch_size
        self.metabase_config = config.get_metabase_config()
    
    async def extract_from_query(
        self, 
        query: str, 
        name: str = "Metabase Paginated Extractor"
    ) -> pd.DataFrame:
        """
        Extract data from a Metabase query with pagination.
        
        Args:
            query: SQL query to execute
            name: Name for logging purposes
            
        Returns:
            DataFrame containing all extracted data
        """
        def create_extractor_func(query, batch_size, offset, name):
            return create_metabase_extractor(
                database_id=self.database_id,
                native_query=query,
                limit=batch_size,
                offset=offset,
                name=name
            )
        
        return await extract_with_pagination(
            extractor_func=create_extractor_func,
            base_query=query,
            batch_size=self.batch_size,
            name=name
        )
    
    async def extract_from_table(
        self, 
        table_name: str, 
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        order_by: Optional[str] = None,
        name: str = "Metabase Table Extractor"
    ) -> pd.DataFrame:
        """
        Extract data from a Metabase table with pagination.
        
        Args:
            table_name: Name of the table to extract from
            columns: List of columns to select (None for all)
            where_clause: WHERE clause for filtering
            order_by: ORDER BY clause for sorting
            name: Name for logging purposes
            
        Returns:
            DataFrame containing all extracted data
        """
        # Build the query
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"
        
        query = f"SELECT {columns_str} FROM {table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        return await self.extract_from_query(query, name)


def create_metabase_paginated_extractor(
    database_id: int, 
    batch_size: int = 2000
) -> MetabasePaginatedExtractor:
    """
    Create a paginated Metabase extractor.
    
    Args:
        database_id: Metabase database ID
        batch_size: Number of records per batch
        
    Returns:
        MetabasePaginatedExtractor instance
        
    Example:
        >>> extractor = create_metabase_paginated_extractor(database_id=1)
        >>> data = await extractor.extract_from_query("SELECT * FROM my_table")
    """
    return MetabasePaginatedExtractor(database_id, batch_size)


# Public API
__all__ = [
    'MetabasePaginatedExtractor',
    'create_metabase_paginated_extractor',
]
