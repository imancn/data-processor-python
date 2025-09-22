# src/pipelines/tools/pagination_utils.py
"""
Pagination utilities for data extraction.

This module provides common pagination patterns that can be reused
across different extractors.
"""

import pandas as pd
from typing import List, Callable, Optional
from core.logging import log_with_timestamp

async def extract_with_pagination(
    extractor_func: Callable,
    base_query: str,
    batch_size: int = 2000,
    name: str = "Pagination Extractor"
) -> pd.DataFrame:
    """
    Extract data using pagination to handle large datasets.
    
    Args:
        extractor_func: Function that creates an extractor for a specific query
        base_query: Base SQL query to paginate
        batch_size: Number of records per batch
        name: Name for logging purposes
        
    Returns:
        Combined DataFrame with all extracted data
    """
    all_data = []
    offset = 0
    total_extracted = 0
    
    while True:
        query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"
        
        # Create extractor for this batch
        extractor = extractor_func(query, batch_size, offset, f"{name} (batch {offset//batch_size + 1})")
        
        # Extract data for this batch
        batch_data = await extractor()
        
        if batch_data.empty:
            log_with_timestamp(f"No more data found at offset {offset}", name)
            break
        
        all_data.append(batch_data)
        total_extracted += len(batch_data)
        log_with_timestamp(f"Extracted batch {offset//batch_size + 1}: {len(batch_data)} records (total: {total_extracted})", name)
        
        # If we got fewer records than the batch size, we've reached the end
        if len(batch_data) < batch_size:
            log_with_timestamp(f"Reached end of data (got {len(batch_data)} < {batch_size})", name)
            break
        
        offset += batch_size
    
    # Combine all batches
    if all_data:
        data = pd.concat(all_data, ignore_index=True)
        log_with_timestamp(f"Total extracted {len(data)} records across {len(all_data)} batches", name)
        return data
    else:
        log_with_timestamp("No data found", name, "warning")
        return pd.DataFrame()

def create_paginated_extractor(
    extractor_func: Callable,
    base_query: str,
    batch_size: int = 2000,
    name: str = "Paginated Extractor"
) -> Callable:
    """
    Create a paginated extractor function.
    
    Args:
        extractor_func: Function that creates an extractor for a specific query
        base_query: Base SQL query to paginate
        batch_size: Number of records per batch
        name: Name for logging purposes
        
    Returns:
        Async function that performs paginated extraction
    """
    async def paginated_extractor() -> pd.DataFrame:
        return await extract_with_pagination(extractor_func, base_query, batch_size, name)
    
    return paginated_extractor

# Public API
__all__ = [
    'extract_with_pagination',
    'create_paginated_extractor',
]
