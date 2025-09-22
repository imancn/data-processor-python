# src/pipelines/tools/extractors/metabase_extractor.py
"""
Metabase extractor for retrieving data from any table in any database added to Metabase.

This module provides functionality to extract data from Metabase using the Metabase API.
It can work with any table in any database that has been added to Metabase.

Example:
    >>> from pipelines.tools.extractors import create_metabase_extractor
    >>> extractor = create_metabase_extractor(
    ...     base_url="https://metabase.example.com",
    ...     api_key="mb_...",
    ...     database_id=1,
    ...     table_id=2
    ... )
    >>> data = await extractor()
"""

from typing import List, Dict, Any, Callable, Optional, Union
import asyncio
import aiohttp
import pandas as pd
from core.config import config
from core.logging import log_with_timestamp


async def extract_from_metabase_table(
    base_url: str,
    api_key: str,
    database_id: int,
    table_id: int,
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: int = None
) -> pd.DataFrame:
    """
    Extract data from a specific table in Metabase.
    
    Args:
        base_url: Metabase base URL (e.g., "https://metabase.example.com")
        api_key: Metabase API key
        database_id: Database ID in Metabase
        table_id: Table ID in Metabase
        limit: Maximum number of rows to extract (None for all)
        offset: Number of rows to skip
        timeout: Request timeout in seconds
        
    Returns:
        pandas DataFrame with the extracted data
    """
    try:
        timeout = timeout or config.timeout
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
        # First, get table metadata to understand the structure
        table_url = f"{base_url.rstrip('/')}/api/table/{table_id}"
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            # Get table metadata
            async with session.get(table_url, headers=headers) as response:
                if response.status == 401:
                    log_with_timestamp("Metabase authentication failed. Check your API key.", "Metabase Extractor", "error")
                    return pd.DataFrame()
                elif response.status == 404:
                    log_with_timestamp(f"Table {table_id} not found in Metabase", "Metabase Extractor", "error")
                    return pd.DataFrame()
                
                response.raise_for_status()
                table_metadata = await response.json()
                
                # Get table name and schema
                table_name = table_metadata.get('name', f'table_{table_id}')
                schema_name = table_metadata.get('schema', 'public')
                
                log_with_timestamp(f"Extracting from table: {schema_name}.{table_name}", "Metabase Extractor")
                
                # Build query to get all data from the table
                query = {
                    "database": database_id,
                    "type": "query",
                    "query": {
                        "source-table": table_id,
                        "limit": limit,
                        "offset": offset
                    }
                }
                
                # Execute the query
                query_url = f"{base_url.rstrip('/')}/api/dataset"
                
                async with session.post(query_url, headers=headers, json=query) as query_response:
                    query_response.raise_for_status()
                    query_result = await query_response.json()
                    
                    # Extract data from query result
                    if 'data' in query_result and 'rows' in query_result['data']:
                        rows = query_result['data']['rows']
                        columns = query_result['data'].get('cols', [])
                        
                        if not rows:
                            log_with_timestamp(f"No data found in table {table_name}", "Metabase Extractor", "warning")
                            return pd.DataFrame()
                        
                        # Create column names from Metabase column metadata
                        if columns:
                            column_names = [col.get('display_name', f'col_{i}') for i, col in enumerate(columns)]
                        else:
                            # Fallback: use generic column names
                            column_names = [f'col_{i}' for i in range(len(rows[0]) if rows else 0)]
                        
                        # Create DataFrame
                        df = pd.DataFrame(rows, columns=column_names)
                        
                        log_with_timestamp(f"Successfully extracted {len(df)} rows from {table_name}", "Metabase Extractor")
                        return df
                    else:
                        log_with_timestamp(f"No data found in query result for table {table_name}", "Metabase Extractor", "warning")
                        return pd.DataFrame()
                        
    except aiohttp.ClientError as e:
        log_with_timestamp(f"Network error during Metabase extraction: {e}", "Metabase Extractor", "error")
        return pd.DataFrame()
    except Exception as e:
        log_with_timestamp(f"Unexpected error during Metabase extraction: {e}", "Metabase Extractor", "error")
        return pd.DataFrame()


async def extract_from_metabase_query(
    base_url: str,
    api_key: str,
    database_id: int,
    native_query: str,
    timeout: int = None
) -> pd.DataFrame:
    """
    Extract data using a native SQL query in Metabase.
    
    Args:
        base_url: Metabase base URL
        api_key: Metabase API key
        database_id: Database ID in Metabase
        native_query: Native SQL query to execute
        timeout: Request timeout in seconds
        
    Returns:
        pandas DataFrame with the extracted data
    """
    try:
        timeout = timeout or config.timeout
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
        # Build native query
        query = {
            "database": database_id,
            "type": "native",
            "native": {
                "query": native_query
            }
        }
        
        query_url = f"{base_url.rstrip('/')}/api/dataset"
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.post(query_url, headers=headers, json=query) as response:
                if response.status == 401:
                    log_with_timestamp("Metabase authentication failed. Check your API key.", "Metabase Extractor", "error")
                    return pd.DataFrame()
                elif response.status == 400:
                    error_data = await response.json()
                    log_with_timestamp(f"Query error: {error_data.get('message', 'Unknown error')}", "Metabase Extractor", "error")
                    return pd.DataFrame()
                
                response.raise_for_status()
                query_result = await response.json()
                
                # Extract data from query result
                if 'data' in query_result and 'rows' in query_result['data']:
                    rows = query_result['data']['rows']
                    columns = query_result['data'].get('cols', [])
                    
                    if not rows:
                        log_with_timestamp("No data found in query result", "Metabase Extractor", "warning")
                        return pd.DataFrame()
                    
                    # Create column names from Metabase column metadata
                    if columns:
                        column_names = [col.get('display_name', f'col_{i}') for i, col in enumerate(columns)]
                    else:
                        # Fallback: use generic column names
                        column_names = [f'col_{i}' for i in range(len(rows[0]) if rows else 0)]
                    
                    # Create DataFrame
                    df = pd.DataFrame(rows, columns=column_names)
                    
                    log_with_timestamp(f"Successfully extracted {len(df)} rows using native query", "Metabase Extractor")
                    return df
                else:
                    log_with_timestamp("No data found in query result", "Metabase Extractor", "warning")
                    return pd.DataFrame()
                    
    except aiohttp.ClientError as e:
        log_with_timestamp(f"Network error during Metabase query execution: {e}", "Metabase Extractor", "error")
        return pd.DataFrame()
    except Exception as e:
        log_with_timestamp(f"Unexpected error during Metabase query execution: {e}", "Metabase Extractor", "error")
        return pd.DataFrame()


async def get_metabase_databases(
    base_url: str,
    api_key: str,
    timeout: int = None
) -> List[Dict[str, Any]]:
    """
    Get list of databases available in Metabase.
    
    Args:
        base_url: Metabase base URL
        api_key: Metabase API key
        timeout: Request timeout in seconds
        
    Returns:
        List of database information dictionaries
    """
    try:
        timeout = timeout or config.timeout
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
        databases_url = f"{base_url.rstrip('/')}/api/database"
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(databases_url, headers=headers) as response:
                if response.status == 401:
                    log_with_timestamp("Metabase authentication failed. Check your API key.", "Metabase Extractor", "error")
                    return []
                
                response.raise_for_status()
                databases = await response.json()
                
                log_with_timestamp(f"Found {len(databases)} databases in Metabase", "Metabase Extractor")
                return databases.get("data", [])
                
    except Exception as e:
        log_with_timestamp(f"Error getting Metabase databases: {e}", "Metabase Extractor", "error")
        return []


async def get_metabase_tables(
    base_url: str,
    api_key: str,
    database_id: int,
    timeout: int = None
) -> List[Dict[str, Any]]:
    """
    Get list of tables in a specific database.
    
    Args:
        base_url: Metabase base URL
        api_key: Metabase API key
        database_id: Database ID in Metabase
        timeout: Request timeout in seconds
        
    Returns:
        List of table information dictionaries
    """
    try:
        timeout = timeout or config.timeout
        headers = {
            "X-API-Key": api_key,
            "Content-Type": "application/json"
        }
        
        tables_url = f"{base_url.rstrip('/')}/api/database/{database_id}/metadata"
        
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.get(tables_url, headers=headers) as response:
                if response.status == 401:
                    log_with_timestamp("Metabase authentication failed. Check your API key.", "Metabase Extractor", "error")
                    return []
                elif response.status == 404:
                    log_with_timestamp(f"Database {database_id} not found in Metabase", "Metabase Extractor", "error")
                    return []
                
                response.raise_for_status()
                metadata = await response.json()
                
                tables = []
                for table in metadata.get('tables', []):
                    tables.append({
                        'id': table.get('id'),
                        'name': table.get('name'),
                        'schema': table.get('schema'),
                        'display_name': table.get('display_name'),
                        'description': table.get('description')
                    })
                
                log_with_timestamp(f"Found {len(tables)} tables in database {database_id}", "Metabase Extractor")
                return tables
                
    except Exception as e:
        log_with_timestamp(f"Error getting Metabase tables: {e}", "Metabase Extractor", "error")
        return []


def create_metabase_table_extractor(
    base_url: str,
    api_key: str,
    database_id: int,
    table_id: int,
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: int = None,
    name: str = "Metabase Table Extractor"
) -> Callable[..., Any]:
    """
    Factory function to create a Metabase table extractor.
    
    Args:
        base_url: Metabase base URL
        api_key: Metabase API key
        database_id: Database ID in Metabase
        table_id: Table ID in Metabase
        limit: Maximum number of rows to extract
        offset: Number of rows to skip
        timeout: Request timeout in seconds
        name: Name for the extractor
        
    Returns:
        Configured extractor function
    """
    async def extractor_func(*args, **kwargs):
        log_with_timestamp(f"Running {name} for table {table_id} in database {database_id}", name)
        return await extract_from_metabase_table(
            base_url=base_url,
            api_key=api_key,
            database_id=database_id,
            table_id=table_id,
            limit=limit,
            offset=offset,
            timeout=timeout
        )
    return extractor_func


def create_metabase_query_extractor(
    base_url: str,
    api_key: str,
    database_id: int,
    native_query: str,
    timeout: int = None,
    name: str = "Metabase Query Extractor"
) -> Callable[..., Any]:
    """
    Factory function to create a Metabase query extractor.
    
    Args:
        base_url: Metabase base URL
        api_key: Metabase API key
        database_id: Database ID in Metabase
        native_query: Native SQL query to execute
        timeout: Request timeout in seconds
        name: Name for the extractor
        
    Returns:
        Configured extractor function
    """
    async def extractor_func(*args, **kwargs):
        log_with_timestamp(f"Running {name} with query: {native_query[:100]}...", name)
        return await extract_from_metabase_query(
            base_url=base_url,
            api_key=api_key,
            database_id=database_id,
            native_query=native_query,
            timeout=timeout
        )
    return extractor_func


def create_metabase_extractor(
    base_url: str = None,
    api_key: str = None,
    database_id: int = None,
    table_id: int = None,
    native_query: str = None,
    limit: Optional[int] = None,
    offset: int = 0,
    timeout: int = None,
    name: str = "Metabase Extractor"
) -> Callable[..., Any]:
    """
    Main factory function to create a Metabase extractor.
    
    This function can create either a table extractor or query extractor based on the parameters provided.
    
    Args:
        base_url: Metabase base URL (uses config if not provided)
        api_key: Metabase API key (uses config if not provided)
        database_id: Database ID in Metabase
        table_id: Table ID for table extraction (optional if using native_query)
        native_query: Native SQL query for query extraction (optional if using table_id)
        limit: Maximum number of rows to extract
        offset: Number of rows to skip
        timeout: Request timeout in seconds
        name: Name for the extractor
        
    Returns:
        Configured extractor function
        
    Raises:
        ValueError: If neither table_id nor native_query is provided, or if both are provided
    """
    # Use config values if not provided
    metabase_config = config.get_metabase_config()
    base_url = base_url or metabase_config.get('base_url')
    api_key = api_key or metabase_config.get('api_key')
    timeout = timeout or metabase_config.get('timeout', config.timeout)
    
    if not base_url or not api_key:
        raise ValueError("Metabase base_url and api_key must be provided or configured")
    
    if not database_id:
        raise ValueError("database_id is required")
    
    # Determine extraction type
    if table_id and native_query:
        raise ValueError("Cannot specify both table_id and native_query. Choose one extraction method.")
    elif table_id:
        return create_metabase_table_extractor(
            base_url=base_url,
            api_key=api_key,
            database_id=database_id,
            table_id=table_id,
            limit=limit,
            offset=offset,
            timeout=timeout,
            name=name
        )
    elif native_query:
        return create_metabase_query_extractor(
            base_url=base_url,
            api_key=api_key,
            database_id=database_id,
            native_query=native_query,
            timeout=timeout,
            name=name
        )
    else:
        raise ValueError("Either table_id or native_query must be provided")
