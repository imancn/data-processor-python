# src/pipelines/tools/extractors/http_extractor.py
"""
HTTP Extractor for data processing pipelines.

This module provides functionality to extract data from HTTP APIs,
supporting various HTTP methods and authentication mechanisms.
"""

from typing import Optional, Dict, Any
import asyncio
import aiohttp
import pandas as pd
from core.config import config
from core.logging import log_with_timestamp

async def extract_from_http(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    name: str = "HTTP Extractor"
) -> pd.DataFrame:
    """
    Extract data from HTTP API endpoint.
    
    Args:
        url: The URL to extract data from
        headers: Optional HTTP headers
        params: Optional query parameters
        method: HTTP method (GET, POST, PUT, DELETE)
        data: Optional request body data
        timeout: Request timeout in seconds
        name: Name for logging purposes
        
    Returns:
        pandas DataFrame containing the extracted data
        
    Raises:
        Exception: If the HTTP request fails
    """
    try:
        log_with_timestamp(f"Starting HTTP extraction from {url}", name)
        
        # Set default headers
        if headers is None:
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'Data-Processor/1.0'
            }
        
        # Create session with timeout
        timeout_config = aiohttp.ClientTimeout(total=timeout)
        
        async with aiohttp.ClientSession(timeout=timeout_config) as session:
            # Prepare request arguments
            request_kwargs = {
                'headers': headers,
                'params': params
            }
            
            # Add data for POST/PUT requests
            if data and method.upper() in ['POST', 'PUT', 'PATCH']:
                request_kwargs['json'] = data
            
            # Make the HTTP request
            async with session.request(method.upper(), url, **request_kwargs) as response:
                # Check if request was successful
                response.raise_for_status()
                
                # Get response content
                content_type = response.headers.get('content-type', '').lower()
                
                if 'application/json' in content_type:
                    data = await response.json()
                    log_with_timestamp(f"Received JSON response with {len(data) if isinstance(data, list) else 1} records", name)
                    
                    # Convert to DataFrame
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif isinstance(data, dict):
                        # If it's a single object, wrap it in a list
                        df = pd.DataFrame([data])
                    else:
                        # If it's not a list or dict, create a single-row DataFrame
                        df = pd.DataFrame([{'data': data}])
                        
                elif 'text/csv' in content_type:
                    # Handle CSV response
                    csv_content = await response.text()
                    from io import StringIO
                    df = pd.read_csv(StringIO(csv_content))
                    log_with_timestamp(f"Received CSV response with {len(df)} records", name)
                    
                else:
                    # Handle other content types as text
                    text_content = await response.text()
                    df = pd.DataFrame([{'content': text_content}])
                    log_with_timestamp(f"Received text response with {len(text_content)} characters", name)
                
                log_with_timestamp(f"Successfully extracted {len(df)} records from {url}", name)
                return df
                
    except aiohttp.ClientError as e:
        error_msg = f"HTTP client error during extraction from {url}: {e}"
        log_with_timestamp(error_msg, name, "error")
        raise Exception(error_msg) from e
        
    except asyncio.TimeoutError as e:
        error_msg = f"Timeout during HTTP extraction from {url} (timeout: {timeout}s)"
        log_with_timestamp(error_msg, name, "error")
        raise Exception(error_msg) from e
        
    except Exception as e:
        error_msg = f"Unexpected error during HTTP extraction from {url}: {e}"
        log_with_timestamp(error_msg, name, "error")
        raise Exception(error_msg) from e

def create_http_extractor(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    name: str = "HTTP Extractor"
) -> callable:
    """
    Create an HTTP extractor function with pre-configured parameters.
    
    Args:
        url: The URL to extract data from
        headers: Optional HTTP headers
        params: Optional query parameters
        method: HTTP method (GET, POST, PUT, DELETE)
        data: Optional request body data
        timeout: Request timeout in seconds
        name: Name for logging purposes
        
    Returns:
        Async function that performs the HTTP extraction
        
    Example:
        >>> extractor = create_http_extractor(
        ...     url="https://api.example.com/data",
        ...     headers={"Authorization": "Bearer token"},
        ...     params={"limit": 100}
        ... )
        >>> data = await extractor()
    """
    async def http_extractor_func() -> pd.DataFrame:
        return await extract_from_http(
            url=url,
            headers=headers,
            params=params,
            method=method,
            data=data,
            timeout=timeout,
            name=name
        )
    
    return http_extractor_func

# Public API
__all__ = [
    'extract_from_http',
    'create_http_extractor',
]
