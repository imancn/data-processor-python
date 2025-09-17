# src/extractors/http_extractor.py
from typing import List, Dict, Any, Callable, Optional
import asyncio
import aiohttp
from core.config import config
from core.logging import log_with_timestamp

async def extract_from_http(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = None
) -> List[Dict[str, Any]]:
    """
    Generic HTTP extractor that returns JSON response as list of records.
    """
    try:
        timeout = timeout or config.timeout
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            async with session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data
            ) as response:
                response.raise_for_status()
                json_response = await response.json()

                if isinstance(json_response, list):
                    return json_response
                elif isinstance(json_response, dict):
                    for key in ['data', 'results', 'items']:
                        if key in json_response and isinstance(json_response[key], list):
                            return json_response[key]
                    return [json_response]
                else:
                    log_with_timestamp(f"Unexpected HTTP response format: {type(json_response)}", "HTTP Extractor", "warning")
                    return []
    except aiohttp.ClientError as e:
        log_with_timestamp(f"HTTP extraction failed for {url}: {e}", "HTTP Extractor", "error")
        return []
    except Exception as e:
        log_with_timestamp(f"An unexpected error occurred during HTTP extraction for {url}: {e}", "HTTP Extractor", "error")
        return []

async def extract_from_paginated_http(
    base_url: str,
    page_param: str = "page",
    limit_param: str = "limit",
    start_page: int = 1,
    page_size: int = 100,
    max_pages: int = 5,
    headers: Optional[Dict[str, str]] = None,
    fixed_params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = None
) -> List[Dict[str, Any]]:
    """
    Extract data from a paginated HTTP API.
    """
    all_data = []
    for page in range(start_page, start_page + max_pages):
        params = {page_param: page, limit_param: page_size}
        if fixed_params:
            params.update(fixed_params)

        log_with_timestamp(f"Fetching page {page} from {base_url}...", "Paginated HTTP Extractor", "debug")
        page_data = await extract_from_http(
            url=base_url,
            headers=headers,
            params=params,
            method=method,
            data=data,
            timeout=timeout
        )
        if not page_data:
            log_with_timestamp(f"No data received for page {page} or end of pagination.", "Paginated HTTP Extractor", "info")
            break
        all_data.extend(page_data)
        log_with_timestamp(f"Fetched {len(page_data)} records from page {page}. Total: {len(all_data)}", "Paginated HTTP Extractor", "debug")
        await asyncio.sleep(0.1)
    return all_data

def create_http_extractor(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = None,
    name: str = "HTTP Extractor"
) -> Callable[..., Any]:
    """Factory function to create a configured HTTP extractor."""
    async def extractor_func(*args, **kwargs):
        log_with_timestamp(f"Running {name} from {url}", name)
        return await extract_from_http(url, headers, params, method, data, timeout)
    return extractor_func

def create_paginated_http_extractor(
    base_url: str,
    page_param: str = "page",
    limit_param: str = "limit",
    start_page: int = 1,
    page_size: int = 100,
    max_pages: int = 5,
    headers: Optional[Dict[str, str]] = None,
    fixed_params: Optional[Dict[str, Any]] = None,
    method: str = "GET",
    data: Optional[Dict[str, Any]] = None,
    timeout: int = None,
    name: str = "Paginated HTTP Extractor"
) -> Callable[..., Any]:
    """Factory function to create a configured paginated HTTP extractor."""
    async def extractor_func(*args, **kwargs):
        log_with_timestamp(f"Running {name} from {base_url}", name)
        return await extract_from_paginated_http(
            base_url, page_param, limit_param, start_page, page_size, max_pages,
            headers, fixed_params, method, data, timeout
        )
    return extractor_func
