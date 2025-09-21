# src/extractors/http_extractor.py
from typing import Optional
import asyncio
import aiohttp
import pandas as pd
from core.config import config
from core.logging import log_with_timestamp

async def extract_from_http(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    method: str = "GET",
    data: Optional[dict] = None,
    timeout: int = None
) -> pd.DataFrame:
    """
    Generic HTTP extractor that returns JSON response as pandas DataFrame.
    """
    try:
        timeout = timeout or config.timeout
        total_attempts = 3
        backoff = 0.7
        json_response = None
        
        for attempt in range(1, total_attempts + 1):
            try:
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
                        break
            except aiohttp.ClientError as e:
                log_with_timestamp(f"Attempt {attempt}/{total_attempts} failed for {url}: {e}", "HTTP Extractor", "warning", category="retry")
                if attempt < total_attempts:
                    await asyncio.sleep(backoff * attempt)
                else:
                    # Last attempt failed, log and return empty DataFrame
                    log_with_timestamp(f"All {total_attempts} attempts failed for {url}: {e}", "HTTP Extractor", "error", category="network")
                    return pd.DataFrame()

        # Convert JSON response to DataFrame (only if we got a successful response)
        if json_response is not None:
            if isinstance(json_response, list):
                return pd.DataFrame(json_response)
            elif isinstance(json_response, dict):
                for key in ['data', 'results', 'items']:
                    if key in json_response and isinstance(json_response[key], list):
                        return pd.DataFrame(json_response[key])
                    elif key in json_response and isinstance(json_response[key], dict):
                        # Handle API response where data is a dict with symbol keys
                        # Convert to DataFrame of individual records
                        data_dict = json_response[key]
                        records = [data_dict[symbol] for symbol in data_dict.keys()]
                        return pd.DataFrame(records)
                # If no standard key found, try to convert the whole response
                return pd.DataFrame([json_response])
            else:
                log_with_timestamp(f"Unexpected HTTP response format: {type(json_response)}", "HTTP Extractor", "warning")
                return pd.DataFrame()
        else:
            # No successful response
            return pd.DataFrame()
            
    except Exception as e:
        log_with_timestamp(f"An unexpected error occurred during HTTP extraction for {url}: {e}", "HTTP Extractor", "error", category="unexpected")
        return pd.DataFrame()

async def extract_from_paginated_http(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    method: str = "GET",
    data: Optional[dict] = None,
    timeout: int = None,
    max_pages: int = 10,
    page_param: str = "page",
    page_size_param: str = "per_page",
    page_size: int = 100
) -> pd.DataFrame:
    """
    HTTP extractor for paginated APIs that returns all pages as a single DataFrame.
    """
    try:
        all_data = []
        page = 1
        
        while page <= max_pages:
            # Add pagination parameters
            page_params = params.copy() if params else {}
            page_params[page_param] = page
            page_params[page_size_param] = page_size
            
            # Extract data for current page
            page_df = await extract_from_http(
                url=url,
                headers=headers,
                params=page_params,
                method=method,
                data=data,
                timeout=timeout
            )
            
            if page_df.empty:
                break
                
            all_data.append(page_df)
            page += 1
            
            # Small delay to avoid rate limiting
            await asyncio.sleep(0.1)
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        log_with_timestamp(f"Paginated HTTP extraction failed for {url}: {e}", "HTTP Extractor", "error")
        return pd.DataFrame()

def create_http_extractor(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    method: str = "GET",
    data: Optional[dict] = None,
    timeout: int = None,
    name: str = "HTTP Extractor"
):
    """
    Factory function to create HTTP extractor with pandas DataFrame output.
    """
    async def extractor() -> pd.DataFrame:
        log_with_timestamp(f"Running {name} from {url}", "HTTP Extractor")
        return await extract_from_http(
            url=url,
            headers=headers,
            params=params,
            method=method,
            data=data,
            timeout=timeout
        )
    
    return extractor

def create_paginated_http_extractor(
    url: str,
    headers: Optional[dict] = None,
    params: Optional[dict] = None,
    method: str = "GET",
    data: Optional[dict] = None,
    timeout: int = None,
    max_pages: int = 10,
    page_param: str = "page",
    page_size_param: str = "per_page",
    page_size: int = 100,
    name: str = "Paginated HTTP Extractor"
):
    """
    Factory function to create paginated HTTP extractor with pandas DataFrame output.
    """
    async def extractor() -> pd.DataFrame:
        log_with_timestamp(f"Running {name} from {url} (max {max_pages} pages)", "HTTP Extractor")
        return await extract_from_paginated_http(
            url=url,
            headers=headers,
            params=params,
            method=method,
            data=data,
            timeout=timeout,
            max_pages=max_pages,
            page_param=page_param,
            page_size_param=page_size_param,
            page_size=page_size
        )
    
    return extractor