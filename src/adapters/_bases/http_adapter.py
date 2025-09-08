import aiohttp
from typing import Optional, Dict, Any


class HttpAdapter:
    def __init__(self, base_url: str, default_headers: Optional[Dict[str, str]] = None, retries: int = 3, backoff_seconds: float = 1.0):
        self.base_url = base_url.rstrip('/')
        self.default_headers = default_headers or {}
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        import ssl
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(connector=connector)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def request_json(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        assert self.session is not None, "HttpAdapter must be used as an async context manager"
        url = f"{self.base_url}/{path.lstrip('/')}"
        merged_headers = {**self.default_headers, **(headers or {})}
        last_err = None
        for attempt in range(self.retries):
            try:
                async with self.session.request(method.upper(), url, params=params, headers=merged_headers) as response:
                    if response.status == 429 or response.status >= 500:
                        last_err = Exception(f"HTTP {response.status}: {await response.text()}")
                        if attempt < self.retries - 1:
                            import asyncio
                            await asyncio.sleep(self.backoff_seconds * (2 ** attempt))
                            continue
                        raise last_err
                    if response.status != 200:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
                    return await response.json()
            except aiohttp.ClientError as e:
                last_err = e
                if attempt < self.retries - 1:
                    import asyncio
                    await asyncio.sleep(self.backoff_seconds * (2 ** attempt))
                    continue
                raise last_err
        raise last_err if last_err else Exception("Unknown API error")

    async def get_json(self, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        return await self.request_json('GET', path, params=params, headers=headers)


