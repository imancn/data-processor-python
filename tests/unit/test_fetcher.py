import pytest
import asyncio
import os
import sys
from unittest.mock import patch

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
from adapters.http.coin_market_cap_http_adapter import CoinMarketCapHttpAdapter

@pytest.mark.asyncio
async def test_fetch_crypto_prices_success():
    mock_response = {"data": {"BTC": {"quote": {"USD": {"price": 50000.00}}}}}
    class DummyResp:
        def __init__(self, data):
            self._data = data
            self.status = 200
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def json(self):
            return self._data
        async def text(self):
            return "ok"

    class DummySession:
        def __init__(self, data):
            self._data = data
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        def request(self, method, url, params=None, headers=None):
            return DummyResp(self._data)
        async def close(self):
            return None

    with patch('aiohttp.ClientSession', return_value=DummySession(mock_response)):
        async with CoinMarketCapHttpAdapter() as adapter:
            result = await adapter.fetch(["BTC"])
        assert len(result) == 1
        assert result[0]["symbol"] == "BTC"
        assert result[0]["price"] == 50000.0
