import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from crypto_price_fetcher.fetcher import fetch_crypto_prices

@pytest.mark.asyncio
async def test_fetch_crypto_prices_success():
    mock_response = {"data": {"BTC": {"quote": {"USD": {"price": 50000.00}}}}}
    with patch('aiohttp.ClientSession') as mock_session:
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.json.return_value = mock_response
        mock_session.return_value.__aenter__.return_value.get.return_value.__aenter__.return_value.raise_for_status.return_value = None
        result = await fetch_crypto_prices(["BTC"])
        assert len(result) == 1
        assert result[0]["symbol"] == "BTC"
        assert result[0]["price"] == 50000.0
