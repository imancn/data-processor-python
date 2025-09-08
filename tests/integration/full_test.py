import asyncio
import sys
import os
from datetime import datetime
import pytest

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
from configurations.config import config
from adapters._bases.clickhouse_adapter import ClickHouseAdapter
from adapters.http.coin_market_cap_http_adapter import CoinMarketCapHttpAdapter

@pytest.mark.asyncio
async def test_clickhouse_connection():
    print("\n🧪 Testing ClickHouse connection...")
    try:
        ch = ClickHouseAdapter()
        success = ch.ping()
        if success:
            print("✅ ClickHouse connection successful")
            return True
        else:
            print("❌ ClickHouse connection failed")
            return False
    except Exception as e:
        print(f"❌ ClickHouse connection error: {e}")
        return False

@pytest.mark.asyncio
async def test_full_fetch():
    print("\n🧪 Testing full data fetch...")
    try:
        async with CoinMarketCapHttpAdapter() as adapter:
            crypto_data = await adapter.fetch(config.symbols)
        if crypto_data and len(crypto_data) > 0:
            print(f"✅ Fetched {len(crypto_data)} records")
            return True
        else:
            print("❌ No data fetched")
            return False
    except Exception as e:
        print(f"❌ Fetch error: {e}")
        return False

@pytest.mark.asyncio
async def test_storage():
    print("\n🧪 Testing data storage...")
    try:
        async with CoinMarketCapHttpAdapter() as adapter:
            crypto_data = await adapter.fetch(config.symbols)
        if crypto_data:
            from adapters.clickhouse.coin_market_cap_clickhouse_adapter import CoinMarketCapClickHouseAdapter
            repo = CoinMarketCapClickHouseAdapter()
            ok = repo.ensure_schema()
            if not ok:
                print("❌ Schema setup failed")
                return False
            success = repo.upsert_prices(crypto_data)
            if success:
                print("✅ Data saved successfully")
                return True
            else:
                print("❌ Data save failed")
                return False
        else:
            print("❌ No data to save")
            return False
    except Exception as e:
        print(f"❌ Storage error: {e}")
        return False

async def main():
    print("🚀 Full Integration Test")
    print("=" * 40)

    tests = [
        ("ClickHouse Connection", test_clickhouse_connection),
        ("Data Fetch", test_full_fetch),
        ("Data Storage", test_storage)
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        if await test_func():
            passed += 1

    print("\n" + "=" * 40)
    print(f"Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All integration tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)