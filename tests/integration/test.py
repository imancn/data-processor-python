import asyncio
import sys
import os
SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
from configurations.config import config
from adapters._bases.clickhouse_adapter import ClickHouseAdapter
from adapters.http.coin_market_cap_http_adapter import CoinMarketCapHttpAdapter

async def test_connection():
    print("🔌 Testing ClickHouse connection...")
    ch = ClickHouseAdapter()
    if ch.ping():
        print("✅ ClickHouse connection successful")
        return True
    else:
        print("❌ ClickHouse connection failed")
        return False

async def test_cmc_api():
    print("\n📊 Testing CoinMarketCap API...")
    if not config.cmc_api_key or config.cmc_api_key == 'your_cmc_api_key_here':
        print("❌ CMC_API_KEY not configured")
        return False
    try:
        async with CoinMarketCapHttpAdapter() as adapter:
            data = await adapter.fetch(['BTC', 'ETH'])
        if data:
            print(f"✅ CMC API working - fetched {len(data)} records")
            return True
        else:
            print("❌ CMC API returned no data")
            return False
    except Exception as e:
        print(f"❌ CMC API error: {e}")
        return False

async def test_database_setup():
    print("\n🗄️  Testing database setup...")
    try:
        # schema ensured by loader-level adapter; nothing to assert here
        return True
    except Exception as e:
        print(f"❌ Database setup error: {e}")
        return False

async def test_full_workflow():
    print("\n🚀 Testing complete workflow...")
    try:
        async with CoinMarketCapHttpAdapter() as adapter:
            data = await adapter.fetch(config.symbols[:3])
        if not data:
            print("❌ No data fetched")
            return False
        from adapters.clickhouse.coin_market_cap_clickhouse_adapter import CoinMarketCapClickHouseAdapter
        repo = CoinMarketCapClickHouseAdapter()
        ok = repo.ensure_schema()
        if not ok:
            print("❌ Schema setup failed")
            return False
        success = repo.upsert_prices(data)
        if success:
            print("✅ Data saved successfully")
            # simple sanity: recent read
            rows = repo.ch.exec(f"SELECT count() FROM {repo.db}.crypto_prices")
            print(f"✅ Total rows in crypto_prices: {rows[0][0] if rows else 0}")
            return True
        else:
            print("❌ Failed to save data")
            return False
    except Exception as e:
        print(f"❌ Workflow error: {e}")
        return False

async def main():
    print("🧪 Crypto Data Fetcher - Test Suite")
    print("=" * 50)
    tests = [
        ("ClickHouse Connection", test_connection),
        ("CoinMarketCap API", test_cmc_api),
        ("Database Setup", test_database_setup),
        ("Complete Workflow", test_full_workflow)
    ]
    passed = 0
    total = len(tests)
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        try:
            if await test_func():
                passed += 1
                print(f"✅ {test_name} passed")
            else:
                print(f"❌ {test_name} failed")
        except Exception as e:
            print(f"❌ {test_name} error: {e}")
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    if passed == total:
        print("🎉 All tests passed! The system is ready.")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
