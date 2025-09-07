import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from crypto_price_fetcher.config import config
from crypto_price_fetcher.storage_native import storage
from crypto_price_fetcher.fetcher import fetch_crypto_prices

async def test_connection():
    print("🔌 Testing ClickHouse connection...")
    if await storage.test_connection():
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
        data = await fetch_crypto_prices(['BTC', 'ETH'])
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
        await storage.create_database()
        print("✅ Database creation successful")
        await storage.create_table()
        print("✅ Table creation successful")
        return True
    except Exception as e:
        print(f"❌ Database setup error: {e}")
        return False

async def test_full_workflow():
    print("\n🚀 Testing complete workflow...")
    try:
        data = await fetch_crypto_prices(config.symbols[:3])
        if not data:
            print("❌ No data fetched")
            return False
        success = await storage.save_crypto_data(data)
        if success:
            print("✅ Data saved successfully")
            recent_data = await storage.get_recent_data(5)
            print(f"✅ Retrieved {len(recent_data)} recent records")
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
