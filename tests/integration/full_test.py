import asyncio
import sys
from datetime import datetime
from crypto_price_fetcher.config import config
from crypto_price_fetcher.storage_native import storage
from crypto_price_fetcher.fetcher import fetch_crypto_prices

async def test_clickhouse_connection():
    print("\n🧪 Testing ClickHouse connection...")
    try:
        success = await storage.test_connection()
        if success:
            print("✅ ClickHouse connection successful")
            return True
        else:
            print("❌ ClickHouse connection failed")
            return False
    except Exception as e:
        print(f"❌ ClickHouse connection error: {e}")
        return False

async def test_full_fetch():
    print("\n🧪 Testing full data fetch...")
    try:
        crypto_data = await fetch_crypto_prices(config.symbols)
        if crypto_data and len(crypto_data) > 0:
            print(f"✅ Fetched {len(crypto_data)} records")
            return True
        else:
            print("❌ No data fetched")
            return False
    except Exception as e:
        print(f"❌ Fetch error: {e}")
        return False

async def test_storage():
    print("\n🧪 Testing data storage...")
    try:
        crypto_data = await fetch_crypto_prices(config.symbols)
        if crypto_data:
            success = await storage.save_crypto_data(crypto_data)
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