import os
import sys
import pytest

def test_config():
    print("🔧 Testing Configuration")
    print("-" * 30)
    SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
    if SRC_DIR not in sys.path:
        sys.path.insert(0, SRC_DIR)
    try:
        from configurations.config import config
    except Exception as e:
        pytest.fail(f"Configuration import failed: {e}")
    print(f"ClickHouse Host: {config.clickhouse_host}")
    print(f"ClickHouse Port: {config.clickhouse_port}")
    print(f"ClickHouse User: {config.clickhouse_user}")
    print(f"ClickHouse DB: {config.clickhouse_db}")
    print(f"Symbols: {config.symbols}")
    print(f"CMC API Key: {'Set' if config.cmc_api_key else 'Not set'}")
    print("✅ Configuration loaded successfully")

def test_file_structure():
    print("\n📁 Testing File Structure")
    print("-" * 30)
    required_files = [
        'src/main.py',
        'src/configurations/config.py',
        'src/adapters/_bases/clickhouse_adapter.py',
        'src/adapters/http/coin_market_cap_http_adapter.py',
        'src/adapters/clickhouse/coin_market_cap_clickhouse_adapter.py',
        'README.md'
    ]
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path}")
            missing_files.append(file_path)
    if missing_files:
        pytest.fail(f"Missing files: {missing_files}")
    print("\n✅ All required files present")

def test_sql_syntax():
    print("\n🗄️  Testing SQL Syntax")
    print("-" * 30)
    print("Skipping SQL file check; schema is created programmatically.")

def test_clickhouse_connection():
    print("\n🔌 Testing ClickHouse Connection")
    print("-" * 30)
    SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'src')
    if SRC_DIR not in sys.path:
        sys.path.insert(0, SRC_DIR)
    try:
        from adapters._bases.clickhouse_adapter import ClickHouseAdapter
        ch = ClickHouseAdapter()
        ok = ch.ping()
        print("✅ ClickHouse connection successful" if ok else "❌ ClickHouse connection failed")
        assert ok
    except Exception as e:
        pytest.fail(f"ClickHouse test error: {e}")

async def main():
    print("🧪 Simple Crypto Data Fetcher Test")
    print("=" * 50)
    tests = [
        ("Configuration", test_config),
        ("File Structure", test_file_structure),
        ("SQL Syntax", test_sql_syntax),
        ("ClickHouse Connection", test_clickhouse_connection)
    ]
    passed = 0
    total = len(tests)
    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    if passed == total:
        print("🎉 All tests passed! The project is ready.")
        print("\nNext steps:")
        print("1. Install dependencies")
        print("2. Set your CMC API key in .env file")
        print("3. Run: ./run.sh run_once")
        return 0
    else:
        print("⚠️  Some tests failed. Please check the issues above.")
        return 1

if __name__ == "__main__":
    import asyncio
    sys.exit(asyncio.run(main()))
