import os
import sys

def test_config():
    print("🔧 Testing Configuration")
    print("-" * 30)
    try:
        from crypto_price_fetcher.config import config
        print(f"ClickHouse Host: {config.clickhouse_host}")
        print(f"ClickHouse Port: {config.clickhouse_port}")
        print(f"ClickHouse User: {config.clickhouse_user}")
        print(f"ClickHouse DB: {config.clickhouse_db}")
        print(f"Symbols: {config.symbols}")
        print(f"CMC API Key: {'Set' if config.cmc_api_key else 'Not set'}")
        print("✅ Configuration loaded successfully")
        return True
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return False

def test_file_structure():
    print("\n📁 Testing File Structure")
    print("-" * 30)
    required_files = [
        'crypto_price_fetcher/__init__.py',
        'crypto_price_fetcher/config.py',
        'crypto_price_fetcher/fetcher.py',
        'crypto_price_fetcher/storage_native.py',
        'crypto_price_fetcher/main.py',
        'requirements.txt',
        '.env',
        'scripts/setup/setup_clickhouse.sql',
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
        print(f"\n❌ Missing files: {missing_files}")
        return False
    else:
        print("\n✅ All required files present")
        return True

def test_sql_syntax():
    print("\n🗄️  Testing SQL Syntax")
    print("-" * 30)
    try:
        with open('scripts/setup/setup_clickhouse.sql', 'r') as f:
            sql_content = f.read()
        required_keywords = [
            'CREATE DATABASE',
            'CREATE TABLE',
            'ENGINE = MergeTree',
            'ORDER BY',
            'CREATE INDEX',
            'CREATE MATERIALIZED VIEW'
        ]
        missing_keywords = []
        for keyword in required_keywords:
            if keyword in sql_content:
                print(f"✅ {keyword}")
            else:
                print(f"❌ {keyword}")
                missing_keywords.append(keyword)
        if missing_keywords:
            print(f"\n❌ Missing SQL keywords: {missing_keywords}")
            return False
        else:
            print("\n✅ SQL syntax looks good")
            return True
    except Exception as e:
        print(f"❌ SQL test error: {e}")
        return False

async def test_clickhouse_connection():
    print("\n🔌 Testing ClickHouse Connection")
    print("-" * 30)
    try:
        from crypto_price_fetcher.storage_native import storage
        if await storage.test_connection():
            print("✅ ClickHouse connection successful")
            return True
        else:
            print("❌ ClickHouse connection failed")
            return False
    except Exception as e:
        print(f"❌ ClickHouse test error: {e}")
        return False

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
            if test_name == "ClickHouse Connection":
                if await test_func():
                    passed += 1
            else:
                if test_func():
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
