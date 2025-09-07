#!/bin/bash
set -e
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}
error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}
warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

check_dependencies() {
    log "Checking dependencies..."
    if ! command -v python3 &> /dev/null; then
        error "Python3 is not installed"
        exit 1
    fi
    python3 -c "import aiohttp, clickhouse_driver, pytz" 2>/dev/null || {
        error "Missing required packages. Installing..."
        sudo apt update
        sudo apt install -y python3-aiohttp python3-clickhouse-driver python3-tz
    }
    log "Dependencies check completed"
}

run_tests() {
    log "Running tests..."
    log "Running unit tests..."
    python3 tests/unit/simple_test.py
    log "Running integration tests..."
    python3 tests/integration/test.py
    python3 tests/integration/full_test.py
    log "All tests completed successfully!"
}

run_once() {
    log "Running crypto price fetcher once..."
    python3 cron_job.py
}

setup_database() {
    log "Setting up database..."
    python3 -c "
import asyncio
from crypto_price_fetcher.storage_native import storage

async def setup():
    await storage.create_database()
    await storage.create_table()
    print('✅ Database setup completed')

asyncio.run(setup())
"
}

drop_database() {
    log "Dropping database..."
    python3 -c "
import asyncio
from crypto_price_fetcher.storage_native import storage

async def drop():
    client = storage._get_client()
    client.execute('DROP TABLE IF EXISTS crypto.crypto_prices')
    print('✅ Table dropped')

asyncio.run(drop())
"
}

setup_cron() {
    log "Setting up cron job..."
    CRON_SCRIPT="$PROJECT_DIR/cron_job.py"
    (crontab -l 2>/dev/null | grep -v "$CRON_SCRIPT"; echo "0 * * * * cd $PROJECT_DIR && python3 $CRON_SCRIPT >> $PROJECT_DIR/cron.log 2>&1") | crontab -
    log "Cron job setup completed. Check with: crontab -l"
}

kill_processes() {
    log "Killing all running processes..."
    pkill -f "cron_job.py" 2>/dev/null || true
    pkill -f "crypto_price_fetcher" 2>/dev/null || true
    log "All processes killed"
}

show_help() {
    echo "Crypto Price Fetcher - Main Execution Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  check          Check dependencies"
    echo "  test           Run all tests"
    echo "  run_once       Run the fetcher once"
    echo "  setup_db       Setup database"
    echo "  drop_db        Drop database"
    echo "  setup_cron     Setup cron job"
    echo "  kill           Kill all running processes"
    echo "  clean          Clean and restart (drop_db + kill + run_once)"
    echo "  help           Show this help"
    echo ""
}

case "${1:-help}" in
    check)
        check_dependencies
        ;;
    test)
        check_dependencies
        run_tests
        ;;
    run_once)
        check_dependencies
        run_once
        ;;
    setup_db)
        check_dependencies
        setup_database
        ;;
    drop_db)
        drop_database
        ;;
    setup_cron)
        check_dependencies
        setup_cron
        ;;
    kill)
        kill_processes
        ;;
    clean)
        kill_processes
        drop_database
        setup_database
        run_once
        ;;
    help|*)
        show_help
        ;;
esac