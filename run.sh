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
    if [ ! -x "$PROJECT_DIR/.venv/bin/python" ]; then
        log "Creating virtualenv..."
        python3 -m venv "$PROJECT_DIR/.venv"
    fi
    PY="$PROJECT_DIR/.venv/bin/python"
    "$PY" -m pip install -U pip >/dev/null 2>&1 || true
    # Ensure runtime deps
    "$PY" - <<'PY'
import sys
def ensure(pkg):
    try:
        __import__(pkg)
    except Exception:
        import subprocess
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', pkg])
for p in ['aiohttp','clickhouse-driver','pytz']:
    ensure(p)
PY
    log "Dependencies check completed"
}

run_tests() {
    log "Running tests..."
    PY="$PROJECT_DIR/.venv/bin/python"
    "$PY" -m pip install -U pytest pytest-asyncio >/dev/null 2>&1 || true
    log "Running pytest suite..."
    "$PY" -m pytest -q || { error "Tests failed"; exit 1; }
    log "All tests completed successfully!"
}

cron_run() {
    JOB_NAME="${1:-cmc_hourly_prices}"
    log "Running cron job: $JOB_NAME"
    PY="$PROJECT_DIR/.venv/bin/python"
    (cd "$PROJECT_DIR/src" && "$PY" -m crons.run "$JOB_NAME")
}

setup_database() {
    log "Setting up database..."
    "$PROJECT_DIR/.venv/bin/python" - <<'PY'
import asyncio
from adapters._bases.clickhouse_adapter import ClickHouseAdapter

async def setup():
    ch = ClickHouseAdapter()
    client = ch.client()
    client.execute('CREATE DATABASE IF NOT EXISTS crypto')
    print('✅ Database setup completed')

asyncio.run(setup())
PY
}

drop_database() {
    log "Dropping database..."
    "$PROJECT_DIR/.venv/bin/python" - <<'PY'
import asyncio
from adapters._bases.clickhouse_adapter import ClickHouseAdapter

async def drop():
    ch = ClickHouseAdapter()
    client = ch.client()
    client.execute('DROP TABLE IF EXISTS crypto.crypto_prices')
    print('✅ Table dropped')

asyncio.run(drop())
PY
}

setup_cron() {
    JOB_NAME="${1:-cmc_hourly_prices}"
    log "Setting up cron job: $JOB_NAME"
    mkdir -p "$PROJECT_DIR/logs"
    (cd "$PROJECT_DIR/src" && "$PROJECT_DIR/.venv/bin/python" -m crons.install "$JOB_NAME")
    log "Cron job setup completed via crons.install. Check with: crontab -l"
}

kill_processes() {
    log "Killing all running processes..."
    pkill -f "crons.run" 2>/dev/null || true
    log "All processes killed"
}

show_help() {
    echo "Crypto Price Fetcher - Main Execution Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  check          Check dependencies"
    echo "  test           Run all tests (via venv)"
    echo "  cron_run NAME  Run a cron job by name (default: cmc_hourly_prices)"
    echo "  cron_backfill NAME HOURS  Backfill a job for past HOURS"
    echo "  setup_db       Setup database"
    echo "  drop_db        Drop database"
    echo "  setup_cron [NAME]  Install cron for job name (default: cmc_hourly_prices)"
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
    cron_run)
        check_dependencies
        cron_run "${2:-cmc_hourly_prices}"
        ;;
    cron_backfill)
        check_dependencies
        JOB_NAME="${2:-cmc_hourly_prices}"
        HOURS="${3:-0}"
        log "Backfilling $JOB_NAME for $HOURS hours"
        (cd "$PROJECT_DIR/src" && "$PROJECT_DIR/.venv/bin/python" -m crons.run "$JOB_NAME" backfill "$HOURS")
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
        setup_cron "${2:-cmc_hourly_prices}"
        ;;
    kill)
        kill_processes
        ;;
    clean)
        kill_processes
        drop_database
        setup_database
        cron_run "cmc_hourly_prices"
        ;;
    help|*)
        show_help
        ;;
esac