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
    JOB_NAME="${1:-cmc_latest_quotes}"
    log "Running cron job: $JOB_NAME"
    PY="$PROJECT_DIR/.venv/bin/python"
    (cd "$PROJECT_DIR" && "$PY" scripts/run.py run "$JOB_NAME")
}

setup_database() {
    log "Setting up database..."
    (cd "$PROJECT_DIR" && "$PROJECT_DIR/.venv/bin/python" - <<'PY'
import sys
sys.path.insert(0, 'src')
from core.config import config
from clickhouse_driver import Client

def setup():
    ch_config = config.get_clickhouse_config()
    client = Client(
        host=ch_config['host'],
        port=ch_config['port'],
        user=ch_config['user'],
        password=ch_config['password']
    )
    client.execute(f"CREATE DATABASE IF NOT EXISTS {ch_config['database']}")
    print('✅ Database setup completed')

setup()
PY
)
}

drop_database() {
    log "Dropping database..."
    (cd "$PROJECT_DIR" && "$PROJECT_DIR/.venv/bin/python" - <<'PY'
import sys
sys.path.insert(0, 'src')
from core.config import config
from clickhouse_driver import Client

def drop():
    ch_config = config.get_clickhouse_config()
    client = Client(
        host=ch_config['host'],
        port=ch_config['port'],
        user=ch_config['user'],
        password=ch_config['password']
    )
    client.execute(f"DROP TABLE IF EXISTS {ch_config['database']}.cmc_latest_quotes")
    client.execute(f"DROP TABLE IF EXISTS {ch_config['database']}.cmc_historical_data")
    client.execute(f"DROP TABLE IF EXISTS {ch_config['database']}.weather_data")
    print('✅ Tables dropped')

drop()
PY
)
}

setup_cron() {
    JOB_NAME="${1:-cmc_latest_quotes}"
    log "Setting up cron job: $JOB_NAME"
    mkdir -p "$PROJECT_DIR/logs"
    log "Cron job setup completed. Use: ./run.sh cron_run $JOB_NAME"
}

kill_processes() {
    log "Killing all running processes..."
    pkill -f "scripts/run.py" 2>/dev/null || true
    log "All processes killed"
}

show_help() {
    echo "Data Processing Framework - Main Execution Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  check          Check dependencies"
    echo "  test           Run all tests (via venv)"
    echo "  cron_run NAME  Run a pipeline by name (default: cmc_latest_quotes)"
    echo "  list           List all available pipelines"
    echo "  setup_db       Setup database"
    echo "  drop_db        Drop database"
    echo "  setup_cron [NAME]  Setup cron for pipeline name (default: cmc_latest_quotes)"
    echo "  kill           Kill all running processes"
    echo "  clean          Clean and restart (drop_db + kill + run_once)"
    echo "  help           Show this help"
    echo ""
    echo "Available pipelines:"
    echo "  - cmc_latest_quotes    CMC latest cryptocurrency quotes"
    echo "  - cmc_historical_data  CMC historical cryptocurrency data"
    echo "  - weather_data         Weather data for Tehran"
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
        cron_run "${2:-cmc_latest_quotes}"
        ;;
    list)
        check_dependencies
        (cd "$PROJECT_DIR" && "$PROJECT_DIR/.venv/bin/python" scripts/run.py list)
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
        setup_cron "${2:-cmc_latest_quotes}"
        ;;
    kill)
        kill_processes
        ;;
    clean)
        kill_processes
        drop_database
        setup_database
        cron_run "cmc_latest_quotes"
        ;;
    help|*)
        show_help
        ;;
esac