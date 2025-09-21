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
    # Use system Python directly
    PY="python3"
    log "Using system Python: $PY"
    log "Dependencies check completed"
}

run_tests() {
    local category="${1:-all}"
    log "Running tests for category: $category"
    PY="python3"
    (cd "$PROJECT_DIR" && "$PY" tests/framework/run_tests.py "$category")
}

list_test_categories() {
    log "Listing available test categories..."
    PY="python3"
    (cd "$PROJECT_DIR" && "$PY" tests/framework/run_tests.py --list)
}

create_test_template() {
    if [ -z "$1" ] || [ -z "$2" ]; then
        error "Category and test name are required. Usage: ./run.sh create_test CATEGORY TEST_NAME"
        echo "Available categories:"
        (cd "$PROJECT_DIR" && python3 tests/test_runner.py --list)
        exit 1
    fi
    local category="$1"
    local test_name="$2"
    log "Creating test template for category: $category, name: $test_name"
    PY="python3"
    (cd "$PROJECT_DIR" && "$PY" tests/test_runner.py --create "$category" "$test_name")
}

cron_run() {
    if [ -z "$1" ]; then
        error "Job name is required. Use: ./run.sh list to see available jobs"
        exit 1
    fi
    JOB_NAME="$1"
    log "Running cron job: $JOB_NAME"
    PY="python3"
    (cd "$PROJECT_DIR" && PYTHONPATH="/home/iman/.local/lib/python3.12/site-packages:$PYTHONPATH" "$PY" scripts/run.py run "$JOB_NAME")
}

setup_database() {
    log "Setting up database..."
    (cd "$PROJECT_DIR" && "python3" - <<'PY'
import sys
sys.path.insert(0, 'src')
from core.config import config
import clickhouse_connect

def setup():
    ch_config = config.get_clickhouse_config()
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password']
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {ch_config['database']}")
    print('✅ Database setup completed')

setup()
PY
)
}

drop_database() {
    log "Dropping database..."
    (cd "$PROJECT_DIR" && "python3" - <<'PY'
import sys
sys.path.insert(0, 'src')
from core.config import config
import clickhouse_connect

def drop():
    ch_config = config.get_clickhouse_config()
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password']
    )
    # Drop entire database and recreate it
    client.command(f"DROP DATABASE IF EXISTS {ch_config['database']}")
    client.command(f"CREATE DATABASE IF NOT EXISTS {ch_config['database']}")
    print('✅ Database dropped and recreated')

drop()
PY
)
}

setup_cron() {
    if [ -z "$1" ]; then
        error "Job name is required. Use: ./run.sh list to see available jobs"
        exit 1
    fi
    JOB_NAME="$1"
    log "Setting up cron job: $JOB_NAME"
    mkdir -p "$PROJECT_DIR/logs"
    log "Cron job setup completed. Use: ./run.sh cron_run $JOB_NAME"
}

kill_processes() {
    log "Killing all running processes..."
    pkill -f "scripts/run.py" 2>/dev/null || true
    log "All processes killed"
}

migrate() {
    log "Running database migrations..."
    (cd "$PROJECT_DIR" && "python3" migrations/migration_manager.py run)
}

migrate_status() {
    log "Checking migration status..."
    (cd "$PROJECT_DIR" && "python3" migrations/migration_manager.py status)
}

backfill() {
    DAYS="${1:-30}"
    shift  # Remove days from arguments
    
    if [ $# -eq 0 ]; then
        # No specific jobs provided, run all jobs
        log "Running backfill for all jobs for $DAYS days..."
        (cd "$PROJECT_DIR" && python3 scripts/backfill.py backfill_all --days "$DAYS")
    else
        # Specific jobs provided
        log "Running backfill for jobs: $* for $DAYS days..."
        (cd "$PROJECT_DIR" && python3 scripts/backfill.py backfill --days "$DAYS" --jobs "$@")
    fi
}


backfill_list() {
    log "Listing available jobs for backfill..."
    (cd "$PROJECT_DIR" && python3 scripts/backfill.py list_jobs)
}

backfill_counts() {
    log "Showing data counts..."
    (cd "$PROJECT_DIR" && python3 scripts/backfill.py counts)
}


show_help() {
    echo "Data Processing Framework - Main Execution Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  check                   Check dependencies"
    echo "  test [CATEGORY]         Run tests (default: all, use 'list_tests' to see categories)"
    echo "  list_tests              List available test categories"
    echo "  create_test CAT NAME    Create test template for category and name"
    echo "  cron_run NAME          Run a pipeline by name"
    echo "  list                   List all available pipelines"
    echo "  setup_db               Setup database"
    echo "  drop_db                Drop and recreate database"
    echo "  migrate                Run database migrations"
    echo "  migrate_status         Show migration status"
    echo "  backfill [DAYS] [JOBS] Backfill historical data (default: 30 days, all jobs)"
    echo "  backfill_list          List available jobs for backfill"
    echo "  backfill_counts        Show data counts in database"
    echo "  setup_cron NAME        Setup cron for pipeline name"
    echo "  kill                   Kill all running processes"
    echo "  clean                  Clean and restart (drop_db + kill + migrate)"
    echo "  help                   Show this help"
    echo ""
    echo "Test Categories:"
    echo "  core                   Core framework tests (config, logging, validation, exceptions)"
    echo "  pipelines              Pipeline system tests (discovery, factory, registry, tools)"
    echo "  validation             Pydantic validation system tests"
    echo "  migrations             Database migration system tests"
    echo "  deployment             Deployment and operations tests"
    echo "  integration            End-to-end integration tests"
    echo "  performance            Performance and load tests"
    echo "  all                    Run all available tests"
    echo ""
    echo "Note: Add your pipeline modules to src/pipelines/ directory"
    echo "      Pipeline modules should follow the pattern: *_pipeline.py"
    echo ""
}

case "${1:-help}" in
    check)
        check_dependencies
        ;;
    test)
        check_dependencies
        run_tests "$2"
        ;;
    list_tests)
        list_test_categories
        ;;
    create_test)
        check_dependencies
        create_test_template "$2" "$3"
        ;;
    cron_run)
        check_dependencies
        cron_run "$2"
        ;;
    list)
        check_dependencies
        (cd "$PROJECT_DIR" && PYTHONPATH="/home/iman/.local/lib/python3.12/site-packages:$PYTHONPATH" python3 scripts/run.py list)
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
        setup_cron "$2"
        ;;
    migrate)
        check_dependencies
        migrate
        ;;
    migrate_status)
        check_dependencies
        migrate_status
        ;;
    backfill)
        check_dependencies
        shift  # Remove 'backfill' from arguments
        backfill "$@"
        ;;
    backfill_list)
        check_dependencies
        backfill_list
        ;;
    backfill_counts)
        check_dependencies
        backfill_counts
        ;;
    kill)
        kill_processes
        ;;
    clean)
        kill_processes
        drop_database
        migrate
        ;;
    help|*)
        show_help
        ;;
esac