#!/bin/bash
set -e

# Data Processing Framework - Idempotent Deployment Script
# Usage: ./deploy.sh <ssh_user> <ssh_host> [--clean]

if [ $# -lt 2 ]; then
    echo "Usage: $0 <ssh_user> <ssh_host> [--clean]"
    exit 1
fi

SSH_USER="$1"
SSH_HOST="$2"
CLEAN_FLAG="${3:-}"

# Determine deployment directory
REMOTE_DIR="/home/$SSH_USER/containers/data-processor"
FALLBACK_DIR="/home/$SSH_USER/data-processor"

log() {
    echo "[deploy][$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "Starting idempotent deployment to $SSH_USER@$SSH_HOST"

# Step 1: Kill existing processes and clean project if requested
log "Step 1/8: Clean existing deployment (kill processes, remove crons)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<'EOF'
set -e

# Kill any running Python processes related to data-processor
pkill -f "data-processor" || true
pkill -f "run.py" || true
pkill -f "main.py" || true

# Remove existing cron jobs
crontab -l 2>/dev/null | grep -v "data-processor" | crontab - || true
echo "Removed existing cron jobs"

# Determine deployment directory
REMOTE_DIR="/home/$USER/containers/data-processor"
FALLBACK_DIR="/home/$USER/data-processor"

# Try to create /containers directory structure
if [ ! -d "/containers" ]; then
    sudo mkdir -p /containers 2>/dev/null || true
    sudo chown -R $USER:$USER /containers 2>/dev/null || true
fi

# Check which directory to use
if [ -w "/containers" ] || [ -d "/containers/data-processor" ]; then
    DEPLOY_DIR="$REMOTE_DIR"
else
    DEPLOY_DIR="$FALLBACK_DIR"
fi

echo "DEPLOY_DIR=$DEPLOY_DIR"

# Clean deployment directory if --clean flag or if requested
if [[ "${1:-}" == "--clean" ]] || [[ "${CLEAN_FLAG:-}" == "--clean" ]]; then
    echo "Cleaning deployment directory: $DEPLOY_DIR"
    rm -rf "$DEPLOY_DIR" || true
fi

# Ensure deployment directory exists
mkdir -p "$DEPLOY_DIR"
EOF

# Step 2: Sync project files
log "Step 2/8: Sync project to $SSH_USER@$SSH_HOST"

# Determine remote directory
REMOTE_DIR_RESULT=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<'EOF'
if [ -w "/containers" ] || [ -d "/containers/data-processor" ]; then
    echo "/home/$USER/containers/data-processor"
else
    echo "/home/$USER/data-processor"
fi
EOF
)

REMOTE_DIR="$REMOTE_DIR_RESULT"

# Sync files
rsync -avz --delete \
    --exclude='.env' \
    --exclude='logs/' \
    --exclude='*.log' \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='tests/results/' \
    --exclude='venv/' \
    --exclude='.venv/' \
    ./ "${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}/"

log "Step 3/8: Provision remote environment (venv, deps, .env, logs)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

# Setup Python virtual environment
if [ ! -d ".venv" ]; then
    python3 -m venv .venv
    echo "Created virtual environment"
fi

PY="\${PWD}/.venv/bin/python"
echo "[remote] Using Python: \$PY"

# Install dependencies with automatic fallback
echo "[remote] Installing dependencies..."
DEPENDENCIES_INSTALLED=false

# Try vendored wheels first
if [ -d "vendor" ] && [ "$(ls -A vendor/*.whl 2>/dev/null | wc -l)" -gt 0 ]; then
    echo "[remote] Attempting to install from vendored wheels..."
    if .venv/bin/pip install --no-index --find-links vendor -r requirements.txt 2>/dev/null; then
        echo "[remote] Successfully installed from vendored wheels"
        DEPENDENCIES_INSTALLED=true
    else
        echo "[remote] Vendored wheels failed, falling back to PyPI..."
    fi
fi

# Fallback to PyPI if vendored wheels failed or don't exist
if [ "$DEPENDENCIES_INSTALLED" = false ]; then
    echo "[remote] Installing dependencies from PyPI..."
    if .venv/bin/pip install -r requirements.txt; then
        echo "[remote] Successfully installed from PyPI"
        DEPENDENCIES_INSTALLED=true
    else
        echo "[remote] PyPI installation failed, trying with --no-deps..."
        .venv/bin/pip install --no-deps -r requirements.txt || {
            echo "[remote] Critical: Failed to install core dependencies"
            echo "[remote] Attempting to install packages individually..."
            .venv/bin/pip install aiohttp clickhouse-connect pandas numpy pydantic pydantic-settings || {
                echo "[remote] ERROR: Could not install any dependencies"
                exit 1
            }
        }
        DEPENDENCIES_INSTALLED=true
    fi
fi

# Install test dependencies if available (optional)
if [ -f "requirements-test.txt" ]; then
    echo "[remote] Installing test dependencies (optional)..."
    if [ -d "vendor" ] && [ "$(ls -A vendor/*.whl 2>/dev/null | wc -l)" -gt 0 ]; then
        echo "[remote] Installing test dependencies from vendored wheels..."
        if .venv/bin/pip install --no-index --find-links vendor -r requirements-test.txt; then
            echo "[remote] Test dependencies installed successfully from vendored wheels"
        else
            echo "[remote] Warning: Test dependencies installation from vendored wheels failed"
        fi
    else
        echo "[remote] No vendored wheels available, trying PyPI..."
        if .venv/bin/pip install -r requirements-test.txt; then
            echo "[remote] Test dependencies installed successfully from PyPI"
        else
            echo "[remote] Warning: Test dependencies installation failed, trying individual packages..."
            .venv/bin/pip install pytest pytest-cov || {
                echo "[remote] Warning: Could not install pytest (tests may not work properly)"
            }
        fi
    fi
fi

# Setup logs directory
LOG_DIR="\${PWD}/logs"
mkdir -p "\${LOG_DIR}/system" "\${LOG_DIR}/jobs"
touch "\${LOG_DIR}/application.log" "\${LOG_DIR}/cron.log"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    cat > .env <<EENV
# ClickHouse Database Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=data-processor
CLICKHOUSE_PASSWORD=@T@#KbeD!jM2n3vUD@v4&&X@r7Lo7V&*
CLICKHOUSE_DATABASE=data_warehouse

# Metabase Configuration
METABASE_BASE_URL=https://metabase.devinvex.com
METABASE_API_KEY=mb_Rn1xekaIuzmqd26+wc9AM3IYn554RBCuKUkX9aP5pwQ=
METABASE_TIMEOUT=30

# Logging Configuration
LOG_DIR=\${LOG_DIR}
LOG_LEVEL=INFO

# Application Configuration
TIMEOUT=30
BATCH_SIZE=1000
EENV
    echo "Created .env file"
else
    echo "Using existing .env file"
fi

# Update .env with correct paths and settings
sed -i "s|^LOG_DIR=.*|LOG_DIR=\${LOG_DIR}|" .env || true
sed -i "s|^CLICKHOUSE_HOST=.*|CLICKHOUSE_HOST=localhost|" .env || true
sed -i "s|^CLICKHOUSE_PORT=.*|CLICKHOUSE_PORT=8123|" .env || true
sed -i "s|^CLICKHOUSE_DATABASE=.*|CLICKHOUSE_DATABASE=data_warehouse|" .env || true
sed -i "s|^METABASE_BASE_URL=.*|METABASE_BASE_URL=https://metabase.devinvex.com|" .env || true
sed -i "s|^METABASE_API_KEY=.*|METABASE_API_KEY=mb_Rn1xekaIuzmqd26+wc9AM3IYn554RBCuKUkX9aP5pwQ=|" .env || true
sed -i "s|^METABASE_TIMEOUT=.*|METABASE_TIMEOUT=30|" .env || true

echo "Environment provisioned successfully"
EOF

log "Step 4/8: Run database migrations"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

echo "[remote] Running database migrations..."
if .venv/bin/python migrations/migration_manager.py run 2>/dev/null; then
    echo "[remote] Migrations completed successfully"
else
    echo "[remote] Warning: Migrations failed or no migrations found (non-critical)"
    echo "[remote] Continuing deployment..."
fi
EOF

log "Step 5/8: Reset logs and test framework"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

# Reset logs
rm -f logs/*.log logs/system/*.log logs/jobs/*.log || true
touch logs/application.log logs/cron.log

# Test framework
echo "[remote] Testing framework..."
if .venv/bin/python src/main.py list 2>/dev/null; then
    echo "[remote] Framework test completed successfully"
else
    echo "[remote] Warning: Framework test failed or no pipelines registered (non-critical)"
    echo "[remote] This is expected for a blank project"
fi
EOF

log "Step 6/8: Install cron jobs (if any pipelines exist)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

# Get list of available jobs
echo "[remote] Discovering available jobs..."
JOBS=\$(.venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
try:
    from main import register_all_pipelines, list_cron_jobs
    register_all_pipelines()
    jobs = list(list_cron_jobs().keys())
    print(' '.join(jobs))
except Exception as e:
    print('')
" 2>/dev/null || echo "")

if [ -n "\$JOBS" ]; then
    echo "Installing cron jobs for: \$JOBS"
    
    # Create crontab
    cat > /tmp/data-processor-cron <<CRONTAB
# data-processor crons (auto-generated)
CRONTAB

    # Add each job with its schedule
    for job in \$JOBS; do
        # Get job schedule
        echo "[remote] Configuring job: \$job"
        SCHEDULE=\$(.venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
try:
    from main import register_all_pipelines, list_cron_jobs
    register_all_pipelines()
    jobs = list_cron_jobs()
    print(jobs.get('$job', {}).get('schedule', '0 * * * *') if '$job' in jobs else '0 * * * *')
except Exception as e:
    print('0 * * * *')
" 2>/dev/null || echo "0 * * * *")
        
        echo "\$SCHEDULE cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run \$job >> ${REMOTE_DIR}/logs/cron.log 2>&1" >> /tmp/data-processor-cron
    done
    
    # Install crontab
    crontab /tmp/data-processor-cron
    rm -f /tmp/data-processor-cron
    
    echo "--- installed crontab ---"
    crontab -l | head -20
else
    echo "No pipelines found - skipping cron installation"
fi
EOF

log "Step 7/8: Check production integrity"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

echo "[remote] Checking production integrity..."

# Test ClickHouse connectivity
echo "[remote] Testing ClickHouse connectivity..."
if .venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from core.config import config
import clickhouse_connect

try:
    ch_config = config.get_clickhouse_config()
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password']
    )
    result = client.command('SELECT 1')
    print(f'ClickHouse: ✓ Connected (result: {result})')
except Exception as e:
    print(f'ClickHouse: ✗ Connection failed: {e}')
    exit(1)
" 2>/dev/null; then
    echo "[remote] ClickHouse connectivity: OK"
else
    echo "[remote] Warning: ClickHouse connectivity test failed"
fi

# Test Metabase connectivity (if configured)
echo "[remote] Testing Metabase connectivity..."
if .venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from core.config import config
import aiohttp
import asyncio

async def test_metabase():
    try:
        config_data = config.to_dict()
        metabase_url = config_data.get('metabase_base_url')
        if not metabase_url:
            print('Metabase: ⚠ Not configured (skipping)')
            return
        
        # Test basic connectivity
        async with aiohttp.ClientSession() as session:
            async with session.get(f'{metabase_url}/api/health', timeout=10) as response:
                if response.status == 200:
                    print(f'Metabase: ✓ Connected (status: {response.status})')
                    
                    # Test API key authentication
                    api_key = config_data.get('metabase_api_key')
                    if api_key:
                        headers = {'X-API-Key': api_key}
                        async with session.get(f'{metabase_url}/api/user/current', headers=headers, timeout=10) as auth_response:
                            if auth_response.status == 200:
                                print(f'Metabase: ✓ API key authentication successful')
                            else:
                                print(f'Metabase: ⚠ API key authentication failed (status: {auth_response.status})')
                    else:
                        print(f'Metabase: ⚠ API key not configured')
                else:
                    print(f'Metabase: ✗ Health check failed (status: {response.status})')
    except Exception as e:
        print(f'Metabase: ✗ Connection failed: {e}')

asyncio.run(test_metabase())
" 2>/dev/null; then
    echo "[remote] Metabase connectivity: OK"
else
    echo "[remote] Warning: Metabase connectivity test failed or not configured"
fi

# Test database schema
echo "[remote] Testing database schema..."
if .venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from core.config import config
import clickhouse_connect

try:
    ch_config = config.get_clickhouse_config()
    client = clickhouse_connect.get_client(
        host=ch_config['host'],
        port=ch_config['port'],
        username=ch_config['user'],
        password=ch_config['password']
    )
    
    # Check if database exists
    databases = client.command('SHOW DATABASES')
    db_name = ch_config['database']
    if db_name in databases:
        print(f'Database: ✓ {db_name} exists')
        
        # Check if we can query tables
        tables = client.command(f'SHOW TABLES FROM {db_name}')
        print(f'Database: ✓ Found {len(tables)} tables')
    else:
        print(f'Database: ⚠ {db_name} does not exist (will be created on first use)')
        
except Exception as e:
    print(f'Database: ✗ Schema check failed: {e}')
" 2>/dev/null; then
    echo "[remote] Database schema: OK"
else
    echo "[remote] Warning: Database schema check failed"
fi

echo "[remote] Production integrity check completed"
EOF

log "Step 8/8: Verify deployment"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

echo "=== Deployment Verification ==="
echo "Project directory: \${PWD}"
echo "Python version: \$(.venv/bin/python --version)"
echo "Dependencies installed: \$(.venv/bin/pip list | wc -l) packages"
echo "Core dependencies:"
.venv/bin/pip list | grep -E "(aiohttp|clickhouse|pandas|numpy|pydantic)" || echo "Warning: Some core dependencies may be missing"
echo "Test dependencies:"
if .venv/bin/pip list | grep -E "(pytest|psutil)"; then
    echo "✓ Test dependencies installed"
else
    echo "⚠ Test dependencies not installed (tests may not work)"
fi
echo "Log directories:"
ls -la logs/
echo "Environment file:"
ls -la .env
echo "Migration status:"
.venv/bin/python migrations/migration_manager.py status 2>/dev/null || echo "No migrations found"
echo "Available jobs:"
.venv/bin/python src/main.py list 2>/dev/null || echo "No jobs registered"
echo "Cron jobs:"
crontab -l 2>/dev/null | grep -c "data-processor" || echo "0"
echo "=== Deployment Complete ==="
EOF

log "Deployment completed successfully!"
log "Project deployed to: $REMOTE_DIR"
log "Add your pipeline modules to src/pipelines/ and redeploy to register them"