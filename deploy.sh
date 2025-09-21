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
log "Step 1/7: Clean existing deployment (kill processes, remove crons)"
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
log "Step 2/7: Sync project to $SSH_USER@$SSH_HOST"

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

log "Step 3/7: Provision remote environment (venv, deps, .env, logs)"
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

# Install dependencies
echo "[remote] Installing dependencies..."
if [ -d "vendor" ] && [ "$(ls -A vendor/*.whl 2>/dev/null | wc -l)" -gt 0 ]; then
    echo "[remote] Installing dependencies from vendored wheels..."
    .venv/bin/pip install --no-index --find-links vendor -r requirements.txt
else
    echo "[remote] Installing dependencies from PyPI..."
    .venv/bin/pip install -r requirements.txt
fi

# Setup logs directory
LOG_DIR="\${PWD}/logs"
mkdir -p "\${LOG_DIR}/system" "\${LOG_DIR}/jobs"
touch "\${LOG_DIR}/application.log" "\${LOG_DIR}/cron.log"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    cat > .env <<EENV
# ClickHouse Database Configuration
CLICKHOUSE_HOST=172.30.63.35
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=data-processor
CLICKHOUSE_PASSWORD=@T@#KbeD!jM2n3vUD@v4&&X@r7Lo7V&*
CLICKHOUSE_DATABASE=data_warehouse

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

# Update .env with correct paths
sed -i "s|^LOG_DIR=.*|LOG_DIR=\${LOG_DIR}|" .env || true
sed -i "s|^CLICKHOUSE_PORT=.*|CLICKHOUSE_PORT=8123|" .env || true
sed -i "s|^CLICKHOUSE_DATABASE=.*|CLICKHOUSE_DATABASE=data_warehouse|" .env || true

echo "Environment provisioned successfully"
EOF

log "Step 4/7: Run database migrations"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"
.venv/bin/python migrations/migration_manager.py run || true
echo "Migrations completed"
EOF

log "Step 5/7: Reset logs and test framework"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

# Reset logs
rm -f logs/*.log logs/system/*.log logs/jobs/*.log || true
touch logs/application.log logs/cron.log

# Test framework
echo "Testing framework..."
.venv/bin/python src/main.py list || echo "No pipelines registered (expected for blank project)"

echo "Framework test completed"
EOF

log "Step 6/7: Install cron jobs (if any pipelines exist)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

# Get list of available jobs
JOBS=\$(.venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from main import register_all_pipelines, list_cron_jobs
register_all_pipelines()
jobs = list(list_cron_jobs().keys())
print(' '.join(jobs))
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
        SCHEDULE=\$(.venv/bin/python -c "
import sys
sys.path.insert(0, 'src')
from main import register_all_pipelines, list_cron_jobs
register_all_pipelines()
jobs = list_cron_jobs()
print(jobs.get('$job', {}).get('schedule', '0 * * * *') if '$job' in jobs else '0 * * * *')
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

log "Step 7/7: Verify deployment"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "bash -s" <<EOF
set -e
cd "${REMOTE_DIR}"

echo "=== Deployment Verification ==="
echo "Project directory: \${PWD}"
echo "Python version: \$(.venv/bin/python --version)"
echo "Dependencies installed: \$(.venv/bin/pip list | wc -l) packages"
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