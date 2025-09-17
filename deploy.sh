#!/usr/bin/env bash
set -euo pipefail

# Usage: ./deploy.sh <ssh_user> <ssh_host> [--clean]
# Example: ./deploy.sh e.soltani 172.30.63.35 --clean

if [ $# -lt 2 ]; then
  echo "Usage: $0 <ssh_user> <ssh_host> [--clean]" >&2
  exit 1
fi

SSH_USER="$1"
SSH_HOST="$2"
CLEAN_FLAG="${3:-}"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_DIR="/home/${SSH_USER}/data-processor"

log() { echo -e "[deploy][$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

log "Step 1/6: Sync project to ${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}"
rsync -az --delete \
  --exclude ".git" \
  --exclude ".venv" \
  --exclude "__pycache__/" \
  --exclude ".pytest_cache/" \
  --exclude "logs/*" \
  "${PROJECT_DIR}/" "${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}/"

log "Step 2/6: Provision remote environment (venv, deps, .env, logs, optional clean)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" bash -lc "'
set -e
DEPLOY_DIR="${REMOTE_DIR}"
CLEAN_FLAG="${CLEAN_FLAG}"

if [ "${CLEAN_FLAG}" = "--clean" ]; then
  echo "[remote] Cleaning old crons and directory: ${DEPLOY_DIR}"
  crontab -l 2>/dev/null | grep -v "${DEPLOY_DIR}" | crontab - 2>/dev/null || true
  rm -rf "${DEPLOY_DIR}"
  mkdir -p "${DEPLOY_DIR}"
fi

mkdir -p "${DEPLOY_DIR}/logs"
cd "${DEPLOY_DIR}"

if [ ! -x "${DEPLOY_DIR}/.venv/bin/python" ]; then
  python3 -m venv "${DEPLOY_DIR}/.venv"
fi
PY="${DEPLOY_DIR}/.venv/bin/python"
$PY -m pip install -U pip >/dev/null 2>&1 || true
$PY -m pip install aiohttp clickhouse-driver pytz pandas >/dev/null 2>&1 || true

# Prepare .env
[ -f .env ] || cp env.example .env
sed -i "s|^CLICKHOUSE_DATABASE=.*|CLICKHOUSE_DATABASE=data_warehouse|" .env

# Ensure migrations directory exists
mkdir -p migrations/sql
'"

log "Step 3/6: Run migrations"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" bash -lc "'
set -e
cd "${REMOTE_DIR}"
.venv/bin/python migrations/migration_manager.py run || true
'"

log "Step 4/6: Run all jobs once (seed)"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" bash -lc "'
set -e
cd "${REMOTE_DIR}"
JOBS=(cmc_latest_quotes cmc_hourly cmc_daily cmc_weekly cmc_monthly cmc_yearly)
for j in "${JOBS[@]}"; do
  echo "[remote] Running job: $j"
  .venv/bin/python scripts/run.py run "$j" || true
done
'"

log "Step 5/6: Install crons"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" bash -lc "'
set -e
cd "${REMOTE_DIR}"
mkdir -p logs
cat > /tmp/data-processor-cron <<CRONTAB
# data-processor crons
*/5 * * * * cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_latest_quotes >> ${REMOTE_DIR}/logs/cron.log 2>&1
0 * * * * cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_hourly >> ${REMOTE_DIR}/logs/cron.log 2>&1
0 0 * * * cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_daily >> ${REMOTE_DIR}/logs/cron.log 2>&1
0 0 * * 1 cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_weekly >> ${REMOTE_DIR}/logs/cron.log 2>&1
0 0 1 * * cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_monthly >> ${REMOTE_DIR}/logs/cron.log 2>&1
0 0 1 1 * cd ${REMOTE_DIR} && ${REMOTE_DIR}/.venv/bin/python scripts/run.py run cmc_yearly >> ${REMOTE_DIR}/logs/cron.log 2>&1
CRONTAB
crontab /tmp/data-processor-cron
rm -f /tmp/data-processor-cron
'"

log "Step 6/6: Verify counts"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" bash -lc "'
set -e
cd "${REMOTE_DIR}"
.venv/bin/python - <<PY
import sys
sys.path.insert(0,'src')
from core.config import config
from clickhouse_driver import Client
c=config.get_clickhouse_config()
cl=Client(host=c['host'],port=c['port'],user=c['user'],password=c['password'],database=c['database'])
for t in ['cmc_latest_quotes','cmc_hourly','cmc_daily','cmc_weekly','cmc_monthly','cmc_yearly']:
    try:
        cnt=cl.execute("SELECT count() FROM %s" % t)[0][0]
        print(t, cnt)
    except Exception as e:
        print(t, 'ERR', e)
PY
'"

log "Deployment complete. Logs at ${REMOTE_DIR}/logs/cron.log"


