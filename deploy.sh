#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <ssh_user> <ssh_host> [job_name]"
  exit 1
fi

SSH_USER="$1"
SSH_HOST="$2"
JOB_NAME="${3:-cmc_hourly_prices}"

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_DIR="/home/${SSH_USER}/data-processor"

echo "[deploy] Syncing project to ${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}"
rsync -az --delete \
  --exclude ".git" \
  --exclude ".venv" \
  --exclude "__pycache__/" \
  --exclude ".pytest_cache/" \
  --exclude "logs/*" \
  "${PROJECT_DIR}/" "${SSH_USER}@${SSH_HOST}:${REMOTE_DIR}/"

echo "[deploy] Provisioning remote environment and installing cron"
ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" \
  "REMOTE_DIR='${REMOTE_DIR}' JOB_NAME='${JOB_NAME}' bash -s" <<'REMOTE'
set -e
DEPLOY_DIR="$REMOTE_DIR"
mkdir -p "$DEPLOY_DIR/logs"
cd "$DEPLOY_DIR"

if [ ! -x "$DEPLOY_DIR/.venv/bin/python" ]; then
  python3 -m venv "$DEPLOY_DIR/.venv"
fi

PY="$DEPLOY_DIR/.venv/bin/python"
"$PY" -m pip install -U pip >/dev/null 2>&1 || true
"$PY" -m pip install aiohttp clickhouse-driver pytz pytest pytest-asyncio >/dev/null 2>&1 || true

bash "$DEPLOY_DIR/run.sh" setup_cron "$JOB_NAME"
echo "Deployment and cron installation completed."
REMOTE

echo "Done."

#!/bin/bash
set -e

# optional local overrides
[ -f .deploy.env ] && . .deploy.env

DEPLOY_USER=e.soltani
DEPLOY_HOST=172.30.63.35
DEPLOY_DIR=/home/e.soltani/data-processor

if ! command -v rsync >/dev/null 2>&1; then
  echo "rsync is required" >&2
  exit 1
fi
if ! command -v ssh >/dev/null 2>&1; then
  echo "ssh is required" >&2
  exit 1
fi

echo "Removing any existing remote directory $DEPLOY_DIR and old crons"
ssh "$DEPLOY_USER@$DEPLOY_HOST" "bash -lc 'set -e; crontab -l 2>/dev/null | grep -v "$DEPLOY_DIR" | grep -v crons.run | crontab - 2>/dev/null || true; rm -rf "$DEPLOY_DIR"; mkdir -p "$DEPLOY_DIR"'"

echo "Syncing project to $DEPLOY_USER@$DEPLOY_HOST:$DEPLOY_DIR"
rsync -az --delete \
  --exclude '.venv' \
  --exclude '.git' \
  --exclude '__pycache__' \
  ./ "$DEPLOY_USER@$DEPLOY_HOST:$DEPLOY_DIR/"

ssh "$DEPLOY_USER@$DEPLOY_HOST" "bash -lc 'set -e; \
  cd \"$DEPLOY_DIR\"; \
  # create venv if missing and install runtime deps\n  if [ ! -x \"$DEPLOY_DIR/.venv/bin/python\" ]; then python3 -m venv \"$DEPLOY_DIR/.venv\"; fi; \
  \"$DEPLOY_DIR/.venv/bin/python\" -m pip install -U pip; \
  \"$DEPLOY_DIR/.venv/bin/python\" -m pip install aiohttp clickhouse-driver pytz; \
  # prepare env and logs\n  [ -f .env ] || cp env.example .env; \
  sed -i \"s|^CLICKHOUSE_HOST=.*|CLICKHOUSE_HOST=127.0.0.1|\" .env; \
  mkdir -p \"$DEPLOY_DIR/logs\"; \
  # run once and install cron with absolute paths\n  cd src; \
  \"$DEPLOY_DIR/.venv/bin/python\" -m crons.run cmc_hourly_prices || true; \
  (crontab -l 2>/dev/null | grep -v \"$DEPLOY_DIR/.venv/bin/python -m crons.run\"; echo \"0 * * * * cd $DEPLOY_DIR/src && $DEPLOY_DIR/.venv/bin/python -m crons.run cmc_hourly_prices >> $DEPLOY_DIR/logs/cron.log 2>&1\") | crontab -; \
  echo \"Deployed. Cron set hourly. Log: $DEPLOY_DIR/logs/cron.log\"'"

echo "Done."


