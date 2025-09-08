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

echo "Creating remote directory $DEPLOY_DIR"
ssh "$DEPLOY_USER@$DEPLOY_HOST" "mkdir -p '$DEPLOY_DIR'"

echo "Syncing project to $DEPLOY_USER@$DEPLOY_HOST:$DEPLOY_DIR"
rsync -az --delete \
  --exclude '.venv' \
  --exclude '.git' \
  --exclude '__pycache__' \
  ./ "$DEPLOY_USER@$DEPLOY_HOST:$DEPLOY_DIR/"

ssh "$DEPLOY_USER@$DEPLOY_HOST" "bash -lc 'set -e; \
  cd \"$DEPLOY_DIR\"; \
  if [ ! -d .venv ]; then python3 -m venv .venv; fi; \
  . .venv/bin/activate; \
  python -m pip install -U pip; \
  if [ -f requirements.txt ]; then python -m pip install -r requirements.txt; else python -m pip install aiohttp clickhouse-driver pytz; fi; \
  [ -f .env ] || cp env.example .env; \
  sed -i \"s|^CLICKHOUSE_HOST=.*|CLICKHOUSE_HOST=172.30.63.35|\" .env; \
  ./run.sh setup_db; \
  ./run.sh run_once || true; \
  mkdir -p \"$DEPLOY_DIR/logs\"; \
  (crontab -l 2>/dev/null | grep -v \"cron_job.py\"; echo \"0 * * * * cd $DEPLOY_DIR && $DEPLOY_DIR/.venv/bin/python3 cron_job.py >> $DEPLOY_DIR/logs/cron.log 2>&1\") | crontab - 2>/dev/null || true; \
  echo \"Deployed. Cron set hourly. Log: $DEPLOY_DIR/logs/cron.log\"'"

echo "Done."


