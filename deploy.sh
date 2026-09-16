#!/bin/bash
# Zero-downtime API deploy: build dir → verify → switch → pm2 reload → live health.
set -euo pipefail

APP_NAME="ondial-webhook"
TARGET_DIR="/var/www/html/Ondial-api"
BUILD_DIR="/var/www/html/Ondial-api-build"
RELEASES_DIR="/var/www/html/.releases/ondial-api"
HEALTH_URL="http://127.0.0.1:9000/health"
PM2_USER="${PM2_USER:-ssm-user}"
PM2_HOME="${PM2_HOME:-/home/${PM2_USER}/.pm2}"
ECOSYSTEM="ecosystem.config.cjs"
REQUIRED_ENV_NAMES=(NODE_ENV PORT)
REPO_SLUG="Web2-Kriraai/Ondial_Webhook"

pm2_as_user() {
  sudo -u "$PM2_USER" -H bash -lc "export PM2_HOME='$PM2_HOME'; $*"
}

require_github_token() {
  TOKEN="${GITHUB_TOKEN:-}"
  if [ -z "$TOKEN" ]; then
    echo "GITHUB_TOKEN is not set. export GITHUB_TOKEN='your_token' && bash ./deploy.sh"
    exit 1
  fi
}

require_disk() {
  local avail_kb
  avail_kb=$(df -Pk /var/www/html | awk 'NR==2 {print $4}')
  if [ "${avail_kb:-0}" -lt 524288 ]; then
    echo "FAIL health-1: less than 512MB free on /var/www/html"
    exit 1
  fi
}

env_names_present() {
  local env_file="$1"
  local missing=0
  local name
  if [ ! -f "$env_file" ]; then
    echo "FAIL: required env file missing (name only, value not printed)"
    return 1
  fi
  for name in "${REQUIRED_ENV_NAMES[@]}"; do
    if ! grep -qE "^${name}=" "$env_file"; then
      echo "FAIL health-1: missing required env name ${name}"
      missing=1
    fi
  done
  if ! grep -qE '^(REDIS_URL|REDIS_HOST)=' "$env_file"; then
    echo "FAIL health-1: missing required env name REDIS_URL or REDIS_HOST"
    missing=1
  fi
  [ "$missing" -eq 0 ]
}

live_health() {
  curl -fsS --max-time 10 "$HEALTH_URL" >/dev/null
}

pm2_online() {
  pm2_as_user "pm2 describe $APP_NAME" | grep -q "status.*online"
}

rollback() {
  local prev="$1"
  echo "ROLLBACK: restoring previous release"
  sudo rsync -a --delete --exclude='.git' --exclude='.env' --exclude='.env.*' --include='.env.example' "$prev/" "$TARGET_DIR/"
  pm2_as_user "cd '$TARGET_DIR' && pm2 reload $ECOSYSTEM --only $APP_NAME --update-env" \
    || pm2_as_user "cd '$TARGET_DIR' && pm2 start $ECOSYSTEM --only $APP_NAME"
  sleep 3
  live_health
  pm2_online
}

echo "Starting zero-downtime deploy for $APP_NAME"
require_github_token
require_disk

git config --global --add safe.directory "$TARGET_DIR" || true
git config --global --add safe.directory "$BUILD_DIR" || true

if [ -d "$BUILD_DIR" ]; then
  sudo rm -rf "$BUILD_DIR"
fi
sudo mkdir -p "$BUILD_DIR"
if [ -d "$TARGET_DIR/.git" ]; then
  sudo cp -a "$TARGET_DIR/.git" "$BUILD_DIR/"
fi

cd "$BUILD_DIR"
REPO_URL="https://${TOKEN}@github.com/${REPO_SLUG}.git"
sudo git remote set-url origin "$REPO_URL"
sudo git fetch origin
CURRENT_BRANCH=$(sudo git rev-parse --abbrev-ref HEAD)
sudo git checkout "$CURRENT_BRANCH"
sudo git pull --ff-only origin "$CURRENT_BRANCH"
SHA=$(sudo git rev-parse --short HEAD)
sudo git remote set-url origin "https://github.com/${REPO_SLUG}.git"

sudo cp "$TARGET_DIR/.env" "$BUILD_DIR/.env"
sudo chmod 600 "$BUILD_DIR/.env"
sudo chown -R "$PM2_USER":"$PM2_USER" "$BUILD_DIR"

if [ -f "$BUILD_DIR/package-lock.json" ]; then
  sudo -u "$PM2_USER" -H bash -lc "cd '$BUILD_DIR' && npm ci"
else
  sudo -u "$PM2_USER" -H bash -lc "cd '$BUILD_DIR' && npm install"
fi

echo "Health check 1 — build verify"
[ -f "$BUILD_DIR/index.js" ] || { echo "FAIL health-1: index.js missing"; exit 1; }
[ -d "$BUILD_DIR/node_modules" ] || { echo "FAIL health-1: node_modules missing"; exit 1; }
env_names_present "$BUILD_DIR/.env"
require_disk
echo "Health check 1 passed"

PREV_DIR="$RELEASES_DIR/$SHA-prev"
sudo mkdir -p "$RELEASES_DIR"
if [ -d "$TARGET_DIR" ]; then
  sudo mkdir -p "$PREV_DIR"
  sudo rsync -a --exclude='.git' --exclude='.env' --exclude='.env.*' --include='.env.example' "$TARGET_DIR/" "$PREV_DIR/"
fi

echo "Switching files (live .env preserved)"
sudo rsync -a --delete \
  --exclude='.git' \
  --exclude='.env' \
  --exclude='.env.*' \
  --include='.env.example' \
  --exclude='deploy.sh' \
  "$BUILD_DIR/" "$TARGET_DIR/"
sudo chmod 600 "$TARGET_DIR/.env"
sudo chown -R "$PM2_USER":"$PM2_USER" "$TARGET_DIR" || true

echo "PM2 reload (not restart)"
pm2_as_user "cd '$TARGET_DIR' && pm2 reload $ECOSYSTEM --only $APP_NAME --update-env" \
  || pm2_as_user "cd '$TARGET_DIR' && pm2 start $ECOSYSTEM --only $APP_NAME"
sleep 5

echo "Health check 2 — live"
if ! live_health || ! pm2_online; then
  echo "FAIL health-2: live process unhealthy"
  rollback "$PREV_DIR"
  echo "Rollback completed; deploy failed"
  exit 1
fi

pm2_as_user "pm2 save"
# Leave BUILD_DIR before deleting it (avoids uv_cwd / ENOENT after switch)
cd /tmp
sudo rm -rf "$BUILD_DIR"
echo "SUCCESS sha=$SHA"
pm2_as_user "pm2 list"
curl -fsS --max-time 10 "$HEALTH_URL"
echo
