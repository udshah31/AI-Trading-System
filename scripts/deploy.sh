#!/usr/bin/env bash
# Runs ON the Oracle Cloud VM (piped over SSH by .github/workflows/ci-cd.yml).
# Checks out one commit, rebuilds and restarts the compose stack in place, then
# verifies it. Secrets live in the VM's .env, never in CI. Setup: DEPLOY.md.
#
#   usage: deploy.sh <commit-sha>
set -euo pipefail

SHA="${1:?usage: deploy.sh <commit-sha>}"
APP_DIR="${APP_DIR:-$HOME/AI-Trading-System}"

cd "$APP_DIR"
if [ ! -f .env ]; then
    echo "❌ $APP_DIR/.env is missing; create it from .env.template (see DEPLOY.md)"
    exit 1
fi

echo "▶ Deploying $SHA"
git fetch --quiet origin
git checkout --quiet --force "$SHA"
git submodule update --init --quiet

# Builds natively on the VM (ARM on Ampere A1) and replaces containers in place,
# so there is only ever one trading bot. --remove-orphans drops services that
# no longer exist in docker-compose.yml.
docker compose up -d --build --remove-orphans

echo "▶ Waiting for the dashboard health check"
for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8000/health >/dev/null 2>&1; then
        if docker compose ps --status running --services | grep -qx trading-system; then
            echo "✅ Deployed $SHA"
            docker compose ps
            exit 0
        fi
    fi
    sleep 5
done

echo "❌ Deploy of $SHA is unhealthy (dashboard /health or trading-system not running)"
docker compose ps
docker compose logs --tail 60 dashboard trading-system
exit 1
