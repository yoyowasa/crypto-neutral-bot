#!/usr/bin/env bash
set -euo pipefail

# Load .env if present (export all)
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

# Require explicit opt‑in for live trading
export EXCHANGE__ALLOW_LIVE=${EXCHANGE__ALLOW_LIVE:-true}

# Use mainnet config example by default if no APP_CONFIG_FILE provided
export APP_CONFIG_FILE=${APP_CONFIG_FILE:-config/app.mainnet.yaml}
# Fallback to example if the specified config file does not exist
if [[ ! -f "$APP_CONFIG_FILE" ]]; then
  APP_CONFIG_FILE="config/app.mainnet.example.yaml"
  export APP_CONFIG_FILE
fi

echo "[start_mainnet] APP_CONFIG_FILE=$APP_CONFIG_FILE"
echo "[start_mainnet] EXCHANGE__ALLOW_LIVE=$EXCHANGE__ALLOW_LIVE"

# Ensure logs directory exists (setup_logging will also create it)
mkdir -p logs

exec python -m bot.app.live_runner --env mainnet "$@"
