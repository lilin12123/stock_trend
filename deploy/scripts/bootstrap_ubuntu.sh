#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root."
  exit 1
fi

APP_USER="${APP_USER:-stocktrend}"
APP_HOME="${APP_HOME:-/opt/stock_trend}"
CONFIG_HOME="${CONFIG_HOME:-/etc/stock-trend}"

apt update
apt install -y git python3 python3-venv python3-pip nginx certbot python3-certbot-nginx curl

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  adduser --system --group --home "${APP_HOME}" "${APP_USER}"
fi

mkdir -p "${APP_HOME}" "${APP_HOME}/data" "${CONFIG_HOME}"
chown -R "${APP_USER}:${APP_USER}" "${APP_HOME}"
chmod 755 "${APP_HOME}"
chmod 750 "${APP_HOME}/data"

echo "Bootstrap finished."
echo "Next: add the GitHub deploy key, then run deploy/scripts/install_app.sh"
