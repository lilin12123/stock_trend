#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Please run as root."
  exit 1
fi

APP_USER="${APP_USER:-stocktrend}"
APP_HOME="${APP_HOME:-/opt/stock_trend}"
REPO_DIR="${REPO_DIR:-${APP_HOME}/repo}"
REPO_URL="${REPO_URL:-git@github.com:lilin12123/stock_trend.git}"

if ! id -u "${APP_USER}" >/dev/null 2>&1; then
  echo "User ${APP_USER} does not exist. Run deploy/scripts/bootstrap_ubuntu.sh first."
  exit 1
fi

if [[ ! -d "${REPO_DIR}/.git" ]]; then
  sudo -u "${APP_USER}" git clone "${REPO_URL}" "${REPO_DIR}"
else
  sudo -u "${APP_USER}" git -C "${REPO_DIR}" pull --ff-only
fi

sudo -u "${APP_USER}" python3 -m venv "${REPO_DIR}/.venv"
sudo -u "${APP_USER}" "${REPO_DIR}/.venv/bin/pip" install --upgrade pip
sudo -u "${APP_USER}" "${REPO_DIR}/.venv/bin/pip" install -r "${REPO_DIR}/requirements.txt"

echo "App install finished."
echo "Next: copy deploy/config.ec2.yaml and env files into /etc/stock-trend/"
