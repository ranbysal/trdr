#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${VPS_TARGET:-}" ]]; then
  echo "VPS_TARGET is required, for example: VPS_TARGET=user@host" >&2
  exit 2
fi

REMOTE_DIR="${TRDR_REMOTE_DIR:-/opt/trdr}"
SERVICE_NAME="${CORRECTED_PAPER_SERVICE_NAME:-corrected-paper}"

echo "syncing repository to ${VPS_TARGET}:${REMOTE_DIR}"
ssh "$VPS_TARGET" "mkdir -p '$REMOTE_DIR'"
rsync -az --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude 'out/' \
  --exclude 'state/' \
  ./ "$VPS_TARGET:$REMOTE_DIR/"

echo "installing corrected paper service on ${VPS_TARGET}"
ssh "$VPS_TARGET" "cd '$REMOTE_DIR' && \
  .venv/bin/python -m pip install -e . && \
  sudo cp services/corrected_paper.service /etc/systemd/system/${SERVICE_NAME}.service && \
  sudo systemctl daemon-reload && \
  sudo systemctl enable '${SERVICE_NAME}' && \
  sudo systemctl restart '${SERVICE_NAME}' && \
  TRDR_HOME='$REMOTE_DIR' CORRECTED_PAPER_SERVICE_NAME='${SERVICE_NAME}' scripts/verify_corrected_paper_vps.sh"

echo "corrected paper deployment command completed"
