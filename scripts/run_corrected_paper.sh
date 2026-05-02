#!/usr/bin/env bash
set -euo pipefail

cd "${TRDR_HOME:-/opt/trdr}"

mkdir -p out/corrected_paper/logs out/corrected_paper/reports state/corrected_paper

exec "${TRDR_VENV_BIN:-.venv/bin}/corrected-paper" watch \
  --config-dir "${CORRECTED_PAPER_CONFIG_DIR:-configs/corrected_paper}" \
  --poll-interval "${CORRECTED_PAPER_POLL_INTERVAL_S:-5}"
