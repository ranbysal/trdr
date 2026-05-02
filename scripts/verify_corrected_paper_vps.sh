#!/usr/bin/env bash
set -euo pipefail

cd "${TRDR_HOME:-/opt/trdr}"

SERVICE_NAME="${CORRECTED_PAPER_SERVICE_NAME:-corrected-paper}"
BIN="${TRDR_VENV_BIN:-.venv/bin}/corrected-paper"
PY="${TRDR_PYTHON:-.venv/bin/python}"
CONFIG_DIR="${CORRECTED_PAPER_CONFIG_DIR:-configs/corrected_paper}"

echo "checking corrected paper binary"
test -x "$BIN"

echo "checking corrected paper config"
"$BIN" validate-config --config-dir "$CONFIG_DIR"

echo "checking systemd service"
systemctl is-enabled "$SERVICE_NAME" >/dev/null
systemctl is-active "$SERVICE_NAME" >/dev/null
systemctl status "$SERVICE_NAME" --no-pager

echo "checking corrected paper artifacts"
test -f out/corrected_v4/execution_events.ndjson
test -s out/corrected_paper/logs/systemd_stdout.log
test -f state/corrected_paper/corrected_paper_state.json
test -f out/corrected_paper/events.ndjson
test -f out/corrected_paper/corrected_paper_journal.ndjson
test -f out/corrected_paper/reports/summary.json
test -f out/corrected_paper/reports/closed_trades.csv

echo "checking corrected paper runtime status"
"$BIN" status --config-dir "$CONFIG_DIR"

echo "checking corrected paper read-only commands"
CORRECTED_PAPER_CONFIG_DIR="$CONFIG_DIR" "$PY" - <<'PY'
import os

from bot_corrected_paper.controller import CorrectedPaperCommandController

controller = CorrectedPaperCommandController(config_dir=os.environ["CORRECTED_PAPER_CONFIG_DIR"])
commands = (
    "/corrected_paper_open",
    "/corrected_paper_recent 5",
    "/corrected_paper_pnl",
    "/corrected_paper_stats",
)
for command in commands:
    response = controller.handle_command(command)
    if not response or not response.startswith("[CORR-PAPER]"):
        raise SystemExit(f"corrected paper command failed: {command}")
    print(f"{command}: ok")
PY

echo "recent corrected paper service log"
journalctl -u "$SERVICE_NAME" -n 50 --no-pager

echo "corrected paper VPS verification checks passed"
