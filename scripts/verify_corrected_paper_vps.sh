#!/usr/bin/env bash
set -euo pipefail

cd "${TRDR_HOME:-/srv/trdr}"

BIN="${TRDR_VENV_BIN:-.venv/bin}/corrected-paper"
PY="${TRDR_PYTHON:-.venv/bin/python}"
CONFIG_DIR="${CORRECTED_PAPER_CONFIG_DIR:-configs/corrected_paper}"
PID_FILE="${CORRECTED_PAPER_PID_FILE:-out/corrected_paper/corrected_paper.pid}"
STDOUT_LOG="${CORRECTED_PAPER_STDOUT_LOG:-out/corrected_paper/logs/nohup_stdout.log}"
STDERR_LOG="${CORRECTED_PAPER_STDERR_LOG:-out/corrected_paper/logs/nohup_stderr.log}"

echo "checking corrected paper binary"
test -x "$BIN"

echo "checking corrected paper config"
"$BIN" validate-config --config-dir "$CONFIG_DIR"

echo "checking corrected paper nohup process"
test -s "$PID_FILE"
pid="$(cat "$PID_FILE")"
kill -0 "$pid" 2>/dev/null
command="$(ps -p "$pid" -o command= || true)"
if [[ "$command" != *"corrected-paper"* && "$command" != *"run_corrected_paper.sh"* ]]; then
  echo "pid file ${PID_FILE} points to non-corrected-paper process ${pid}: ${command}" >&2
  exit 1
fi
test -f "$STDOUT_LOG"
test -f "$STDERR_LOG"

echo "checking corrected paper artifacts"
test -f out/corrected_v4/execution_events.ndjson
test -s "$STDOUT_LOG"
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

echo "recent corrected paper nohup log"
tail -n 50 "$STDOUT_LOG"

echo "corrected paper VPS verification checks passed"
