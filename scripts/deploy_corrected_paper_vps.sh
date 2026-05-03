#!/usr/bin/env bash
set -euo pipefail

REMOTE_DIR="${TRDR_REMOTE_DIR:-/srv/trdr}"
PID_FILE="${CORRECTED_PAPER_PID_FILE:-out/corrected_paper/corrected_paper.pid}"
STDOUT_LOG="${CORRECTED_PAPER_STDOUT_LOG:-out/corrected_paper/logs/nohup_stdout.log}"
STDERR_LOG="${CORRECTED_PAPER_STDERR_LOG:-out/corrected_paper/logs/nohup_stderr.log}"

if [[ -n "${VPS_TARGET:-}" ]]; then
  echo "starting corrected paper on ${VPS_TARGET}:${REMOTE_DIR}"
  ssh "$VPS_TARGET" "cd '$REMOTE_DIR' && TRDR_HOME='$REMOTE_DIR' scripts/deploy_corrected_paper_vps.sh"
  exit 0
fi

cd "$REMOTE_DIR"

mkdir -p out/corrected_paper/logs out/corrected_paper/reports state/corrected_paper

echo "installing corrected paper package in ${REMOTE_DIR}"
.venv/bin/python -m pip install -e .

if [[ -s "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE")"
  if kill -0 "$old_pid" 2>/dev/null; then
    old_command="$(ps -p "$old_pid" -o command= || true)"
    if [[ "$old_command" != *"corrected-paper"* && "$old_command" != *"run_corrected_paper.sh"* ]]; then
      echo "pid file ${PID_FILE} points to non-corrected-paper process ${old_pid}: ${old_command}" >&2
      exit 1
    fi
    echo "stopping existing corrected paper process ${old_pid}"
    kill "$old_pid"
    for _ in {1..20}; do
      if ! kill -0 "$old_pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$old_pid" 2>/dev/null; then
      echo "existing corrected paper process ${old_pid} did not stop after SIGTERM" >&2
      exit 1
    fi
  fi
fi

echo "starting corrected paper with nohup"
TRDR_HOME="$REMOTE_DIR" nohup scripts/run_corrected_paper.sh >"$STDOUT_LOG" 2>"$STDERR_LOG" &
new_pid="$!"
echo "$new_pid" >"$PID_FILE"

sleep "${CORRECTED_PAPER_STARTUP_GRACE_S:-3}"

if ! kill -0 "$new_pid" 2>/dev/null; then
  echo "corrected paper failed to stay running; see ${STDOUT_LOG} and ${STDERR_LOG}" >&2
  exit 1
fi

TRDR_HOME="$REMOTE_DIR" scripts/verify_corrected_paper_vps.sh

echo "corrected paper nohup deployment completed with pid ${new_pid}"
