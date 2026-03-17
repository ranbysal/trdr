# tmux Runbook

## Start

```bash
cd /workspaces/trdr
tmux new-session -d -s futures-signals
tmux send-keys -t futures-signals 'set -a && source .env && set +a && futures-bot signals --config-dir configs --out out/signals_live' C-m
```

## Attach

```bash
tmux attach -t futures-signals
```

## Detach

Press `Ctrl+B` then `D`.

## Stop Safely

```bash
tmux send-keys -t futures-signals C-c
```

Wait for the process to exit, then confirm the session is idle or close it:

```bash
tmux kill-session -t futures-signals
```

## Logs

- `out/signals_live/live_events.ndjson`
- `out/signals_live/signal_engine.ndjson`
- `out/signals_live/signal_events.ndjson`
- `out/signals_live/runtime_state.json`

## Restart

```bash
tmux kill-session -t futures-signals
tmux new-session -d -s futures-signals
tmux send-keys -t futures-signals 'set -a && source .env && set +a && futures-bot signals --config-dir configs --out out/signals_live' C-m
```
