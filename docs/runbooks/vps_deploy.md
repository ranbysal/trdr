# VPS Deploy

## Environment

Create and load an env file before starting the bot:

```bash
cd /workspaces/trdr
set -a
source .env
set +a
```

Required live env:

```bash
export DATABENTO_API_KEY=...
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_CHAT_ID=...
```

Optional operational env:

```bash
export FUTURES_BOT_HEARTBEAT_HOURS=4
export FUTURES_BOT_BARS_STALE_TIMEOUT_S=180
export FUTURES_BOT_QUOTE_STALE_TIMEOUT_S=30
export FUTURES_BOT_TELEGRAM_POLL_INTERVAL_S=2
```

## Startup Command

```bash
futures-bot signals --config-dir configs --out out/signals_live
```

## Restart Behavior

- Use a process supervisor such as `systemd` or `tmux`.
- Prefer `Restart=always` with a short restart delay under `systemd`.
- Runtime state is persisted to `out/signals_live/runtime_state.json`.
- Active ideas snapshot remains at `out/signals_live/active_ideas.json`.

## Logs

- Signal engine debug: `out/signals_live/signal_engine.ndjson`
- Live runtime events: `out/signals_live/live_events.ndjson`
- Signal lifecycle events: `out/signals_live/signal_events.ndjson`
- Runtime state: `out/signals_live/runtime_state.json`

## Safe Stop / Start

Stop:

```bash
systemctl stop futures-bot-signals
```

Start:

```bash
systemctl start futures-bot-signals
```

If you are not using `systemd`, stop the `tmux` session or terminate the process cleanly with `SIGTERM`.
