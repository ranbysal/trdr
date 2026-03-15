# futures-bot

Signal-only futures market watcher for live monitoring and Telegram trade alerts.

## Requirements

- Python 3.11
- Dependencies: `databento`, `numpy`, `pandas`, `pyyaml`
- Optional mock/live-test dependency: `websockets`
- Dev dependencies: `pytest`

## Quickstart

```bash
make deps
make test
```

## CLI

```bash
futures-bot --help
futures-bot backtest --help
futures-bot signals --help
futures-bot validate-config --help
```

## Architecture

- Primary path: `live data -> features/regime/strategies -> signal engine -> Telegram notifier`
- The bot does not place, modify, or cancel live orders.
- Strategy math remains in the existing strategy modules.
- Signal lifecycle is tracked through `NEW_SIGNAL`, `ENTRY_ZONE_ACTIVE`, `ENTRY_MISSED`,
  `IN_POSITION_ASSUMED_FALSE`, `PARTIAL_TAKE_SUGGESTED`, `STOP_TO_BREAKEVEN_SUGGESTED`,
  `TP_EXTENSION_SUGGESTED`, `THESIS_INVALIDATED`, and `CLOSE_SIGNAL`.
- Alert state is written to `active_ideas.json` and alert events to `signal_events.ndjson`.

## Running Live Signals

Set environment variables for live mode:

```bash
export DATABENTO_API_KEY="your-databento-key"
export DATABENTO_DATASET="GLBX.MDP3"
export TELEGRAM_BOT_TOKEN="123456:token"
export TELEGRAM_CHAT_ID="123456789"
```

Run the live watcher:

```bash
futures-bot signals --config-dir configs --out out/signals_live
```

For deterministic replay without Telegram delivery:

```bash
futures-bot signals --config-dir configs --data path/to/bars.csv --out out/signals_replay
```

## Notes

- Historical replay backtests remain available through `futures-bot backtest`.
- Legacy paper-execution modules are no longer on the primary CLI path.
- `src/futures_bot/broker/read_only.py` contains placeholders for future read-only broker/account polling only.

## Codespaces and Tests

If dependencies are missing or environment is stale, rebuild the container:

```bash
# In VS Code command palette
Dev Containers: Rebuild Container
```

The devcontainer post-create step will:
- create `.venv` with `--system-site-packages`
- upgrade `pip/setuptools/wheel`
- install `requirements-dev.txt` (and fall back to apt-provided packages when pip cannot reach PyPI)

Run tests with the venv interpreter:

```bash
make test
# or directly
.venv/bin/pytest -q
```
