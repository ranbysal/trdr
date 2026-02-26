# futures-bot

Production-grade scaffold for a deterministic futures signal bot.

## Requirements

- Python 3.11
- Dependencies: `numpy`, `pandas`, `pyyaml`
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
futures-bot paper --help
futures-bot validate-config --help
```

## Project Layout

- `src/` package layout
- `configs/` runtime configuration YAML scaffolding
- `tests/` unit test scaffolding

No trading strategy logic is implemented in this scaffold.

## Codespaces Rebuild and Tests

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
