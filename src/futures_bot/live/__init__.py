"""Live feed ingestion and signal runner helpers."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = ["DatabentoLiveClient", "run_live_signals"]

_EXPORTS = {
    "DatabentoLiveClient": ("futures_bot.live.databento_adapter", "DatabentoLiveClient"),
    "run_live_signals": ("futures_bot.live.live_runner", "run_live_signals"),
}


def __getattr__(name: str) -> Any:
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
