"""Bot 3 paper execution companion for Prop V2."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "ExecutorConfig",
    "ExecutorV3LiveRunner",
    "MarketBar",
    "PaperExecutor",
    "PaperRiskSizer",
    "PaperTradeJournal",
    "SignalEvent",
    "SubmitSignalResult",
    "build_signal_id",
]

_EXPORTS = {
    "ExecutorConfig": ("bot_exec_v3.models", "ExecutorConfig"),
    "ExecutorV3LiveRunner": ("bot_exec_v3.live", "ExecutorV3LiveRunner"),
    "MarketBar": ("bot_exec_v3.models", "MarketBar"),
    "PaperExecutor": ("bot_exec_v3.executor", "PaperExecutor"),
    "PaperRiskSizer": ("bot_exec_v3.risk", "PaperRiskSizer"),
    "PaperTradeJournal": ("bot_exec_v3.journal", "PaperTradeJournal"),
    "SignalEvent": ("bot_exec_v3.models", "SignalEvent"),
    "SubmitSignalResult": ("bot_exec_v3.models", "SubmitSignalResult"),
    "build_signal_id": ("bot_exec_v3.models", "build_signal_id"),
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
