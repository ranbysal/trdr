"""Backtest replay and metrics helpers."""

from __future__ import annotations

from typing import Any

__all__ = ["compute_backtest_metrics", "run_corrected_validation_replay", "run_replay_backtest"]


def compute_backtest_metrics(*args: Any, **kwargs: Any) -> Any:
    from futures_bot.backtest.metrics import compute_backtest_metrics as _compute_backtest_metrics

    return _compute_backtest_metrics(*args, **kwargs)


def run_corrected_validation_replay(*args: Any, **kwargs: Any) -> Any:
    from futures_bot.backtest.corrected_replay import run_corrected_validation_replay as _run_corrected_validation_replay

    return _run_corrected_validation_replay(*args, **kwargs)


def run_replay_backtest(*args: Any, **kwargs: Any) -> Any:
    from futures_bot.backtest.replay_runner import run_replay_backtest as _run_replay_backtest

    return _run_replay_backtest(*args, **kwargs)
