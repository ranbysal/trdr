"""Backtest metric calculations."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def compute_backtest_metrics(
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
) -> dict[str, Any]:
    trade_count = int(len(trades))
    if trade_count == 0:
        return {
            "trade_count": 0,
            "win_rate": 0.0,
            "avg_R": 0.0,
            "expectancy_R": 0.0,
            "pnl_net": 0.0,
            "max_drawdown": 0.0,
            "per_symbol": {},
            "per_strategy": {},
        }

    wins = trades["pnl_net"] > 0.0
    win_rate = float(wins.mean())
    avg_r = float(trades["r_multiple"].mean())

    win_r = trades.loc[trades["r_multiple"] > 0.0, "r_multiple"]
    loss_r = trades.loc[trades["r_multiple"] <= 0.0, "r_multiple"]
    avg_win_r = float(win_r.mean()) if not win_r.empty else 0.0
    avg_loss_r = float(loss_r.mean()) if not loss_r.empty else 0.0
    expectancy_r = (win_rate * avg_win_r) + ((1.0 - win_rate) * avg_loss_r)

    pnl_net = float(trades["pnl_net"].sum())
    max_drawdown = _max_drawdown(equity_curve)

    return {
        "trade_count": trade_count,
        "win_rate": round(win_rate, 6),
        "avg_R": round(avg_r, 6),
        "expectancy_R": round(float(expectancy_r), 6),
        "pnl_net": round(pnl_net, 6),
        "max_drawdown": round(max_drawdown, 6),
        "per_symbol": _group_breakdown(trades, "symbol"),
        "per_strategy": _group_breakdown(trades, "strategy"),
    }


def _max_drawdown(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0
    eq = equity_curve["equity"].astype(float)
    running_peak = eq.cummax()
    dd = (eq - running_peak) / running_peak.replace(0.0, np.nan)
    return float(dd.min())


def _group_breakdown(trades: pd.DataFrame, key: str) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for group, frame in trades.groupby(key):
        wins = frame["pnl_net"] > 0.0
        win_rate = float(wins.mean()) if len(frame) else 0.0

        win_r = frame.loc[frame["r_multiple"] > 0.0, "r_multiple"]
        loss_r = frame.loc[frame["r_multiple"] <= 0.0, "r_multiple"]
        avg_win_r = float(win_r.mean()) if not win_r.empty else 0.0
        avg_loss_r = float(loss_r.mean()) if not loss_r.empty else 0.0
        expectancy_r = (win_rate * avg_win_r) + ((1.0 - win_rate) * avg_loss_r)

        out[str(group)] = {
            "trade_count": int(len(frame)),
            "win_rate": round(win_rate, 6),
            "avg_R": round(float(frame["r_multiple"].mean()), 6),
            "expectancy_R": round(float(expectancy_r), 6),
            "pnl_net": round(float(frame["pnl_net"].sum()), 6),
        }
    return out
