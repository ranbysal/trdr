"""Backtest metric calculations."""

from __future__ import annotations

from typing import Any

import pandas as pd


def compute_backtest_metrics(
    trades: pd.DataFrame,
    equity_curve: pd.DataFrame,
) -> dict[str, Any]:
    if trades.empty:
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

    r_col = _r_column(trades)
    trade_count = int(len(trades))
    win_rate = float((trades["net_pnl"].astype(float) > 0.0).mean())
    avg_r = float(trades[r_col].astype(float).mean())
    pnl_net = float(trades["net_pnl"].astype(float).sum())
    max_drawdown = _max_drawdown(equity_curve)

    return {
        "trade_count": trade_count,
        "win_rate": round(win_rate, 6),
        "avg_R": round(avg_r, 6),
        "expectancy_R": round(avg_r, 6),
        "pnl_net": round(pnl_net, 6),
        "max_drawdown": round(max_drawdown, 6),
        "per_symbol": _group_breakdown(trades, key="symbol", r_col=r_col),
        "per_strategy": _group_breakdown(trades, key="strategy", r_col=r_col),
    }


def _group_breakdown(
    trades: pd.DataFrame,
    *,
    key: str,
    r_col: str,
) -> dict[str, dict[str, float | int]]:
    out: dict[str, dict[str, float | int]] = {}
    for group, frame in trades.groupby(key):
        avg_r = float(frame[r_col].astype(float).mean()) if not frame.empty else 0.0
        out[str(group)] = {
            "trade_count": int(len(frame)),
            "win_rate": round(float((frame["net_pnl"].astype(float) > 0.0).mean()) if not frame.empty else 0.0, 6),
            "avg_R": round(avg_r, 6),
            "expectancy_R": round(avg_r, 6),
            "pnl_net": round(float(frame["net_pnl"].astype(float).sum()) if not frame.empty else 0.0, 6),
        }
    return out


def _max_drawdown(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty or "drawdown" not in equity_curve:
        return 0.0
    return float(equity_curve["drawdown"].astype(float).min())


def _r_column(trades: pd.DataFrame) -> str:
    if "realized_R" in trades.columns:
        return "realized_R"
    if "r_multiple" in trades.columns:
        return "r_multiple"
    raise ValueError("Trades frame must include either 'realized_R' or 'r_multiple'")
