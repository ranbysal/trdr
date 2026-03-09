from __future__ import annotations

import pandas as pd

from futures_bot.backtest.metrics import compute_backtest_metrics


def test_compute_backtest_metrics_basic() -> None:
    trades = pd.DataFrame(
        [
            {"timestamp_et": "2026-01-01T10:00:00-05:00", "strategy": "strat_a_orb", "symbol": "NQ", "pnl_net": 100.0, "r_multiple": 1.0},
            {"timestamp_et": "2026-01-01T10:30:00-05:00", "strategy": "strat_b_vwap_rev", "symbol": "YM", "pnl_net": -50.0, "r_multiple": -0.5},
        ]
    )
    equity = pd.DataFrame(
        [
            {"timestamp_et": "2026-01-01T10:00:00-05:00", "equity": 100100.0, "pnl_cum": 100.0},
            {"timestamp_et": "2026-01-01T10:30:00-05:00", "equity": 100050.0, "pnl_cum": 50.0},
        ]
    )

    summary = compute_backtest_metrics(trades, equity)

    assert summary["trade_count"] == 2
    assert summary["win_rate"] == 0.5
    assert summary["avg_R"] == 0.25
    assert summary["expectancy_R"] == 0.25
    assert summary["pnl_net"] == 50.0
    assert summary["max_drawdown"] < 0.0
    assert "NQ" in summary["per_symbol"]
    assert "strat_a_orb" in summary["per_strategy"]
