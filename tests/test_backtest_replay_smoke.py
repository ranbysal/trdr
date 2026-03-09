from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.backtest.replay_runner import run_replay_backtest
from futures_bot.config.loader import load_instruments
from futures_bot.core.enums import StrategyModule

ET = ZoneInfo("America/New_York")


def _write_1m_csv(path: Path) -> None:
    fields = ["timestamp_et", "symbol", "open", "high", "low", "close", "volume"]
    start = datetime(2026, 1, 12, 9, 30, tzinfo=ET)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for i in range(125):
            ts = start + timedelta(minutes=i)
            close = 100.0 + (0.02 * i)
            high = close + 0.5
            low = close - 0.5
            if ts.hour == 10 and ts.minute == 0:
                close = 106.0
                high = 108.0
                low = 105.5
            if ts.hour == 11 and ts.minute >= 20:
                close = 100.0
                high = 100.5
                low = 99.0
            writer.writerow(
                {
                    "timestamp_et": ts.isoformat(),
                    "symbol": "NQ",
                    "open": f"{close:.2f}",
                    "high": f"{high:.2f}",
                    "low": f"{low:.2f}",
                    "close": f"{close:.2f}",
                    "volume": "1000",
                }
            )


def test_backtest_replay_smoke(tmp_path: Path) -> None:
    data = tmp_path / "bars.csv"
    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"
    _write_1m_csv(data)

    result_a = run_replay_backtest(
        data_path=data,
        out_dir=out_a,
        instruments_by_symbol=load_instruments("configs"),
        enabled_strategies={StrategyModule.STRAT_A_ORB},
    )
    result_b = run_replay_backtest(
        data_path=data,
        out_dir=out_b,
        instruments_by_symbol=load_instruments("configs"),
        enabled_strategies={StrategyModule.STRAT_A_ORB},
    )

    trades_a = Path(result_a["trades_path"]).read_text(encoding="utf-8")
    trades_b = Path(result_b["trades_path"]).read_text(encoding="utf-8")
    assert trades_a == trades_b

    summary_a = json.loads(Path(result_a["summary_path"]).read_text(encoding="utf-8"))
    summary_b = json.loads(Path(result_b["summary_path"]).read_text(encoding="utf-8"))
    assert summary_a == summary_b
    assert {"trade_count", "win_rate", "avg_R", "expectancy_R", "pnl_net", "max_drawdown"} <= set(summary_a)

    assert Path(result_a["equity_curve_path"]).exists()
