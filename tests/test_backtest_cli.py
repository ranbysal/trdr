from __future__ import annotations

import json
import csv
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.cli import main

ET = ZoneInfo("America/New_York")


def _write_strategy_a_replay_csv(path: Path) -> None:
    fields = ["timestamp_et", "symbol", "open", "high", "low", "close", "volume"]
    start_day = datetime(2026, 1, 5, 9, 30, tzinfo=ET)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for day_offset in range(6):
            session_start = start_day + timedelta(days=day_offset)
            for minute_offset in range(41):
                ts = session_start + timedelta(minutes=minute_offset)
                close, high, low, volume = _bar_for_day(day_offset=day_offset, minute_offset=minute_offset)
                writer.writerow(
                    {
                        "timestamp_et": ts.isoformat(),
                        "symbol": "NQ",
                        "open": f"{close:.2f}",
                        "high": f"{high:.2f}",
                        "low": f"{low:.2f}",
                        "close": f"{close:.2f}",
                        "volume": str(volume),
                    }
                )


def _bar_for_day(*, day_offset: int, minute_offset: int) -> tuple[float, float, float, int]:
    if day_offset < 5:
        close = 100.0 + (0.5 * day_offset) + (0.03 * minute_offset)
        return close, close + 0.25, close - 0.25, 100

    if minute_offset < 15:
        close = 103.00
        return close, 103.50, 102.50, 100
    if minute_offset < 30:
        close = 103.40
        return close, 103.60, 103.10, 100
    if minute_offset == 30:
        close = 104.00
        return close, 104.10, 103.40, 300
    if minute_offset == 31:
        close = 103.90
        return close, 104.15, 103.70, 150
    if minute_offset == 32:
        close = 105.50
        return close, 105.60, 104.80, 200
    close = 103.20
    return close, 103.40, 103.00, 80


def test_backtest_cli_writes_required_reports(tmp_path: Path) -> None:
    data_path = tmp_path / "bars.csv"
    out_dir = tmp_path / "backtest_out"
    _write_strategy_a_replay_csv(data_path)

    rc = main(
        [
            "backtest",
            "--data",
            str(data_path),
            "--config-dir",
            "configs",
            "--out",
            str(out_dir),
            "--strategies",
            "A",
        ]
    )

    assert rc == 0
    trades_path = out_dir / "trades.csv"
    equity_curve_path = out_dir / "equity_curve.csv"
    summary_path = out_dir / "summary.json"

    assert trades_path.exists()
    assert equity_curve_path.exists()
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert {"trade_count", "expectancy_R", "max_drawdown"} <= set(summary)
    assert summary["trade_count"] >= 1
