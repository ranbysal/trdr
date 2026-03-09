from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.cli import main

ET = ZoneInfo("America/New_York")


def _write_smoke_csv(path: Path) -> None:
    fields = [
        "ts",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "session_vwap",
        "ema9_5m",
        "ema21_5m",
        "ema20_5m_slope",
        "atr_14_5m",
        "atr_14_1m_price",
        "rvol_3bar_aggregate_5m",
        "vol_strong_1m",
        "data_ok",
        "quote_ok",
        "trade_eligible",
        "lockout",
        "family_freeze",
        "raw_regime",
        "is_weak_neutral",
        "confidence",
    ]
    start = datetime(2026, 1, 12, 8, 0, tzinfo=ET)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for i in range(180):
            ts = start + timedelta(minutes=i)
            ts_s = ts.isoformat()
            writer.writerow(
                {
                    "ts": ts_s,
                    "symbol": "NQ",
                    "open": "100.0",
                    "high": "106.0" if ts.hour == 10 and ts.minute == 1 else "101.0",
                    "low": "99.0",
                    "close": "105.5" if ts.hour == 10 and ts.minute == 0 else "100.0",
                    "volume": "1000",
                    "session_vwap": "99.5",
                    "ema9_5m": "102.0",
                    "ema21_5m": "100.0",
                    "ema20_5m_slope": "0.0",
                    "atr_14_5m": "2.0",
                    "atr_14_1m_price": "1.0",
                    "rvol_3bar_aggregate_5m": "1.2",
                    "vol_strong_1m": "true",
                    "data_ok": "true",
                    "quote_ok": "true",
                    "trade_eligible": "true",
                    "lockout": "false",
                    "family_freeze": "false",
                    "raw_regime": "trend",
                    "is_weak_neutral": "false",
                    "confidence": "1.0",
                }
            )
            writer.writerow(
                {
                    "ts": ts_s,
                    "symbol": "YM",
                    "open": "110.0",
                    "high": "111.0",
                    "low": "109.0",
                    "close": "110.5" if ts.hour == 10 and ts.minute == 0 else "110.0",
                    "volume": "800",
                    "session_vwap": "108.0",
                    "ema9_5m": "110.0",
                    "ema21_5m": "110.0",
                    "ema20_5m_slope": "0.0",
                    "atr_14_5m": "2.5",
                    "atr_14_1m_price": "1.2",
                    "rvol_3bar_aggregate_5m": "1.0",
                    "vol_strong_1m": "false",
                    "data_ok": "true",
                    "quote_ok": "true",
                    "trade_eligible": "true",
                    "lockout": "false",
                    "family_freeze": "false",
                    "raw_regime": "neutral",
                    "is_weak_neutral": "true",
                    "confidence": "0.5",
                }
            )
            mgc_close = 100.0 + (0.02 * i)
            if ts.hour == 8 and ts.minute == 20:
                mgc_close = 103.0
            writer.writerow(
                {
                    "ts": ts_s,
                    "symbol": "MGC",
                    "open": f"{mgc_close:.2f}",
                    "high": f"{(mgc_close + 1.0):.2f}",
                    "low": f"{(mgc_close - 1.0):.2f}",
                    "close": f"{mgc_close:.2f}",
                    "volume": "700",
                    "session_vwap": "99.0",
                    "ema9_5m": "100.0",
                    "ema21_5m": "99.0",
                    "ema20_5m_slope": "0.8",
                    "atr_14_5m": "1.5",
                    "atr_14_1m_price": "0.8",
                    "rvol_3bar_aggregate_5m": "1.2",
                    "vol_strong_1m": "true",
                    "data_ok": "true",
                    "quote_ok": "true",
                    "trade_eligible": "true",
                    "lockout": "false",
                    "family_freeze": "false",
                    "raw_regime": "trend",
                    "is_weak_neutral": "false",
                    "confidence": "1.0",
                }
            )
            sil_close = 50.0 + (0.01 * i)
            writer.writerow(
                {
                    "ts": ts_s,
                    "symbol": "SIL",
                    "open": f"{sil_close:.2f}",
                    "high": f"{(sil_close + 0.4):.2f}",
                    "low": f"{(sil_close - 0.4):.2f}",
                    "close": f"{sil_close:.2f}",
                    "volume": "600",
                    "session_vwap": "50.0",
                    "ema9_5m": "50.0",
                    "ema21_5m": "50.0",
                    "ema20_5m_slope": "0.2",
                    "atr_14_5m": "1.0",
                    "atr_14_1m_price": "0.5",
                    "rvol_3bar_aggregate_5m": "1.0",
                    "vol_strong_1m": "true",
                    "data_ok": "true",
                    "quote_ok": "true",
                    "trade_eligible": "true",
                    "lockout": "false",
                    "family_freeze": "false",
                    "raw_regime": "trend",
                    "is_weak_neutral": "false",
                    "confidence": "1.0",
                }
            )


def test_multistrategy_paper_smoke(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    out_dir = tmp_path / "out"
    _write_smoke_csv(csv_path)

    rc = main(
        [
            "paper",
            "--data",
            str(csv_path),
            "--config-dir",
            "configs",
            "--out",
            str(out_dir),
            "--strategies",
            "A,B,C,D",
        ]
    )
    assert rc == 0

    log_path = out_dir / "trade_logs.json"
    assert log_path.exists()
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert lines
    events = [json.loads(line) for line in lines]
    strategies = {e.get("strategy") for e in events if "strategy" in e}
    assert "strat_a_orb" in strategies
    assert "strat_b_vwap_rev" in strategies
    assert "strat_c_metals_orb" in strategies
