from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.config.loader import load_instruments
from futures_bot.core.enums import StrategyModule
from futures_bot.pipeline import multistrategy_paper
from futures_bot.pipeline.multistrategy_paper import run_multistrategy_paper_loop
from futures_bot.strategies.strategy_d_pair import PairSignal

ET = ZoneInfo("America/New_York")


def _write_bcd_csv(path: Path) -> None:
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
        for i in range(120):
            ts = start + timedelta(minutes=i)
            base = {
                "volume": "1000",
                "data_ok": "true",
                "quote_ok": "true",
                "trade_eligible": "true",
                "lockout": "false",
                "family_freeze": "false",
            }
            writer.writerow(
                {
                    **base,
                    "ts": ts.isoformat(),
                    "symbol": "YM",
                    "open": "110.0",
                    "high": "111.0",
                    "low": "109.0",
                    "close": "110.5" if ts.hour == 10 and ts.minute == 0 else "110.0",
                    "session_vwap": "108.0",
                    "ema9_5m": "110.0",
                    "ema21_5m": "110.0",
                    "ema20_5m_slope": "0.0",
                    "atr_14_5m": "2.0",
                    "atr_14_1m_price": "1.0",
                    "rvol_3bar_aggregate_5m": "1.0",
                    "vol_strong_1m": "false",
                    "raw_regime": "neutral",
                    "is_weak_neutral": "true",
                    "confidence": "0.5",
                }
            )
            mgc_close = 100.0 + (0.03 * i)
            if ts.hour == 9 and ts.minute == 41:
                mgc_close = 103.0
            mgc_vwap = 200.0 if ts < datetime(2026, 1, 12, 9, 40, tzinfo=ET) else 99.0
            writer.writerow(
                {
                    **base,
                    "ts": ts.isoformat(),
                    "symbol": "MGC",
                    "open": f"{mgc_close:.2f}",
                    "high": f"{(mgc_close + 1.2):.2f}",
                    "low": f"{(mgc_close - 1.0):.2f}",
                    "close": f"{mgc_close:.2f}",
                    "session_vwap": f"{mgc_vwap:.1f}",
                    "ema9_5m": "101.0",
                    "ema21_5m": "100.0",
                    "ema20_5m_slope": "0.7",
                    "atr_14_5m": "1.2",
                    "atr_14_1m_price": "0.7",
                    "rvol_3bar_aggregate_5m": "1.2",
                    "vol_strong_1m": "true",
                    "raw_regime": "trend",
                    "is_weak_neutral": "false",
                    "confidence": "1.0",
                }
            )
            sil_close = 50.0 + (0.02 * i)
            writer.writerow(
                {
                    **base,
                    "ts": ts.isoformat(),
                    "symbol": "SIL",
                    "open": f"{sil_close:.2f}",
                    "high": f"{(sil_close + 0.5):.2f}",
                    "low": f"{(sil_close - 0.5):.2f}",
                    "close": f"{sil_close:.2f}",
                    "session_vwap": "49.8",
                    "ema9_5m": "50.0",
                    "ema21_5m": "49.8",
                    "ema20_5m_slope": "0.2",
                    "atr_14_5m": "0.9",
                    "atr_14_1m_price": "0.5",
                    "rvol_3bar_aggregate_5m": "1.1",
                    "vol_strong_1m": "true",
                    "raw_regime": "trend",
                    "is_weak_neutral": "false",
                    "confidence": "1.0",
                }
            )


def test_bcd_wired_through_risk_vault(tmp_path: Path, monkeypatch) -> None:
    csv_path = tmp_path / "bars.csv"
    out_dir = tmp_path / "out"
    _write_bcd_csv(csv_path)

    def _approved_pair_signal(**_kwargs):
        return PairSignal(
            approved=True,
            reason_code="APPROVED",
            strategy=StrategyModule.STRAT_D_PAIR,
            lead_symbol="MGC",
            hedge_symbol="SIL",
            side="short_spread",
            zscore=2.2,
            stop_risk_proxy=2.2,
            hedge_beta=1.0,
            ar1_phi=0.4,
            half_life_bars=5.0,
        )

    monkeypatch.setattr(multistrategy_paper, "evaluate_pair_signal", _approved_pair_signal)

    log_path = run_multistrategy_paper_loop(
        data_path=csv_path,
        out_dir=out_dir,
        instruments_by_symbol=load_instruments("configs"),
        enabled_strategies={
            StrategyModule.STRAT_B_VWAP_REV,
            StrategyModule.STRAT_C_METALS_ORB,
            StrategyModule.STRAT_D_PAIR,
        },
    )
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    events = [json.loads(line) for line in lines]
    strategies = {e.get("strategy") for e in events if "strategy" in e}
    assert "strat_b_vwap_rev" in strategies
    assert "strat_c_metals_orb" in strategies
    assert "strat_d_pair" in strategies
    assert any(e.get("event") == "pair_entry_filled" for e in events)
