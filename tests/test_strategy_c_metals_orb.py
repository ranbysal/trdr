from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide
from futures_bot.core.types import Bar1m
from futures_bot.strategies.strategy_c_metals_orb import StrategyCMetalsORB
from futures_bot.strategies.strategy_c_models import StrategyCFeatureSnapshot

ET = ZoneInfo("America/New_York")


def _bar(ts: datetime, *, high: float, low: float, close: float, symbol: str = "MGC") -> Bar1m:
    return Bar1m(ts=ts, symbol=symbol, open=close, high=high, low=low, close=close, volume=1000.0)


def _build_or(engine: StrategyCMetalsORB, *, day: datetime, symbol: str = "MGC") -> None:
    for i in range(15):
        ts = day.replace(hour=8, minute=0, second=0, microsecond=0) + timedelta(minutes=i)
        engine.update_or_state(_bar(ts, high=2400.0 + i * 0.2, low=2390.0 - i * 0.2, close=2395.0, symbol=symbol))
    engine.update_or_state(_bar(day.replace(hour=8, minute=15, second=0, microsecond=0), high=2401.0, low=2391.0, close=2396.0, symbol=symbol))


def test_strategy_c_signal_with_ema20_slope_rule() -> None:
    engine = StrategyCMetalsORB()
    day = datetime(2026, 1, 15, tzinfo=ET)
    _build_or(engine, day=day, symbol="MGC")
    features = StrategyCFeatureSnapshot(
        session_vwap=2394.0,
        ema20_5m_slope=0.3,
        atr_14_5m=5.0,
        vol_strong_1m=True,
        rvol_3bar_aggregate_5m=1.2,
        tier1_lockout_active=False,
    )
    eval_result, signal = engine.evaluate_breakout_candidate(
        bar=_bar(day.replace(hour=9, minute=0), high=2410.0, low=2400.0, close=2410.5, symbol="MGC"),
        features=features,
        tick_size=0.1,
    )
    assert eval_result.approved
    assert signal is not None
    assert signal.side is OrderSide.BUY


def test_strategy_c_rejects_wrong_ema20_slope_direction() -> None:
    engine = StrategyCMetalsORB()
    day = datetime(2026, 1, 15, tzinfo=ET)
    _build_or(engine, day=day, symbol="SIL")
    features = StrategyCFeatureSnapshot(
        session_vwap=31.0,
        ema20_5m_slope=0.2,
        atr_14_5m=0.7,
        vol_strong_1m=True,
        rvol_3bar_aggregate_5m=1.2,
        tier1_lockout_active=False,
    )
    eval_result, signal = engine.evaluate_breakout_candidate(
        bar=_bar(day.replace(hour=9, minute=5), high=30.2, low=29.0, close=28.9, symbol="SIL"),
        features=features,
        tick_size=0.01,
    )
    assert not eval_result.approved
    assert eval_result.reason_code == "EMA20_SLOPE_FILTER_FAILED"
    assert signal is None

