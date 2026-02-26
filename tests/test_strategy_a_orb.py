from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime
from futures_bot.core.types import Bar1m
from futures_bot.strategies.strategy_a_models import StrategyAFeatureSnapshot
from futures_bot.strategies.strategy_a_orb import StrategyAORB

ET = ZoneInfo("America/New_York")


def _bar(ts: datetime, *, high: float, low: float, close: float, symbol: str = "NQ") -> Bar1m:
    return Bar1m(ts=ts, symbol=symbol, open=close, high=high, low=low, close=close, volume=1000.0)


def _features(**kwargs: object) -> StrategyAFeatureSnapshot:
    defaults: dict[str, object] = {
        "raw_regime": Regime.TREND,
        "low_volume_trend_streak_5m": 0,
        "vol_strong_1m": True,
        "rvol_3bar_aggregate_5m": 1.2,
        "session_vwap": 100.0,
        "ema9_5m": 105.0,
        "ema21_5m": 103.0,
        "atr_14_5m": 4.0,
        "tier1_lockout_active": False,
    }
    defaults.update(kwargs)
    return StrategyAFeatureSnapshot(**defaults)


def _build_or(engine: StrategyAORB, *, day: datetime, symbol: str = "NQ") -> None:
    for i in range(15):
        ts = day.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(minutes=i)
        engine.update_or_state(_bar(ts, high=102.0 + i * 0.1, low=98.0 - i * 0.1, close=100.0, symbol=symbol))
    # 09:45 bar finalizes the OR window.
    engine.update_or_state(_bar(day.replace(hour=9, minute=45, second=0, microsecond=0), high=101.0, low=99.0, close=100.0, symbol=symbol))


def test_or_construction() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    _build_or(engine, day=day, symbol="NQ")

    state = engine.update_or_state(_bar(day.replace(hour=9, minute=46), high=100.0, low=99.0, close=99.5))

    assert state.is_complete is True
    assert state.bar_count == 15
    assert round(state.or_high or 0.0, 4) == round(102.0 + 14 * 0.1, 4)
    assert round(state.or_low or 0.0, 4) == round(98.0 - 14 * 0.1, 4)
    assert state.or_width is not None and state.or_width > 0.0


def test_long_trigger_and_entry_plan() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    _build_or(engine, day=day, symbol="NQ")

    trigger = _bar(day.replace(hour=10, minute=0), high=104.0, low=100.0, close=104.5, symbol="NQ")
    out = engine.evaluate_breakout_candidate(bar=trigger, features=_features(atr_14_5m=4.2), tick_size=0.25)

    assert out.approved is True
    assert out.signal is not None
    assert out.signal.side is OrderSide.BUY
    assert out.entry_plan is not None
    assert out.entry_plan.entry_stop > 0.0
    assert out.entry_plan.stop_limit_chase_ticks == 4


def test_short_trigger() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    _build_or(engine, day=day, symbol="YM")

    trigger = _bar(day.replace(hour=10, minute=5), high=99.0, low=95.0, close=94.5, symbol="YM")
    out = engine.evaluate_breakout_candidate(
        bar=trigger,
        features=_features(session_vwap=96.0, ema9_5m=90.0, ema21_5m=93.0, atr_14_5m=4.0),
        tick_size=1.0,
    )

    assert out.approved is True
    assert out.signal is not None
    assert out.signal.side is OrderSide.SELL
    assert out.entry_plan is not None
    assert out.entry_plan.stop_limit_chase_ticks == 3


def test_regime_qualification_override_with_vol_strong() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    _build_or(engine, day=day)
    trigger = _bar(day.replace(hour=10, minute=0), high=104.0, low=100.0, close=104.5)

    reject = engine.evaluate_breakout_candidate(
        bar=trigger,
        features=_features(low_volume_trend_streak_5m=3, vol_strong_1m=False),
        tick_size=0.25,
    )
    accept = engine.evaluate_breakout_candidate(
        bar=trigger,
        features=_features(low_volume_trend_streak_5m=3, vol_strong_1m=True),
        tick_size=0.25,
    )

    assert reject.approved is False
    assert reject.reason_code == "UNQUALIFIED_TREND"
    assert accept.approved is True


def test_or_width_sanity_rejection() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    # Narrow OR: width 0.5
    for i in range(15):
        ts = day.replace(hour=9, minute=30) + timedelta(minutes=i)
        engine.update_or_state(_bar(ts, high=100.2, low=99.7, close=100.0))
    engine.update_or_state(_bar(day.replace(hour=9, minute=45), high=100.1, low=99.8, close=100.0))

    trigger = _bar(day.replace(hour=10, minute=0), high=101.0, low=99.0, close=101.5)
    out = engine.evaluate_breakout_candidate(bar=trigger, features=_features(atr_14_5m=3.0), tick_size=0.25)

    assert out.approved is False
    assert out.reason_code == "OR_WIDTH_SANITY_FAILED"


def test_time_window_rejection() -> None:
    engine = StrategyAORB()
    day = datetime(2026, 1, 6, tzinfo=ET)
    _build_or(engine, day=day)

    late_bar = _bar(day.replace(hour=10, minute=46), high=105.0, low=100.0, close=105.5)
    out = engine.evaluate_breakout_candidate(bar=late_bar, features=_features(), tick_size=0.25)

    assert out.approved is False
    assert out.reason_code == "OUTSIDE_ENTRY_WINDOW"
