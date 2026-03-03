from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime
from futures_bot.strategies.strategy_b_models import StrategyBFeatureSnapshot
from futures_bot.strategies.strategy_b_vwap_rev import StrategyBVWAPReversion

ET = ZoneInfo("America/New_York")


def test_strategy_b_weak_neutral_vwap_reversion_signal() -> None:
    strat = StrategyBVWAPReversion()
    features = StrategyBFeatureSnapshot(
        raw_regime=Regime.NEUTRAL,
        is_weak_neutral=True,
        confidence=0.6,
        session_vwap=100.0,
        atr_14_5m=4.0,
        rvol_3bar_aggregate_5m=1.2,
        close_price=104.0,
        data_ok=True,
    )
    eval_result, signal = strat.evaluate_candidate(
        ts=datetime(2026, 1, 14, 10, 0, tzinfo=ET),
        symbol="NQ",
        features=features,
    )
    assert eval_result.approved
    assert signal is not None
    assert signal.side is OrderSide.SELL


def test_strategy_b_rejects_non_weak_neutral() -> None:
    strat = StrategyBVWAPReversion()
    features = StrategyBFeatureSnapshot(
        raw_regime=Regime.TREND,
        is_weak_neutral=False,
        confidence=1.0,
        session_vwap=100.0,
        atr_14_5m=4.0,
        rvol_3bar_aggregate_5m=1.2,
        close_price=96.0,
        data_ok=True,
    )
    eval_result, signal = strat.evaluate_candidate(
        ts=datetime(2026, 1, 14, 10, 0, tzinfo=ET),
        symbol="NQ",
        features=features,
    )
    assert not eval_result.approved
    assert eval_result.reason_code == "WEAK_NEUTRAL_GATE_FAILED"
    assert signal is None

