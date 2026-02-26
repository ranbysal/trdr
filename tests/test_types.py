from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from futures_bot.core.enums import ApprovalStatus, Family, OrderSide, Regime, StrategyModule
from futures_bot.core.types import InstrumentMeta, SignalCandidate


def test_instrument_meta_is_immutable() -> None:
    meta = InstrumentMeta(
        symbol="ES",
        root_symbol="ES",
        family=Family.EQUITIES,
        tick_size=0.25,
        tick_value=12.5,
        point_value=50.0,
        commission_rt=4.8,
        symbol_type="future",
        micro_equivalent="MES",
        contract_units=1.0,
    )

    with pytest.raises(FrozenInstanceError):
        meta.symbol = "NQ"  # type: ignore[misc]


def test_signal_candidate_typed_enums() -> None:
    signal = SignalCandidate(
        ts=datetime.now(timezone.utc),
        strategy=StrategyModule.STRAT_A_ORB,
        symbol="ES",
        side=OrderSide.BUY,
        regime=Regime.TREND,
        score=0.75,
    )

    assert signal.strategy is StrategyModule.STRAT_A_ORB
    assert signal.side is OrderSide.BUY
    assert signal.regime is Regime.TREND
    assert ApprovalStatus.APPROVED.value == "approved"
