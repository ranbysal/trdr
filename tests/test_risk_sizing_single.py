from __future__ import annotations

from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta
from futures_bot.risk.models import SingleLegSizingRequest
from futures_bot.risk.sizing_single import size_single_leg, size_with_micro_routing


def _meta(
    symbol: str,
    *,
    tick_size: float,
    tick_value: float,
    commission_rt: float,
    family: Family = Family.EQUITIES,
    micro_equivalent: str = "MNQ",
) -> InstrumentMeta:
    return InstrumentMeta(
        symbol=symbol,
        root_symbol="NQ",
        family=family,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=tick_value / tick_size,
        commission_rt=commission_rt,
        symbol_type="future",
        micro_equivalent=micro_equivalent,
        contract_units=1.0,
    )


def test_single_leg_sizing_math() -> None:
    nq = _meta("NQ", tick_size=0.25, tick_value=5.0, commission_rt=4.8)
    req = SingleLegSizingRequest(
        instrument=nq,
        equity=100_000.0,
        risk_pct=0.01,
        entry_price=20_000.0,
        stop_price=19_980.0,
        atr_14_1m_price=10.0,
    )

    decision = size_single_leg(req)

    assert decision.approved is True
    assert decision.contracts == 2
    assert decision.stop_ticks == 80
    assert round(decision.slippage_est_ticks, 2) == 4.20


def test_micro_routing_recompute_correctness() -> None:
    nq = _meta("NQ", tick_size=0.25, tick_value=5.0, commission_rt=4.8, micro_equivalent="MNQ")
    mnq = _meta("MNQ", tick_size=0.25, tick_value=0.5, commission_rt=1.2, micro_equivalent="MNQ")

    req = SingleLegSizingRequest(
        instrument=nq,
        equity=10_000.0,
        risk_pct=0.001,
        entry_price=20_000.0,
        stop_price=19_997.5,
        atr_14_1m_price=1.0,
    )

    decision = size_with_micro_routing(req, instruments_by_symbol={"NQ": nq, "MNQ": mnq})

    assert decision.approved is True
    assert decision.routed_symbol == "MNQ"
    assert decision.contracts == 1
    assert decision.stop_ticks == 10
