"""Single-leg sizing for strategies A/B/C (no pair sizing)."""

from __future__ import annotations

import math
from typing import Mapping

from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta
from futures_bot.risk.models import SingleLegSizingRequest, SizingDecision
from futures_bot.risk.slippage import estimate_slippage_ticks


def compute_stop_ticks(entry_price: float, stop_price: float, tick_size: float) -> int:
    """Return stop distance in ticks using ceil(abs(entry-stop)/tick_size)."""
    if tick_size <= 0.0:
        raise ValueError("tick_size must be positive")
    return int(math.ceil(abs(entry_price - stop_price) / tick_size))


def size_single_leg(request: SingleLegSizingRequest) -> SizingDecision:
    """Compute contract count and cost-adjusted risk for a single symbol."""
    stop_ticks = compute_stop_ticks(request.entry_price, request.stop_price, request.instrument.tick_size)
    if stop_ticks <= 0:
        return SizingDecision(
            approved=False,
            reason_code="INVALID_STOP_DISTANCE",
            routed_symbol=request.instrument.symbol,
            contracts=0,
            risk_dollars=float(request.equity * request.risk_pct),
            stop_ticks=stop_ticks,
            slippage_est_ticks=0.0,
            adjusted_risk_per_contract=0.0,
        )

    risk_dollars = float(request.equity * request.risk_pct)
    atr_ticks = float(request.atr_14_1m_price / request.instrument.tick_size)
    slip = estimate_slippage_ticks(request.instrument.symbol, atr_ticks)

    risk_per_contract = float(stop_ticks * request.instrument.tick_value)
    adjusted = risk_per_contract + (slip.slippage_est_ticks * request.instrument.tick_value) + request.instrument.commission_rt

    if adjusted <= 0.0:
        return SizingDecision(
            approved=False,
            reason_code="INVALID_RISK_PER_CONTRACT",
            routed_symbol=request.instrument.symbol,
            contracts=0,
            risk_dollars=risk_dollars,
            stop_ticks=stop_ticks,
            slippage_est_ticks=slip.slippage_est_ticks,
            adjusted_risk_per_contract=adjusted,
        )

    contracts = int(math.floor(risk_dollars / adjusted))
    if contracts < 1:
        return SizingDecision(
            approved=False,
            reason_code="SIZE_LT_ONE",
            routed_symbol=request.instrument.symbol,
            contracts=0,
            risk_dollars=risk_dollars,
            stop_ticks=stop_ticks,
            slippage_est_ticks=slip.slippage_est_ticks,
            adjusted_risk_per_contract=adjusted,
        )

    return SizingDecision(
        approved=True,
        reason_code="APPROVED",
        routed_symbol=request.instrument.symbol,
        contracts=contracts,
        risk_dollars=risk_dollars,
        stop_ticks=stop_ticks,
        slippage_est_ticks=slip.slippage_est_ticks,
        adjusted_risk_per_contract=adjusted,
    )


def size_with_micro_routing(
    request: SingleLegSizingRequest,
    *,
    instruments_by_symbol: Mapping[str, InstrumentMeta],
) -> SizingDecision:
    """Size on mini first, then route to micro for equity roots when needed."""
    mini_decision = size_single_leg(request)
    if mini_decision.approved:
        return mini_decision

    if request.instrument.family is not Family.EQUITIES:
        return mini_decision

    micro_symbol = request.instrument.micro_equivalent
    if micro_symbol == request.instrument.symbol:
        return mini_decision

    micro = instruments_by_symbol.get(micro_symbol)
    if micro is None:
        return SizingDecision(
            approved=False,
            reason_code="MICRO_INSTRUMENT_NOT_FOUND",
            routed_symbol=micro_symbol,
            contracts=0,
            risk_dollars=mini_decision.risk_dollars,
            stop_ticks=mini_decision.stop_ticks,
            slippage_est_ticks=mini_decision.slippage_est_ticks,
            adjusted_risk_per_contract=mini_decision.adjusted_risk_per_contract,
        )

    routed_request = SingleLegSizingRequest(
        instrument=micro,
        equity=request.equity,
        risk_pct=request.risk_pct,
        entry_price=request.entry_price,
        stop_price=request.stop_price,
        atr_14_1m_price=request.atr_14_1m_price,
    )
    routed_decision = size_single_leg(routed_request)
    if routed_decision.approved:
        return routed_decision

    return SizingDecision(
        approved=False,
        reason_code="SIZE_LT_ONE_AFTER_MICRO_ROUTING",
        routed_symbol=micro.symbol,
        contracts=0,
        risk_dollars=routed_decision.risk_dollars,
        stop_ticks=routed_decision.stop_ticks,
        slippage_est_ticks=routed_decision.slippage_est_ticks,
        adjusted_risk_per_contract=routed_decision.adjusted_risk_per_contract,
    )
