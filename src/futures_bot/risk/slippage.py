"""Deterministic slippage estimation for CRO risk model."""

from __future__ import annotations

from futures_bot.policy import cro_policy
from futures_bot.risk.models import SlippageEstimate


def slippage_coeff_k(symbol: str) -> float:
    """Return instrument slippage coefficient `k` for CRO model."""
    if symbol not in cro_policy.slippage_k_by_symbol:
        raise ValueError(f"Unsupported symbol for slippage model: {symbol}")
    return cro_policy.slippage_k_by_symbol[symbol]


def slippage_base_ticks(symbol: str) -> float:
    """Return base slippage ticks for instrument."""
    if symbol not in cro_policy.slippage_base_ticks_by_symbol:
        raise ValueError(f"Unsupported symbol for slippage model: {symbol}")
    return cro_policy.slippage_base_ticks_by_symbol[symbol]


def estimate_slippage_ticks(symbol: str, atr_14_1m_in_ticks: float) -> SlippageEstimate:
    """Estimate dynamic slippage in ticks from ATR-in-ticks input."""
    k = slippage_coeff_k(symbol)
    base = slippage_base_ticks(symbol)
    est = base + (k * float(atr_14_1m_in_ticks))
    return SlippageEstimate(
        symbol=symbol,
        atr_14_1m_in_ticks=float(atr_14_1m_in_ticks),
        base_ticks=base,
        k_instrument=k,
        slippage_est_ticks=est,
    )
