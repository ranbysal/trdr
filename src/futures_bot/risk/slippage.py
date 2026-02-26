"""Deterministic slippage estimation for CRO risk model."""

from __future__ import annotations

from futures_bot.risk.models import SlippageEstimate

_K_BY_SYMBOL: dict[str, float] = {
    "NQ": 0.08,
    "MNQ": 0.08,
    "YM": 0.06,
    "MYM": 0.06,
    "MGC": 0.10,
    "SIL": 0.15,
}

_BASE_TICKS_BY_SYMBOL: dict[str, float] = {
    "SIL": 2.0,
}


def slippage_coeff_k(symbol: str) -> float:
    """Return instrument slippage coefficient `k` for CRO model."""
    if symbol not in _K_BY_SYMBOL:
        raise ValueError(f"Unsupported symbol for slippage model: {symbol}")
    return _K_BY_SYMBOL[symbol]


def slippage_base_ticks(symbol: str) -> float:
    """Return base slippage ticks for instrument."""
    return _BASE_TICKS_BY_SYMBOL.get(symbol, 1.0)


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
