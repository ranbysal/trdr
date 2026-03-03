"""Strategy D: Pair trading helpers (EWLS + AR(1) half-life)."""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np

from futures_bot.core.enums import StrategyModule


@dataclass(frozen=True, slots=True)
class PairSignal:
    approved: bool
    reason_code: str
    strategy: StrategyModule
    lead_symbol: str
    hedge_symbol: str
    side: str
    zscore: float
    stop_risk_proxy: float
    hedge_beta: float
    ar1_phi: float
    half_life_bars: float


def ewls_beta(lead_prices: np.ndarray, hedge_prices: np.ndarray, lam: float = 0.97) -> float:
    """Exponentially weighted least-squares beta for lead ~ beta * hedge."""
    if lead_prices.size != hedge_prices.size or lead_prices.size < 3:
        raise ValueError("EWLS requires equal-length arrays with >=3 samples")
    if not (0.0 < lam <= 1.0):
        raise ValueError("lam must be in (0, 1]")

    n = int(lead_prices.size)
    powers = np.arange(n - 1, -1, -1, dtype=float)
    w = np.power(lam, powers)
    x = hedge_prices.astype(float)
    y = lead_prices.astype(float)
    denom = float(np.sum(w * x * x))
    if denom <= 0.0:
        return 0.0
    numer = float(np.sum(w * x * y))
    return numer / denom


def spread_zscore(lead_prices: np.ndarray, hedge_prices: np.ndarray, beta: float, window: int = 60) -> float:
    if lead_prices.size != hedge_prices.size or lead_prices.size < window:
        raise ValueError("zscore requires equal-length arrays with size >= window")
    spread = lead_prices.astype(float) - (beta * hedge_prices.astype(float))
    tail = spread[-window:]
    mean = float(np.mean(tail))
    std = float(np.std(tail, ddof=0))
    if std <= 0.0:
        return 0.0
    return float((tail[-1] - mean) / std)


def fit_ar1_phi(series: np.ndarray) -> float:
    if series.size < 3:
        raise ValueError("AR(1) fit requires >=3 samples")
    y = series[1:].astype(float)
    x = series[:-1].astype(float)
    denom = float(np.dot(x, x))
    if denom <= 0.0:
        return 0.0
    return float(np.dot(x, y) / denom)


def ar1_half_life(phi: float) -> float:
    abs_phi = abs(float(phi))
    if abs_phi <= 0.0:
        return 0.0
    if abs_phi >= 1.0:
        return math.inf
    return float(-math.log(2.0) / math.log(abs_phi))


def evaluate_pair_signal(
    *,
    lead_symbol: str,
    hedge_symbol: str,
    lead_prices: np.ndarray,
    hedge_prices: np.ndarray,
    ewls_lambda: float = 0.97,
    z_window: int = 60,
    entry_abs_z: float = 2.0,
    max_abs_z: float = 4.5,
    max_half_life_bars: float = 80.0,
    data_ok: bool = True,
) -> PairSignal:
    if not data_ok:
        return PairSignal(
            approved=False,
            reason_code="DATA_NOT_OK",
            strategy=StrategyModule.STRAT_D_PAIR,
            lead_symbol=lead_symbol,
            hedge_symbol=hedge_symbol,
            side="none",
            zscore=0.0,
            stop_risk_proxy=0.0,
            hedge_beta=0.0,
            ar1_phi=0.0,
            half_life_bars=math.inf,
        )

    beta = ewls_beta(lead_prices, hedge_prices, lam=ewls_lambda)
    z = spread_zscore(lead_prices, hedge_prices, beta=beta, window=z_window)
    spread = lead_prices.astype(float) - (beta * hedge_prices.astype(float))
    phi = fit_ar1_phi(spread[-max(z_window, 20) :])
    half_life = ar1_half_life(phi)
    stop_proxy = abs(z)

    if not math.isfinite(half_life) or half_life > max_half_life_bars:
        return PairSignal(
            approved=False,
            reason_code="HALF_LIFE_TOO_LONG",
            strategy=StrategyModule.STRAT_D_PAIR,
            lead_symbol=lead_symbol,
            hedge_symbol=hedge_symbol,
            side="none",
            zscore=z,
            stop_risk_proxy=stop_proxy,
            hedge_beta=beta,
            ar1_phi=phi,
            half_life_bars=half_life,
        )

    if stop_proxy < entry_abs_z or stop_proxy > max_abs_z:
        return PairSignal(
            approved=False,
            reason_code="ABS_Z_NOT_IN_ENTRY_RANGE",
            strategy=StrategyModule.STRAT_D_PAIR,
            lead_symbol=lead_symbol,
            hedge_symbol=hedge_symbol,
            side="none",
            zscore=z,
            stop_risk_proxy=stop_proxy,
            hedge_beta=beta,
            ar1_phi=phi,
            half_life_bars=half_life,
        )

    # If z>0 spread is rich -> short spread; if z<0 spread is cheap -> long spread.
    side = "short_spread" if z > 0.0 else "long_spread"
    return PairSignal(
        approved=True,
        reason_code="APPROVED",
        strategy=StrategyModule.STRAT_D_PAIR,
        lead_symbol=lead_symbol,
        hedge_symbol=hedge_symbol,
        side=side,
        zscore=z,
        stop_risk_proxy=stop_proxy,
        hedge_beta=beta,
        ar1_phi=phi,
        half_life_bars=half_life,
    )

