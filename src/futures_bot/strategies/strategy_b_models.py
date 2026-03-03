"""Data models for Strategy B (Equity VWAP Reversion)."""

from __future__ import annotations

from dataclasses import dataclass

from futures_bot.core.enums import Regime


@dataclass(frozen=True, slots=True)
class StrategyBFeatureSnapshot:
    raw_regime: Regime
    is_weak_neutral: bool
    confidence: float
    session_vwap: float
    atr_14_5m: float
    rvol_3bar_aggregate_5m: float | None
    close_price: float
    data_ok: bool


@dataclass(frozen=True, slots=True)
class StrategyBEvaluation:
    approved: bool
    reason_code: str

