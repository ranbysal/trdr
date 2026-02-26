"""Data models for Strategy A (Equity ORB)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from futures_bot.core.enums import OrderSide, Regime
from futures_bot.core.types import SignalCandidate


@dataclass(frozen=True, slots=True)
class ORSessionState:
    symbol: str
    session_date: date
    or_high: float | None = None
    or_low: float | None = None
    bar_count: int = 0
    is_complete: bool = False

    @property
    def or_width(self) -> float | None:
        if self.or_high is None or self.or_low is None:
            return None
        return self.or_high - self.or_low

    @property
    def or_midpoint(self) -> float | None:
        if self.or_high is None or self.or_low is None:
            return None
        return (self.or_high + self.or_low) / 2.0


@dataclass(frozen=True, slots=True)
class StrategyAFeatureSnapshot:
    raw_regime: Regime
    low_volume_trend_streak_5m: int
    vol_strong_1m: bool
    rvol_3bar_aggregate_5m: float | None
    session_vwap: float
    ema9_5m: float
    ema21_5m: float
    atr_14_5m: float
    tier1_lockout_active: bool


@dataclass(frozen=True, slots=True)
class StrategyAEntryPlan:
    side: OrderSide
    entry_stop: float
    stop_limit_chase_ticks: int
    initial_stop: float
    tp1_price: float
    tp2_price: float
    tp1_size_frac: float
    tp2_size_frac: float
    tp3_size_frac: float
    tp3_trail_rule: str
    breakeven_stop_after_tp1: float
    flatten_by: datetime


@dataclass(frozen=True, slots=True)
class StrategyAEvaluation:
    approved: bool
    reason_code: str
    signal: SignalCandidate | None
    entry_plan: StrategyAEntryPlan | None
