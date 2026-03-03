"""Data models for Strategy C (Metals ORB)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from futures_bot.core.enums import OrderSide


@dataclass(frozen=True, slots=True)
class MetalsORSessionState:
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
class StrategyCFeatureSnapshot:
    session_vwap: float
    ema20_5m_slope: float
    atr_14_5m: float
    vol_strong_1m: bool
    rvol_3bar_aggregate_5m: float | None
    tier1_lockout_active: bool


@dataclass(frozen=True, slots=True)
class StrategyCEntryPlan:
    side: OrderSide
    entry_stop: float
    initial_stop: float
    tp1_price: float
    tp2_price: float
    flatten_by: datetime


@dataclass(frozen=True, slots=True)
class StrategyCEvaluation:
    approved: bool
    reason_code: str
    entry_plan: StrategyCEntryPlan | None

