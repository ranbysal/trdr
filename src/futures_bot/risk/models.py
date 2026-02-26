"""Core risk-vault models and decision objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta


@dataclass(frozen=True, slots=True)
class SlippageEstimate:
    symbol: str
    atr_14_1m_in_ticks: float
    base_ticks: float
    k_instrument: float
    slippage_est_ticks: float


@dataclass(frozen=True, slots=True)
class SingleLegSizingRequest:
    instrument: InstrumentMeta
    equity: float
    risk_pct: float
    entry_price: float
    stop_price: float
    atr_14_1m_price: float


@dataclass(frozen=True, slots=True)
class SizingDecision:
    approved: bool
    reason_code: str
    routed_symbol: str
    contracts: int
    risk_dollars: float
    stop_ticks: int
    slippage_est_ticks: float
    adjusted_risk_per_contract: float


@dataclass(frozen=True, slots=True)
class RiskDecision:
    approved: bool
    reason_code: str
    details: dict[str, float | str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OpenRiskEntry:
    symbol: str
    family: Family
    risk_dollars: float


@dataclass(frozen=True, slots=True)
class CooldownState:
    consecutive_losses: int
    cooldown_until: datetime | None
