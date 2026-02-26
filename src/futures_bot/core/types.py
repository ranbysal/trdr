"""Strongly typed domain models for bot state and events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from futures_bot.core.enums import ApprovalStatus, Family, OrderSide, Regime, StrategyModule


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    symbol: str
    root_symbol: str
    family: Family
    tick_size: float
    tick_value: float
    point_value: float
    commission_rt: float
    symbol_type: str
    micro_equivalent: str
    contract_units: float


@dataclass(frozen=True, slots=True)
class Bar1m:
    ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class Bar5m:
    ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class Quote1s:
    ts: datetime
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    ts: datetime
    event_id: str
    category: str
    severity: str
    headline: str


@dataclass(frozen=True, slots=True)
class SignalCandidate:
    ts: datetime
    strategy: StrategyModule
    symbol: str
    side: OrderSide
    regime: Regime
    score: float


@dataclass(frozen=True, slots=True)
class RiskApproval:
    ts: datetime
    status: ApprovalStatus
    reason: str
    max_qty: int


@dataclass(frozen=True, slots=True)
class ExecutionOrder:
    ts: datetime
    order_id: str
    symbol: str
    side: OrderSide
    qty: int
    tif: str
    limit_price: float | None = None
    stop_price: float | None = None


@dataclass(frozen=True, slots=True)
class FillEvent:
    ts: datetime
    fill_id: str
    order_id: str
    symbol: str
    side: OrderSide
    qty: int
    price: float
    fee: float


@dataclass(frozen=True, slots=True)
class PositionState:
    ts: datetime
    symbol: str
    qty: int
    avg_price: float
    unrealized_pnl: float
    realized_pnl: float


@dataclass(frozen=True, slots=True)
class PairPositionState:
    ts: datetime
    lead_symbol: str
    hedge_symbol: str
    lead_qty: int
    hedge_qty: int
    spread: float
    unrealized_pnl: float
