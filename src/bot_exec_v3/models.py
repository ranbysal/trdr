"""Typed models for Bot 3 paper execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Mapping


class Direction(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class SignalStatus(str, Enum):
    RECEIVED = "received"
    REJECTED = "rejected"
    ORDER_PENDING = "order_pending"
    POSITION_OPEN = "position_open"
    POSITION_CLOSED = "position_closed"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    CLOSED = "closed"


class PositionStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"


class FillType(str, Enum):
    ENTRY = "entry"
    TP1 = "tp1"
    TP2 = "tp2"
    TP3 = "tp3"
    STOP = "stop"
    CLOSE = "close"


@dataclass(frozen=True, slots=True)
class SignalEvent:
    signal_id: str
    source_bot: str
    instrument: str
    direction: Direction
    setup_type: str
    session: str
    confluence: float
    formed_timestamp_et: datetime
    entry: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    notes: str
    freshness_seconds: int

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "SignalEvent":
        return cls(
            signal_id=str(payload["signal_id"]),
            source_bot=str(payload["source_bot"]),
            instrument=str(payload["instrument"]),
            direction=Direction(str(payload["direction"]).upper()),
            setup_type=str(payload["setup_type"]),
            session=str(payload["session"]),
            confluence=float(payload["confluence"]),
            formed_timestamp_et=datetime.fromisoformat(str(payload["formed_timestamp_et"])),
            entry=float(payload["entry"]),
            stop=float(payload["stop"]),
            tp1=float(payload["tp1"]),
            tp2=float(payload["tp2"]),
            tp3=float(payload["tp3"]),
            notes=str(payload.get("notes", "")),
            freshness_seconds=int(payload["freshness_seconds"]),
        )


@dataclass(frozen=True, slots=True)
class MarketBar:
    instrument: str
    timestamp_et: datetime
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True, slots=True)
class SizingConfig:
    default_contracts: int
    contracts_by_instrument: dict[str, int] = field(default_factory=dict)
    risk_per_trade_percent: float | None = None
    account_size: float | None = None
    point_value_by_instrument: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExecutorConfig:
    enabled: bool
    source_bot: str
    signal_queue_path: Path
    sqlite_path: Path
    freshness_seconds: int
    paper_mode: bool
    sizing: SizingConfig


@dataclass(frozen=True, slots=True)
class PositionPlan:
    quantity: int
    tp1_quantity: int
    tp2_quantity: int
    tp3_quantity: int
    point_value: float


@dataclass(frozen=True, slots=True)
class SubmitSignalResult:
    accepted: bool
    signal_id: str
    reason: str | None
    order_id: str | None
    position_size: int | None


@dataclass(frozen=True, slots=True)
class MarketUpdateResult:
    filled_order_ids: tuple[str, ...] = ()
    updated_position_ids: tuple[str, ...] = ()
    closed_position_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PendingOrderRecord:
    order_id: str
    signal_id: str
    instrument: str
    direction: Direction
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    quantity: int
    tp1_quantity: int
    tp2_quantity: int
    tp3_quantity: int
    point_value: float
    status: OrderStatus


@dataclass(frozen=True, slots=True)
class OpenPositionRecord:
    position_id: str
    order_id: str
    signal_id: str
    instrument: str
    direction: Direction
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    tp3_price: float
    quantity_initial: int
    quantity_open: int
    tp1_quantity: int
    tp2_quantity: int
    tp3_quantity: int
    tp1_filled_quantity: int
    tp2_filled_quantity: int
    tp3_filled_quantity: int
    point_value: float
    realized_pnl: float
    status: PositionStatus
