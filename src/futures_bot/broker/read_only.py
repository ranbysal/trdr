"""Read-only broker monitoring interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_id: str
    broker: str
    captured_at: datetime
    net_liquidation: float
    buying_power: float
    currency: str


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    symbol: str
    qty: float
    avg_price: float
    unrealized_pnl: float
    realized_pnl: float
    captured_at: datetime


class ReadOnlyBrokerClient(Protocol):
    async def fetch_account_snapshot(self) -> AccountSnapshot:
        """Poll account state without any execution capability."""

    async def fetch_open_positions(self) -> list[PositionSnapshot]:
        """Poll current positions without any execution capability."""
