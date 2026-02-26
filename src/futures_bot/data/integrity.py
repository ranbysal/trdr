"""Integrity helpers and structured log event definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from futures_bot.core.types import Bar1m


class IntegrityError(Exception):
    """Raised when ingestion integrity checks fail."""


@dataclass(frozen=True, slots=True)
class LogEvent:
    """Structured log record for deterministic test assertions."""

    ts: datetime
    code: str
    symbol: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def bars_ohlcv_equal(left: Bar1m, right: Bar1m) -> bool:
    """Return True when two bars have identical OHLCV values."""
    return (
        left.open == right.open
        and left.high == right.high
        and left.low == right.low
        and left.close == right.close
        and left.volume == right.volume
    )
