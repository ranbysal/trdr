"""Time utility functions for ET-aware timestamp handling."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def to_et(dt: datetime) -> datetime:
    """Convert a datetime to US/Eastern timezone; naive datetimes are assumed ET."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def is_1m_boundary(dt: datetime) -> bool:
    """True when timestamp is on a 1-minute candle boundary."""
    et = to_et(dt)
    return et.second == 0 and et.microsecond == 0


def is_5m_boundary(dt: datetime) -> bool:
    """True when timestamp is on a 5-minute candle boundary."""
    et = to_et(dt)
    return is_1m_boundary(et) and et.minute % 5 == 0
