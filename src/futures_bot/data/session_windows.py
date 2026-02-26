"""Session window helpers used by ingestion integrity checks."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family

ET = ZoneInfo("America/New_York")


def to_et(ts: datetime) -> datetime:
    """Normalize timestamps to America/New_York timezone."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)


def is_equities_rth(ts: datetime) -> bool:
    """US equities regular trading hours: 09:30 <= t < 16:00 ET."""
    et = to_et(ts)
    current = et.timetz().replace(tzinfo=None)
    return time(9, 30) <= current < time(16, 0)


def is_equities_strategy_window(ts: datetime) -> bool:
    """Narrow strategy execution window inside RTH."""
    et = to_et(ts)
    current = et.timetz().replace(tzinfo=None)
    return time(9, 35) <= current < time(15, 55)


def is_metals_strategy_window(ts: datetime) -> bool:
    """Representative COMEX intraday window for metals execution."""
    et = to_et(ts)
    current = et.timetz().replace(tzinfo=None)
    return time(8, 20) <= current < time(13, 25)


def is_active_session(ts: datetime, family: Family) -> bool:
    """Active session gate used by data-gap integrity logic."""
    if family is Family.EQUITIES:
        return is_equities_rth(ts)
    if family is Family.METALS:
        return is_metals_strategy_window(ts)
    return False
