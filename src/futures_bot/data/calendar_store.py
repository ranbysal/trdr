"""Calendar lockout modeling for Tier1 macro events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class Tier1Event:
    event_id: str
    ts_et: datetime
    affected_family: Family
    affected_symbols: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class LockoutStatus:
    is_locked_out: bool
    cancel_resting_entries: bool
    code: str | None = None


class CalendarStore:
    """Stores Tier1 events and evaluates lockout windows in ET."""

    def __init__(self) -> None:
        self._events: list[Tier1Event] = []

    def add_tier1_event(
        self,
        *,
        event_id: str,
        ts_et: datetime,
        affected_family: Family,
        affected_symbols: set[str] | None = None,
    ) -> None:
        symbols = frozenset(affected_symbols) if affected_symbols is not None else None
        self._events.append(
            Tier1Event(
                event_id=event_id,
                ts_et=_to_et(ts_et),
                affected_family=affected_family,
                affected_symbols=symbols,
            )
        )

    def lockout_status(self, *, at_et: datetime, family: Family, symbol: str | None = None) -> LockoutStatus:
        ts = _to_et(at_et)
        locked = False
        cancel_entries = False

        for event in self._events:
            if not _event_applies(event=event, family=family, symbol=symbol):
                continue

            lockout_start = event.ts_et - timedelta(minutes=15)
            lockout_end = event.ts_et + timedelta(minutes=20)
            cancel_resting_entries_at = event.ts_et - timedelta(minutes=2)

            if lockout_start <= ts <= lockout_end:
                locked = True
            if cancel_resting_entries_at <= ts <= lockout_end:
                cancel_entries = True

        return LockoutStatus(
            is_locked_out=locked,
            cancel_resting_entries=cancel_entries,
            code="CALENDAR_LOCKOUT" if locked else None,
        )


def _event_applies(*, event: Tier1Event, family: Family, symbol: str | None) -> bool:
    if event.affected_family is not family:
        return False

    if event.affected_symbols is None:
        return True

    return symbol is not None and symbol in event.affected_symbols


def _to_et(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)
