"""Master CME schedule helpers for supported live futures symbols."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
_OPEN_TIME = time(hour=18, minute=0)
_HALT_START = time(hour=17, minute=0)
_HALT_END = time(hour=18, minute=0)


def market_is_open(now_et: datetime) -> bool:
    now_et = _as_et(now_et)
    weekday = now_et.weekday()
    current = now_et.timetz().replace(tzinfo=None)
    if weekday == 5:
        return False
    if weekday == 6:
        return current >= _OPEN_TIME
    if weekday == 4:
        return current < _HALT_START
    return not (_HALT_START <= current < _HALT_END)


def in_daily_halt(now_et: datetime) -> bool:
    now_et = _as_et(now_et)
    weekday = now_et.weekday()
    current = now_et.timetz().replace(tzinfo=None)
    return weekday in {0, 1, 2, 3} and _HALT_START <= current < _HALT_END


def next_open_time(now_et: datetime) -> datetime:
    now_et = _as_et(now_et)
    if market_is_open(now_et):
        return now_et

    weekday = now_et.weekday()
    current = now_et.timetz().replace(tzinfo=None)
    if weekday in {0, 1, 2, 3} and current < _HALT_END:
        return _combine(now_et, _HALT_END)
    if weekday == 4:
        return _next_sunday_open(now_et)
    if weekday == 5:
        return _combine(now_et + timedelta(days=1), _OPEN_TIME)
    if weekday == 6 and current < _OPEN_TIME:
        return _combine(now_et, _OPEN_TIME)
    return _combine(now_et, _OPEN_TIME)


def next_halt_time(now_et: datetime) -> datetime:
    now_et = _as_et(now_et)
    weekday = now_et.weekday()
    current = now_et.timetz().replace(tzinfo=None)

    if weekday in {0, 1, 2, 3}:
        if current < _HALT_START:
            return _combine(now_et, _HALT_START)
        if current < _HALT_END:
            return _combine(now_et, _HALT_START)
        return _combine(now_et + timedelta(days=1), _HALT_START)
    if weekday == 4:
        return _combine(now_et, _HALT_START) if current < _HALT_START else _combine(now_et + timedelta(days=3), _HALT_START)
    if weekday == 5:
        return _combine(now_et + timedelta(days=2), _HALT_START)
    if weekday == 6:
        return _combine(now_et + timedelta(days=1), _HALT_START)
    return _combine(now_et, _HALT_START)


def schedule_state(now_et: datetime) -> str:
    now_et = _as_et(now_et)
    if in_daily_halt(now_et):
        return "daily_halt"
    if market_is_open(now_et):
        return "open"
    return "closed"


def _as_et(value: datetime) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError("schedule helpers require a timezone-aware datetime")
    return value.astimezone(ET)


def _combine(base: datetime, value: time) -> datetime:
    return datetime.combine(base.date(), value, tzinfo=ET)


def _next_sunday_open(now_et: datetime) -> datetime:
    days_ahead = (6 - now_et.weekday()) % 7
    sunday = now_et + timedelta(days=days_ahead)
    return _combine(sunday, _OPEN_TIME)
