"""Anchored session state and rollover helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class CurrentAnchoredSession:
    instrument_symbol: str
    session_date: date
    anchor_ts: datetime
    has_started: bool


@dataclass(frozen=True, slots=True)
class PreviousCompletedAnchoredSession:
    instrument_symbol: str
    session_date: date
    anchor_ts: datetime


@dataclass(frozen=True, slots=True)
class InstrumentSessionState:
    instrument_symbol: str
    anchor_time: time
    timezone: str
    current_session: CurrentAnchoredSession
    previous_completed_session: PreviousCompletedAnchoredSession | None = None


def anchor_timestamp_for_date(
    session_date: date,
    *,
    anchor_time: time,
    timezone: str | ZoneInfo = ET,
) -> datetime:
    tz = _as_zoneinfo(timezone)
    return datetime.combine(session_date, anchor_time, tzinfo=tz)


def roll_instrument_session_state(
    state: InstrumentSessionState | None,
    *,
    ts: datetime,
    instrument_symbol: str,
    anchor_time: time,
    timezone: str | ZoneInfo = ET,
) -> InstrumentSessionState:
    tz = _as_zoneinfo(timezone)
    ts_local = _to_timezone(ts, tz)
    session_date = ts_local.date()
    anchor_ts = anchor_timestamp_for_date(session_date, anchor_time=anchor_time, timezone=tz)
    has_started = ts_local >= anchor_ts

    if state is None or state.instrument_symbol != instrument_symbol or state.current_session.session_date != session_date:
        previous_completed = state.previous_completed_session if state is not None else None
        if state is not None and state.current_session.has_started:
            previous_completed = PreviousCompletedAnchoredSession(
                instrument_symbol=state.instrument_symbol,
                session_date=state.current_session.session_date,
                anchor_ts=state.current_session.anchor_ts,
            )
        return InstrumentSessionState(
            instrument_symbol=instrument_symbol,
            anchor_time=anchor_time,
            timezone=tz.key,
            current_session=CurrentAnchoredSession(
                instrument_symbol=instrument_symbol,
                session_date=session_date,
                anchor_ts=anchor_ts,
                has_started=has_started,
            ),
            previous_completed_session=previous_completed,
        )

    if state.current_session.has_started == has_started:
        return state

    return InstrumentSessionState(
        instrument_symbol=state.instrument_symbol,
        anchor_time=state.anchor_time,
        timezone=state.timezone,
        current_session=CurrentAnchoredSession(
            instrument_symbol=state.current_session.instrument_symbol,
            session_date=state.current_session.session_date,
            anchor_ts=state.current_session.anchor_ts,
            has_started=has_started,
        ),
        previous_completed_session=state.previous_completed_session,
    )


def effective_anchored_session(
    state: InstrumentSessionState,
    *,
    ts: datetime,
) -> CurrentAnchoredSession | PreviousCompletedAnchoredSession | None:
    tz = _as_zoneinfo(state.timezone)
    ts_local = _to_timezone(ts, tz)
    if ts_local >= state.current_session.anchor_ts:
        return state.current_session
    return state.previous_completed_session


def effective_anchor_timestamp(
    state: InstrumentSessionState,
    *,
    ts: datetime,
) -> datetime | None:
    session = effective_anchored_session(state, ts=ts)
    if session is None:
        return None
    return session.anchor_ts


def _to_timezone(ts: datetime, timezone: ZoneInfo) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone)
    return ts.astimezone(timezone)


def _as_zoneinfo(value: str | ZoneInfo) -> ZoneInfo:
    if isinstance(value, ZoneInfo):
        return value
    return ZoneInfo(value)
