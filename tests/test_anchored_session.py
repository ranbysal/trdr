from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from futures_bot.features.anchored_session import (
    PreviousCompletedAnchoredSession,
    effective_anchored_session,
    roll_instrument_session_state,
)

ET = ZoneInfo("America/New_York")


def test_anchored_session_rollover_moves_current_to_previous_completed() -> None:
    state = roll_instrument_session_state(
        None,
        ts=datetime(2026, 1, 5, 10, 0, tzinfo=ET),
        instrument_symbol="NQ",
        anchor_time=time(9, 30),
    )

    rolled = roll_instrument_session_state(
        state,
        ts=datetime(2026, 1, 6, 8, 59, tzinfo=ET),
        instrument_symbol="NQ",
        anchor_time=time(9, 30),
    )

    assert rolled.current_session.session_date == date(2026, 1, 6)
    assert rolled.current_session.has_started is False
    assert rolled.previous_completed_session is not None
    assert rolled.previous_completed_session.session_date == date(2026, 1, 5)


def test_pre_anchor_uses_previous_completed_anchored_session() -> None:
    state = roll_instrument_session_state(
        None,
        ts=datetime(2026, 1, 5, 10, 0, tzinfo=ET),
        instrument_symbol="YM",
        anchor_time=time(9, 30),
    )
    state = roll_instrument_session_state(
        state,
        ts=datetime(2026, 1, 6, 9, 0, tzinfo=ET),
        instrument_symbol="YM",
        anchor_time=time(9, 30),
    )

    effective = effective_anchored_session(state, ts=datetime(2026, 1, 6, 9, 0, tzinfo=ET))

    assert isinstance(effective, PreviousCompletedAnchoredSession)
    assert effective.session_date == date(2026, 1, 5)
