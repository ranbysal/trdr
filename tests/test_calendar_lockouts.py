from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family
from futures_bot.data.calendar_store import CalendarStore

ET = ZoneInfo("America/New_York")


def test_tier1_lockout_boundaries_at_exact_session_open() -> None:
    store = CalendarStore()
    event_ts = datetime(2026, 1, 6, 9, 30, tzinfo=ET)
    store.add_tier1_event(event_id="cpi", ts_et=event_ts, affected_family=Family.EQUITIES)

    before_start = store.lockout_status(
        at_et=datetime(2026, 1, 6, 9, 14, 59, tzinfo=ET),
        family=Family.EQUITIES,
        symbol="ES",
    )
    at_start = store.lockout_status(
        at_et=datetime(2026, 1, 6, 9, 15, 0, tzinfo=ET),
        family=Family.EQUITIES,
        symbol="ES",
    )
    at_open = store.lockout_status(
        at_et=datetime(2026, 1, 6, 9, 30, 0, tzinfo=ET),
        family=Family.EQUITIES,
        symbol="ES",
    )

    assert before_start.is_locked_out is False
    assert at_start.is_locked_out is True
    assert at_open.is_locked_out is True


def test_cancel_resting_entries_time() -> None:
    store = CalendarStore()
    event_ts = datetime(2026, 1, 6, 9, 30, tzinfo=ET)
    store.add_tier1_event(event_id="cpi", ts_et=event_ts, affected_family=Family.EQUITIES)

    before_cancel = store.lockout_status(
        at_et=datetime(2026, 1, 6, 9, 27, 59, tzinfo=ET),
        family=Family.EQUITIES,
        symbol="ES",
    )
    at_cancel = store.lockout_status(
        at_et=datetime(2026, 1, 6, 9, 28, 0, tzinfo=ET),
        family=Family.EQUITIES,
        symbol="ES",
    )

    assert before_cancel.cancel_resting_entries is False
    assert at_cancel.cancel_resting_entries is True
