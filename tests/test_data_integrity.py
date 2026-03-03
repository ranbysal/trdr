from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.core.types import Bar1m
from futures_bot.data.bar_store import SymbolBarStore

ET = ZoneInfo("America/New_York")


def _bar(ts: datetime, close: float = 101.0) -> Bar1m:
    return Bar1m(ts=ts, symbol="ES", open=100.0, high=102.0, low=99.5, close=close, volume=10.0)


def test_duplicate_bar_identical_is_ignored() -> None:
    ts = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    store = SymbolBarStore("ES")

    first = store.ingest(_bar(ts), provisional=False, is_active_session=True)
    second = store.ingest(_bar(ts), provisional=False, is_active_session=True)

    assert first.accepted is True
    assert second.ignored is True
    assert store.data_ok is True
    assert any(event.code == "DUPLICATE_BAR_IGNORED" for event in store.logs)


def test_post_final_revision_sets_data_bad() -> None:
    ts = datetime(2026, 1, 5, 9, 31, tzinfo=ET)
    store = SymbolBarStore("ES")

    store.ingest(_bar(ts, close=101.0), provisional=False, is_active_session=True)
    result = store.ingest(_bar(ts, close=100.5), provisional=True, is_active_session=True)

    assert result.accepted is False
    assert store.data_ok is False
    assert any(event.code == "BAR_REVISION_AFTER_FINAL" for event in store.logs)


def test_gap_detection_marks_data_bad_when_not_opening_exception() -> None:
    ts0 = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    ts1 = ts0 + timedelta(minutes=3)
    store = SymbolBarStore("ES")

    store.ingest(_bar(ts0), provisional=False, is_active_session=True)
    store.ingest(_bar(ts1), provisional=False, is_active_session=True)

    assert store.data_ok is False
    assert any(event.code == "GAP_FLAG" for event in store.logs)


def test_opening_gap_flag_does_not_force_data_bad() -> None:
    ts0 = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    ts1 = ts0 + timedelta(minutes=3)
    store = SymbolBarStore("ES")

    store.ingest(_bar(ts0), provisional=False, is_active_session=True)
    store.ingest(
        _bar(ts1),
        provisional=False,
        is_active_session=True,
        opening_gap=True,
    )

    assert store.data_ok is True
    assert any(event.code == "GAP_FLAG" for event in store.logs)


def test_rejects_non_minute_aligned_bar_timestamp() -> None:
    ts = datetime(2026, 1, 5, 9, 30, 1, tzinfo=ET)
    store = SymbolBarStore("ES")

    result = store.ingest(_bar(ts), provisional=False, is_active_session=True)

    assert result.accepted is False
    assert store.data_ok is False
    assert any(event.code == "BAR_TS_NOT_MINUTE_ALIGNED" for event in store.logs)
