from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family
from futures_bot.core.types import Quote1s
from futures_bot.features.data_quality import evaluate_quote_health

ET = ZoneInfo("America/New_York")


def _quote(
    ts: datetime,
    *,
    bid: float = 100.0,
    ask: float = 100.25,
    bid_size: float = 10.0,
    ask_size: float = 12.0,
) -> Quote1s:
    return Quote1s(
        ts=ts,
        symbol="ES",
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
    )


def test_quote_timestamp_drift_marks_data_bad() -> None:
    bar_ts = datetime(2026, 1, 5, 9, 30, 0, tzinfo=ET)
    q = _quote(bar_ts + timedelta(seconds=3))
    out = evaluate_quote_health(family=Family.EQUITIES, quote=q, bar_timestamp=bar_ts, now=bar_ts)

    assert out.data_ok is False
    assert "QUOTE_TIMESTAMP_DRIFT" in out.codes


def test_stale_quote_marks_data_bad() -> None:
    quote_ts = datetime(2026, 1, 5, 9, 30, 0, tzinfo=ET)
    now = quote_ts + timedelta(seconds=6)
    bar_ts = quote_ts
    q = _quote(quote_ts)
    out = evaluate_quote_health(family=Family.EQUITIES, quote=q, bar_timestamp=bar_ts, now=now)

    assert out.data_ok is False
    assert "STALE_QUOTE" in out.codes


def test_inconsistent_quote_marks_data_bad() -> None:
    ts = datetime(2026, 1, 5, 9, 30, 0, tzinfo=ET)
    crossed = _quote(ts, bid=100.25, ask=100.25)
    out_crossed = evaluate_quote_health(family=Family.EQUITIES, quote=crossed, bar_timestamp=ts, now=ts)
    assert out_crossed.data_ok is False
    assert "QUOTE_CROSSED_OR_LOCKED" in out_crossed.codes

    zero_sizes = _quote(ts, bid_size=0.0, ask_size=0.0)
    out_zero = evaluate_quote_health(family=Family.EQUITIES, quote=zero_sizes, bar_timestamp=ts, now=ts)
    assert out_zero.data_ok is False
    assert "QUOTE_ZERO_BOTH_SIDES" in out_zero.codes

