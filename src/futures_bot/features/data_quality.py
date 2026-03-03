"""Live data quality checks for 1-minute WebSocket operation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from futures_bot.core.enums import Family
from futures_bot.core.types import Quote1s

TIMESTAMP_DRIFT_MAX_SECONDS = 2.0
STALE_QUOTE_MAX_SECONDS = 5.0
BAR_GAP_MAX_MINUTES = 2


@dataclass(frozen=True, slots=True)
class DataQualityResult:
    family: Family
    data_ok: bool
    codes: tuple[str, ...] = field(default_factory=tuple)


def is_minute_aligned(ts: datetime) -> bool:
    """Return True when timestamp lands exactly on 1-minute grid."""
    return ts.second == 0 and ts.microsecond == 0


def evaluate_bar_timing(
    *,
    family: Family,
    current_bar_ts: datetime,
    previous_bar_ts: datetime | None,
    is_active_session: bool,
) -> DataQualityResult:
    """Evaluate 1m bar alignment and gap policy for DATA_OK."""
    codes: list[str] = []

    if not is_minute_aligned(current_bar_ts):
        codes.append("BAR_TS_NOT_MINUTE_ALIGNED")

    if is_active_session and previous_bar_ts is not None:
        delta_minutes = int((current_bar_ts - previous_bar_ts).total_seconds() // 60)
        if delta_minutes > BAR_GAP_MAX_MINUTES:
            codes.append("BAR_GAP_GT_2M")

    return DataQualityResult(
        family=family,
        data_ok=(len(codes) == 0),
        codes=tuple(codes),
    )


def evaluate_quote_health(
    *,
    family: Family,
    quote: Quote1s,
    bar_timestamp: datetime,
    now: datetime,
) -> DataQualityResult:
    """Apply quote consistency thresholds and return DATA_OK state."""
    codes: list[str] = []

    drift_seconds = abs((quote.ts - bar_timestamp).total_seconds())
    if drift_seconds > TIMESTAMP_DRIFT_MAX_SECONDS:
        codes.append("QUOTE_TIMESTAMP_DRIFT")

    stale_seconds = (now - quote.ts).total_seconds()
    if stale_seconds > STALE_QUOTE_MAX_SECONDS:
        codes.append("STALE_QUOTE")

    if quote.bid >= quote.ask:
        codes.append("QUOTE_CROSSED_OR_LOCKED")
    if quote.bid_size <= 0.0 and quote.ask_size <= 0.0:
        codes.append("QUOTE_ZERO_BOTH_SIDES")

    return DataQualityResult(
        family=family,
        data_ok=(len(codes) == 0),
        codes=tuple(codes),
    )

