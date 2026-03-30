"""Feature calculation layer shared by all strategies."""

from futures_bot.features.atr_rank import compute_atr_pct_rank
from futures_bot.features.anchored_session import (
    CurrentAnchoredSession,
    InstrumentSessionState,
    PreviousCompletedAnchoredSession,
    anchor_timestamp_for_date,
    effective_anchor_timestamp,
    effective_anchored_session,
    roll_instrument_session_state,
)
from futures_bot.features.data_quality import evaluate_bar_timing, evaluate_quote_health, is_minute_aligned
from futures_bot.features.history_readiness import Readiness, classify_sample_count
from futures_bot.features.indicators_1m import compute_indicators_1m
from futures_bot.features.indicators_5m import compute_indicators_5m
from futures_bot.features.rvol import compute_rvol_tod, median_rvol_3bar
from futures_bot.features.vwap import compute_anchored_vwap_1m, compute_session_vwap_1m, session_start_time

__all__ = [
    "CurrentAnchoredSession",
    "InstrumentSessionState",
    "PreviousCompletedAnchoredSession",
    "Readiness",
    "anchor_timestamp_for_date",
    "classify_sample_count",
    "compute_anchored_vwap_1m",
    "compute_atr_pct_rank",
    "compute_indicators_1m",
    "compute_indicators_5m",
    "compute_rvol_tod",
    "evaluate_bar_timing",
    "effective_anchor_timestamp",
    "effective_anchored_session",
    "evaluate_quote_health",
    "is_minute_aligned",
    "compute_session_vwap_1m",
    "median_rvol_3bar",
    "roll_instrument_session_state",
    "session_start_time",
]
