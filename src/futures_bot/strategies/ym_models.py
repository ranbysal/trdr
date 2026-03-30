"""Typed models for the corrected YM signal architecture."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from futures_bot.core.types import SignalCandidate


class YMSignalRejection(str, Enum):
    SYMBOL_NOT_IN_SCOPE = "symbol_not_in_scope"
    LIQUIDITY_GATE_FAILED = "liquidity_gate_failed"
    MACRO_NEWS_BLOCKED = "macro_news_blocked"
    INVALID_ATR = "invalid_atr"
    NO_MEAN_REVERSION_OR_EMA_CONTINUATION_SETUP = "no_mean_reversion_or_ema_continuation_setup"


class YMSignalSetup(str, Enum):
    PRIMARY_MEAN_REVERSION = "primary_mean_reversion"
    SECONDARY_EMA_CONTINUATION = "secondary_ema_continuation"


@dataclass(frozen=True, slots=True)
class YMSignalFeatures:
    ts: datetime
    symbol: str
    close_price: float
    anchored_vwap: float
    ema_fast: float
    ema_slow: float
    atr_5m: float
    liquidity_ok: bool
    macro_blocked: bool
    choch_confirmed: bool = False
    fvg_present: bool = False
    intermarket_confirmed: bool | None = None


@dataclass(frozen=True, slots=True)
class YMSignalResult:
    signal: SignalCandidate
    setup: YMSignalSetup
    vwap_distance_atr: float
    choch_confirmed: bool
    fvg_present: bool
    intermarket_confirmed: bool | None


@dataclass(frozen=True, slots=True)
class YMSignalEvaluation:
    approved: bool
    rejection_reason: YMSignalRejection | None
    candidate: YMSignalResult | None
