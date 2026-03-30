"""Typed models for the corrected Gold signal architecture."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from futures_bot.core.types import SignalCandidate


class GoldSignalRejection(str, Enum):
    SYMBOL_NOT_IN_SCOPE = "symbol_not_in_scope"
    LIQUIDITY_GATE_FAILED = "liquidity_gate_failed"
    MACRO_NEWS_BLOCKED = "macro_news_blocked"
    INVALID_ATR = "invalid_atr"
    NO_MEAN_REVERSION_OR_STRUCTURAL_SETUP = "no_mean_reversion_or_structural_setup"


class GoldSignalSetup(str, Enum):
    PRIMARY_MEAN_REVERSION = "primary_mean_reversion"
    SECONDARY_STRUCTURAL_ORDER_BLOCK = "secondary_structural_order_block"


@dataclass(frozen=True, slots=True)
class GoldSignalFeatures:
    ts: datetime
    symbol: str
    close_price: float
    anchored_vwap: float
    atr_5m: float
    liquidity_ok: bool
    macro_blocked: bool
    pullback_price: float | None = None
    structure_break_price: float | None = None
    order_block_low: float | None = None
    order_block_high: float | None = None
    choch_confirmed: bool = False
    fvg_present: bool = False


@dataclass(frozen=True, slots=True)
class GoldSignalResult:
    signal: SignalCandidate
    setup: GoldSignalSetup
    vwap_distance_atr: float
    choch_confirmed: bool
    fvg_present: bool


@dataclass(frozen=True, slots=True)
class GoldSignalEvaluation:
    approved: bool
    rejection_reason: GoldSignalRejection | None
    candidate: GoldSignalResult | None
