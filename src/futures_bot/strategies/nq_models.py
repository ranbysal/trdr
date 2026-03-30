"""Typed models for the corrected NQ signal architecture."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from futures_bot.core.types import SignalCandidate


class NQSignalRejection(str, Enum):
    SYMBOL_NOT_IN_SCOPE = "symbol_not_in_scope"
    LIQUIDITY_GATE_FAILED = "liquidity_gate_failed"
    MACRO_NEWS_BLOCKED = "macro_news_blocked"
    INVALID_ATR = "invalid_atr"
    EMA_ALIGNMENT_FAILED = "ema_alignment_failed"
    STRUCTURE_CONTINUATION_FAILED = "structure_continuation_failed"
    ORDER_BLOCK_ALIGNMENT_FAILED = "order_block_alignment_failed"


class NQSignalSetup(str, Enum):
    STRUCTURAL_CONTINUATION = "structural_continuation"


@dataclass(frozen=True, slots=True)
class NQSignalFeatures:
    ts: datetime
    symbol: str
    close_price: float
    ema_fast: float
    ema_slow: float
    pullback_price: float
    structure_break_price: float
    order_block_low: float
    order_block_high: float
    atr_5m: float
    liquidity_ok: bool
    macro_blocked: bool
    choch_confirmed: bool = False
    fvg_present: bool = False
    intermarket_confirmed: bool | None = None


@dataclass(frozen=True, slots=True)
class NQSignalResult:
    signal: SignalCandidate
    setup: NQSignalSetup
    order_block_aligned: bool
    choch_confirmed: bool
    fvg_present: bool
    intermarket_confirmed: bool | None


@dataclass(frozen=True, slots=True)
class NQSignalEvaluation:
    approved: bool
    rejection_reason: NQSignalRejection | None
    candidate: NQSignalResult | None
