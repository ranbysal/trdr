"""Signal lifecycle models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from futures_bot.core.enums import StrategyModule


class SignalLifecycleState(str, Enum):
    NEW_SIGNAL = "NEW_SIGNAL"
    ENTRY_ZONE_ACTIVE = "ENTRY_ZONE_ACTIVE"
    ENTRY_MISSED = "ENTRY_MISSED"
    IN_POSITION_ASSUMED_FALSE = "IN_POSITION_ASSUMED_FALSE"
    PARTIAL_TAKE_SUGGESTED = "PARTIAL_TAKE_SUGGESTED"
    STOP_TO_BREAKEVEN_SUGGESTED = "STOP_TO_BREAKEVEN_SUGGESTED"
    TP_EXTENSION_SUGGESTED = "TP_EXTENSION_SUGGESTED"
    THESIS_INVALIDATED = "THESIS_INVALIDATED"
    CLOSE_SIGNAL = "CLOSE_SIGNAL"


class AlertKind(str, Enum):
    NEW_SIGNAL = "new_signal"
    UPDATE = "update"
    INVALIDATION = "invalidation"
    CLOSE = "close"


@dataclass(slots=True)
class SignalIdea:
    idea_id: str
    strategy: StrategyModule
    symbol: str
    symbol_display: str
    side: str
    entry_low: float
    entry_high: float
    stop_loss: float
    tp1: float
    tp2: float
    invalidation: str
    partial_profit_guidance: str
    timestamp: datetime
    flatten_by: datetime
    regime: str
    confidence: float
    strategy_context: str
    last_price: float
    pair_hedge_symbol: str | None = None
    pair_beta: float = 0.0
    pair_stop_proxy: float = 0.0
    current_state: SignalLifecycleState = SignalLifecycleState.IN_POSITION_ASSUMED_FALSE
    closed: bool = False
    entry_seen: bool = False
    partial_sent: bool = False
    breakeven_sent: bool = False
    extension_sent: bool = False
    state_history: list[SignalLifecycleState] = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.idea_id

    def material_signature(self) -> tuple[object, ...]:
        return (
            self.strategy.value,
            self.symbol_display,
            self.side,
            round(self.entry_low, 8),
            round(self.entry_high, 8),
            round(self.stop_loss, 8),
            round(self.tp1, 8),
            round(self.tp2, 8),
            self.invalidation,
            self.regime,
            round(self.confidence, 6),
            self.strategy_context,
            self.flatten_by.isoformat(),
        )
