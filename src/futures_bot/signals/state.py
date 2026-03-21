"""Alert state manager for signal lifecycle tracking."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.runtime.ndjson_writer import NdjsonWriter
from futures_bot.signals.models import AlertKind, SignalIdea, SignalLifecycleState


class AlertStateManager:
    def __init__(
        self,
        *,
        out_dir: str | Path,
        state_dir: str | Path | None = None,
        notifier: TelegramNotifier | None = None,
        on_emit: Callable[[SignalIdea, AlertKind, SignalLifecycleState, TelegramDelivery], None] | None = None,
    ) -> None:
        out_path = Path(out_dir)
        snapshot_path = Path(state_dir) if state_dir is not None else out_path
        out_path.mkdir(parents=True, exist_ok=True)
        snapshot_path.mkdir(parents=True, exist_ok=True)
        self._notifier = notifier or TelegramNotifier()
        self._on_emit = on_emit
        self._event_log = NdjsonWriter(out_path / "signal_events.ndjson")
        self._snapshot_path = snapshot_path / "active_ideas.json"
        self._active: dict[str, SignalIdea] = {}

    def register(self, idea: SignalIdea) -> None:
        existing = self._active.get(idea.key)
        if existing is not None and existing.material_signature() == idea.material_signature():
            return
        kind = AlertKind.NEW_SIGNAL
        if existing is not None:
            kind = AlertKind.UPDATE
            idea.entry_seen = existing.entry_seen
            idea.partial_sent = existing.partial_sent
            idea.breakeven_sent = existing.breakeven_sent
            idea.extension_sent = existing.extension_sent
            idea.state_history = list(existing.state_history)
            idea.current_state = existing.current_state
        self._active[idea.key] = idea
        self._emit(kind=kind, idea=idea, state=SignalLifecycleState.NEW_SIGNAL)
        idea.current_state = SignalLifecycleState.IN_POSITION_ASSUMED_FALSE
        self._persist_snapshot()

    def active_symbols(self) -> set[str]:
        symbols: set[str] = set()
        for idea in self._active.values():
            if idea.closed:
                continue
            symbols.add(idea.symbol)
            if idea.pair_hedge_symbol:
                symbols.add(idea.pair_hedge_symbol)
        return symbols

    def active_count(self) -> int:
        return sum(1 for idea in self._active.values() if not idea.closed)

    def snapshot_records(self) -> list[dict[str, Any]]:
        return [self._snapshot_record(idea) for idea in self._active.values() if not idea.closed]

    def restore(self, records: list[dict[str, Any]]) -> None:
        restored: dict[str, SignalIdea] = {}
        for record in records:
            try:
                idea = self._idea_from_record(record)
            except (KeyError, TypeError, ValueError):
                continue
            restored[idea.key] = idea
        self._active = restored
        self._persist_snapshot()

    def process_market(
        self,
        *,
        ts: datetime,
        symbol: str,
        high: float,
        low: float,
        close: float,
        regime: str,
        confidence: float,
        latest_prices: dict[str, float],
    ) -> None:
        for idea in list(self._active.values()):
            if idea.closed:
                continue
            if symbol not in {idea.symbol, idea.pair_hedge_symbol}:
                continue
            if idea.pair_hedge_symbol is None:
                self._process_single(
                    idea=idea,
                    ts=ts,
                    high=high,
                    low=low,
                    close=close,
                    regime=regime,
                    confidence=confidence,
                )
            else:
                self._process_pair(
                    idea=idea,
                    ts=ts,
                    latest_prices=latest_prices,
                    regime=regime,
                    confidence=confidence,
                )
        self._persist_snapshot()

    def flush(self) -> None:
        self._event_log.flush()

    def _process_single(
        self,
        *,
        idea: SignalIdea,
        ts: datetime,
        high: float,
        low: float,
        close: float,
        regime: str,
        confidence: float,
    ) -> None:
        idea.last_price = close
        if ts >= idea.flatten_by:
            terminal_state = SignalLifecycleState.CLOSE_SIGNAL if idea.entry_seen else SignalLifecycleState.ENTRY_MISSED
            note = "setup timed out after entry zone activity" if idea.entry_seen else "entry window expired before trigger"
            self._emit(kind=AlertKind.CLOSE if idea.entry_seen else AlertKind.UPDATE, idea=idea, state=terminal_state, note=note)
            self._close(idea)
            return

        if self._crossed_stop(idea=idea, high=high, low=low):
            self._emit(
                kind=AlertKind.INVALIDATION,
                idea=idea,
                state=SignalLifecycleState.THESIS_INVALIDATED,
                note="price breached the invalidation level",
            )
            self._close(idea)
            return

        if not idea.entry_seen and self._touched_entry_zone(idea=idea, high=high, low=low):
            idea.entry_seen = True
            idea.current_state = SignalLifecycleState.ENTRY_ZONE_ACTIVE
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.ENTRY_ZONE_ACTIVE,
                note="entry range is active; continue to wait for confirmation",
            )

        if not idea.entry_seen:
            return

        if not idea.partial_sent and self._hit_target(level=idea.tp1, side=idea.side, high=high, low=low):
            idea.partial_sent = True
            idea.current_state = SignalLifecycleState.PARTIAL_TAKE_SUGGESTED
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.PARTIAL_TAKE_SUGGESTED,
                note="TP1 reached; take the planned partial now",
            )

        if idea.partial_sent and not idea.breakeven_sent:
            idea.breakeven_sent = True
            idea.current_state = SignalLifecycleState.STOP_TO_BREAKEVEN_SUGGESTED
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.STOP_TO_BREAKEVEN_SUGGESTED,
                note="after TP1, stop can be tightened to breakeven",
            )

        if not idea.extension_sent and self._hit_target(level=idea.tp2, side=idea.side, high=high, low=low):
            if self._extension_favored(side=idea.side, close=close, tp2=idea.tp2, regime=regime, confidence=confidence):
                idea.extension_sent = True
                idea.current_state = SignalLifecycleState.TP_EXTENSION_SUGGESTED
                self._emit(
                    kind=AlertKind.UPDATE,
                    idea=idea,
                    state=SignalLifecycleState.TP_EXTENSION_SUGGESTED,
                    note="TP2 reached with trend support; consider holding a runner for extension",
                )
                return
            self._emit(
                kind=AlertKind.CLOSE,
                idea=idea,
                state=SignalLifecycleState.CLOSE_SIGNAL,
                note="TP2 reached without extension conditions; close the setup",
            )
            self._close(idea)

    def _process_pair(
        self,
        *,
        idea: SignalIdea,
        ts: datetime,
        latest_prices: dict[str, float],
        regime: str,
        confidence: float,
    ) -> None:
        hedge_symbol = idea.pair_hedge_symbol
        if hedge_symbol is None:
            return
        lead_price = latest_prices.get(idea.symbol)
        hedge_price = latest_prices.get(hedge_symbol)
        if lead_price is None or hedge_price is None:
            return
        spread = lead_price - (idea.pair_beta * hedge_price)
        idea.last_price = spread

        if ts >= idea.flatten_by:
            terminal_state = SignalLifecycleState.CLOSE_SIGNAL if idea.entry_seen else SignalLifecycleState.ENTRY_MISSED
            self._emit(
                kind=AlertKind.CLOSE if idea.entry_seen else AlertKind.UPDATE,
                idea=idea,
                state=terminal_state,
                note="pair setup expired",
            )
            self._close(idea)
            return

        if self._pair_invalidated(idea=idea, spread=spread):
            self._emit(
                kind=AlertKind.INVALIDATION,
                idea=idea,
                state=SignalLifecycleState.THESIS_INVALIDATED,
                note="spread breached the invalidation proxy",
            )
            self._close(idea)
            return

        if not idea.entry_seen and idea.entry_low <= spread <= idea.entry_high:
            idea.entry_seen = True
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.ENTRY_ZONE_ACTIVE,
                note="pair spread is trading in the entry zone",
            )

        if not idea.entry_seen:
            return

        if not idea.partial_sent and self._pair_target_hit(idea=idea, spread=spread, level=idea.tp1):
            idea.partial_sent = True
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.PARTIAL_TAKE_SUGGESTED,
                note="spread reached TP1 proxy; take a partial if involved",
            )

        if idea.partial_sent and not idea.breakeven_sent:
            idea.breakeven_sent = True
            self._emit(
                kind=AlertKind.UPDATE,
                idea=idea,
                state=SignalLifecycleState.STOP_TO_BREAKEVEN_SUGGESTED,
                note="spread mean-reverted enough to tighten risk to breakeven",
            )

        if self._pair_target_hit(idea=idea, spread=spread, level=idea.tp2):
            if regime == "trend" and confidence >= 0.8:
                if not idea.extension_sent:
                    idea.extension_sent = True
                    self._emit(
                        kind=AlertKind.UPDATE,
                        idea=idea,
                        state=SignalLifecycleState.TP_EXTENSION_SUGGESTED,
                        note="spread keeps extending; higher TP may be available",
                    )
                return
            self._emit(
                kind=AlertKind.CLOSE,
                idea=idea,
                state=SignalLifecycleState.CLOSE_SIGNAL,
                note="pair target proxy reached; close the idea",
            )
            self._close(idea)

    def _emit(
        self,
        *,
        kind: AlertKind,
        idea: SignalIdea,
        state: SignalLifecycleState,
        note: str | None = None,
    ) -> None:
        delivery = self._notifier.send(kind=kind, idea=idea, state=state, note=note)
        idea.state_history.append(state)
        idea.current_state = state
        self._event_log.write(
            {
                "ts": datetime.now(UTC).isoformat(),
                "event": "signal_alert",
                "alert_kind": kind.value,
                "state": state.value,
                "idea_id": idea.idea_id,
                "strategy": idea.strategy.value,
                "symbol": idea.symbol_display,
                "side": idea.side,
                "delivered": delivery.delivered,
                "delivery_error": delivery.error,
                "message": delivery.message,
                "note": note,
            }
        )
        if self._on_emit is not None:
            self._on_emit(idea, kind, state, delivery)

    def _close(self, idea: SignalIdea) -> None:
        idea.closed = True
        self._active.pop(idea.key, None)

    def _persist_snapshot(self) -> None:
        records = self.snapshot_records()
        tmp_path = self._snapshot_path.with_suffix(f"{self._snapshot_path.suffix}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(records, indent=2, sort_keys=True))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, self._snapshot_path)

    def _snapshot_record(self, idea: SignalIdea) -> dict[str, Any]:
        payload = asdict(idea)
        payload["strategy"] = idea.strategy.value
        payload["current_state"] = idea.current_state.value
        payload["state_history"] = [state.value for state in idea.state_history]
        payload["timestamp"] = idea.timestamp.isoformat()
        payload["flatten_by"] = idea.flatten_by.isoformat()
        return payload

    def _idea_from_record(self, payload: dict[str, Any]) -> SignalIdea:
        return SignalIdea(
            idea_id=str(payload["idea_id"]),
            strategy=self._strategy_from_value(str(payload["strategy"])),
            symbol=str(payload["symbol"]),
            symbol_display=str(payload["symbol_display"]),
            side=str(payload["side"]),
            entry_low=float(payload["entry_low"]),
            entry_high=float(payload["entry_high"]),
            stop_loss=float(payload["stop_loss"]),
            tp1=float(payload["tp1"]),
            tp2=float(payload["tp2"]),
            invalidation=str(payload["invalidation"]),
            partial_profit_guidance=str(payload["partial_profit_guidance"]),
            timestamp=datetime.fromisoformat(str(payload["timestamp"])),
            flatten_by=datetime.fromisoformat(str(payload["flatten_by"])),
            regime=str(payload["regime"]),
            confidence=float(payload["confidence"]),
            strategy_context=str(payload["strategy_context"]),
            last_price=float(payload["last_price"]),
            pair_hedge_symbol=str(payload["pair_hedge_symbol"]) if payload.get("pair_hedge_symbol") is not None else None,
            pair_beta=float(payload.get("pair_beta", 0.0)),
            pair_stop_proxy=float(payload.get("pair_stop_proxy", 0.0)),
            current_state=SignalLifecycleState(str(payload.get("current_state", SignalLifecycleState.IN_POSITION_ASSUMED_FALSE.value))),
            closed=bool(payload.get("closed", False)),
            entry_seen=bool(payload.get("entry_seen", False)),
            partial_sent=bool(payload.get("partial_sent", False)),
            breakeven_sent=bool(payload.get("breakeven_sent", False)),
            extension_sent=bool(payload.get("extension_sent", False)),
            state_history=[
                SignalLifecycleState(str(item)) for item in list(payload.get("state_history", []))
            ],
        )

    def _strategy_from_value(self, value: str):
        from futures_bot.core.enums import StrategyModule

        return StrategyModule(value)

    def _touched_entry_zone(self, *, idea: SignalIdea, high: float, low: float) -> bool:
        return low <= idea.entry_high and high >= idea.entry_low

    def _crossed_stop(self, *, idea: SignalIdea, high: float, low: float) -> bool:
        if idea.side == "BUY":
            return low <= idea.stop_loss
        return high >= idea.stop_loss

    def _hit_target(self, *, level: float, side: str, high: float, low: float) -> bool:
        if side == "BUY":
            return high >= level
        return low <= level

    def _extension_favored(self, *, side: str, close: float, tp2: float, regime: str, confidence: float) -> bool:
        if regime != "trend" or confidence < 0.8:
            return False
        return close >= tp2 if side == "BUY" else close <= tp2

    def _pair_invalidated(self, *, idea: SignalIdea, spread: float) -> bool:
        if idea.side == "LONG_SPREAD":
            return spread <= idea.stop_loss
        return spread >= idea.stop_loss

    def _pair_target_hit(self, *, idea: SignalIdea, spread: float, level: float) -> bool:
        if idea.side == "LONG_SPREAD":
            return spread >= level
        return spread <= level
