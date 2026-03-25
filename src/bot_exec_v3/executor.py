"""Deterministic paper execution engine for Bot 3."""

from __future__ import annotations

from datetime import datetime

from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import (
    Direction,
    ExecutionEventType,
    ExecutionRuntimeEvent,
    ExecutorConfig,
    FillType,
    MarketBar,
    MarketUpdateResult,
    OpenPositionRecord,
    SignalEvent,
    SignalStatus,
    SubmitSignalResult,
)
from bot_exec_v3.risk import PaperRiskSizer


class PaperExecutor:
    def __init__(
        self,
        *,
        config: ExecutorConfig,
        journal: PaperTradeJournal,
        risk_sizer: PaperRiskSizer,
    ) -> None:
        self._config = config
        self._journal = journal
        self._risk_sizer = risk_sizer

    def submit_signal(self, signal: SignalEvent, *, received_at_et: datetime) -> SubmitSignalResult:
        if self._journal.signal_exists(signal.signal_id):
            return SubmitSignalResult(
                accepted=False,
                signal_id=signal.signal_id,
                reason="duplicate signal_id",
                order_id=None,
                position_size=None,
                events=(
                    ExecutionRuntimeEvent(
                        event_type=ExecutionEventType.SIGNAL_REJECTED,
                        signal_id=signal.signal_id,
                        instrument=signal.instrument,
                        timestamp_et=received_at_et,
                        message="duplicate signal_id",
                    ),
                ),
            )
        if signal.source_bot != self._config.source_bot:
            self._journal.record_signal(
                signal=signal,
                received_at_et=received_at_et,
                status=SignalStatus.REJECTED,
                rejection_reason="unexpected source_bot",
            )
            return SubmitSignalResult(
                False,
                signal.signal_id,
                "unexpected source_bot",
                None,
                None,
                (
                    ExecutionRuntimeEvent(
                        event_type=ExecutionEventType.SIGNAL_REJECTED,
                        signal_id=signal.signal_id,
                        instrument=signal.instrument,
                        timestamp_et=received_at_et,
                        message="unexpected source_bot",
                    ),
                ),
            )
        freshness_limit = signal.freshness_seconds if signal.freshness_seconds > 0 else self._config.freshness_seconds
        signal_age_seconds = (received_at_et - signal.formed_timestamp_et).total_seconds()
        if signal_age_seconds > freshness_limit:
            self._journal.record_signal(
                signal=signal,
                received_at_et=received_at_et,
                status=SignalStatus.REJECTED,
                rejection_reason="stale signal",
            )
            return SubmitSignalResult(
                False,
                signal.signal_id,
                "stale signal",
                None,
                None,
                (
                    ExecutionRuntimeEvent(
                        event_type=ExecutionEventType.SIGNAL_REJECTED,
                        signal_id=signal.signal_id,
                        instrument=signal.instrument,
                        timestamp_et=received_at_et,
                        message="stale signal",
                    ),
                ),
            )
        validation_error = self._validate_signal(signal)
        if validation_error is not None:
            self._journal.record_signal(
                signal=signal,
                received_at_et=received_at_et,
                status=SignalStatus.REJECTED,
                rejection_reason=validation_error,
            )
            return SubmitSignalResult(
                False,
                signal.signal_id,
                validation_error,
                None,
                None,
                (
                    ExecutionRuntimeEvent(
                        event_type=ExecutionEventType.SIGNAL_REJECTED,
                        signal_id=signal.signal_id,
                        instrument=signal.instrument,
                        timestamp_et=received_at_et,
                        message=validation_error,
                    ),
                ),
            )

        plan = self._risk_sizer.size_signal(signal)
        order_id = f"ord_{signal.signal_id}"
        self._journal.record_signal(
            signal=signal,
            received_at_et=received_at_et,
            status=SignalStatus.RECEIVED,
        )
        self._journal.create_order(
            order_id=order_id,
            signal=signal,
            quantity=plan.quantity,
            tp1_quantity=plan.tp1_quantity,
            tp2_quantity=plan.tp2_quantity,
            tp3_quantity=plan.tp3_quantity,
            point_value=plan.point_value,
            submitted_at_et=received_at_et,
        )
        self._journal.update_signal_status(
            signal_id=signal.signal_id,
            status=SignalStatus.ORDER_PENDING,
            updated_at_et=received_at_et,
        )
        self._journal.record_pnl_snapshot(
            snapshot_id=f"pnl_{signal.signal_id}_pending",
            signal_id=signal.signal_id,
            position_id=None,
            status=SignalStatus.ORDER_PENDING.value,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            as_of_timestamp_et=received_at_et,
            note="signal received and pending bracket order created",
        )
        return SubmitSignalResult(
            True,
            signal.signal_id,
            None,
            order_id,
            plan.quantity,
            (
                ExecutionRuntimeEvent(
                    event_type=ExecutionEventType.SIGNAL_RECEIVED,
                    signal_id=signal.signal_id,
                    instrument=signal.instrument,
                    timestamp_et=received_at_et,
                    message="signal received and pending paper order created",
                    order_id=order_id,
                    quantity=plan.quantity,
                    price=signal.entry,
                ),
            ),
        )

    def on_market_bar(self, bar: MarketBar) -> MarketUpdateResult:
        filled_order_ids, order_events = self._fill_pending_orders(bar)
        updated_positions, closed_positions, position_events = self._update_open_positions(bar)
        return MarketUpdateResult(
            filled_order_ids=filled_order_ids,
            updated_position_ids=updated_positions,
            closed_position_ids=closed_positions,
            events=order_events + position_events,
        )

    def _fill_pending_orders(self, bar: MarketBar) -> tuple[tuple[str, ...], tuple[ExecutionRuntimeEvent, ...]]:
        filled_order_ids: list[str] = []
        events: list[ExecutionRuntimeEvent] = []
        for order in self._journal.get_pending_orders(bar.instrument):
            if not self._bar_reaches_price(bar=bar, price=order.entry_price):
                continue
            self._journal.mark_order_filled(order_id=order.order_id, filled_at_et=bar.timestamp_et)
            position_id = self._journal.create_position_from_order(order=order, opened_at_et=bar.timestamp_et)
            self._journal.update_signal_status(
                signal_id=order.signal_id,
                status=SignalStatus.POSITION_OPEN,
                updated_at_et=bar.timestamp_et,
            )
            self._journal.record_fill(
                fill_id=f"fill_{order.order_id}_entry",
                signal_id=order.signal_id,
                order_id=order.order_id,
                position_id=position_id,
                fill_type=FillType.ENTRY,
                quantity=order.quantity,
                price=order.entry_price,
                realized_pnl=0.0,
                fill_timestamp_et=bar.timestamp_et,
                notes="entry filled when market reached the resting entry price",
            )
            self._journal.record_pnl_snapshot(
                snapshot_id=f"pnl_{order.signal_id}_open",
                signal_id=order.signal_id,
                position_id=position_id,
                status=SignalStatus.POSITION_OPEN.value,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                as_of_timestamp_et=bar.timestamp_et,
                note="position opened",
            )
            filled_order_ids.append(order.order_id)
            events.append(
                ExecutionRuntimeEvent(
                    event_type=ExecutionEventType.ORDER_FILLED,
                    signal_id=order.signal_id,
                    instrument=order.instrument,
                    timestamp_et=bar.timestamp_et,
                    message="pending paper order filled",
                    order_id=order.order_id,
                    position_id=position_id,
                    fill_type=FillType.ENTRY,
                    price=order.entry_price,
                    quantity=order.quantity,
                    realized_pnl=0.0,
                )
            )
        return tuple(filled_order_ids), tuple(events)

    def _update_open_positions(
        self, bar: MarketBar
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[ExecutionRuntimeEvent, ...]]:
        updated_position_ids: list[str] = []
        closed_position_ids: list[str] = []
        events: list[ExecutionRuntimeEvent] = []
        for position in self._journal.get_open_positions(bar.instrument):
            if self._stop_triggered(position=position, bar=bar):
                realized = self._realized_pnl(position=position, exit_price=position.stop_price, quantity=position.quantity_open)
                self._journal.apply_position_fill(
                    position_id=position.position_id,
                    fill_type=FillType.STOP,
                    quantity=position.quantity_open,
                    realized_pnl_delta=realized,
                    filled_at_et=bar.timestamp_et,
                )
                self._journal.record_fill(
                    fill_id=f"fill_{position.position_id}_stop",
                    signal_id=position.signal_id,
                    order_id=position.order_id,
                    position_id=position.position_id,
                    fill_type=FillType.STOP,
                    quantity=position.quantity_open,
                    price=position.stop_price,
                    realized_pnl=realized,
                    fill_timestamp_et=bar.timestamp_et,
                    notes="stop hit",
                )
                events.append(
                    ExecutionRuntimeEvent(
                        event_type=ExecutionEventType.STOP_HIT,
                        signal_id=position.signal_id,
                        instrument=position.instrument,
                        timestamp_et=bar.timestamp_et,
                        message="stop hit",
                        order_id=position.order_id,
                        position_id=position.position_id,
                        fill_type=FillType.STOP,
                        price=position.stop_price,
                        quantity=position.quantity_open,
                        realized_pnl=realized,
                    )
                )
                self._close_position(position=position, closed_at_et=bar.timestamp_et, note="stop hit")
                updated_position_ids.append(position.position_id)
                closed_position_ids.append(position.position_id)
                events.append(self._position_closed_event(position=position, closed_at_et=bar.timestamp_et))
                continue

            if self._target_triggered(position.direction, position.tp1_price, bar) and position.tp1_filled_quantity < position.tp1_quantity:
                updated_position_ids.append(position.position_id)
                position_id = position.position_id
                events.append(
                    self._take_profit(
                        position=position,
                        fill_type=FillType.TP1,
                        quantity=position.tp1_quantity,
                        price=position.tp1_price,
                        filled_at_et=bar.timestamp_et,
                        note="tp1 hit",
                    )
                )
                position = self._refresh_open_position(position_id)
                if position is None:
                    closed_position_ids.append(position_id)
                    events.append(self._position_closed_event_from_ids(position_id=position_id, signal_id=position_id.removeprefix("pos_"), instrument=bar.instrument, closed_at_et=bar.timestamp_et))
                    continue

            if position.tp2_quantity > 0 and self._target_triggered(position.direction, position.tp2_price, bar) and position.tp2_filled_quantity < position.tp2_quantity:
                updated_position_ids.append(position.position_id)
                position_id = position.position_id
                events.append(
                    self._take_profit(
                        position=position,
                        fill_type=FillType.TP2,
                        quantity=position.tp2_quantity,
                        price=position.tp2_price,
                        filled_at_et=bar.timestamp_et,
                        note="tp2 hit",
                    )
                )
                position = self._refresh_open_position(position_id)
                if position is None:
                    closed_position_ids.append(position_id)
                    events.append(self._position_closed_event_from_ids(position_id=position_id, signal_id=position_id.removeprefix("pos_"), instrument=bar.instrument, closed_at_et=bar.timestamp_et))
                    continue

            if position.tp3_quantity > 0 and self._target_triggered(position.direction, position.tp3_price, bar) and position.tp3_filled_quantity < position.tp3_quantity:
                updated_position_ids.append(position.position_id)
                events.append(
                    self._take_profit(
                        position=position,
                        fill_type=FillType.TP3,
                        quantity=position.tp3_quantity,
                        price=position.tp3_price,
                        filled_at_et=bar.timestamp_et,
                        note="tp3 hit",
                    )
                )
                refreshed = self._refresh_open_position(position.position_id)
                if refreshed is None:
                    closed_position_ids.append(position.position_id)
                    events.append(self._position_closed_event(position=position, closed_at_et=bar.timestamp_et))
                else:
                    position = refreshed
        return tuple(updated_position_ids), tuple(closed_position_ids), tuple(events)

    def _take_profit(
        self,
        *,
        position: OpenPositionRecord,
        fill_type: FillType,
        quantity: int,
        price: float,
        filled_at_et: datetime,
        note: str,
    ) -> ExecutionRuntimeEvent:
        if quantity <= 0:
            return ExecutionRuntimeEvent(
                event_type=ExecutionEventType.TP_HIT,
                signal_id=position.signal_id,
                instrument=position.instrument,
                timestamp_et=filled_at_et,
                message=note,
                order_id=position.order_id,
                position_id=position.position_id,
                fill_type=fill_type,
                price=price,
                quantity=0,
                realized_pnl=0.0,
            )
        realized = self._realized_pnl(position=position, exit_price=price, quantity=quantity)
        self._journal.apply_position_fill(
            position_id=position.position_id,
            fill_type=fill_type,
            quantity=quantity,
            realized_pnl_delta=realized,
            filled_at_et=filled_at_et,
        )
        self._journal.record_fill(
            fill_id=f"fill_{position.position_id}_{fill_type.value}",
            signal_id=position.signal_id,
            order_id=position.order_id,
            position_id=position.position_id,
            fill_type=fill_type,
            quantity=quantity,
            price=price,
            realized_pnl=realized,
            fill_timestamp_et=filled_at_et,
            notes=note,
        )
        refreshed = self._refresh_open_position(position.position_id)
        if refreshed is not None:
            self._journal.record_pnl_snapshot(
                snapshot_id=f"pnl_{position.signal_id}_{fill_type.value}",
                signal_id=position.signal_id,
                position_id=position.position_id,
                status=refreshed.status.value,
                realized_pnl=refreshed.realized_pnl,
                unrealized_pnl=0.0,
                as_of_timestamp_et=filled_at_et,
                note=note,
            )
            if refreshed.quantity_open <= 0:
                self._close_position(position=refreshed, closed_at_et=filled_at_et, note=note)
        return ExecutionRuntimeEvent(
            event_type=ExecutionEventType.TP_HIT,
            signal_id=position.signal_id,
            instrument=position.instrument,
            timestamp_et=filled_at_et,
            message=note,
            order_id=position.order_id,
            position_id=position.position_id,
            fill_type=fill_type,
            price=price,
            quantity=quantity,
            realized_pnl=realized,
        )

    def _close_position(self, *, position: OpenPositionRecord, closed_at_et: datetime, note: str) -> None:
        refreshed = self._refresh_open_position(position.position_id)
        if refreshed is None:
            return
        self._journal.close_position(position_id=position.position_id, closed_at_et=closed_at_et)
        self._journal.mark_order_closed(order_id=position.order_id, closed_at_et=closed_at_et)
        self._journal.update_signal_status(
            signal_id=position.signal_id,
            status=SignalStatus.POSITION_CLOSED,
            updated_at_et=closed_at_et,
        )
        self._journal.record_pnl_snapshot(
            snapshot_id=f"pnl_{position.signal_id}_closed",
            signal_id=position.signal_id,
            position_id=position.position_id,
            status=SignalStatus.POSITION_CLOSED.value,
            realized_pnl=refreshed.realized_pnl,
            unrealized_pnl=0.0,
            as_of_timestamp_et=closed_at_et,
            note=note,
        )

    def _refresh_open_position(self, position_id: str) -> OpenPositionRecord | None:
        return self._journal.get_open_position(position_id)

    def _position_closed_event(
        self, *, position: OpenPositionRecord, closed_at_et: datetime
    ) -> ExecutionRuntimeEvent:
        return self._position_closed_event_from_ids(
            position_id=position.position_id,
            signal_id=position.signal_id,
            instrument=position.instrument,
            closed_at_et=closed_at_et,
        )

    def _position_closed_event_from_ids(
        self, *, position_id: str, signal_id: str, instrument: str, closed_at_et: datetime
    ) -> ExecutionRuntimeEvent:
        return ExecutionRuntimeEvent(
            event_type=ExecutionEventType.POSITION_CLOSED,
            signal_id=signal_id,
            instrument=instrument,
            timestamp_et=closed_at_et,
            message="position closed",
            position_id=position_id,
        )

    def _validate_signal(self, signal: SignalEvent) -> str | None:
        if signal.formed_timestamp_et.tzinfo is None:
            return "formed_timestamp_et must be timezone-aware"
        if signal.entry <= 0.0 or signal.stop <= 0.0 or signal.tp1 <= 0.0 or signal.tp2 <= 0.0 or signal.tp3 <= 0.0:
            return "price levels must be positive"
        if signal.freshness_seconds <= 0:
            return "freshness_seconds must be positive"
        if signal.direction is Direction.LONG:
            if not (signal.stop < signal.entry < signal.tp1 < signal.tp2 < signal.tp3):
                return "invalid long price ladder"
        else:
            if not (signal.stop > signal.entry > signal.tp1 > signal.tp2 > signal.tp3):
                return "invalid short price ladder"
        return None

    def _bar_reaches_price(self, *, bar: MarketBar, price: float) -> bool:
        return bar.low <= price <= bar.high

    def _stop_triggered(self, *, position: OpenPositionRecord, bar: MarketBar) -> bool:
        if position.direction is Direction.LONG:
            return bar.low <= position.stop_price
        return bar.high >= position.stop_price

    def _target_triggered(self, direction: Direction, price: float, bar: MarketBar) -> bool:
        if direction is Direction.LONG:
            return bar.high >= price
        return bar.low <= price

    def _realized_pnl(self, *, position: OpenPositionRecord, exit_price: float, quantity: int) -> float:
        if position.direction is Direction.LONG:
            return (exit_price - position.entry_price) * quantity * position.point_value
        return (position.entry_price - exit_price) * quantity * position.point_value
