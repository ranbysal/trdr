"""Paper trading execution engine for Strategy A ORB."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from futures_bot.core.enums import Family, OrderSide
from futures_bot.core.ids import utc_timestamp_id
from futures_bot.core.reason_codes import (
    DATA_NOT_OK,
    FAMILY_FREEZE_ACTIVE,
    PENDING_ENTRY_CANCELLED_LOCKOUT,
)
from futures_bot.core.types import Bar1m
from futures_bot.data.calendar_store import LockoutStatus
from futures_bot.pipeline.orb_pipeline import ORBFeatureSnapshot, ORBRiskVaultState, ORBSymbolSnapshot, run_strategy_a_orb_pipeline
from futures_bot.regime.models import FamilyRegimeState
from futures_bot.risk.slippage import estimate_slippage_ticks
from futures_bot.runtime.ndjson_writer import NdjsonWriter
from futures_bot.strategies.strategy_a_models import StrategyAEntryPlan, StrategyAPositionExitState
from futures_bot.strategies.strategy_a_orb import StrategyAORB


@dataclass(frozen=True, slots=True)
class PaperEngineStepResult:
    submitted_entry: bool
    filled_entry: bool
    exited_position: bool
    reason_code: str | None


@dataclass(slots=True)
class _PendingEntry:
    order_id: str
    submitted_at: datetime
    symbol_snapshot: ORBSymbolSnapshot
    feature_snapshot: ORBFeatureSnapshot
    family_regime_state: FamilyRegimeState
    risk_state: ORBRiskVaultState
    entry_plan: StrategyAEntryPlan
    contracts: int
    routed_symbol: str
    expected_slippage_ticks: float


@dataclass(slots=True)
class _OpenPosition:
    position_id: str
    symbol: str
    routed_symbol: str
    side: OrderSide
    opened_at: datetime
    entry_price: float
    contracts_initial: int
    contracts_open: int
    tp1_remaining: int
    tp2_remaining: int
    tp3_remaining: int
    entry_plan: StrategyAEntryPlan
    exit_state: StrategyAPositionExitState
    risk_state: ORBRiskVaultState
    atr_14_1m_price: float
    cumulative_realized_pnl: float = 0.0


class StrategyAPaperEngine:
    """Stateful paper execution engine over Strategy A pipeline outputs."""

    def __init__(self, *, trade_log_path: str | Path = "trade_logs.json") -> None:
        self._log = NdjsonWriter(trade_log_path)
        self._pending_by_symbol: dict[str, _PendingEntry] = {}
        self._open_by_symbol: dict[str, _OpenPosition] = {}
        self._family_freeze: dict[Family, bool] = {}
        self._session_realized_pnl: float = 0.0
        self._session_day: datetime.date | None = None

    def set_family_freeze(self, *, family: Family, frozen: bool, reason: str) -> None:
        self._family_freeze[family] = frozen
        self._log.write(
            {
                "ts": datetime.utcnow().isoformat(),
                "event": "risk_event",
                "code": "FAMILY_FREEZE_SET",
                "family": family.value,
                "frozen": frozen,
                "reason": reason,
            }
        )

    def step(
        self,
        *,
        strategy: StrategyAORB,
        symbol_snapshot: ORBSymbolSnapshot,
        feature_snapshot: ORBFeatureSnapshot,
        family_regime_state: FamilyRegimeState,
        lockout_state: LockoutStatus,
        risk_state: ORBRiskVaultState,
        data_ok: bool,
        quote_ok: bool,
        family_freeze: bool = False,
    ) -> PaperEngineStepResult:
        bar = symbol_snapshot.bar_1m
        symbol = bar.symbol
        family = symbol_snapshot.instrument.family

        self._roll_session_if_needed(bar.ts, risk_state)
        if family_freeze:
            self._family_freeze[family] = True

        if lockout_state.cancel_resting_entries and symbol in self._pending_by_symbol:
            self._pending_by_symbol.pop(symbol)
            self._log.write(
                {
                    "ts": bar.ts.isoformat(),
                    "event": "risk_event",
                    "code": PENDING_ENTRY_CANCELLED_LOCKOUT,
                    "symbol": symbol,
                }
            )

        filled = self._maybe_fill_entry(
            strategy=strategy,
            bar=bar,
            feature_snapshot=feature_snapshot,
            symbol=symbol,
        )
        exited = self._update_open_position(
            strategy=strategy,
            bar=bar,
            feature_snapshot=feature_snapshot,
            symbol=symbol,
        )

        if symbol in self._open_by_symbol:
            return PaperEngineStepResult(
                submitted_entry=False,
                filled_entry=filled,
                exited_position=exited,
                reason_code=None,
            )

        if not data_ok or not quote_ok:
            self._log.write(
                {
                    "ts": bar.ts.isoformat(),
                    "event": "risk_event",
                    "code": DATA_NOT_OK,
                    "symbol": symbol,
                    "details": {"data_ok": data_ok, "quote_ok": quote_ok},
                }
            )
            return PaperEngineStepResult(False, filled, exited, DATA_NOT_OK)

        if self._family_freeze.get(family, False):
            self._log.write(
                {
                    "ts": bar.ts.isoformat(),
                    "event": "risk_event",
                    "code": FAMILY_FREEZE_ACTIVE,
                    "family": family.value,
                    "symbol": symbol,
                }
            )
            return PaperEngineStepResult(False, filled, exited, FAMILY_FREEZE_ACTIVE)

        if symbol in self._pending_by_symbol:
            return PaperEngineStepResult(False, filled, exited, None)

        packet = run_strategy_a_orb_pipeline(
            strategy=strategy,
            symbol_snapshot=symbol_snapshot,
            feature_snapshot=feature_snapshot,
            family_regime_state=family_regime_state,
            lockout_state=lockout_state,
            risk_state=risk_state,
        )
        if not packet.approved or packet.entry_plan is None or packet.sizing is None:
            code = packet.reason_code
            self._log.write(
                {
                    "ts": bar.ts.isoformat(),
                    "event": "risk_event",
                    "code": code,
                    "symbol": symbol,
                }
            )
            return PaperEngineStepResult(False, filled, exited, code)

        atr_ticks = symbol_snapshot.atr_14_1m_price / symbol_snapshot.instrument.tick_size
        slip = estimate_slippage_ticks(symbol, atr_ticks)
        pending = _PendingEntry(
            order_id=utc_timestamp_id(f"paper_entry_{symbol}"),
            submitted_at=bar.ts,
            symbol_snapshot=symbol_snapshot,
            feature_snapshot=feature_snapshot,
            family_regime_state=family_regime_state,
            risk_state=risk_state,
            entry_plan=packet.entry_plan,
            contracts=packet.sizing.contracts,
            routed_symbol=packet.sizing.routed_symbol,
            expected_slippage_ticks=slip.slippage_est_ticks,
        )
        self._pending_by_symbol[symbol] = pending
        self._log.write(
            {
                "ts": bar.ts.isoformat(),
                "event": "order_submitted",
                "order_type": "entry_stop",
                "order_id": pending.order_id,
                "symbol": symbol,
                "side": packet.entry_plan.side.value,
                "qty": pending.contracts,
                "entry_stop": packet.entry_plan.entry_stop,
                "expected_slippage_ticks": pending.expected_slippage_ticks,
            }
        )
        return PaperEngineStepResult(True, filled, exited, None)

    def _maybe_fill_entry(
        self,
        *,
        strategy: StrategyAORB,
        bar: Bar1m,
        feature_snapshot: ORBFeatureSnapshot,
        symbol: str,
    ) -> bool:
        pending = self._pending_by_symbol.get(symbol)
        if pending is None:
            return False

        side = pending.entry_plan.side
        trigger = (
            (side is OrderSide.BUY and bar.high >= pending.entry_plan.entry_stop)
            or (side is OrderSide.SELL and bar.low <= pending.entry_plan.entry_stop)
        )
        if not trigger:
            return False

        routed_instr = pending.risk_state.instruments_by_symbol[pending.routed_symbol]
        tick_size = routed_instr.tick_size
        slip_price = pending.expected_slippage_ticks * tick_size
        if side is OrderSide.BUY:
            fill_price = pending.entry_plan.entry_stop + slip_price
        else:
            fill_price = pending.entry_plan.entry_stop - slip_price

        state = strategy.get_or_state(symbol)
        if state is None or state.or_midpoint is None:
            or_midpoint = pending.entry_plan.initial_stop
        else:
            or_midpoint = state.or_midpoint

        exit_state = strategy.initialize_exit_state(
            side=side,
            fill_price=fill_price,
            or_midpoint=or_midpoint,
            atr_14_5m=pending.feature_snapshot.atr_14_5m,
            tick_size=tick_size,
        )

        tp1 = int(math.floor(pending.contracts * pending.entry_plan.tp1_size_frac))
        tp2 = int(math.floor(pending.contracts * pending.entry_plan.tp2_size_frac))
        tp3 = max(0, pending.contracts - tp1 - tp2)

        pos = _OpenPosition(
            position_id=utc_timestamp_id(f"paper_pos_{symbol}"),
            symbol=symbol,
            routed_symbol=pending.routed_symbol,
            side=side,
            opened_at=bar.ts,
            entry_price=fill_price,
            contracts_initial=pending.contracts,
            contracts_open=pending.contracts,
            tp1_remaining=tp1,
            tp2_remaining=tp2,
            tp3_remaining=tp3,
            entry_plan=pending.entry_plan,
            exit_state=exit_state,
            risk_state=pending.risk_state,
            atr_14_1m_price=pending.symbol_snapshot.atr_14_1m_price,
        )
        self._open_by_symbol[symbol] = pos
        self._pending_by_symbol.pop(symbol, None)

        self._log.write(
            {
                "ts": bar.ts.isoformat(),
                "event": "entry_filled",
                "position_id": pos.position_id,
                "symbol": symbol,
                "side": side.value,
                "qty": pos.contracts_initial,
                "fill_price": fill_price,
                "entry_stop": pending.entry_plan.entry_stop,
                "slippage_ticks": pending.expected_slippage_ticks,
                "initial_stop": pos.exit_state.active_stop,
                "tp1_price": pos.exit_state.tp1_price,
                "tp2_price": pos.exit_state.tp2_price,
            }
        )
        return True

    def _update_open_position(
        self,
        *,
        strategy: StrategyAORB,
        bar: Bar1m,
        feature_snapshot: ORBFeatureSnapshot,
        symbol: str,
    ) -> bool:
        pos = self._open_by_symbol.get(symbol)
        if pos is None:
            return False

        ema9_1m = feature_snapshot.ema9_1m if feature_snapshot.ema9_1m is not None else bar.close
        tick_size = pos.risk_state.instruments_by_symbol[pos.routed_symbol].tick_size
        pos.exit_state = strategy.update_exit_state_for_bar(
            state=pos.exit_state,
            bar_high=bar.high,
            bar_low=bar.low,
            ema9_1m=ema9_1m,
            tick_size=tick_size,
        )

        stop_fill_px = self._stop_fill_price(
            pos=pos,
            tick_size=tick_size,
            atr_14_1m_price=pos.atr_14_1m_price,
            fallback_ticks=1.0,
        )

        # TP1 leg: stop or limit.
        if pos.tp1_remaining > 0:
            qty = min(pos.tp1_remaining, pos.contracts_open)
            if self._stop_hit_price(pos=pos, bar=bar, stop_price=pos.exit_state.active_stop):
                self._close_qty(pos=pos, qty=qty, exit_price=stop_fill_px, reason="TP1_STOP_EXIT", ts=bar.ts)
                pos.tp1_remaining -= qty
            elif self._tp1_hit(pos=pos, bar=bar):
                self._close_qty(pos=pos, qty=qty, exit_price=pos.exit_state.tp1_price, reason="TP1_EXIT", ts=bar.ts)
                pos.tp1_remaining -= qty

        # TP2 leg: stop or limit.
        if pos.tp2_remaining > 0:
            qty = min(pos.tp2_remaining, pos.contracts_open)
            if self._stop_hit_price(pos=pos, bar=bar, stop_price=pos.exit_state.active_stop):
                self._close_qty(pos=pos, qty=qty, exit_price=stop_fill_px, reason="TP2_STOP_EXIT", ts=bar.ts)
                pos.tp2_remaining -= qty
            elif self._tp2_hit(pos=pos, bar=bar):
                self._close_qty(pos=pos, qty=qty, exit_price=pos.exit_state.tp2_price, reason="TP2_EXIT", ts=bar.ts)
                pos.tp2_remaining -= qty

        # TP3 leg: trailing stop only.
        if pos.tp3_remaining > 0 and self._stop_hit_price(pos=pos, bar=bar, stop_price=pos.exit_state.tp3_stop):
            qty = min(pos.tp3_remaining, pos.contracts_open)
            self._close_qty(pos=pos, qty=qty, exit_price=stop_fill_px, reason="TP3_TRAIL_EXIT", ts=bar.ts)
            pos.tp3_remaining -= qty

        self._finalize_position_if_flat(pos=pos, ts=bar.ts)
        return pos.contracts_open == 0

    def _stop_hit(self, *, pos: _OpenPosition, bar: Bar1m) -> bool:
        if pos.side is OrderSide.BUY:
            return bar.low <= pos.exit_state.active_stop
        return bar.high >= pos.exit_state.active_stop

    def _stop_hit_price(self, *, pos: _OpenPosition, bar: Bar1m, stop_price: float) -> bool:
        if pos.side is OrderSide.BUY:
            return bar.low <= stop_price
        return bar.high >= stop_price

    def _tp3_stop_hit(self, *, pos: _OpenPosition, bar: Bar1m) -> bool:
        if not pos.exit_state.trail_active:
            return False
        if pos.side is OrderSide.BUY:
            return bar.low <= pos.exit_state.tp3_stop
        return bar.high >= pos.exit_state.tp3_stop

    def _tp1_hit(self, *, pos: _OpenPosition, bar: Bar1m) -> bool:
        if pos.side is OrderSide.BUY:
            return bar.high >= pos.exit_state.tp1_price
        return bar.low <= pos.exit_state.tp1_price

    def _tp2_hit(self, *, pos: _OpenPosition, bar: Bar1m) -> bool:
        if pos.side is OrderSide.BUY:
            return bar.high >= pos.exit_state.tp2_price
        return bar.low <= pos.exit_state.tp2_price

    def _stop_fill_price(
        self,
        *,
        pos: _OpenPosition,
        tick_size: float,
        atr_14_1m_price: float,
        fallback_ticks: float,
    ) -> float:
        if atr_14_1m_price > 0.0:
            atr_ticks = atr_14_1m_price / tick_size
            slip_ticks = estimate_slippage_ticks(pos.symbol, atr_ticks).slippage_est_ticks
        else:
            slip_ticks = fallback_ticks
        slip_px = slip_ticks * tick_size
        if pos.side is OrderSide.BUY:
            return pos.exit_state.active_stop - slip_px
        return pos.exit_state.active_stop + slip_px

    def _close_qty(self, *, pos: _OpenPosition, qty: int, exit_price: float, reason: str, ts: datetime) -> None:
        if qty <= 0:
            return
        pos.contracts_open -= qty
        if pos.side is OrderSide.BUY:
            pnl = (exit_price - pos.entry_price) * qty * pos.risk_state.instruments_by_symbol[pos.routed_symbol].point_value
        else:
            pnl = (pos.entry_price - exit_price) * qty * pos.risk_state.instruments_by_symbol[pos.routed_symbol].point_value
        pos.cumulative_realized_pnl += pnl
        self._session_realized_pnl += pnl
        pos.risk_state.daily_halt_manager.update_realized_pnl(realized_pnl=self._session_realized_pnl)

        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "position_update",
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "exit_reason": reason,
                "qty_closed": qty,
                "qty_open": pos.contracts_open,
                "exit_price": exit_price,
                "active_stop": pos.exit_state.active_stop,
                "tp3_stop": pos.exit_state.tp3_stop,
                "realized_pnl_delta": pnl,
                "realized_pnl_cum": pos.cumulative_realized_pnl,
            }
        )

    def _finalize_position_if_flat(self, *, pos: _OpenPosition, ts: datetime) -> None:
        if pos.contracts_open != 0:
            return
        pos.risk_state.caps_manager.record_close_position(symbol=pos.routed_symbol)
        pos.risk_state.cooldown_manager.record_closed_trade(
            module_id=pos.risk_state.module_id,
            symbol=pos.symbol,
            net_realized_pnl_after_costs=pos.cumulative_realized_pnl,
            closed_at=ts,
        )
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "position_closed",
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "side": pos.side.value,
                "entry_price": pos.entry_price,
                "contracts_initial": pos.contracts_initial,
                "realized_pnl": pos.cumulative_realized_pnl,
                "entry_plan": asdict(pos.entry_plan),
            }
        )
        self._open_by_symbol.pop(pos.symbol, None)

    def _roll_session_if_needed(self, ts: datetime, risk_state: ORBRiskVaultState) -> None:
        day = ts.date()
        if self._session_day == day:
            return
        self._session_day = day
        self._session_realized_pnl = 0.0
        risk_state.daily_halt_manager.reset_session(session_start_equity=risk_state.equity)
