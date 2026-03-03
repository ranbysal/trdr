"""Strategy A: Equity ORB signal generation (no order placement)."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import Bar1m, SignalCandidate
from futures_bot.regime.engine import qualified_trend_for_breakout
from futures_bot.regime.models import QualifiedTrendInputs
from futures_bot.strategies.strategy_a_models import (
    ORSessionState,
    StrategyAEntryPlan,
    StrategyAEvaluation,
    StrategyAFeatureSnapshot,
    StrategyAPositionExitState,
)

ET = ZoneInfo("America/New_York")
_ALLOWED_SYMBOLS = {"NQ", "YM"}
_CHASE_TICKS = {"NQ": 4, "MNQ": 4, "YM": 3, "MYM": 3}

_OR_START = time(9, 30)
_OR_END = time(9, 45)
_ENTRY_END = time(10, 45)
_FLATTEN_TIME = time(11, 30)


class StrategyAORB:
    """Stateful OR tracker with pure breakout candidate evaluation."""

    def __init__(self) -> None:
        self._or_states: dict[str, ORSessionState] = {}

    def update_or_state(self, bar: Bar1m) -> ORSessionState:
        """Update deterministic OR state on each new 1m bar close."""
        ts_et = _to_et(bar.ts)
        symbol = bar.symbol
        session_date = ts_et.date()

        prev = self._or_states.get(symbol)
        if prev is None or prev.session_date != session_date:
            prev = ORSessionState(symbol=symbol, session_date=session_date)

        t = ts_et.timetz().replace(tzinfo=None)
        state = prev

        if _OR_START <= t < _OR_END:
            or_high = bar.high if state.or_high is None else max(state.or_high, bar.high)
            or_low = bar.low if state.or_low is None else min(state.or_low, bar.low)
            state = replace(state, or_high=or_high, or_low=or_low, bar_count=state.bar_count + 1)

        if t >= _OR_END and state.bar_count > 0 and not state.is_complete:
            state = replace(state, is_complete=True)

        self._or_states[symbol] = state
        return state

    def get_or_state(self, symbol: str) -> ORSessionState | None:
        return self._or_states.get(symbol)

    def evaluate_breakout_candidate(
        self,
        *,
        bar: Bar1m,
        features: StrategyAFeatureSnapshot,
        tick_size: float,
    ) -> StrategyAEvaluation:
        """Evaluate Strategy A breakout conditions at 1m bar close."""
        ts_et = _to_et(bar.ts)
        symbol = bar.symbol
        t = ts_et.timetz().replace(tzinfo=None)

        if symbol not in _ALLOWED_SYMBOLS:
            return _reject("SYMBOL_NOT_IN_SCOPE")

        if not (_OR_START <= t <= _ENTRY_END):
            return _reject("OUTSIDE_ENTRY_WINDOW")

        state = self._or_states.get(symbol)
        if state is None or state.session_date != ts_et.date() or not state.is_complete:
            return _reject("OR_NOT_READY")

        if state.or_high is None or state.or_low is None or state.or_midpoint is None:
            return _reject("OR_NOT_READY")

        side = _infer_breakout_side(close=bar.close, or_high=state.or_high, or_low=state.or_low)
        if side is None:
            return _reject("NO_BREAKOUT_TRIGGER")

        qualified_trend = qualified_trend_for_breakout(
            QualifiedTrendInputs(
                family_raw_regime=features.raw_regime,
                low_volume_trend_streak_5m=features.low_volume_trend_streak_5m,
                trigger_rvol_tod_1m=None,
                trigger_vol_strong_1m=features.vol_strong_1m,
            )
        )
        if not qualified_trend:
            return _reject("UNQUALIFIED_TREND")

        if features.tier1_lockout_active:
            return _reject("TIER1_LOCKOUT_ACTIVE")

        volume_ok = features.vol_strong_1m or (
            features.rvol_3bar_aggregate_5m is not None and features.rvol_3bar_aggregate_5m >= 1.10
        )
        if not volume_ok:
            return _reject("VOLUME_NOT_STRONG")

        if side is OrderSide.BUY and not (bar.close > features.session_vwap):
            return _reject("VWAP_SIDE_FILTER_FAILED")
        if side is OrderSide.SELL and not (bar.close < features.session_vwap):
            return _reject("VWAP_SIDE_FILTER_FAILED")

        if side is OrderSide.BUY and not (features.ema9_5m > features.ema21_5m):
            return _reject("EMA_ALIGNMENT_FAILED")
        if side is OrderSide.SELL and not (features.ema9_5m < features.ema21_5m):
            return _reject("EMA_ALIGNMENT_FAILED")

        if features.atr_14_5m <= 0.0:
            return _reject("INVALID_ATR_14_5M")

        or_width = state.or_width
        if or_width is None:
            return _reject("OR_NOT_READY")

        width_ratio = or_width / features.atr_14_5m
        if width_ratio < 1.0 or width_ratio > 4.5:
            return _reject("OR_WIDTH_SANITY_FAILED")

        plan = self.produce_entry_plan(
            symbol=symbol,
            side=side,
            or_state=state,
            atr_14_5m=features.atr_14_5m,
            tick_size=tick_size,
            trigger_ts=ts_et,
        )

        signal = SignalCandidate(
            ts=bar.ts,
            strategy=StrategyModule.STRAT_A_ORB,
            symbol=symbol,
            side=side,
            regime=features.raw_regime,
            score=0.0,
        )
        return StrategyAEvaluation(approved=True, reason_code="APPROVED", signal=signal, entry_plan=plan)

    def produce_entry_plan(
        self,
        *,
        symbol: str,
        side: OrderSide,
        or_state: ORSessionState,
        atr_14_5m: float,
        tick_size: float,
        trigger_ts: datetime,
    ) -> StrategyAEntryPlan:
        """Compute entry, initial stop, TP levels, and management parameters."""
        if symbol not in _CHASE_TICKS:
            raise ValueError(f"Unsupported symbol for chase configuration: {symbol}")

        if or_state.or_high is None or or_state.or_low is None or or_state.or_midpoint is None:
            raise ValueError("OR state is incomplete")

        entry_stop = (
            or_state.or_high + tick_size if side is OrderSide.BUY else or_state.or_low - tick_size
        )

        if side is OrderSide.BUY:
            initial_stop = max(or_state.or_midpoint, entry_stop - (0.8 * atr_14_5m))
            risk_r = entry_stop - initial_stop
            tp1 = entry_stop + risk_r
            tp2 = entry_stop + (2.0 * risk_r)
            breakeven_stop = entry_stop + tick_size
        else:
            initial_stop = min(or_state.or_midpoint, entry_stop + (0.8 * atr_14_5m))
            risk_r = initial_stop - entry_stop
            tp1 = entry_stop - risk_r
            tp2 = entry_stop - (2.0 * risk_r)
            breakeven_stop = entry_stop - tick_size

        if risk_r <= 0.0:
            raise ValueError("Invalid initial stop produced non-positive R")

        flatten_by = trigger_ts.replace(
            hour=_FLATTEN_TIME.hour,
            minute=_FLATTEN_TIME.minute,
            second=0,
            microsecond=0,
        )

        return StrategyAEntryPlan(
            side=side,
            entry_stop=entry_stop,
            stop_limit_chase_ticks=_CHASE_TICKS[symbol],
            initial_stop=initial_stop,
            tp1_price=tp1,
            tp2_price=tp2,
            tp1_size_frac=0.5,
            tp2_size_frac=0.3,
            tp3_size_frac=0.2,
            tp3_trail_activate_price=tp2,
            tp3_initial_stop=initial_stop,
            tp3_trail_rule="trail remaining 20% on 1m EMA9 close rule",
            breakeven_stop_after_tp1=breakeven_stop,
            flatten_by=flatten_by,
        )

    def initialize_exit_state(
        self,
        *,
        side: OrderSide,
        fill_price: float,
        or_midpoint: float,
        atr_14_5m: float,
        tick_size: float,
    ) -> StrategyAPositionExitState:
        """Initialize dynamic stop/TP state at the time position is opened."""
        if side is OrderSide.BUY:
            active_stop = max(or_midpoint, fill_price - (0.8 * atr_14_5m))
            active_r = fill_price - active_stop
            tp1 = fill_price + active_r
            tp2 = fill_price + (2.0 * active_r)
            tp3_stop = active_stop
        else:
            active_stop = min(or_midpoint, fill_price + (0.8 * atr_14_5m))
            active_r = active_stop - fill_price
            tp1 = fill_price - active_r
            tp2 = fill_price - (2.0 * active_r)
            tp3_stop = active_stop

        if active_r <= 0.0:
            raise ValueError("Invalid dynamic exit state: non-positive R")

        return StrategyAPositionExitState(
            side=side,
            entry_price=fill_price,
            active_stop=active_stop,
            active_r=active_r,
            tp1_price=tp1,
            tp2_price=tp2,
            tp3_stop=tp3_stop,
            tp1_touched=False,
            trail_active=False,
        )

    def update_exit_state_for_bar(
        self,
        *,
        state: StrategyAPositionExitState,
        bar_high: float,
        bar_low: float,
        ema9_1m: float,
        tick_size: float,
    ) -> StrategyAPositionExitState:
        """Apply TP1 breakeven and TP3 EMA9 trailing transitions for one 1m bar."""
        active_stop = state.active_stop
        tp3_stop = state.tp3_stop
        tp1_touched = state.tp1_touched
        trail_active = state.trail_active

        if state.side is OrderSide.BUY:
            tp1_touched = tp1_touched or (bar_high >= state.tp1_price)
            if tp1_touched:
                active_stop = max(active_stop, state.entry_price + tick_size)
            trail_active = trail_active or (bar_high >= state.tp2_price)
            if trail_active:
                tp3_stop = max(tp3_stop, ema9_1m)
            tp3_stop = max(tp3_stop, active_stop)
        else:
            tp1_touched = tp1_touched or (bar_low <= state.tp1_price)
            if tp1_touched:
                active_stop = min(active_stop, state.entry_price - tick_size)
            trail_active = trail_active or (bar_low <= state.tp2_price)
            if trail_active:
                tp3_stop = min(tp3_stop, ema9_1m)
            tp3_stop = min(tp3_stop, active_stop)

        return StrategyAPositionExitState(
            side=state.side,
            entry_price=state.entry_price,
            active_stop=active_stop,
            active_r=state.active_r,
            tp1_price=state.tp1_price,
            tp2_price=state.tp2_price,
            tp3_stop=tp3_stop,
            tp1_touched=tp1_touched,
            trail_active=trail_active,
        )


def _infer_breakout_side(*, close: float, or_high: float, or_low: float) -> OrderSide | None:
    if close > or_high:
        return OrderSide.BUY
    if close < or_low:
        return OrderSide.SELL
    return None


def _to_et(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)


def _reject(code: str) -> StrategyAEvaluation:
    return StrategyAEvaluation(approved=False, reason_code=code, signal=None, entry_plan=None)
