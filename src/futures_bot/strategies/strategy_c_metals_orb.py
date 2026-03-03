"""Strategy C: Metals ORB signal generation."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, time
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import Bar1m, SignalCandidate
from futures_bot.strategies.strategy_c_models import (
    MetalsORSessionState,
    StrategyCEntryPlan,
    StrategyCEvaluation,
    StrategyCFeatureSnapshot,
)

ET = ZoneInfo("America/New_York")
_ALLOWED_SYMBOLS = {"MGC", "SIL"}
_OR_START = time(8, 0)
_OR_END = time(8, 15)
_ENTRY_END = time(10, 45)
_FLATTEN_TIME = time(11, 30)


class StrategyCMetalsORB:
    """Stateful ORB for metals family with EMA20 slope filter."""

    def __init__(self) -> None:
        self._or_states: dict[str, MetalsORSessionState] = {}

    def update_or_state(self, bar: Bar1m) -> MetalsORSessionState:
        ts_et = _to_et(bar.ts)
        symbol = bar.symbol
        session_date = ts_et.date()
        prev = self._or_states.get(symbol)
        if prev is None or prev.session_date != session_date:
            prev = MetalsORSessionState(symbol=symbol, session_date=session_date)

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

    def evaluate_breakout_candidate(
        self,
        *,
        bar: Bar1m,
        features: StrategyCFeatureSnapshot,
        tick_size: float,
    ) -> tuple[StrategyCEvaluation, SignalCandidate | None]:
        ts_et = _to_et(bar.ts)
        symbol = bar.symbol
        t = ts_et.timetz().replace(tzinfo=None)

        if symbol not in _ALLOWED_SYMBOLS:
            return _reject("SYMBOL_NOT_IN_SCOPE")
        if not (_OR_START <= t <= _ENTRY_END):
            return _reject("OUTSIDE_ENTRY_WINDOW")
        if features.tier1_lockout_active:
            return _reject("TIER1_LOCKOUT_ACTIVE")

        state = self._or_states.get(symbol)
        if state is None or state.session_date != ts_et.date() or not state.is_complete:
            return _reject("OR_NOT_READY")
        if state.or_high is None or state.or_low is None or state.or_midpoint is None:
            return _reject("OR_NOT_READY")

        side = _infer_breakout_side(close=bar.close, or_high=state.or_high, or_low=state.or_low)
        if side is None:
            return _reject("NO_BREAKOUT_TRIGGER")

        # Metals spec gate: EMA20 slope direction confirmation.
        if side is OrderSide.BUY and features.ema20_5m_slope <= 0.0:
            return _reject("EMA20_SLOPE_FILTER_FAILED")
        if side is OrderSide.SELL and features.ema20_5m_slope >= 0.0:
            return _reject("EMA20_SLOPE_FILTER_FAILED")

        if side is OrderSide.BUY and not (bar.close > features.session_vwap):
            return _reject("VWAP_SIDE_FILTER_FAILED")
        if side is OrderSide.SELL and not (bar.close < features.session_vwap):
            return _reject("VWAP_SIDE_FILTER_FAILED")

        if features.atr_14_5m <= 0.0:
            return _reject("INVALID_ATR_14_5M")

        width = state.or_width
        if width is None:
            return _reject("OR_NOT_READY")
        width_ratio = width / features.atr_14_5m
        if width_ratio < 1.0 or width_ratio > 4.5:
            return _reject("OR_WIDTH_SANITY_FAILED")

        if not (features.vol_strong_1m or (features.rvol_3bar_aggregate_5m is not None and features.rvol_3bar_aggregate_5m >= 1.10)):
            return _reject("VOLUME_NOT_STRONG")

        entry_stop = state.or_high + tick_size if side is OrderSide.BUY else state.or_low - tick_size
        if side is OrderSide.BUY:
            initial_stop = max(state.or_midpoint, entry_stop - (0.8 * features.atr_14_5m))
            risk_r = entry_stop - initial_stop
            tp1 = entry_stop + risk_r
            tp2 = entry_stop + (2.0 * risk_r)
        else:
            initial_stop = min(state.or_midpoint, entry_stop + (0.8 * features.atr_14_5m))
            risk_r = initial_stop - entry_stop
            tp1 = entry_stop - risk_r
            tp2 = entry_stop - (2.0 * risk_r)
        if risk_r <= 0.0:
            return _reject("INVALID_STOP_DISTANCE")

        plan = StrategyCEntryPlan(
            side=side,
            entry_stop=entry_stop,
            initial_stop=initial_stop,
            tp1_price=tp1,
            tp2_price=tp2,
            flatten_by=ts_et.replace(hour=_FLATTEN_TIME.hour, minute=_FLATTEN_TIME.minute, second=0, microsecond=0),
        )
        signal = SignalCandidate(
            ts=bar.ts,
            strategy=StrategyModule.STRAT_C_METALS_ORB,
            symbol=symbol,
            side=side,
            regime=Regime.TREND,
            score=80.0,
        )
        return StrategyCEvaluation(approved=True, reason_code="APPROVED", entry_plan=plan), signal


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


def _reject(code: str) -> tuple[StrategyCEvaluation, SignalCandidate | None]:
    return StrategyCEvaluation(approved=False, reason_code=code, entry_plan=None), None

