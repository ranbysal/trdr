"""Strategy B: Equity VWAP Reversion signal generation."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import SignalCandidate
from futures_bot.strategies.strategy_b_models import StrategyBEvaluation, StrategyBFeatureSnapshot

ET = ZoneInfo("America/New_York")
_ENTRY_START = time(9, 30)
_ENTRY_END = time(10, 45)
_ALLOWED_SYMBOLS = {"NQ", "YM"}


class StrategyBVWAPReversion:
    """Weak-neutral VWAP mean-reversion candidate selector."""

    def evaluate_candidate(
        self,
        *,
        ts: datetime,
        symbol: str,
        features: StrategyBFeatureSnapshot,
    ) -> tuple[StrategyBEvaluation, SignalCandidate | None]:
        t = _to_et(ts).timetz().replace(tzinfo=None)
        if symbol not in _ALLOWED_SYMBOLS:
            return _reject("SYMBOL_NOT_IN_SCOPE")
        if not (_ENTRY_START <= t <= _ENTRY_END):
            return _reject("OUTSIDE_ENTRY_WINDOW")
        if not features.data_ok:
            return _reject("DATA_NOT_OK")

        # Spec gate: Strategy B only operates in weak-neutral conditions.
        if not (features.raw_regime is Regime.NEUTRAL and features.is_weak_neutral and features.confidence <= 0.6):
            return _reject("WEAK_NEUTRAL_GATE_FAILED")

        if features.atr_14_5m <= 0.0:
            return _reject("INVALID_ATR_14_5M")

        dist = features.close_price - features.session_vwap
        dist_atr = abs(dist) / features.atr_14_5m
        # Keep reversion entries in a bounded volatility envelope.
        if dist_atr < 0.75 or dist_atr > 2.5:
            return _reject("VWAP_REVERSION_DISTANCE_INVALID")

        if features.rvol_3bar_aggregate_5m is not None and features.rvol_3bar_aggregate_5m > 1.6:
            return _reject("VOLATILITY_TOO_HIGH")

        side = OrderSide.SELL if dist > 0 else OrderSide.BUY
        signal = SignalCandidate(
            ts=ts,
            strategy=StrategyModule.STRAT_B_VWAP_REV,
            symbol=symbol,
            side=side,
            regime=features.raw_regime,
            score=70.0 + min(dist_atr * 10.0, 20.0),
        )
        return StrategyBEvaluation(approved=True, reason_code="APPROVED"), signal


def _reject(code: str) -> tuple[StrategyBEvaluation, SignalCandidate | None]:
    return StrategyBEvaluation(approved=False, reason_code=code), None


def _to_et(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)

