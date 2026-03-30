"""Corrected NQ signal logic."""

from __future__ import annotations

from futures_bot.config.models import NQStrategyConfig
from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import SignalCandidate
from futures_bot.strategies.nq_models import (
    NQSignalEvaluation,
    NQSignalFeatures,
    NQSignalRejection,
    NQSignalResult,
    NQSignalSetup,
)


class NQSignalStrategy:
    """NQ structural continuation and pullback logic with optional context only."""

    def __init__(self, config: NQStrategyConfig) -> None:
        self._config = config

    def evaluate(self, *, features: NQSignalFeatures) -> NQSignalEvaluation:
        if features.symbol != self._config.symbol:
            return _reject(NQSignalRejection.SYMBOL_NOT_IN_SCOPE)
        if not features.liquidity_ok:
            return _reject(NQSignalRejection.LIQUIDITY_GATE_FAILED)
        if features.macro_blocked:
            return _reject(NQSignalRejection.MACRO_NEWS_BLOCKED)
        if features.atr_5m <= 0.0:
            return _reject(NQSignalRejection.INVALID_ATR)

        side = _ema_side(features)
        if side is None:
            return _reject(NQSignalRejection.EMA_ALIGNMENT_FAILED)
        if not _structure_continues(features, side=side):
            return _reject(NQSignalRejection.STRUCTURE_CONTINUATION_FAILED)
        if not _order_block_aligned(features, side=side):
            return _reject(NQSignalRejection.ORDER_BLOCK_ALIGNMENT_FAILED)

        score = 82.0 + _optional_context_score(
            use_choch=self._config.context.use_choch,
            choch_confirmed=features.choch_confirmed,
            use_fvg=self._config.context.use_fvg,
            fvg_present=features.fvg_present,
            confirmation_enabled=self._config.confirmation.enabled,
            intermarket_confirmed=features.intermarket_confirmed,
        )
        signal = SignalCandidate(
            ts=features.ts,
            strategy=StrategyModule.STRAT_NQ_SIGNAL,
            symbol=features.symbol,
            side=side,
            regime=Regime.TREND,
            score=score,
        )
        return NQSignalEvaluation(
            approved=True,
            rejection_reason=None,
            candidate=NQSignalResult(
                signal=signal,
                setup=NQSignalSetup.STRUCTURAL_CONTINUATION,
                order_block_aligned=True,
                choch_confirmed=features.choch_confirmed,
                fvg_present=features.fvg_present,
                intermarket_confirmed=features.intermarket_confirmed,
            ),
        )


def _ema_side(features: NQSignalFeatures) -> OrderSide | None:
    if features.ema_fast > features.ema_slow and features.close_price >= features.ema_fast:
        return OrderSide.BUY
    if features.ema_fast < features.ema_slow and features.close_price <= features.ema_fast:
        return OrderSide.SELL
    return None


def _structure_continues(features: NQSignalFeatures, *, side: OrderSide) -> bool:
    if side is OrderSide.BUY:
        return features.close_price >= features.structure_break_price
    return features.close_price <= features.structure_break_price


def _order_block_aligned(features: NQSignalFeatures, *, side: OrderSide) -> bool:
    inside_block = features.order_block_low <= features.pullback_price <= features.order_block_high
    if not inside_block:
        return False
    if side is OrderSide.BUY:
        return features.ema_slow <= features.pullback_price <= features.ema_fast
    return features.ema_fast <= features.pullback_price <= features.ema_slow


def _optional_context_score(
    *,
    use_choch: bool,
    choch_confirmed: bool,
    use_fvg: bool,
    fvg_present: bool,
    confirmation_enabled: bool,
    intermarket_confirmed: bool | None,
) -> float:
    score = 0.0
    if use_choch and choch_confirmed:
        score += 4.0
    if use_fvg and fvg_present:
        score += 4.0
    if confirmation_enabled and intermarket_confirmed is True:
        score += 2.0
    return score


def _reject(reason: NQSignalRejection) -> NQSignalEvaluation:
    return NQSignalEvaluation(approved=False, rejection_reason=reason, candidate=None)
