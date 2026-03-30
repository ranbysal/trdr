"""Corrected Gold signal logic."""

from __future__ import annotations

from futures_bot.config.models import GoldStrategyConfig
from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import SignalCandidate
from futures_bot.strategies.gold_models import (
    GoldSignalEvaluation,
    GoldSignalFeatures,
    GoldSignalRejection,
    GoldSignalResult,
    GoldSignalSetup,
)

_PRIMARY_MIN_DISTANCE_ATR = 0.45
_PRIMARY_MAX_DISTANCE_ATR = 1.80


class GoldSignalStrategy:
    """Gold anchored VWAP mean reversion primary, structural OB secondary."""

    def __init__(self, config: GoldStrategyConfig) -> None:
        self._config = config

    def evaluate(self, *, features: GoldSignalFeatures) -> GoldSignalEvaluation:
        if features.symbol != self._config.symbol:
            return _reject(GoldSignalRejection.SYMBOL_NOT_IN_SCOPE)
        if not features.liquidity_ok:
            return _reject(GoldSignalRejection.LIQUIDITY_GATE_FAILED)
        if features.macro_blocked:
            return _reject(GoldSignalRejection.MACRO_NEWS_BLOCKED)
        if features.atr_5m <= 0.0:
            return _reject(GoldSignalRejection.INVALID_ATR)

        vwap_distance_atr = (features.close_price - features.anchored_vwap) / features.atr_5m
        primary_side = _primary_mean_reversion_side(vwap_distance_atr)
        if primary_side is not None:
            return _approve(
                features=features,
                side=primary_side,
                setup=GoldSignalSetup.PRIMARY_MEAN_REVERSION,
                regime=Regime.NEUTRAL,
                base_score=77.0 + min(abs(vwap_distance_atr) * 4.0, 7.0),
                vwap_distance_atr=vwap_distance_atr,
                use_choch=self._config.context.use_choch,
                use_fvg=self._config.context.use_fvg,
            )

        secondary_side = _secondary_structural_side(features)
        if secondary_side is not None:
            return _approve(
                features=features,
                side=secondary_side,
                setup=GoldSignalSetup.SECONDARY_STRUCTURAL_ORDER_BLOCK,
                regime=Regime.TREND,
                base_score=74.0,
                vwap_distance_atr=vwap_distance_atr,
                use_choch=self._config.context.use_choch,
                use_fvg=self._config.context.use_fvg,
            )

        return _reject(GoldSignalRejection.NO_MEAN_REVERSION_OR_STRUCTURAL_SETUP)


def _primary_mean_reversion_side(vwap_distance_atr: float) -> OrderSide | None:
    if -_PRIMARY_MAX_DISTANCE_ATR <= vwap_distance_atr <= -_PRIMARY_MIN_DISTANCE_ATR:
        return OrderSide.BUY
    if _PRIMARY_MIN_DISTANCE_ATR <= vwap_distance_atr <= _PRIMARY_MAX_DISTANCE_ATR:
        return OrderSide.SELL
    return None


def _secondary_structural_side(features: GoldSignalFeatures) -> OrderSide | None:
    if (
        features.pullback_price is None
        or features.structure_break_price is None
        or features.order_block_low is None
        or features.order_block_high is None
    ):
        return None

    inside_block = features.order_block_low <= features.pullback_price <= features.order_block_high
    if not inside_block:
        return None
    if features.close_price >= features.structure_break_price:
        return OrderSide.BUY
    if features.close_price <= features.structure_break_price:
        return OrderSide.SELL
    return None


def _approve(
    *,
    features: GoldSignalFeatures,
    side: OrderSide,
    setup: GoldSignalSetup,
    regime: Regime,
    base_score: float,
    vwap_distance_atr: float,
    use_choch: bool,
    use_fvg: bool,
) -> GoldSignalEvaluation:
    score = base_score
    if use_choch and features.choch_confirmed:
        score += 2.0
    if use_fvg and features.fvg_present:
        score += 2.0

    signal = SignalCandidate(
        ts=features.ts,
        strategy=StrategyModule.STRAT_GOLD_SIGNAL,
        symbol=features.symbol,
        side=side,
        regime=regime,
        score=score,
    )
    return GoldSignalEvaluation(
        approved=True,
        rejection_reason=None,
        candidate=GoldSignalResult(
            signal=signal,
            setup=setup,
            vwap_distance_atr=vwap_distance_atr,
            choch_confirmed=features.choch_confirmed,
            fvg_present=features.fvg_present,
        ),
    )


def _reject(reason: GoldSignalRejection) -> GoldSignalEvaluation:
    return GoldSignalEvaluation(approved=False, rejection_reason=reason, candidate=None)
