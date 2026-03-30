"""Corrected YM signal logic."""

from __future__ import annotations

from futures_bot.config.models import YMStrategyConfig
from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import SignalCandidate
from futures_bot.strategies.ym_models import (
    YMSignalEvaluation,
    YMSignalFeatures,
    YMSignalRejection,
    YMSignalResult,
    YMSignalSetup,
)

_PRIMARY_MIN_DISTANCE_ATR = 0.50
_PRIMARY_MAX_DISTANCE_ATR = 1.75


class YMSignalStrategy:
    """YM anchored VWAP mean reversion primary, EMA continuation secondary."""

    def __init__(self, config: YMStrategyConfig) -> None:
        self._config = config

    def evaluate(self, *, features: YMSignalFeatures) -> YMSignalEvaluation:
        if features.symbol != self._config.symbol:
            return _reject(YMSignalRejection.SYMBOL_NOT_IN_SCOPE)
        if not features.liquidity_ok:
            return _reject(YMSignalRejection.LIQUIDITY_GATE_FAILED)
        if features.macro_blocked:
            return _reject(YMSignalRejection.MACRO_NEWS_BLOCKED)
        if features.atr_5m <= 0.0:
            return _reject(YMSignalRejection.INVALID_ATR)

        vwap_distance_atr = (features.close_price - features.anchored_vwap) / features.atr_5m
        primary_side = _primary_mean_reversion_side(vwap_distance_atr)
        if primary_side is not None:
            return _approve(
                features=features,
                side=primary_side,
                setup=YMSignalSetup.PRIMARY_MEAN_REVERSION,
                regime=Regime.NEUTRAL,
                base_score=78.0 + min(abs(vwap_distance_atr) * 4.0, 6.0),
                vwap_distance_atr=vwap_distance_atr,
                confirmation_enabled=self._config.confirmation.enabled,
                use_choch=self._config.context.use_choch,
                use_fvg=self._config.context.use_fvg,
            )

        secondary_side = _secondary_ema_continuation_side(features)
        if secondary_side is not None:
            return _approve(
                features=features,
                side=secondary_side,
                setup=YMSignalSetup.SECONDARY_EMA_CONTINUATION,
                regime=Regime.TREND,
                base_score=72.0,
                vwap_distance_atr=vwap_distance_atr,
                confirmation_enabled=self._config.confirmation.enabled,
                use_choch=self._config.context.use_choch,
                use_fvg=self._config.context.use_fvg,
            )

        return _reject(YMSignalRejection.NO_MEAN_REVERSION_OR_EMA_CONTINUATION_SETUP)


def _primary_mean_reversion_side(vwap_distance_atr: float) -> OrderSide | None:
    if -_PRIMARY_MAX_DISTANCE_ATR <= vwap_distance_atr <= -_PRIMARY_MIN_DISTANCE_ATR:
        return OrderSide.BUY
    if _PRIMARY_MIN_DISTANCE_ATR <= vwap_distance_atr <= _PRIMARY_MAX_DISTANCE_ATR:
        return OrderSide.SELL
    return None


def _secondary_ema_continuation_side(features: YMSignalFeatures) -> OrderSide | None:
    if (
        features.close_price > features.anchored_vwap
        and features.ema_fast > features.ema_slow
        and features.close_price >= features.ema_fast
    ):
        return OrderSide.BUY
    if (
        features.close_price < features.anchored_vwap
        and features.ema_fast < features.ema_slow
        and features.close_price <= features.ema_fast
    ):
        return OrderSide.SELL
    return None


def _approve(
    *,
    features: YMSignalFeatures,
    side: OrderSide,
    setup: YMSignalSetup,
    regime: Regime,
    base_score: float,
    vwap_distance_atr: float,
    confirmation_enabled: bool,
    use_choch: bool,
    use_fvg: bool,
) -> YMSignalEvaluation:
    score = base_score + _optional_context_score(
        use_choch=use_choch,
        choch_confirmed=features.choch_confirmed,
        use_fvg=use_fvg,
        fvg_present=features.fvg_present,
        confirmation_enabled=confirmation_enabled,
        intermarket_confirmed=features.intermarket_confirmed,
    )
    signal = SignalCandidate(
        ts=features.ts,
        strategy=StrategyModule.STRAT_YM_SIGNAL,
        symbol=features.symbol,
        side=side,
        regime=regime,
        score=score,
    )
    return YMSignalEvaluation(
        approved=True,
        rejection_reason=None,
        candidate=YMSignalResult(
            signal=signal,
            setup=setup,
            vwap_distance_atr=vwap_distance_atr,
            choch_confirmed=features.choch_confirmed,
            fvg_present=features.fvg_present,
            intermarket_confirmed=features.intermarket_confirmed,
        ),
    )


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
        score += 2.0
    if use_fvg and fvg_present:
        score += 2.0
    if confirmation_enabled and intermarket_confirmed is True:
        score += 1.5
    return score


def _reject(reason: YMSignalRejection) -> YMSignalEvaluation:
    return YMSignalEvaluation(approved=False, rejection_reason=reason, candidate=None)
