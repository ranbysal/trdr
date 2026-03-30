from __future__ import annotations

from datetime import datetime, timezone

from futures_bot.config.models import GoldStrategyConfig, IntermarketConfirmationConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.enums import OrderSide, StrategyModule
from futures_bot.strategies.gold_models import GoldSignalSetup
from futures_bot.strategies.gold_signal import GoldSignalStrategy
from futures_bot.strategies.nq_models import NQSignalFeatures, NQSignalSetup
from futures_bot.strategies.nq_signal import NQSignalStrategy
from futures_bot.strategies.ym_models import YMSignalFeatures, YMSignalSetup
from futures_bot.strategies.ym_signal import YMSignalStrategy
from futures_bot.strategies.gold_models import GoldSignalFeatures


def test_nq_structural_logic_fires_without_mandatory_choch_or_fvg() -> None:
    strategy = NQSignalStrategy(
        NQStrategyConfig(
            hard_risk_per_trade_dollars=750.0,
            daily_halt_loss_dollars=1_500.0,
        )
    )

    evaluation = strategy.evaluate(
        features=NQSignalFeatures(
            ts=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
            symbol="NQ",
            close_price=20_520.0,
            ema_fast=20_510.0,
            ema_slow=20_480.0,
            pullback_price=20_495.0,
            structure_break_price=20_500.0,
            order_block_low=20_490.0,
            order_block_high=20_505.0,
            atr_5m=18.0,
            liquidity_ok=True,
            macro_blocked=False,
            choch_confirmed=False,
            fvg_present=False,
        )
    )

    assert evaluation.approved is True
    assert evaluation.candidate is not None
    assert evaluation.candidate.setup is NQSignalSetup.STRUCTURAL_CONTINUATION
    assert evaluation.candidate.signal.side is OrderSide.BUY
    assert evaluation.candidate.signal.strategy is StrategyModule.STRAT_NQ_SIGNAL


def test_ym_mean_reversion_logic_can_fire_standalone() -> None:
    strategy = YMSignalStrategy(
        YMStrategyConfig(
            hard_risk_per_trade_dollars=500.0,
            daily_halt_loss_dollars=1_500.0,
        )
    )

    evaluation = strategy.evaluate(
        features=YMSignalFeatures(
            ts=datetime(2026, 1, 5, 15, 5, tzinfo=timezone.utc),
            symbol="YM",
            close_price=42_180.0,
            anchored_vwap=42_200.0,
            ema_fast=42_170.0,
            ema_slow=42_190.0,
            atr_5m=20.0,
            liquidity_ok=True,
            macro_blocked=False,
        )
    )

    assert evaluation.approved is True
    assert evaluation.candidate is not None
    assert evaluation.candidate.setup is YMSignalSetup.PRIMARY_MEAN_REVERSION
    assert evaluation.candidate.signal.side is OrderSide.BUY


def test_gold_mean_reversion_logic_can_fire_standalone() -> None:
    strategy = GoldSignalStrategy(
        GoldStrategyConfig(
            hard_risk_per_trade_dollars=400.0,
            daily_halt_loss_dollars=1_200.0,
        )
    )

    evaluation = strategy.evaluate(
        features=GoldSignalFeatures(
            ts=datetime(2026, 1, 5, 15, 10, tzinfo=timezone.utc),
            symbol="GC",
            close_price=2_652.0,
            anchored_vwap=2_640.0,
            atr_5m=12.0,
            liquidity_ok=True,
            macro_blocked=False,
        )
    )

    assert evaluation.approved is True
    assert evaluation.candidate is not None
    assert evaluation.candidate.setup is GoldSignalSetup.PRIMARY_MEAN_REVERSION
    assert evaluation.candidate.signal.side is OrderSide.SELL
    assert evaluation.candidate.signal.strategy is StrategyModule.STRAT_GOLD_SIGNAL


def test_optional_contextual_features_do_not_block_otherwise_valid_trades() -> None:
    strategy = NQSignalStrategy(
        NQStrategyConfig(
            hard_risk_per_trade_dollars=750.0,
            daily_halt_loss_dollars=1_500.0,
            confirmation=IntermarketConfirmationConfig(confirm_with_symbol="YM", enabled=True),
        )
    )

    evaluation = strategy.evaluate(
        features=NQSignalFeatures(
            ts=datetime(2026, 1, 5, 15, 15, tzinfo=timezone.utc),
            symbol="NQ",
            close_price=20_520.0,
            ema_fast=20_510.0,
            ema_slow=20_480.0,
            pullback_price=20_495.0,
            structure_break_price=20_500.0,
            order_block_low=20_490.0,
            order_block_high=20_505.0,
            atr_5m=18.0,
            liquidity_ok=True,
            macro_blocked=False,
            choch_confirmed=False,
            fvg_present=False,
            intermarket_confirmed=False,
        )
    )

    assert evaluation.approved is True
    assert evaluation.candidate is not None
    assert evaluation.candidate.signal.side is OrderSide.BUY


def test_no_cross_instrument_hard_dependency_exists() -> None:
    strategy = YMSignalStrategy(
        YMStrategyConfig(
            hard_risk_per_trade_dollars=500.0,
            daily_halt_loss_dollars=1_500.0,
            confirmation=IntermarketConfirmationConfig(confirm_with_symbol="NQ", enabled=True),
        )
    )

    evaluation = strategy.evaluate(
        features=YMSignalFeatures(
            ts=datetime(2026, 1, 5, 15, 20, tzinfo=timezone.utc),
            symbol="YM",
            close_price=42_220.0,
            anchored_vwap=42_200.0,
            ema_fast=42_205.0,
            ema_slow=42_180.0,
            atr_5m=20.0,
            liquidity_ok=True,
            macro_blocked=False,
            intermarket_confirmed=None,
        )
    )

    assert evaluation.approved is True
    assert evaluation.candidate is not None
    assert evaluation.candidate.setup is YMSignalSetup.PRIMARY_MEAN_REVERSION
