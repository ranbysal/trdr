"""Minimal integration pipeline for Strategy A ORB."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

from futures_bot.core.enums import OrderSide, Regime, StrategyModule
from futures_bot.core.types import Bar1m, InstrumentMeta, SignalCandidate
from futures_bot.data.calendar_store import LockoutStatus
from futures_bot.regime.models import FamilyRegimeState
from futures_bot.risk.cooldowns import ConsecutiveLossCooldownManager
from futures_bot.risk.daily_halt import DailyHaltManager
from futures_bot.risk.models import SingleLegSizingRequest, SizingDecision
from futures_bot.risk.portfolio_caps import PortfolioCapsManager
from futures_bot.risk.sizing_single import size_with_micro_routing
from futures_bot.scoring.strategy_a_scoring import (
    StrategyAScoreBreakdown,
    StrategyAScoringConfig,
    compute_pattern_quality,
    compute_strategy_a_score,
)
from futures_bot.strategies.strategy_a_models import StrategyAEntryPlan, StrategyAEvaluation, StrategyAFeatureSnapshot
from futures_bot.strategies.strategy_a_orb import StrategyAORB

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True, slots=True)
class ORBSymbolSnapshot:
    bar_1m: Bar1m
    instrument: InstrumentMeta
    atr_14_1m_price: float


@dataclass(frozen=True, slots=True)
class ORBFeatureSnapshot:
    session_vwap: float
    ema9_5m: float
    ema21_5m: float
    atr_14_5m: float
    vol_strong_1m: bool
    rvol_3bar_aggregate_5m: float | None
    exec_quality: float = 1.0
    ema9_1m: float | None = None


@dataclass(frozen=True, slots=True)
class ORBRiskVaultState:
    module_id: str
    equity: float
    risk_pct: float
    instruments_by_symbol: dict[str, InstrumentMeta]
    caps_manager: PortfolioCapsManager
    cooldown_manager: ConsecutiveLossCooldownManager
    daily_halt_manager: DailyHaltManager


@dataclass(frozen=True, slots=True)
class ORBSignalPacket:
    approved: bool
    reason_code: str
    signal: SignalCandidate | None
    entry_plan: StrategyAEntryPlan | None
    sizing: SizingDecision | None
    score_breakdown: StrategyAScoreBreakdown | None


def run_strategy_a_orb_pipeline(
    *,
    strategy: StrategyAORB,
    symbol_snapshot: ORBSymbolSnapshot,
    feature_snapshot: ORBFeatureSnapshot,
    family_regime_state: FamilyRegimeState,
    lockout_state: LockoutStatus,
    risk_state: ORBRiskVaultState,
    scoring_config: StrategyAScoringConfig | None = None,
) -> ORBSignalPacket:
    """Run a minimal deterministic pipeline from data/features/regime/risk to ORB signal."""
    bar = symbol_snapshot.bar_1m
    symbol = bar.symbol
    now_et = _to_et(bar.ts)

    strategy.update_or_state(bar)

    if lockout_state.is_locked_out:
        return _reject("TIER1_LOCKOUT_ACTIVE")

    if risk_state.cooldown_manager.is_in_cooldown(
        module_id=risk_state.module_id,
        symbol=symbol,
        now=bar.ts,
    ):
        return _reject("COOLDOWN_ACTIVE")

    if risk_state.caps_manager.has_open_position(symbol=symbol):
        return _reject("POSITION_ALREADY_OPEN")

    halt = risk_state.daily_halt_manager.can_open_new_entry()
    if not halt.approved:
        return _reject(halt.reason_code)

    strat_features = StrategyAFeatureSnapshot(
        raw_regime=family_regime_state.raw_regime,
        low_volume_trend_streak_5m=family_regime_state.low_volume_trend_streak_5m,
        vol_strong_1m=feature_snapshot.vol_strong_1m,
        rvol_3bar_aggregate_5m=feature_snapshot.rvol_3bar_aggregate_5m,
        session_vwap=feature_snapshot.session_vwap,
        ema9_5m=feature_snapshot.ema9_5m,
        ema21_5m=feature_snapshot.ema21_5m,
        atr_14_5m=feature_snapshot.atr_14_5m,
        tier1_lockout_active=lockout_state.is_locked_out,
    )

    eval_result = strategy.evaluate_breakout_candidate(
        bar=bar,
        features=strat_features,
        tick_size=symbol_snapshot.instrument.tick_size,
    )
    if not eval_result.approved or eval_result.signal is None or eval_result.entry_plan is None:
        return _reject(eval_result.reason_code)

    state = strategy.update_or_state(bar)
    or_width = state.or_width
    if or_width is None:
        return _reject("OR_NOT_READY")

    breakdown = _compute_score(
        bar=bar,
        signal=eval_result.signal,
        or_width=or_width,
        features=feature_snapshot,
        family_regime_state=family_regime_state,
        config=scoring_config,
    )

    if breakdown.action == "reject":
        return ORBSignalPacket(
            approved=False,
            reason_code="SCORE_BELOW_THRESHOLD",
            signal=None,
            entry_plan=None,
            sizing=None,
            score_breakdown=breakdown,
        )

    effective_risk_pct = risk_state.risk_pct * breakdown.risk_fraction
    sizing_req = SingleLegSizingRequest(
        instrument=symbol_snapshot.instrument,
        equity=risk_state.equity,
        risk_pct=effective_risk_pct,
        entry_price=eval_result.entry_plan.entry_stop,
        stop_price=eval_result.entry_plan.initial_stop,
        atr_14_1m_price=symbol_snapshot.atr_14_1m_price,
    )
    sizing = size_with_micro_routing(sizing_req, instruments_by_symbol=risk_state.instruments_by_symbol)
    if not sizing.approved:
        return ORBSignalPacket(
            approved=False,
            reason_code=sizing.reason_code,
            signal=None,
            entry_plan=None,
            sizing=sizing,
            score_breakdown=breakdown,
        )

    proposed_risk = float(sizing.contracts) * float(sizing.adjusted_risk_per_contract)
    cap_decision = risk_state.caps_manager.check_new_position(
        family=symbol_snapshot.instrument.family,
        symbol=sizing.routed_symbol,
        proposed_risk_dollars=proposed_risk,
    )
    if not cap_decision.approved:
        return ORBSignalPacket(
            approved=False,
            reason_code=cap_decision.reason_code,
            signal=None,
            entry_plan=None,
            sizing=sizing,
            score_breakdown=breakdown,
        )

    risk_state.caps_manager.record_open_position(
        family=symbol_snapshot.instrument.family,
        symbol=sizing.routed_symbol,
        risk_dollars=proposed_risk,
    )

    scored_signal = SignalCandidate(
        ts=eval_result.signal.ts,
        strategy=StrategyModule.STRAT_A_ORB,
        symbol=eval_result.signal.symbol,
        side=eval_result.signal.side,
        regime=eval_result.signal.regime,
        score=breakdown.final_score,
    )

    return ORBSignalPacket(
        approved=True,
        reason_code="APPROVED",
        signal=scored_signal,
        entry_plan=eval_result.entry_plan,
        sizing=sizing,
        score_breakdown=breakdown,
    )


def _compute_score(
    *,
    bar: Bar1m,
    signal: SignalCandidate,
    or_width: float,
    features: ORBFeatureSnapshot,
    family_regime_state: FamilyRegimeState,
    config: StrategyAScoringConfig | None,
) -> StrategyAScoreBreakdown:
    regime_quality = 1.0 if family_regime_state.raw_regime is Regime.TREND else 0.0
    if family_regime_state.is_weak_neutral and regime_quality > 0.0:
        regime_quality = 0.6

    session_quality = _session_quality(_to_et(bar.ts).timetz().replace(tzinfo=None))

    if signal.side is OrderSide.BUY:
        vwap_align = 1.0 if bar.close > features.session_vwap else 0.0
    else:
        vwap_align = 1.0 if bar.close < features.session_vwap else 0.0

    pattern_quality = compute_pattern_quality(
        or_width=or_width,
        atr_14_5m=features.atr_14_5m,
        target_ratio=(config.target_ratio if config is not None else StrategyAScoringConfig().target_ratio),
        tolerance_ratio=(
            config.tolerance_ratio if config is not None else StrategyAScoringConfig().tolerance_ratio
        ),
    )

    if features.vol_strong_1m:
        volume_quality = 1.0
    elif features.rvol_3bar_aggregate_5m is not None and features.rvol_3bar_aggregate_5m >= 1.10:
        volume_quality = 0.8
    else:
        volume_quality = 0.0

    width_ratio = or_width / features.atr_14_5m if features.atr_14_5m > 0.0 else 0.0
    vol_sanity = 1.0 if 1.0 <= width_ratio <= 4.5 else 0.0

    return compute_strategy_a_score(
        regime_quality=regime_quality,
        session_quality=session_quality,
        vwap_align=vwap_align,
        pattern_quality=pattern_quality,
        volume_quality=volume_quality,
        vol_sanity=vol_sanity,
        exec_quality=features.exec_quality,
        config=config,
    )


def _session_quality(t: time) -> float:
    if time(9, 45) <= t <= time(10, 15):
        return 1.0
    if time(10, 15) < t <= time(10, 30):
        return 0.8
    if time(9, 30) <= t <= time(10, 45):
        return 0.6
    return 0.0


def _to_et(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)


def _reject(code: str) -> ORBSignalPacket:
    return ORBSignalPacket(
        approved=False,
        reason_code=code,
        signal=None,
        entry_plan=None,
        sizing=None,
        score_breakdown=None,
    )
