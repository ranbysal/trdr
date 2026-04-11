"""Deterministic orchestrator for the corrected futures signal architecture."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time
from enum import Enum
from typing import Iterable, TypeAlias

import pandas as pd

from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.enums import Family, StrategyModule
from futures_bot.core.types import InstrumentMeta, SignalCandidate
from futures_bot.features import (
    InstrumentSessionState,
    effective_anchor_timestamp,
    roll_instrument_session_state,
)
from futures_bot.pipeline.portfolio_orchestrator import StrategyCandidate
from futures_bot.risk.daily_halt import DailyHaltManager
from futures_bot.risk.models import OpenPositionMtmSnapshot, SingleLegSizingRequest, SizingDecision
from futures_bot.risk.sizing_single import size_single_leg_with_hard_risk_cap
from futures_bot.strategies.gold_models import GoldSignalFeatures, GoldSignalResult
from futures_bot.strategies.gold_signal import GoldSignalStrategy
from futures_bot.strategies.nq_models import NQSignalFeatures, NQSignalResult
from futures_bot.strategies.nq_signal import NQSignalStrategy
from futures_bot.strategies.ym_models import YMSignalFeatures, YMSignalResult
from futures_bot.strategies.ym_signal import YMSignalStrategy


class EvaluationStage(str, Enum):
    MARKET_SESSION_STATE_UPDATE = "market_session_state_update"
    ANCHORED_SESSION_SELECTION = "anchored_session_selection"
    INDICATOR_UPDATE = "indicator_update"
    LIQUIDITY_NEWS_GATING = "liquidity_news_gating"
    INSTRUMENT_SIGNAL_EVALUATION = "instrument_signal_evaluation"
    HARD_RISK_CAP_SIZING = "hard_risk_cap_sizing"
    DAILY_HALT_CHECK = "daily_halt_check"
    FINAL_SIGNAL_OUTPUT = "final_signal_output"


class StageStatus(str, Enum):
    PASSED = "passed"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class StageEvent:
    stage: EvaluationStage
    status: StageStatus
    reason: str


@dataclass(frozen=True, slots=True)
class BaseEvaluationRequest:
    bars_1m: pd.DataFrame
    instrument: InstrumentMeta
    session_start_equity: float
    realized_pnl: float
    open_positions: tuple[OpenPositionMtmSnapshot, ...]
    liquidity_ok: bool
    macro_blocked: bool
    choch_confirmed: bool = False
    fvg_present: bool = False
    intermarket_confirmed: bool | None = None


@dataclass(frozen=True, slots=True)
class NQEvaluationRequest(BaseEvaluationRequest):
    pullback_price: float = 0.0
    structure_break_price: float = 0.0
    order_block_low: float = 0.0
    order_block_high: float = 0.0


@dataclass(frozen=True, slots=True)
class YMEvaluationRequest(BaseEvaluationRequest):
    pass


@dataclass(frozen=True, slots=True)
class GoldEvaluationRequest(BaseEvaluationRequest):
    pullback_price: float | None = None
    structure_break_price: float | None = None
    order_block_low: float | None = None
    order_block_high: float | None = None


@dataclass(frozen=True, slots=True)
class AcceptedSignalOutput:
    signal: SignalCandidate
    candidate: NQSignalResult | YMSignalResult | GoldSignalResult
    sizing: SizingDecision
    stage_events: tuple[StageEvent, ...]
    session_state: InstrumentSessionState
    strategy_candidate: StrategyCandidate


@dataclass(frozen=True, slots=True)
class RejectedSignalOutput:
    rejection_reason: str
    stage_events: tuple[StageEvent, ...]
    session_state: InstrumentSessionState


@dataclass(frozen=True, slots=True)
class RiskRejectedSignalOutput:
    rejection_reason: str
    sizing: SizingDecision
    stage_events: tuple[StageEvent, ...]
    session_state: InstrumentSessionState


@dataclass(frozen=True, slots=True)
class DailyHaltRejectedSignalOutput:
    rejection_reason: str
    stage_events: tuple[StageEvent, ...]
    session_state: InstrumentSessionState


@dataclass(frozen=True, slots=True)
class LiquidityNewsRejectedSignalOutput:
    rejection_reason: str
    stage_events: tuple[StageEvent, ...]
    session_state: InstrumentSessionState


CorrectedOrchestratorOutput: TypeAlias = (
    AcceptedSignalOutput
    | RejectedSignalOutput
    | RiskRejectedSignalOutput
    | DailyHaltRejectedSignalOutput
    | LiquidityNewsRejectedSignalOutput
)


class CorrectedSignalOrchestrator:
    """Shared pipeline with explicit stages and separate instrument evaluators."""

    def __init__(
        self,
        *,
        nq_config: NQStrategyConfig,
        ym_config: YMStrategyConfig,
        gold_config: GoldStrategyConfig,
    ) -> None:
        self._nq_config = nq_config
        self._ym_config = ym_config
        self._gold_config = gold_config
        self._nq_strategy = NQSignalStrategy(nq_config)
        self._ym_strategy = YMSignalStrategy(ym_config)
        self._gold_strategy = GoldSignalStrategy(gold_config)
        self._daily_halt_manager = DailyHaltManager()
        self._session_states: dict[str, InstrumentSessionState] = {}
        self._indicator_caches: dict[str, _IncrementalIndicatorCache] = {}

    @property
    def nq_config(self) -> NQStrategyConfig:
        return self._nq_config

    @property
    def ym_config(self) -> YMStrategyConfig:
        return self._ym_config

    @property
    def gold_config(self) -> GoldStrategyConfig:
        return self._gold_config

    def evaluate_nq(self, request: NQEvaluationRequest) -> CorrectedOrchestratorOutput:
        return self._evaluate_nq(request)

    def evaluate_ym(self, request: YMEvaluationRequest) -> CorrectedOrchestratorOutput:
        return self._evaluate_ym(request)

    def evaluate_gold(self, request: GoldEvaluationRequest) -> CorrectedOrchestratorOutput:
        return self._evaluate_gold(request)

    def _evaluate_nq(self, request: NQEvaluationRequest) -> CorrectedOrchestratorOutput:
        stage_events, session_state, data = self._prepare_pipeline(
            request=request,
            anchor_time=self._nq_config.anchor_time,
            timezone=self._nq_config.timezone,
        )
        if isinstance(data, _RejectedStageData):
            return self._finalize_stage_rejection(data=data, stage_events=stage_events, session_state=session_state)

        liquidity_rejection = self._liquidity_gate(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if liquidity_rejection is not None:
            return liquidity_rejection

        evaluation = self._nq_strategy.evaluate(
            features=NQSignalFeatures(
                ts=data.latest_ts,
                symbol=request.instrument.symbol,
                close_price=data.latest_close,
                ema_fast=data.ema_fast,
                ema_slow=data.ema_slow,
                pullback_price=request.pullback_price,
                structure_break_price=request.structure_break_price,
                order_block_low=request.order_block_low,
                order_block_high=request.order_block_high,
                atr_5m=data.atr_5m,
                liquidity_ok=request.liquidity_ok,
                macro_blocked=request.macro_blocked,
                choch_confirmed=request.choch_confirmed,
                fvg_present=request.fvg_present,
                intermarket_confirmed=request.intermarket_confirmed,
            )
        )
        if not evaluation.approved or evaluation.candidate is None:
            return self._reject_signal(
                stage_events=stage_events,
                session_state=session_state,
                reason=evaluation.rejection_reason.value if evaluation.rejection_reason is not None else "signal_rejected",
            )
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.INSTRUMENT_SIGNAL_EVALUATION,
                status=StageStatus.PASSED,
                reason="instrument_signal_approved",
            )
        )

        sizing = self._size_signal(
            instrument=request.instrument,
            entry_price=data.latest_close,
            stop_price=min(request.order_block_low, data.ema_slow),
            atr_14_1m_price=data.atr_14_1m_price,
            hard_risk_dollars=self._nq_config.hard_risk_per_trade_dollars,
            stage_events=stage_events,
        )
        if isinstance(sizing, RiskRejectedSignalOutput):
            return sizing

        halt = self._daily_halt_check(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if halt is not None:
            return halt

        return self._accept(
            candidate=evaluation.candidate,
            sizing=sizing,
            stage_events=stage_events,
            session_state=session_state,
            instrument=request.instrument,
        )

    def _evaluate_ym(self, request: YMEvaluationRequest) -> CorrectedOrchestratorOutput:
        stage_events, session_state, data = self._prepare_pipeline(
            request=request,
            anchor_time=self._ym_config.anchor_time,
            timezone=self._ym_config.timezone,
        )
        if isinstance(data, _RejectedStageData):
            return self._finalize_stage_rejection(data=data, stage_events=stage_events, session_state=session_state)

        liquidity_rejection = self._liquidity_gate(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if liquidity_rejection is not None:
            return liquidity_rejection

        evaluation = self._ym_strategy.evaluate(
            features=YMSignalFeatures(
                ts=data.latest_ts,
                symbol=request.instrument.symbol,
                close_price=data.latest_close,
                anchored_vwap=data.anchored_vwap,
                ema_fast=data.ema_fast,
                ema_slow=data.ema_slow,
                atr_5m=data.atr_5m,
                liquidity_ok=request.liquidity_ok,
                macro_blocked=request.macro_blocked,
                choch_confirmed=request.choch_confirmed,
                fvg_present=request.fvg_present,
                intermarket_confirmed=request.intermarket_confirmed,
            )
        )
        if not evaluation.approved or evaluation.candidate is None:
            return self._reject_signal(
                stage_events=stage_events,
                session_state=session_state,
                reason=evaluation.rejection_reason.value if evaluation.rejection_reason is not None else "signal_rejected",
            )
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.INSTRUMENT_SIGNAL_EVALUATION,
                status=StageStatus.PASSED,
                reason="instrument_signal_approved",
            )
        )

        stop_price = (
            data.latest_close - data.atr_5m
            if evaluation.candidate.signal.side.value == "buy"
            else data.latest_close + data.atr_5m
        )
        sizing = self._size_signal(
            instrument=request.instrument,
            entry_price=data.latest_close,
            stop_price=stop_price,
            atr_14_1m_price=data.atr_14_1m_price,
            hard_risk_dollars=self._ym_config.hard_risk_per_trade_dollars,
            stage_events=stage_events,
        )
        if isinstance(sizing, RiskRejectedSignalOutput):
            return sizing

        halt = self._daily_halt_check(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if halt is not None:
            return halt

        return self._accept(
            candidate=evaluation.candidate,
            sizing=sizing,
            stage_events=stage_events,
            session_state=session_state,
            instrument=request.instrument,
        )

    def _evaluate_gold(self, request: GoldEvaluationRequest) -> CorrectedOrchestratorOutput:
        stage_events, session_state, data = self._prepare_pipeline(
            request=request,
            anchor_time=self._gold_config.anchor_time,
            timezone=self._gold_config.timezone,
        )
        if isinstance(data, _RejectedStageData):
            return self._finalize_stage_rejection(data=data, stage_events=stage_events, session_state=session_state)

        liquidity_rejection = self._liquidity_gate(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if liquidity_rejection is not None:
            return liquidity_rejection

        evaluation = self._gold_strategy.evaluate(
            features=GoldSignalFeatures(
                ts=data.latest_ts,
                symbol=request.instrument.symbol,
                close_price=data.latest_close,
                anchored_vwap=data.anchored_vwap,
                atr_5m=data.atr_5m,
                liquidity_ok=request.liquidity_ok,
                macro_blocked=request.macro_blocked,
                pullback_price=request.pullback_price,
                structure_break_price=request.structure_break_price,
                order_block_low=request.order_block_low,
                order_block_high=request.order_block_high,
                choch_confirmed=request.choch_confirmed,
                fvg_present=request.fvg_present,
            )
        )
        if not evaluation.approved or evaluation.candidate is None:
            return self._reject_signal(
                stage_events=stage_events,
                session_state=session_state,
                reason=evaluation.rejection_reason.value if evaluation.rejection_reason is not None else "signal_rejected",
            )
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.INSTRUMENT_SIGNAL_EVALUATION,
                status=StageStatus.PASSED,
                reason="instrument_signal_approved",
            )
        )

        if evaluation.candidate.setup.value == "secondary_structural_order_block":
            if evaluation.candidate.signal.side.value == "buy":
                stop_price = request.order_block_low if request.order_block_low is not None else data.latest_close - data.atr_5m
            else:
                stop_price = request.order_block_high if request.order_block_high is not None else data.latest_close + data.atr_5m
        else:
            stop_price = (
                data.latest_close - data.atr_5m
                if evaluation.candidate.signal.side.value == "buy"
                else data.latest_close + data.atr_5m
            )

        sizing = self._size_signal(
            instrument=request.instrument,
            entry_price=data.latest_close,
            stop_price=stop_price,
            atr_14_1m_price=data.atr_14_1m_price,
            hard_risk_dollars=self._gold_config.hard_risk_per_trade_dollars,
            stage_events=stage_events,
        )
        if isinstance(sizing, RiskRejectedSignalOutput):
            return sizing

        halt = self._daily_halt_check(
            request=request,
            stage_events=stage_events,
            session_state=session_state,
        )
        if halt is not None:
            return halt

        return self._accept(
            candidate=evaluation.candidate,
            sizing=sizing,
            stage_events=stage_events,
            session_state=session_state,
            instrument=request.instrument,
        )

    def _prepare_pipeline(
        self,
        *,
        request: BaseEvaluationRequest,
        anchor_time: time,
        timezone: str,
    ) -> tuple[list[StageEvent], InstrumentSessionState, _PreparedIndicatorData | _RejectedStageData]:
        stage_events: list[StageEvent] = []
        bars = _prepare_bars(request.bars_1m)
        latest_ts = pd.Timestamp(bars["ts"].iloc[-1]).to_pydatetime()

        session_state = roll_instrument_session_state(
            self._session_states.get(request.instrument.symbol),
            ts=latest_ts,
            instrument_symbol=request.instrument.symbol,
            anchor_time=anchor_time,
            timezone=timezone,
        )
        self._session_states[request.instrument.symbol] = session_state
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.MARKET_SESSION_STATE_UPDATE,
                status=StageStatus.PASSED,
                reason="market_session_updated",
            )
        )

        anchor_ts = effective_anchor_timestamp(session_state, ts=latest_ts)
        if anchor_ts is None:
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.ANCHORED_SESSION_SELECTION,
                    status=StageStatus.REJECTED,
                    reason="no_effective_anchored_session",
                )
            )
            return stage_events, session_state, _RejectedStageData(reason="no_effective_anchored_session")
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.ANCHORED_SESSION_SELECTION,
                status=StageStatus.PASSED,
                reason="anchored_session_selected",
            )
        )

        prepared_data = self._prepare_indicator_data(
            instrument_symbol=request.instrument.symbol,
            bars=bars,
            anchor_ts=anchor_ts,
        )
        if isinstance(prepared_data, _RejectedStageData):
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.INDICATOR_UPDATE,
                    status=StageStatus.REJECTED,
                    reason="indicator_data_unavailable",
                )
            )
            return stage_events, session_state, _RejectedStageData(reason="indicator_data_unavailable")
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.INDICATOR_UPDATE,
                status=StageStatus.PASSED,
                reason="indicators_updated",
            )
        )
        return stage_events, session_state, prepared_data

    def _prepare_indicator_data(
        self,
        *,
        instrument_symbol: str,
        bars: pd.DataFrame,
        anchor_ts: datetime,
    ) -> _PreparedIndicatorData | _RejectedStageData:
        cache = self._indicator_caches.get(instrument_symbol)
        latest_ts = pd.Timestamp(bars["ts"].iloc[-1]).to_pydatetime()
        row_count = len(bars.index)

        if cache is None or self._cache_requires_rebuild(cache=cache, bars=bars, latest_ts=latest_ts):
            cache = _IncrementalIndicatorCache()
            self._indicator_caches[instrument_symbol] = cache
            cache.reset_active_session(anchor_ts)
            self._rebuild_indicator_cache(cache=cache, bars=bars, anchor_ts=anchor_ts)
        elif row_count == cache.processed_count + 1:
            if cache.active_anchor_ts != anchor_ts:
                cache.reset_active_session(anchor_ts)
            self._apply_bar_to_indicator_cache(cache=cache, row=bars.iloc[-1], anchor_ts=anchor_ts)
        elif cache.active_anchor_ts != anchor_ts:
            cache = _IncrementalIndicatorCache()
            self._indicator_caches[instrument_symbol] = cache
            cache.reset_active_session(anchor_ts)
            self._rebuild_indicator_cache(cache=cache, bars=bars, anchor_ts=anchor_ts)

        if not all(
            math.isfinite(value)
            for value in (
                cache.latest_anchored_vwap,
                cache.latest_atr_1m,
                cache.latest_ema_fast,
                cache.latest_ema_slow,
                cache.latest_atr_5m,
            )
        ):
            return _RejectedStageData(reason="indicator_data_unavailable")
        return _PreparedIndicatorData(
            latest_ts=latest_ts,
            latest_close=float(bars["close"].iloc[-1]),
            anchored_vwap=cache.latest_anchored_vwap,
            ema_fast=cache.latest_ema_fast,
            ema_slow=cache.latest_ema_slow,
            atr_5m=cache.latest_atr_5m,
            atr_14_1m_price=cache.latest_atr_1m,
        )

    def _cache_requires_rebuild(
        self,
        *,
        cache: _IncrementalIndicatorCache,
        bars: pd.DataFrame,
        latest_ts: datetime,
    ) -> bool:
        row_count = len(bars.index)
        if cache.processed_count == 0:
            return True
        if row_count < cache.processed_count:
            return True
        if row_count == cache.processed_count:
            return cache.last_ts != latest_ts
        if row_count != cache.processed_count + 1:
            return True
        next_ts = pd.Timestamp(bars["ts"].iloc[-1]).to_pydatetime()
        if cache.last_ts is None:
            return True
        return next_ts <= cache.last_ts

    def _rebuild_indicator_cache(
        self,
        *,
        cache: _IncrementalIndicatorCache,
        bars: pd.DataFrame,
        anchor_ts: datetime,
    ) -> None:
        for row in bars.itertuples(index=False):
            self._apply_bar_to_indicator_cache(cache=cache, row=row, anchor_ts=anchor_ts)

    def _apply_bar_to_indicator_cache(
        self,
        *,
        cache: _IncrementalIndicatorCache,
        row: object,
        anchor_ts: datetime,
    ) -> None:
        ts = pd.Timestamp(_row_value(row, "ts")).to_pydatetime()
        open_price = float(_row_value(row, "open"))
        high_price = float(_row_value(row, "high"))
        low_price = float(_row_value(row, "low"))
        close_price = float(_row_value(row, "close"))
        volume = float(_row_value(row, "volume"))

        tr_1m = _compute_true_range_value(
            high=high_price,
            low=low_price,
            previous_close=cache.last_close_1m,
        )
        cache.latest_atr_1m = cache.atr_14_1m_window.append(tr_1m)
        cache.last_close_1m = close_price
        cache.last_ts = ts
        cache.processed_count += 1

        if ts < anchor_ts:
            return

        cache.active_cum_pv += close_price * volume
        cache.active_cum_v += volume
        if cache.active_cum_v <= 0.0:
            cache.latest_anchored_vwap = math.nan
            return

        cache.latest_anchored_vwap = cache.active_cum_pv / cache.active_cum_v
        bucket_start = pd.Timestamp(ts).floor("5min")
        if not cache.active_5m_bars or cache.active_5m_bars[-1].bucket_start != bucket_start:
            cache.active_5m_bars.append(
                _FiveMinuteBar(
                    bucket_start=bucket_start,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    session_vwap=cache.latest_anchored_vwap,
                )
            )
        else:
            current_bar = cache.active_5m_bars[-1]
            current_bar.high = max(current_bar.high, high_price)
            current_bar.low = min(current_bar.low, low_price)
            current_bar.close = close_price
            current_bar.volume += volume
            current_bar.session_vwap = cache.latest_anchored_vwap

        close_values = [bar.close for bar in cache.active_5m_bars]
        cache.latest_ema_fast = _ema_latest(close_values, span=9)
        cache.latest_ema_slow = _ema_latest(close_values, span=21)
        cache.latest_atr_5m = _rolling_atr_latest(cache.active_5m_bars, window=14)

    def _liquidity_gate(
        self,
        *,
        request: BaseEvaluationRequest,
        stage_events: list[StageEvent],
        session_state: InstrumentSessionState,
    ) -> LiquidityNewsRejectedSignalOutput | None:
        if not request.liquidity_ok:
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.LIQUIDITY_NEWS_GATING,
                    status=StageStatus.REJECTED,
                    reason="liquidity_gate_failed",
                )
            )
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                    status=StageStatus.REJECTED,
                    reason="rejected_due_to_liquidity_news",
                )
            )
            return LiquidityNewsRejectedSignalOutput(
                rejection_reason="liquidity_gate_failed",
                stage_events=tuple(stage_events),
                session_state=session_state,
            )
        if request.macro_blocked:
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.LIQUIDITY_NEWS_GATING,
                    status=StageStatus.REJECTED,
                    reason="macro_news_blocked",
                )
            )
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                    status=StageStatus.REJECTED,
                    reason="rejected_due_to_liquidity_news",
                )
            )
            return LiquidityNewsRejectedSignalOutput(
                rejection_reason="macro_news_blocked",
                stage_events=tuple(stage_events),
                session_state=session_state,
            )

        stage_events.append(
            StageEvent(
                stage=EvaluationStage.LIQUIDITY_NEWS_GATING,
                status=StageStatus.PASSED,
                reason="liquidity_news_gate_passed",
            )
        )
        return None

    def _reject_signal(
        self,
        *,
        stage_events: list[StageEvent],
        session_state: InstrumentSessionState,
        reason: str,
    ) -> RejectedSignalOutput:
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.INSTRUMENT_SIGNAL_EVALUATION,
                status=StageStatus.REJECTED,
                reason=reason,
            )
        )
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                status=StageStatus.REJECTED,
                reason="rejected_signal",
            )
        )
        return RejectedSignalOutput(
            rejection_reason=reason,
            stage_events=tuple(stage_events),
            session_state=session_state,
        )

    def _size_signal(
        self,
        *,
        instrument: InstrumentMeta,
        entry_price: float,
        stop_price: float,
        atr_14_1m_price: float,
        hard_risk_dollars: float,
        stage_events: list[StageEvent],
    ) -> SizingDecision | RiskRejectedSignalOutput:
        sizing = size_single_leg_with_hard_risk_cap(
            SingleLegSizingRequest(
                instrument=instrument,
                equity=hard_risk_dollars,
                risk_pct=1.0,
                entry_price=entry_price,
                stop_price=stop_price,
                atr_14_1m_price=atr_14_1m_price,
            ),
            hard_max_risk_dollars=hard_risk_dollars,
        )
        if not sizing.approved:
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.HARD_RISK_CAP_SIZING,
                    status=StageStatus.REJECTED,
                    reason=sizing.reason_code,
                )
            )
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                    status=StageStatus.REJECTED,
                    reason="rejected_due_to_risk",
                )
            )
            return RiskRejectedSignalOutput(
                rejection_reason=sizing.reason_code,
                sizing=sizing,
                stage_events=tuple(stage_events),
                session_state=self._session_states[instrument.symbol],
            )

        stage_events.append(
            StageEvent(
                stage=EvaluationStage.HARD_RISK_CAP_SIZING,
                status=StageStatus.PASSED,
                reason="hard_risk_cap_passed",
            )
        )
        return sizing

    def _daily_halt_check(
        self,
        *,
        request: BaseEvaluationRequest,
        stage_events: list[StageEvent],
        session_state: InstrumentSessionState,
    ) -> DailyHaltRejectedSignalOutput | None:
        self._daily_halt_manager.reset_session(session_start_equity=request.session_start_equity)
        self._daily_halt_manager.update_realized_pnl(realized_pnl=request.realized_pnl)
        self._daily_halt_manager.update_open_positions(open_positions=request.open_positions)
        decision = self._daily_halt_manager.can_open_new_entry()
        if not decision.approved:
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.DAILY_HALT_CHECK,
                    status=StageStatus.REJECTED,
                    reason=decision.reason_code,
                )
            )
            stage_events.append(
                StageEvent(
                    stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                    status=StageStatus.REJECTED,
                    reason="rejected_due_to_daily_halt",
                )
            )
            return DailyHaltRejectedSignalOutput(
                rejection_reason=decision.reason_code,
                stage_events=tuple(stage_events),
                session_state=session_state,
            )

        stage_events.append(
            StageEvent(
                stage=EvaluationStage.DAILY_HALT_CHECK,
                status=StageStatus.PASSED,
                reason="daily_halt_check_passed",
            )
        )
        return None

    def _accept(
        self,
        *,
        candidate: NQSignalResult | YMSignalResult | GoldSignalResult,
        sizing: SizingDecision,
        stage_events: list[StageEvent],
        session_state: InstrumentSessionState,
        instrument: InstrumentMeta,
    ) -> AcceptedSignalOutput:
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                status=StageStatus.PASSED,
                reason="accepted_signal",
            )
        )
        signal = candidate.signal
        strategy_candidate = StrategyCandidate(
            strategy=signal.strategy,
            family=instrument.family,
            symbols=(signal.symbol,),
            score=signal.score,
            slippage_est_ticks=sizing.slippage_est_ticks,
        )
        return AcceptedSignalOutput(
            signal=signal,
            candidate=candidate,
            sizing=sizing,
            stage_events=tuple(stage_events),
            session_state=session_state,
            strategy_candidate=strategy_candidate,
        )

    def _finalize_stage_rejection(
        self,
        *,
        data: _RejectedStageData,
        stage_events: list[StageEvent],
        session_state: InstrumentSessionState,
    ) -> RejectedSignalOutput:
        stage_events.append(
            StageEvent(
                stage=EvaluationStage.FINAL_SIGNAL_OUTPUT,
                status=StageStatus.REJECTED,
                reason="rejected_signal",
            )
        )
        return RejectedSignalOutput(
            rejection_reason=data.reason,
            stage_events=tuple(stage_events),
            session_state=session_state,
        )


@dataclass(frozen=True, slots=True)
class _PreparedIndicatorData:
    latest_ts: datetime
    latest_close: float
    anchored_vwap: float
    ema_fast: float
    ema_slow: float
    atr_5m: float
    atr_14_1m_price: float


@dataclass(frozen=True, slots=True)
class _RejectedStageData:
    reason: str


def _prepare_bars(bars: pd.DataFrame) -> pd.DataFrame:
    required = {"ts", "open", "high", "low", "close", "volume"}
    missing = required.difference(bars.columns)
    if missing:
        raise ValueError(f"bars_1m missing required columns: {sorted(missing)}")
    prepared = bars
    ts = prepared["ts"]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        prepared = bars.copy()
        prepared["ts"] = pd.to_datetime(prepared["ts"], errors="raise")
        ts = prepared["ts"]
    if ts.is_monotonic_increasing:
        if isinstance(prepared.index, pd.RangeIndex) and prepared.index.start == 0 and prepared.index.step == 1:
            return prepared
        return prepared.reset_index(drop=True)
    if prepared is bars:
        prepared = bars.copy()
    return prepared.sort_values("ts", kind="mergesort").reset_index(drop=True)


@dataclass(slots=True)
class _RollingMeanWindow:
    window: int
    values: list[float] = field(default_factory=list)
    sum_value: float = 0.0

    def append(self, value: float) -> float:
        self.values.append(value)
        self.sum_value += value
        if len(self.values) > self.window:
            self.sum_value -= self.values.pop(0)
        if len(self.values) < self.window:
            return math.nan
        return self.sum_value / float(self.window)


@dataclass(slots=True)
class _FiveMinuteBar:
    bucket_start: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    session_vwap: float


@dataclass(slots=True)
class _IncrementalIndicatorCache:
    processed_count: int = 0
    last_ts: datetime | None = None
    last_close_1m: float | None = None
    latest_atr_1m: float = math.nan
    atr_14_1m_window: _RollingMeanWindow = field(default_factory=lambda: _RollingMeanWindow(window=14))
    active_anchor_ts: datetime | None = None
    active_cum_pv: float = 0.0
    active_cum_v: float = 0.0
    latest_anchored_vwap: float = math.nan
    active_5m_bars: list[_FiveMinuteBar] = field(default_factory=list)
    latest_ema_fast: float = math.nan
    latest_ema_slow: float = math.nan
    latest_atr_5m: float = math.nan

    def reset_active_session(self, anchor_ts: datetime | None) -> None:
        self.active_anchor_ts = anchor_ts
        self.active_cum_pv = 0.0
        self.active_cum_v = 0.0
        self.latest_anchored_vwap = math.nan
        self.active_5m_bars = []
        self.latest_ema_fast = math.nan
        self.latest_ema_slow = math.nan
        self.latest_atr_5m = math.nan


def _row_value(row: object, key: str) -> object:
    if hasattr(row, key):
        return getattr(row, key)
    return row[key]


def _compute_true_range_value(*, high: float, low: float, previous_close: float | None) -> float:
    if previous_close is None:
        return high - low
    return max(high - low, abs(high - previous_close), abs(low - previous_close))


def _ema_latest(values: Iterable[float], *, span: int) -> float:
    sequence = list(values)
    if len(sequence) < span:
        return math.nan
    alpha = 2.0 / (float(span) + 1.0)
    ema_value = sequence[0]
    for value in sequence[1:]:
        ema_value = (alpha * value) + ((1.0 - alpha) * ema_value)
    return ema_value


def _rolling_atr_latest(bars: list[_FiveMinuteBar], *, window: int) -> float:
    if len(bars) < window:
        return math.nan
    tr_values: list[float] = []
    previous_close: float | None = None
    for bar in bars:
        tr_values.append(_compute_true_range_value(high=bar.high, low=bar.low, previous_close=previous_close))
        previous_close = bar.close
    if len(tr_values) < window:
        return math.nan
    return sum(tr_values[-window:]) / float(window)
