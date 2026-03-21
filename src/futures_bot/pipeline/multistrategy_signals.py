"""Unified multi-strategy signal loop for live watching and Telegram alerts."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np

from futures_bot.alerts.telegram import TelegramNotifier
from futures_bot.core.enums import Family, OrderSide, Regime, StrategyModule
from futures_bot.core.types import Bar1m, InstrumentMeta
from futures_bot.pipeline.portfolio_orchestrator import StrategyCandidate, resolve_strategy_conflicts
from futures_bot.runtime.ndjson_writer import NdjsonWriter
from futures_bot.scoring.strategy_a_scoring import (
    StrategyAScoringConfig,
    compute_pattern_quality,
    compute_strategy_a_score,
)
from futures_bot.signals.models import SignalIdea
from futures_bot.signals.state import AlertStateManager
from futures_bot.strategies.strategy_a_models import StrategyAFeatureSnapshot
from futures_bot.strategies.strategy_a_orb import StrategyAORB
from futures_bot.strategies.strategy_b_models import StrategyBFeatureSnapshot
from futures_bot.strategies.strategy_b_vwap_rev import StrategyBVWAPReversion
from futures_bot.strategies.strategy_c_metals_orb import StrategyCMetalsORB
from futures_bot.strategies.strategy_c_models import StrategyCEntryPlan, StrategyCFeatureSnapshot
from futures_bot.strategies.strategy_d_pair import PairSignal, evaluate_pair_signal


@dataclass(frozen=True, slots=True)
class _MarketRow:
    ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    session_vwap: float
    ema9_5m: float
    ema21_5m: float
    ema20_5m_slope: float
    atr_14_5m: float
    atr_14_1m_price: float
    rvol_3bar_aggregate_5m: float | None
    low_volume_trend_streak_5m: int
    vol_strong_1m: bool
    data_ok: bool
    quote_ok: bool
    trade_eligible: bool
    lockout: bool
    family_freeze: bool
    raw_regime: Regime
    is_weak_neutral: bool
    confidence: float
    market_open: bool = True
    signals_active: bool = True
    schedule_state: str = "open"


@dataclass(slots=True)
class _CandidatePlan:
    candidate: StrategyCandidate
    side: OrderSide | str
    module_id: str
    order_kind: str
    symbol: str
    entry_price: float
    initial_stop: float
    tp1_price: float
    tp2_price: float
    flatten_by: datetime
    regime: str
    confidence: float
    strategy_context: str
    chase_ticks: int = 0
    pair_hedge_symbol: str | None = None
    pair_beta: float = 0.0
    pair_stop_proxy: float = 0.0


def run_multistrategy_signal_loop(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    state_dir: str | Path | None = None,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    notifier: TelegramNotifier | None = None,
) -> Path:
    rows = _load_rows(data_path)
    engine = MultiStrategySignalEngine(
        out_dir=out_dir,
        state_dir=state_dir,
        instruments_by_symbol=instruments_by_symbol,
        enabled_strategies=enabled_strategies,
        notifier=notifier,
    )
    for row in rows:
        engine.process_row(row)
    engine.flush()
    return Path(out_dir) / "signal_events.ndjson"


class MultiStrategySignalEngine:
    """Stateful multi-strategy signal engine for streaming/live use."""

    def __init__(
        self,
        *,
        out_dir: str | Path,
        state_dir: str | Path | None = None,
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        notifier: TelegramNotifier | None = None,
        alert_emit_callback: Callable[..., None] | None = None,
        signal_register_callback: Callable[[SignalIdea], None] | None = None,
    ) -> None:
        self._runner = _MultiStrategySignalRunner(
            out_dir=Path(out_dir),
            state_dir=Path(state_dir) if state_dir is not None else None,
            instruments_by_symbol=instruments_by_symbol,
            enabled_strategies=enabled_strategies,
            notifier=notifier,
            alert_emit_callback=alert_emit_callback,
            signal_register_callback=signal_register_callback,
        )

    def process_row(self, row: dict[str, Any] | _MarketRow) -> None:
        if isinstance(row, _MarketRow):
            self._runner.step(row)
            return
        self._runner.step(_row_from_mapping(row))

    def flush(self) -> None:
        self._runner.flush()

    def active_count(self) -> int:
        return self._runner.active_count()

    def snapshot_records(self) -> list[dict[str, Any]]:
        return self._runner.snapshot_records()

    def restore(self, records: list[dict[str, Any]]) -> None:
        self._runner.restore(records)


class _MultiStrategySignalRunner:
    def __init__(
        self,
        *,
        out_dir: Path,
        state_dir: Path | None,
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        notifier: TelegramNotifier | None,
        alert_emit_callback: Callable[..., None] | None,
        signal_register_callback: Callable[[SignalIdea], None] | None,
    ) -> None:
        self._debug_log = NdjsonWriter(out_dir / "signal_engine.ndjson")
        self._state = AlertStateManager(
            out_dir=out_dir,
            state_dir=state_dir,
            notifier=notifier,
            on_emit=alert_emit_callback,
        )
        self._instruments = instruments_by_symbol
        self._enabled = enabled_strategies
        self._signal_register_callback = signal_register_callback
        self._a = StrategyAORB()
        self._b = StrategyBVWAPReversion()
        self._c = StrategyCMetalsORB()
        self._family_freeze: dict[Family, bool] = {}
        self._price_hist: dict[str, list[float]] = {}
        self._last_close: dict[str, float] = {}
        self._window_log_keys: set[tuple[str, str, str]] = set()
        self._rejection_log_keys: set[tuple[str, str, str, str]] = set()

    def flush(self) -> None:
        self._state.flush()
        self._debug_log.flush()

    def active_count(self) -> int:
        return self._state.active_count()

    def snapshot_records(self) -> list[dict[str, Any]]:
        return self._state.snapshot_records()

    def restore(self, records: list[dict[str, Any]]) -> None:
        self._state.restore(records)

    def step(self, row: _MarketRow) -> None:
        self._price_hist.setdefault(row.symbol, []).append(row.close)
        if len(self._price_hist[row.symbol]) > 500:
            self._price_hist[row.symbol] = self._price_hist[row.symbol][-500:]
        self._last_close[row.symbol] = row.close

        family = _family_for_symbol(row.symbol)
        if family is None:
            return
        instrument = self._instrument_or_synthetic(row.symbol, family)
        if row.family_freeze:
            self._family_freeze[instrument.family] = True

        self._state.process_market(
            ts=row.ts,
            symbol=row.symbol,
            high=row.high,
            low=row.low,
            close=row.close,
            regime=row.raw_regime.value,
            confidence=row.confidence,
            latest_prices=self._last_close,
        )

        if not row.market_open:
            self._log_strategy_rejections(row=row, reason_code=f"MARKET_{row.schedule_state.upper()}")
            return
        if not row.signals_active:
            self._log_strategy_rejections(row=row, reason_code="SIGNALS_PAUSED")
            return

        plans = self._collect_candidate_plans(row, instrument)
        if not plans:
            return
        selected, decisions = resolve_strategy_conflicts(
            candidates=[plan.candidate for plan in plans],
            open_symbols=self._state.active_symbols(),
        )
        selected_set = {(candidate.strategy, candidate.symbols) for candidate in selected}
        for decision in decisions:
            if decision.accepted:
                continue
            self._debug_log.write(
                {
                    "event": "SIGNAL_CANDIDATE_REJECTED",
                    "reason_code": decision.reason_code,
                    "score": decision.candidate.score,
                    "strategy": decision.candidate.strategy.value,
                    "symbol": "/".join(decision.candidate.symbols),
                    "timestamp_et": row.ts.isoformat(),
                }
            )
            self._debug_log.write(
                {
                    "event": "REJECTION_REASON",
                    "reason_code": decision.reason_code,
                    "score": decision.candidate.score,
                    "strategy": decision.candidate.strategy.value,
                    "symbol": "/".join(decision.candidate.symbols),
                    "timestamp_et": row.ts.isoformat(),
                    "symbols": list(decision.candidate.symbols),
                }
            )
        for plan in plans:
            if (plan.candidate.strategy, plan.candidate.symbols) not in selected_set:
                continue
            self._debug_log.write(
                {
                    "event": "SIGNAL_CANDIDATE_ACCEPTED",
                    "reason_code": None,
                    "score": plan.candidate.score,
                    "strategy": plan.candidate.strategy.value,
                    "symbol": "/".join(plan.candidate.symbols),
                    "timestamp_et": row.ts.isoformat(),
                }
            )
            idea = self._build_idea(plan=plan, row=row, instrument=instrument)
            self._state.register(idea)
            if self._signal_register_callback is not None:
                self._signal_register_callback(idea)

    def _collect_candidate_plans(self, row: _MarketRow, instrument: InstrumentMeta) -> list[_CandidatePlan]:
        plans: list[_CandidatePlan] = []
        bar = _bar_from_row(row)
        self._a.update_or_state(bar)
        self._c.update_or_state(bar)

        if StrategyModule.STRAT_A_ORB in self._enabled:
            gate = self._global_gate(row=row, family=instrument.family)
            if gate is None:
                self._log_window_active(row=row, strategy=StrategyModule.STRAT_A_ORB)
                plan = self._evaluate_a(row=row, bar=bar, instrument=instrument)
                if plan is not None:
                    plans.append(plan)
            else:
                self._log_rejection(row=row, strategy=StrategyModule.STRAT_A_ORB, reason_code=gate)
        if StrategyModule.STRAT_B_VWAP_REV in self._enabled:
            gate = self._global_gate(row=row, family=instrument.family)
            if gate is None:
                self._log_window_active(row=row, strategy=StrategyModule.STRAT_B_VWAP_REV)
                plan = self._evaluate_b(row=row, instrument=instrument)
                if plan is not None:
                    plans.append(plan)
            else:
                self._log_rejection(row=row, strategy=StrategyModule.STRAT_B_VWAP_REV, reason_code=gate)
        if StrategyModule.STRAT_C_METALS_ORB in self._enabled and row.symbol == "MGC":
            gate = self._global_gate(row=row, family=instrument.family)
            if gate is None:
                self._log_window_active(row=row, strategy=StrategyModule.STRAT_C_METALS_ORB)
                plan = self._evaluate_c(row=row, bar=bar, instrument=instrument)
                if plan is not None:
                    plans.append(plan)
            else:
                self._log_rejection(row=row, strategy=StrategyModule.STRAT_C_METALS_ORB, reason_code=gate)
        if StrategyModule.STRAT_D_PAIR in self._enabled and row.symbol == "MGC":
            gate = self._global_gate(row=row, family=Family.METALS)
            if gate is None:
                self._log_window_active(row=row, strategy=StrategyModule.STRAT_D_PAIR)
                plan = self._evaluate_d(row=row)
                if plan is not None:
                    plans.append(plan)
            else:
                self._log_rejection(row=row, strategy=StrategyModule.STRAT_D_PAIR, reason_code=gate)
        return plans

    def _evaluate_a(self, *, row: _MarketRow, bar: Bar1m, instrument: InstrumentMeta) -> _CandidatePlan | None:
        features = StrategyAFeatureSnapshot(
            raw_regime=row.raw_regime,
            low_volume_trend_streak_5m=row.low_volume_trend_streak_5m,
            vol_strong_1m=row.vol_strong_1m,
            rvol_3bar_aggregate_5m=row.rvol_3bar_aggregate_5m,
            session_vwap=row.session_vwap,
            ema9_5m=row.ema9_5m,
            ema21_5m=row.ema21_5m,
            atr_14_5m=row.atr_14_5m,
            tier1_lockout_active=row.lockout,
        )
        eval_result = self._a.evaluate_breakout_candidate(bar=bar, features=features, tick_size=instrument.tick_size)
        if not eval_result.approved or eval_result.signal is None or eval_result.entry_plan is None:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_A_ORB, reason_code="STRATEGY_RULES_NOT_MET")
            return None
        state = self._a.get_or_state(row.symbol)
        if state is None or state.or_width is None:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_A_ORB, reason_code="OR_STATE_NOT_READY")
            return None
        pattern_quality = compute_pattern_quality(
            or_width=state.or_width,
            atr_14_5m=row.atr_14_5m,
            target_ratio=StrategyAScoringConfig().target_ratio,
            tolerance_ratio=StrategyAScoringConfig().tolerance_ratio,
        )
        breakdown = compute_strategy_a_score(
            regime_quality=1.0 if row.raw_regime is Regime.TREND else 0.0,
            session_quality=1.0,
            vwap_align=1.0,
            pattern_quality=pattern_quality,
            volume_quality=1.0 if row.vol_strong_1m else 0.8,
            vol_sanity=1.0,
            exec_quality=1.0,
        )
        if breakdown.action == "reject":
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_A_ORB, reason_code="SCORE_REJECT", score=breakdown.final_score)
            return None
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_A_ORB,
                family=instrument.family,
                symbols=(row.symbol,),
                score=breakdown.final_score,
                slippage_est_ticks=0.0,
            ),
            side=eval_result.entry_plan.side,
            module_id=StrategyModule.STRAT_A_ORB.value,
            order_kind="breakout",
            symbol=row.symbol,
            entry_price=eval_result.entry_plan.entry_stop,
            initial_stop=eval_result.entry_plan.initial_stop,
            tp1_price=eval_result.entry_plan.tp1_price,
            tp2_price=eval_result.entry_plan.tp2_price,
            flatten_by=eval_result.entry_plan.flatten_by,
            regime=row.raw_regime.value,
            confidence=breakdown.final_score / 100.0,
            strategy_context=StrategyModule.STRAT_A_ORB.value,
            chase_ticks=eval_result.entry_plan.stop_limit_chase_ticks,
        )

    def _evaluate_b(self, *, row: _MarketRow, instrument: InstrumentMeta) -> _CandidatePlan | None:
        features = StrategyBFeatureSnapshot(
            raw_regime=row.raw_regime,
            is_weak_neutral=row.is_weak_neutral,
            confidence=row.confidence,
            session_vwap=row.session_vwap,
            atr_14_5m=row.atr_14_5m,
            rvol_3bar_aggregate_5m=row.rvol_3bar_aggregate_5m,
            close_price=row.close,
            data_ok=row.data_ok,
        )
        eval_result, signal = self._b.evaluate_candidate(ts=row.ts, symbol=row.symbol, features=features)
        if not eval_result.approved or signal is None:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_B_VWAP_REV, reason_code="STRATEGY_RULES_NOT_MET")
            return None
        stop_distance = max(0.8 * row.atr_14_5m, instrument.tick_size)
        initial_stop = row.close - stop_distance if signal.side is OrderSide.BUY else row.close + stop_distance
        tp1 = row.close + stop_distance if signal.side is OrderSide.BUY else row.close - stop_distance
        tp2 = row.close + (2.0 * stop_distance) if signal.side is OrderSide.BUY else row.close - (2.0 * stop_distance)
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_B_VWAP_REV,
                family=instrument.family,
                symbols=(row.symbol,),
                score=signal.score,
                slippage_est_ticks=0.0,
            ),
            side=signal.side,
            module_id=StrategyModule.STRAT_B_VWAP_REV.value,
            order_kind="reversion",
            symbol=row.symbol,
            entry_price=row.close,
            initial_stop=initial_stop,
            tp1_price=tp1,
            tp2_price=tp2,
            flatten_by=row.ts.replace(hour=11, minute=30, second=0, microsecond=0),
            regime=row.raw_regime.value,
            confidence=row.confidence,
            strategy_context=StrategyModule.STRAT_B_VWAP_REV.value,
        )

    def _evaluate_c(self, *, row: _MarketRow, bar: Bar1m, instrument: InstrumentMeta) -> _CandidatePlan | None:
        features = StrategyCFeatureSnapshot(
            session_vwap=row.session_vwap,
            ema20_5m_slope=row.ema20_5m_slope,
            atr_14_5m=row.atr_14_5m,
            vol_strong_1m=row.vol_strong_1m,
            rvol_3bar_aggregate_5m=row.rvol_3bar_aggregate_5m,
            tier1_lockout_active=row.lockout,
        )
        eval_result, signal = self._c.evaluate_breakout_candidate(
            bar=bar,
            features=features,
            tick_size=instrument.tick_size,
        )
        if not eval_result.approved or signal is None or eval_result.entry_plan is None:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_C_METALS_ORB, reason_code="STRATEGY_RULES_NOT_MET")
            return None
        plan: StrategyCEntryPlan = eval_result.entry_plan
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_C_METALS_ORB,
                family=instrument.family,
                symbols=(row.symbol,),
                score=signal.score,
                slippage_est_ticks=0.0,
            ),
            side=plan.side,
            module_id=StrategyModule.STRAT_C_METALS_ORB.value,
            order_kind="breakout",
            symbol=row.symbol,
            entry_price=plan.entry_stop,
            initial_stop=plan.initial_stop,
            tp1_price=plan.tp1_price,
            tp2_price=plan.tp2_price,
            flatten_by=plan.flatten_by,
            regime=Regime.TREND.value,
            confidence=max(row.confidence, signal.score / 100.0),
            strategy_context=StrategyModule.STRAT_C_METALS_ORB.value,
            chase_ticks=2,
        )

    def _evaluate_d(self, *, row: _MarketRow) -> _CandidatePlan | None:
        if "MGC" not in self._price_hist or "SIL" not in self._price_hist:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_D_PAIR, reason_code="PAIR_HISTORY_NOT_READY")
            return None
        lead_hist = self._price_hist["MGC"][-80:]
        hedge_hist = self._price_hist["SIL"][-80:]
        window = min(len(lead_hist), len(hedge_hist))
        lead = np.asarray(lead_hist[-window:], dtype=float)
        hedge = np.asarray(hedge_hist[-window:], dtype=float)
        if lead.size < 60 or hedge.size < 60:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_D_PAIR, reason_code="PAIR_HISTORY_NOT_READY")
            return None
        pair: PairSignal = evaluate_pair_signal(
            lead_symbol="MGC",
            hedge_symbol="SIL",
            lead_prices=lead,
            hedge_prices=hedge,
            data_ok=row.data_ok and row.quote_ok,
        )
        if not pair.approved:
            self._log_rejection(row=row, strategy=StrategyModule.STRAT_D_PAIR, reason_code="STRATEGY_RULES_NOT_MET")
            return None
        score = 85.0 + min(abs(pair.zscore), 10.0)
        entry_spread = float(lead[-1] - (pair.hedge_beta * hedge[-1]))
        stop_proxy = max(pair.stop_risk_proxy, 0.25)
        if pair.side == "short_spread":
            tp1 = entry_spread - (0.5 * stop_proxy)
            tp2 = entry_spread - stop_proxy
            stop_loss = entry_spread + stop_proxy
        else:
            tp1 = entry_spread + (0.5 * stop_proxy)
            tp2 = entry_spread + stop_proxy
            stop_loss = entry_spread - stop_proxy
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_D_PAIR,
                family=Family.METALS,
                symbols=("MGC", "SIL"),
                score=score,
                slippage_est_ticks=0.0,
            ),
            side=pair.side,
            module_id=StrategyModule.STRAT_D_PAIR.value,
            order_kind="pair",
            symbol="MGC",
            entry_price=entry_spread,
            initial_stop=stop_loss,
            tp1_price=tp1,
            tp2_price=tp2,
            flatten_by=row.ts.replace(hour=11, minute=30, second=0, microsecond=0),
            regime="mean_reversion",
            confidence=min(0.99, score / 100.0),
            strategy_context=f"z={pair.zscore:.2f}, beta={pair.hedge_beta:.3f}, hl={pair.half_life_bars:.1f}",
            pair_hedge_symbol="SIL",
            pair_beta=pair.hedge_beta,
            pair_stop_proxy=stop_proxy,
        )

    def _build_idea(self, *, plan: _CandidatePlan, row: _MarketRow, instrument: InstrumentMeta) -> SignalIdea:
        side = _display_side(plan.side)
        symbol_display = plan.symbol if plan.pair_hedge_symbol is None else f"{plan.symbol}/{plan.pair_hedge_symbol}"
        idea_id = f"{plan.module_id}:{symbol_display}:{side}"
        entry_low, entry_high = _entry_range(
            entry=plan.entry_price,
            side=plan.side,
            tick_size=instrument.tick_size,
            chase_ticks=plan.chase_ticks,
        )
        invalidation = _invalidation_text(
            side=side,
            stop_loss=plan.initial_stop,
            flatten_by=plan.flatten_by,
        )
        partial_profit_guidance = _partial_guidance(
            order_kind=plan.order_kind,
            side=side,
            tp1=plan.tp1_price,
            tp2=plan.tp2_price,
        )
        last_price = row.close if plan.pair_hedge_symbol is None else plan.entry_price
        return SignalIdea(
            idea_id=idea_id,
            strategy=plan.candidate.strategy,
            symbol=plan.symbol,
            symbol_display=symbol_display,
            side=side,
            entry_low=entry_low,
            entry_high=entry_high,
            stop_loss=plan.initial_stop,
            tp1=plan.tp1_price,
            tp2=plan.tp2_price,
            invalidation=invalidation,
            partial_profit_guidance=partial_profit_guidance,
            timestamp=row.ts,
            flatten_by=plan.flatten_by,
            regime=plan.regime,
            confidence=plan.confidence,
            strategy_context=plan.strategy_context,
            last_price=last_price,
            pair_hedge_symbol=plan.pair_hedge_symbol,
            pair_beta=plan.pair_beta,
            pair_stop_proxy=plan.pair_stop_proxy,
        )

    def _global_gate(self, *, row: _MarketRow, family: Family) -> str | None:
        if row.lockout:
            return "TIER1_LOCKOUT_ACTIVE"
        if not row.data_ok or not row.quote_ok:
            return "DATA_NOT_OK"
        if not row.trade_eligible:
            return "TRADE_NOT_ELIGIBLE"
        if row.atr_14_5m <= 0.0:
            return "ATR_KILL_SWITCH"
        if self._family_freeze.get(family, False):
            return "FAMILY_FREEZE_ACTIVE"
        return None

    def _instrument_or_synthetic(self, symbol: str, family: Family) -> InstrumentMeta:
        instrument = self._instruments.get(symbol)
        if instrument is not None:
            return instrument
        if family is Family.EQUITIES:
            tick_size = 0.25 if symbol in {"NQ", "MNQ"} else 1.0
            tick_value = 0.5 if symbol in {"MNQ", "MYM"} else 5.0
            point_value = tick_value / tick_size
            micro = "MNQ" if symbol in {"NQ", "MNQ"} else "MYM" if symbol in {"YM", "MYM"} else symbol
            return InstrumentMeta(
                symbol=symbol,
                root_symbol=symbol,
                family=family,
                tick_size=tick_size,
                tick_value=tick_value,
                point_value=point_value,
                commission_rt=1.20,
                symbol_type="future",
                micro_equivalent=micro,
                contract_units=1.0,
            )
        if family is Family.METALS:
            tick_size = 0.01 if symbol == "SIL" else 0.10
            tick_value = 2.5 if symbol == "SIL" else 1.0
            point_value = tick_value / tick_size
            return InstrumentMeta(
                symbol=symbol,
                root_symbol=symbol,
                family=family,
                tick_size=tick_size,
                tick_value=tick_value,
                point_value=point_value,
                commission_rt=1.30,
                symbol_type="future",
                micro_equivalent=symbol,
                contract_units=1.0,
            )
        raise KeyError(f"Unknown instrument for synthetic fallback: {symbol}")

    def _log_window_active(self, *, row: _MarketRow, strategy: StrategyModule) -> None:
        key = (row.ts.replace(second=0, microsecond=0).isoformat(), row.symbol, strategy.value)
        if key in self._window_log_keys:
            return
        self._window_log_keys.add(key)
        self._debug_log.write(
            {
                "event": "STRATEGY_WINDOW_ACTIVE",
                "timestamp_et": row.ts.isoformat(),
                "symbol": row.symbol,
                "strategy": strategy.value,
                "reason_code": None,
                "score": None,
            }
        )

    def _log_strategy_rejections(self, *, row: _MarketRow, reason_code: str) -> None:
        for strategy in sorted(self._enabled, key=lambda item: item.value):
            if strategy is StrategyModule.STRAT_C_METALS_ORB and row.symbol != "MGC":
                continue
            if strategy is StrategyModule.STRAT_D_PAIR and row.symbol != "MGC":
                continue
            self._log_rejection(row=row, strategy=strategy, reason_code=reason_code)

    def _log_rejection(
        self,
        *,
        row: _MarketRow,
        strategy: StrategyModule,
        reason_code: str,
        score: float | None = None,
    ) -> None:
        minute_key = row.ts.replace(second=0, microsecond=0).isoformat()
        dedupe_key = (minute_key, row.symbol, strategy.value, reason_code)
        if dedupe_key in self._rejection_log_keys:
            return
        self._rejection_log_keys.add(dedupe_key)
        payload = {
            "timestamp_et": row.ts.isoformat(),
            "symbol": row.symbol,
            "strategy": strategy.value,
            "reason_code": reason_code,
            "score": score,
        }
        self._debug_log.write({"event": "SIGNAL_CANDIDATE_REJECTED", **payload})
        self._debug_log.write({"event": "REJECTION_REASON", **payload})


def _bar_from_row(row: _MarketRow) -> Bar1m:
    return Bar1m(
        ts=row.ts,
        symbol=row.symbol,
        open=row.open,
        high=row.high,
        low=row.low,
        close=row.close,
        volume=row.volume,
    )


def _entry_range(*, entry: float, side: OrderSide | str, tick_size: float, chase_ticks: int) -> tuple[float, float]:
    if chase_ticks <= 0:
        width = max(tick_size, 0.0)
        return entry - width, entry + width
    width = chase_ticks * tick_size
    if side == OrderSide.BUY or side == "long_spread":
        return entry, entry + width
    return entry - width, entry


def _display_side(side: OrderSide | str) -> str:
    if isinstance(side, OrderSide):
        return side.value.upper()
    return str(side).upper()


def _invalidation_text(*, side: str, stop_loss: float, flatten_by: datetime) -> str:
    if side in {"BUY", "LONG_SPREAD"}:
        return f"thesis fails below {stop_loss:.2f} or after {flatten_by.isoformat()}"
    return f"thesis fails above {stop_loss:.2f} or after {flatten_by.isoformat()}"


def _partial_guidance(*, order_kind: str, side: str, tp1: float, tp2: float) -> str:
    if order_kind == "pair":
        return f"scale out near {tp1:.2f}; reassess runner into {tp2:.2f}"
    if side == "BUY":
        return f"trim into strength at {tp1:.2f}; let balance work toward {tp2:.2f}"
    return f"trim into weakness at {tp1:.2f}; let balance work toward {tp2:.2f}"


def _load_rows(path: str | Path) -> list[_MarketRow]:
    rows: list[_MarketRow] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            rows.append(_parse_row(raw))
    rows.sort(key=lambda row: (row.ts, row.symbol))
    return rows


def _parse_row(raw: dict[str, str]) -> _MarketRow:
    ts = datetime.fromisoformat(raw["ts"])
    return _MarketRow(
        ts=ts,
        symbol=raw["symbol"],
        open=float(raw["open"]),
        high=float(raw["high"]),
        low=float(raw["low"]),
        close=float(raw["close"]),
        volume=float(raw.get("volume", "0") or 0.0),
        session_vwap=float(raw.get("session_vwap", raw["close"])),
        ema9_5m=float(raw.get("ema9_5m", raw["close"])),
        ema21_5m=float(raw.get("ema21_5m", raw["close"])),
        ema20_5m_slope=float(raw.get("ema20_5m_slope", "0.0") or 0.0),
        atr_14_5m=float(raw.get("atr_14_5m", "1.0") or 1.0),
        atr_14_1m_price=float(raw.get("atr_14_1m_price", "1.0") or 1.0),
        rvol_3bar_aggregate_5m=_float_or_none(raw.get("rvol_3bar_aggregate_5m")),
        low_volume_trend_streak_5m=int(raw.get("low_volume_trend_streak_5m", "0") or 0),
        vol_strong_1m=_to_bool(raw.get("vol_strong_1m", "true")),
        data_ok=_to_bool(raw.get("data_ok", "true")),
        quote_ok=_to_bool(raw.get("quote_ok", "true")),
        trade_eligible=_to_bool(raw.get("trade_eligible", "true")),
        lockout=_to_bool(raw.get("lockout", "false")),
        family_freeze=_to_bool(raw.get("family_freeze", "false")),
        raw_regime=_to_regime(raw.get("raw_regime", "trend")),
        is_weak_neutral=_to_bool(raw.get("is_weak_neutral", "false")),
        confidence=float(raw.get("confidence", "1.0") or 1.0),
        market_open=_to_bool(raw.get("market_open", "true")),
        signals_active=_to_bool(raw.get("signals_active", "true")),
        schedule_state=str(raw.get("schedule_state", "open")),
    )


def _row_from_mapping(raw: dict[str, Any]) -> _MarketRow:
    return _MarketRow(
        ts=_coerce_datetime(raw["ts"]),
        symbol=str(raw["symbol"]),
        open=float(raw["open"]),
        high=float(raw["high"]),
        low=float(raw["low"]),
        close=float(raw["close"]),
        volume=float(raw.get("volume", 0.0)),
        session_vwap=float(raw.get("session_vwap", raw["close"])),
        ema9_5m=float(raw.get("ema9_5m", raw["close"])),
        ema21_5m=float(raw.get("ema21_5m", raw["close"])),
        ema20_5m_slope=float(raw.get("ema20_5m_slope", 0.0)),
        atr_14_5m=float(raw.get("atr_14_5m", 1.0)),
        atr_14_1m_price=float(raw.get("atr_14_1m_price", 1.0)),
        rvol_3bar_aggregate_5m=_float_or_none(raw.get("rvol_3bar_aggregate_5m")),
        low_volume_trend_streak_5m=int(raw.get("low_volume_trend_streak_5m", 0)),
        vol_strong_1m=bool(raw.get("vol_strong_1m", True)),
        data_ok=bool(raw.get("data_ok", True)),
        quote_ok=bool(raw.get("quote_ok", True)),
        trade_eligible=bool(raw.get("trade_eligible", True)),
        lockout=bool(raw.get("lockout", False)),
        family_freeze=bool(raw.get("family_freeze", False)),
        raw_regime=_coerce_regime(raw.get("raw_regime", Regime.TREND)),
        is_weak_neutral=bool(raw.get("is_weak_neutral", False)),
        confidence=float(raw.get("confidence", 1.0)),
        market_open=bool(raw.get("market_open", True)),
        signals_active=bool(raw.get("signals_active", True)),
        schedule_state=str(raw.get("schedule_state", "open")),
    )


def _to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _to_regime(value: str) -> Regime:
    text = value.strip().lower()
    if text == Regime.TREND.value:
        return Regime.TREND
    if text == Regime.CHOP.value:
        return Regime.CHOP
    return Regime.NEUTRAL


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    return float(text)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _coerce_regime(value: Any) -> Regime:
    if isinstance(value, Regime):
        return value
    return _to_regime(str(value))


def _family_for_symbol(symbol: str) -> Family | None:
    if symbol in {"NQ", "MNQ", "YM", "MYM"}:
        return Family.EQUITIES
    if symbol in {"GC", "MGC", "SIL"}:
        return Family.METALS
    return None
