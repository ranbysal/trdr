"""Unified multi-strategy paper loop for A/B/C/D."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from futures_bot.core.enums import Family, OrderSide, Regime, StrategyModule
from futures_bot.core.ids import utc_timestamp_id
from futures_bot.core.types import Bar1m, InstrumentMeta
from futures_bot.pipeline.portfolio_orchestrator import StrategyCandidate, resolve_strategy_conflicts
from futures_bot.risk.cooldowns import ConsecutiveLossCooldownManager
from futures_bot.risk.daily_halt import DailyHaltManager
from futures_bot.risk.models import SingleLegSizingRequest
from futures_bot.risk.portfolio_caps import PortfolioCapsManager
from futures_bot.risk.sizing_single import size_with_micro_routing
from futures_bot.risk.slippage import estimate_slippage_ticks
from futures_bot.runtime.ndjson_writer import NdjsonWriter
from futures_bot.scoring.strategy_a_scoring import (
    StrategyAScoringConfig,
    compute_pattern_quality,
    compute_strategy_a_score,
)
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
    chase_ticks: int = 0
    tp1_frac: float = 0.5
    tp2_frac: float = 0.3
    tp3_frac: float = 0.2
    pair_hedge_symbol: str | None = None
    pair_beta: float = 0.0
    pair_stop_proxy: float = 0.0


@dataclass(slots=True)
class _PendingOrder:
    order_id: str
    strategy: StrategyModule
    module_id: str
    symbol: str
    routed_symbol: str
    side: OrderSide
    qty: int
    entry_stop: float
    chase_ticks: int
    tick_size: float
    atr_14_1m_price: float
    initial_stop: float
    tp1_price: float
    tp2_price: float
    flatten_by: datetime
    tp1_frac: float
    tp2_frac: float
    tp3_frac: float


@dataclass(slots=True)
class _OpenSingle:
    position_id: str
    strategy: StrategyModule
    module_id: str
    symbol: str
    routed_symbol: str
    family: Family
    side: OrderSide
    entry_price: float
    qty_initial: int
    qty_open: int
    qty_tp1: int
    qty_tp2: int
    qty_tp3: int
    initial_stop: float
    active_stop: float
    tp1_price: float
    tp2_price: float
    tp3_stop: float
    trail_active: bool
    flatten_by: datetime
    point_value: float
    commission_rt: float
    atr_14_1m_price: float
    initial_risk_dollars: float
    realized_pnl: float = 0.0
    last_exit_reason: str | None = None


@dataclass(slots=True)
class _OpenPair:
    position_id: str
    module_id: str
    lead_symbol: str
    hedge_symbol: str
    side: str
    lead_qty: int
    hedge_qty: int
    beta: float
    stop_proxy: float
    entry_spread: float
    flatten_by: datetime
    lead_entry: float
    hedge_entry: float
    initial_risk_dollars: float
    realized_pnl: float = 0.0


def run_multistrategy_paper_loop(
    *,
    data_path: str | Path,
    out_dir: str | Path,
    instruments_by_symbol: dict[str, InstrumentMeta],
    enabled_strategies: set[StrategyModule],
    risk_pct: float = 0.0030,
) -> Path:
    rows = _load_rows(data_path)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    log_path = out_path / "trade_logs.json"
    runner = _MultiStrategyPaperRunner(
        log_path=log_path,
        instruments_by_symbol=instruments_by_symbol,
        enabled_strategies=enabled_strategies,
        risk_pct=risk_pct,
    )
    for row in rows:
        runner.step(row)
    runner.flush()
    return log_path


class MultiStrategyPaperEngine:
    """Stateful paper engine wrapper for streaming/live use."""

    def __init__(
        self,
        *,
        log_path: str | Path,
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        risk_pct: float = 0.0030,
    ) -> None:
        self._runner = _MultiStrategyPaperRunner(
            log_path=Path(log_path),
            instruments_by_symbol=instruments_by_symbol,
            enabled_strategies=enabled_strategies,
            risk_pct=risk_pct,
        )

    def process_row(self, row: dict[str, Any]) -> None:
        self._runner.step(_row_from_mapping(row))

    def flush(self) -> None:
        self._runner.flush()


class _MultiStrategyPaperRunner:
    def __init__(
        self,
        *,
        log_path: Path,
        instruments_by_symbol: dict[str, InstrumentMeta],
        enabled_strategies: set[StrategyModule],
        risk_pct: float,
    ) -> None:
        self._log = NdjsonWriter(log_path)
        self._instruments = instruments_by_symbol
        self._enabled = enabled_strategies
        self._risk_pct = risk_pct
        self._a = StrategyAORB()
        self._b = StrategyBVWAPReversion()
        self._c = StrategyCMetalsORB()
        self._caps = PortfolioCapsManager(equity=100_000.0)
        self._cooldowns = ConsecutiveLossCooldownManager()
        self._halt = DailyHaltManager()
        self._halt.reset_session(session_start_equity=100_000.0)
        self._pending: dict[str, _PendingOrder] = {}
        self._open_single: dict[str, _OpenSingle] = {}
        self._open_pair: dict[tuple[str, str], _OpenPair] = {}
        self._family_freeze: dict[Family, bool] = {}
        self._price_hist: dict[str, list[float]] = {}
        self._last_close: dict[str, float] = {}
        self._last_atr_1m_price: dict[str, float] = {}
        self._session_day: datetime.date | None = None
        self._session_realized_pnl: float = 0.0

    def flush(self) -> None:
        self._log.flush()

    def step(self, row: _MarketRow) -> None:
        self._roll_session_if_needed(row.ts)
        self._price_hist.setdefault(row.symbol, []).append(row.close)
        if len(self._price_hist[row.symbol]) > 500:
            self._price_hist[row.symbol] = self._price_hist[row.symbol][-500:]
        self._last_close[row.symbol] = row.close
        self._last_atr_1m_price[row.symbol] = row.atr_14_1m_price

        family = _family_for_symbol(row.symbol)
        if family is None:
            return
        instrument = self._instrument_or_synthetic(row.symbol, family)
        if row.family_freeze:
            self._family_freeze[instrument.family] = True

        self._fill_pending_if_triggered(row)
        self._update_open_positions(row)
        self._update_open_pairs(row)

        plans = self._collect_candidate_plans(row, instrument)
        if not plans:
            return
        selected, decisions = resolve_strategy_conflicts(
            candidates=[p.candidate for p in plans],
            open_symbols=self._currently_open_symbols(),
        )
        selected_set = {(c.strategy, c.symbols) for c in selected}
        for decision in decisions:
            if not decision.accepted:
                self._log.write(
                    {
                        "ts": row.ts.isoformat(),
                        "event": "risk_event",
                        "code": decision.reason_code,
                        "strategy": decision.candidate.strategy.value,
                        "symbols": list(decision.candidate.symbols),
                    }
                )
        for plan in plans:
            if (plan.candidate.strategy, plan.candidate.symbols) in selected_set:
                self._route_selected_plan(plan=plan, row=row, instrument=instrument)

    def _collect_candidate_plans(self, row: _MarketRow, instrument: InstrumentMeta) -> list[_CandidatePlan]:
        plans: list[_CandidatePlan] = []
        bar = _bar_from_row(row)
        self._a.update_or_state(bar)
        self._c.update_or_state(bar)

        if StrategyModule.STRAT_A_ORB in self._enabled:
            gate = self._global_gate(row=row, module_id=StrategyModule.STRAT_A_ORB.value, symbol=row.symbol, family=instrument.family)
            if gate is None:
                a_plan = self._evaluate_a(row=row, bar=bar, instrument=instrument)
                if a_plan is not None:
                    plans.append(a_plan)
        if StrategyModule.STRAT_B_VWAP_REV in self._enabled:
            gate = self._global_gate(row=row, module_id=StrategyModule.STRAT_B_VWAP_REV.value, symbol=row.symbol, family=instrument.family)
            if gate is None:
                b_plan = self._evaluate_b(row=row, instrument=instrument)
                if b_plan is not None:
                    plans.append(b_plan)
        if StrategyModule.STRAT_C_METALS_ORB in self._enabled and row.symbol == "MGC":
            gate = self._global_gate(row=row, module_id=StrategyModule.STRAT_C_METALS_ORB.value, symbol=row.symbol, family=instrument.family)
            if gate is None:
                c_plan = self._evaluate_c(row=row, bar=bar, instrument=instrument)
                if c_plan is not None:
                    plans.append(c_plan)
        if StrategyModule.STRAT_D_PAIR in self._enabled and row.symbol == "MGC":
            d_plan = self._evaluate_d(row=row)
            if d_plan is not None:
                plans.append(d_plan)
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
            return None
        state = self._a.get_or_state(row.symbol)
        if state is None or state.or_width is None:
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
            return None
        slip = _estimate_slippage(symbol=row.symbol, atr_14_1m_price=row.atr_14_1m_price, tick_size=instrument.tick_size)
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_A_ORB,
                family=instrument.family,
                symbols=(row.symbol,),
                score=breakdown.final_score,
                slippage_est_ticks=slip,
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
            chase_ticks=eval_result.entry_plan.stop_limit_chase_ticks,
            tp1_frac=eval_result.entry_plan.tp1_size_frac,
            tp2_frac=eval_result.entry_plan.tp2_size_frac,
            tp3_frac=eval_result.entry_plan.tp3_size_frac,
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
            return None
        stop_distance = max(0.8 * row.atr_14_5m, instrument.tick_size)
        initial_stop = row.close - stop_distance if signal.side is OrderSide.BUY else row.close + stop_distance
        tp1 = row.close + stop_distance if signal.side is OrderSide.BUY else row.close - stop_distance
        tp2 = row.close + (2.0 * stop_distance) if signal.side is OrderSide.BUY else row.close - (2.0 * stop_distance)
        flatten_by = row.ts.replace(hour=11, minute=30, second=0, microsecond=0)
        slip = _estimate_slippage(symbol=row.symbol, atr_14_1m_price=row.atr_14_1m_price, tick_size=instrument.tick_size)
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_B_VWAP_REV,
                family=instrument.family,
                symbols=(row.symbol,),
                score=signal.score,
                slippage_est_ticks=slip,
            ),
            side=signal.side,
            module_id=StrategyModule.STRAT_B_VWAP_REV.value,
            order_kind="reversion",
            symbol=row.symbol,
            entry_price=row.close,
            initial_stop=initial_stop,
            tp1_price=tp1,
            tp2_price=tp2,
            flatten_by=flatten_by,
            tp1_frac=0.5,
            tp2_frac=0.5,
            tp3_frac=0.0,
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
            return None
        plan: StrategyCEntryPlan = eval_result.entry_plan
        slip = _estimate_slippage(symbol=row.symbol, atr_14_1m_price=row.atr_14_1m_price, tick_size=instrument.tick_size)
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_C_METALS_ORB,
                family=instrument.family,
                symbols=(row.symbol,),
                score=signal.score,
                slippage_est_ticks=slip,
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
            chase_ticks=2,
            tp1_frac=0.5,
            tp2_frac=0.3,
            tp3_frac=0.2,
        )

    def _evaluate_d(self, *, row: _MarketRow) -> _CandidatePlan | None:
        if "MGC" not in self._price_hist or "SIL" not in self._price_hist:
            return None
        lead_hist = self._price_hist["MGC"][-80:]
        hedge_hist = self._price_hist["SIL"][-80:]
        window = min(len(lead_hist), len(hedge_hist))
        lead = np.asarray(lead_hist[-window:], dtype=float)
        hedge = np.asarray(hedge_hist[-window:], dtype=float)
        if lead.size < 60 or hedge.size < 60:
            return None
        gate = self._global_gate(
            row=row,
            module_id=StrategyModule.STRAT_D_PAIR.value,
            symbol="MGC",
            family=Family.METALS,
        )
        if gate is not None:
            return None
        pair: PairSignal = evaluate_pair_signal(
            lead_symbol="MGC",
            hedge_symbol="SIL",
            lead_prices=lead,
            hedge_prices=hedge,
            data_ok=row.data_ok and row.quote_ok,
        )
        if not pair.approved:
            return None
        slippage = (
            _estimate_slippage(symbol="MGC", atr_14_1m_price=row.atr_14_1m_price, tick_size=0.1)
            + _estimate_slippage(symbol="SIL", atr_14_1m_price=row.atr_14_1m_price, tick_size=0.01)
        )
        score = 85.0 + min(abs(pair.zscore), 10.0)
        return _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=StrategyModule.STRAT_D_PAIR,
                family=Family.METALS,
                symbols=("MGC", "SIL"),
                score=score,
                slippage_est_ticks=slippage,
            ),
            side=pair.side,
            module_id=StrategyModule.STRAT_D_PAIR.value,
            order_kind="pair",
            symbol="MGC",
            entry_price=float(lead[-1]),
            initial_stop=float(lead[-1]),
            tp1_price=float(lead[-1]),
            tp2_price=float(lead[-1]),
            flatten_by=row.ts.replace(hour=11, minute=30, second=0, microsecond=0),
            pair_hedge_symbol="SIL",
            pair_beta=pair.hedge_beta,
            pair_stop_proxy=pair.stop_risk_proxy,
        )

    def _route_selected_plan(self, *, plan: _CandidatePlan, row: _MarketRow, instrument: InstrumentMeta) -> None:
        if plan.order_kind == "pair":
            self._open_pair_position(plan=plan, ts=row.ts)
            return

        sizing = size_with_micro_routing(
            SingleLegSizingRequest(
                instrument=instrument,
                equity=100_000.0,
                risk_pct=self._risk_pct,
                entry_price=plan.entry_price,
                stop_price=plan.initial_stop,
                atr_14_1m_price=row.atr_14_1m_price,
            ),
            instruments_by_symbol=self._instruments,
        )
        if not sizing.approved:
            self._log.write(
                {
                    "ts": row.ts.isoformat(),
                    "event": "risk_event",
                    "code": sizing.reason_code,
                    "strategy": plan.candidate.strategy.value,
                    "symbol": plan.symbol,
                }
            )
            return
        proposed_risk = float(sizing.contracts) * float(sizing.adjusted_risk_per_contract)
        cap = self._caps.check_new_position(
            family=instrument.family,
            symbol=sizing.routed_symbol,
            proposed_risk_dollars=proposed_risk,
        )
        if not cap.approved:
            self._log.write(
                {
                    "ts": row.ts.isoformat(),
                    "event": "risk_event",
                    "code": cap.reason_code,
                    "strategy": plan.candidate.strategy.value,
                    "symbol": plan.symbol,
                }
            )
            return
        self._caps.record_open_position(
            family=instrument.family,
            symbol=sizing.routed_symbol,
            risk_dollars=proposed_risk,
        )

        if plan.order_kind == "breakout":
            order = _PendingOrder(
                order_id=utc_timestamp_id(f"paper_entry_{plan.symbol}"),
                strategy=plan.candidate.strategy,
                module_id=plan.module_id,
                symbol=plan.symbol,
                routed_symbol=sizing.routed_symbol,
                side=plan.side if isinstance(plan.side, OrderSide) else OrderSide.BUY,
                qty=sizing.contracts,
                entry_stop=plan.entry_price,
                chase_ticks=plan.chase_ticks,
                tick_size=instrument.tick_size,
                atr_14_1m_price=row.atr_14_1m_price,
                initial_stop=plan.initial_stop,
                tp1_price=plan.tp1_price,
                tp2_price=plan.tp2_price,
                flatten_by=plan.flatten_by,
                tp1_frac=plan.tp1_frac,
                tp2_frac=plan.tp2_frac,
                tp3_frac=plan.tp3_frac,
            )
            self._pending[plan.symbol] = order
            self._log.write(
                {
                    "ts": row.ts.isoformat(),
                    "event": "order_submitted",
                    "strategy": plan.candidate.strategy.value,
                    "order_type": "entry_stop_limit_chase",
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "qty": order.qty,
                    "entry_stop": order.entry_stop,
                    "chase_ticks": order.chase_ticks,
                }
            )
            return
        self._open_single_position_from_plan(
            ts=row.ts,
            plan=plan,
            instrument=instrument,
            routed_symbol=sizing.routed_symbol,
            qty=sizing.contracts,
            fill_price=row.close,
            atr_14_1m_price=row.atr_14_1m_price,
        )

    def _fill_pending_if_triggered(self, row: _MarketRow) -> None:
        pending = self._pending.get(row.symbol)
        if pending is None:
            return
        trigger = (pending.side is OrderSide.BUY and row.high >= pending.entry_stop) or (
            pending.side is OrderSide.SELL and row.low <= pending.entry_stop
        )
        if not trigger:
            return
        slip_ticks = _estimate_slippage(
            symbol=pending.symbol,
            atr_14_1m_price=pending.atr_14_1m_price,
            tick_size=pending.tick_size,
        )
        chase_px = pending.chase_ticks * pending.tick_size
        if pending.side is OrderSide.BUY:
            fill_price = min(pending.entry_stop + chase_px, pending.entry_stop + (slip_ticks * pending.tick_size))
        else:
            fill_price = max(pending.entry_stop - chase_px, pending.entry_stop - (slip_ticks * pending.tick_size))
        instrument = self._instrument_or_synthetic(pending.symbol, _family_for_symbol(pending.symbol) or Family.EQUITIES)
        plan = _CandidatePlan(
            candidate=StrategyCandidate(
                strategy=pending.strategy,
                family=instrument.family,
                symbols=(pending.symbol,),
                score=0.0,
                slippage_est_ticks=slip_ticks,
            ),
            side=pending.side,
            module_id=pending.module_id,
            order_kind="breakout",
            symbol=pending.symbol,
            entry_price=pending.entry_stop,
            initial_stop=pending.initial_stop,
            tp1_price=pending.tp1_price,
            tp2_price=pending.tp2_price,
            flatten_by=pending.flatten_by,
            tp1_frac=pending.tp1_frac,
            tp2_frac=pending.tp2_frac,
            tp3_frac=pending.tp3_frac,
        )
        self._open_single_position_from_plan(
            ts=row.ts,
            plan=plan,
            instrument=instrument,
            routed_symbol=pending.routed_symbol,
            qty=pending.qty,
            fill_price=fill_price,
            atr_14_1m_price=pending.atr_14_1m_price,
        )
        self._pending.pop(row.symbol, None)

    def _open_single_position_from_plan(
        self,
        *,
        ts: datetime,
        plan: _CandidatePlan,
        instrument: InstrumentMeta,
        routed_symbol: str,
        qty: int,
        fill_price: float,
        atr_14_1m_price: float,
    ) -> None:
        tp1_qty = int(qty * plan.tp1_frac)
        tp2_qty = int(qty * plan.tp2_frac)
        tp3_qty = max(0, qty - tp1_qty - tp2_qty)
        pos = _OpenSingle(
            position_id=utc_timestamp_id(f"paper_pos_{plan.symbol}"),
            strategy=plan.candidate.strategy,
            module_id=plan.module_id,
            symbol=plan.symbol,
            routed_symbol=routed_symbol,
            family=instrument.family,
            side=plan.side if isinstance(plan.side, OrderSide) else OrderSide.BUY,
            entry_price=fill_price,
            qty_initial=qty,
            qty_open=qty,
            qty_tp1=tp1_qty,
            qty_tp2=tp2_qty,
            qty_tp3=tp3_qty,
            initial_stop=plan.initial_stop,
            active_stop=plan.initial_stop,
            tp1_price=plan.tp1_price,
            tp2_price=plan.tp2_price,
            tp3_stop=plan.initial_stop,
            trail_active=False,
            flatten_by=plan.flatten_by,
            point_value=instrument.point_value,
            commission_rt=instrument.commission_rt,
            atr_14_1m_price=atr_14_1m_price,
            initial_risk_dollars=abs(fill_price - plan.initial_stop) * qty * instrument.point_value,
            realized_pnl=-(instrument.commission_rt * qty * 0.5),
        )
        self._open_single[plan.symbol] = pos
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "entry_filled",
                "strategy": plan.candidate.strategy.value,
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "routed_symbol": pos.routed_symbol,
                "side": pos.side.value,
                "qty": qty,
                "fill_price": fill_price,
                "entry_stop": plan.entry_price,
                "initial_stop": plan.initial_stop,
                "tp1_price": plan.tp1_price,
                "tp2_price": plan.tp2_price,
            }
        )

    def _update_open_positions(self, row: _MarketRow) -> None:
        pos = self._open_single.get(row.symbol)
        if pos is None:
            return
        if row.ts >= pos.flatten_by:
            self._close_single(pos=pos, qty=pos.qty_open, price=row.close, ts=row.ts, reason="FLATTEN_CUTOFF")
            return
        if pos.side is OrderSide.BUY:
            if row.low <= pos.active_stop:
                self._close_single(pos=pos, qty=pos.qty_open, price=pos.active_stop, ts=row.ts, reason="STOP_EXIT")
                return
            if pos.qty_tp1 > 0 and row.high >= pos.tp1_price:
                qty = min(pos.qty_tp1, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp1_price, ts=row.ts, reason="TP1_EXIT")
                pos.active_stop = max(
                    pos.active_stop,
                    pos.entry_price + self._instrument_or_synthetic(pos.symbol, pos.family).tick_size,
                )
                pos.qty_tp1 = 0
            if pos.qty_tp2 > 0 and row.high >= pos.tp2_price:
                qty = min(pos.qty_tp2, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp2_price, ts=row.ts, reason="TP2_EXIT")
                pos.qty_tp2 = 0
                pos.trail_active = True
            if pos.trail_active:
                pos.tp3_stop = max(pos.tp3_stop, row.close)
            if pos.qty_tp3 > 0 and row.low <= pos.tp3_stop:
                qty = min(pos.qty_tp3, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp3_stop, ts=row.ts, reason="TP3_TRAIL_EXIT")
                pos.qty_tp3 = 0
        else:
            if row.high >= pos.active_stop:
                self._close_single(pos=pos, qty=pos.qty_open, price=pos.active_stop, ts=row.ts, reason="STOP_EXIT")
                return
            if pos.qty_tp1 > 0 and row.low <= pos.tp1_price:
                qty = min(pos.qty_tp1, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp1_price, ts=row.ts, reason="TP1_EXIT")
                pos.active_stop = min(
                    pos.active_stop,
                    pos.entry_price - self._instrument_or_synthetic(pos.symbol, pos.family).tick_size,
                )
                pos.qty_tp1 = 0
            if pos.qty_tp2 > 0 and row.low <= pos.tp2_price:
                qty = min(pos.qty_tp2, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp2_price, ts=row.ts, reason="TP2_EXIT")
                pos.qty_tp2 = 0
                pos.trail_active = True
            if pos.trail_active:
                pos.tp3_stop = min(pos.tp3_stop, row.close)
            if pos.qty_tp3 > 0 and row.high >= pos.tp3_stop:
                qty = min(pos.qty_tp3, pos.qty_open)
                self._close_single(pos=pos, qty=qty, price=pos.tp3_stop, ts=row.ts, reason="TP3_TRAIL_EXIT")
                pos.qty_tp3 = 0
        if pos.qty_open == 0:
            self._finalize_single(pos=pos, ts=row.ts)

    def _close_single(self, *, pos: _OpenSingle, qty: int, price: float, ts: datetime, reason: str) -> None:
        if qty <= 0 or pos.qty_open <= 0:
            return
        qty = min(qty, pos.qty_open)
        pos.qty_open -= qty
        gross = (price - pos.entry_price) * qty * pos.point_value
        if pos.side is OrderSide.SELL:
            gross = (pos.entry_price - price) * qty * pos.point_value
        slip_ticks = _estimate_slippage(
            symbol=pos.symbol,
            atr_14_1m_price=pos.atr_14_1m_price,
            tick_size=self._instrument_or_synthetic(pos.symbol, pos.family).tick_size,
        )
        slip_cost = slip_ticks * self._instrument_or_synthetic(pos.symbol, pos.family).tick_value * qty
        commission = pos.commission_rt * qty * 0.5
        delta = gross - slip_cost - commission
        pos.realized_pnl += delta
        pos.last_exit_reason = reason
        self._session_realized_pnl += delta
        self._halt.update_realized_pnl(realized_pnl=self._session_realized_pnl)
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "position_update",
                "strategy": pos.strategy.value,
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "qty_closed": qty,
                "qty_open": pos.qty_open,
                "exit_reason": reason,
                "exit_price": price,
                "gross_pnl_delta": gross,
                "slippage_cost_delta": slip_cost,
                "commission_delta": commission,
                "realized_pnl_delta": delta,
                "realized_pnl_cum": pos.realized_pnl,
            }
        )

    def _finalize_single(self, *, pos: _OpenSingle, ts: datetime) -> None:
        self._caps.record_close_position(symbol=pos.routed_symbol)
        self._cooldowns.record_closed_trade(
            module_id=pos.module_id,
            symbol=pos.symbol,
            net_realized_pnl_after_costs=pos.realized_pnl,
            closed_at=ts,
        )
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "position_closed",
                "strategy": pos.strategy.value,
                "position_id": pos.position_id,
                "symbol": pos.symbol,
                "side": pos.side.value,
                "entry_price": pos.entry_price,
                "contracts_initial": pos.qty_initial,
                "exit_reason": pos.last_exit_reason,
                "realized_pnl": pos.realized_pnl,
                "initial_risk_dollars": pos.initial_risk_dollars,
            }
        )
        self._open_single.pop(pos.symbol, None)

    def _open_pair_position(self, *, plan: _CandidatePlan, ts: datetime) -> None:
        if plan.pair_hedge_symbol is None:
            return
        hedge_symbol = plan.pair_hedge_symbol
        if hedge_symbol not in self._last_close:
            return
        lead_instr = self._instrument_or_synthetic("MGC", Family.METALS)
        hedge_instr = self._instrument_or_synthetic(hedge_symbol, Family.METALS)
        lead_qty = 1
        hedge_qty = max(1, int(round(abs(plan.pair_beta) * lead_qty)))
        lead_cap = self._caps.check_new_position(
            family=lead_instr.family,
            symbol="MGC",
            proposed_risk_dollars=200.0,
        )
        hedge_cap = self._caps.check_new_position(
            family=hedge_instr.family,
            symbol=hedge_symbol,
            proposed_risk_dollars=200.0,
        )
        if not lead_cap.approved or not hedge_cap.approved:
            self._log.write(
                {
                    "ts": ts.isoformat(),
                    "event": "risk_event",
                    "code": "PAIR_CAP_REJECTED",
                    "strategy": StrategyModule.STRAT_D_PAIR.value,
                    "symbols": ["MGC", hedge_symbol],
                }
            )
            return
        self._caps.record_open_position(family=lead_instr.family, symbol="MGC", risk_dollars=200.0)
        self._caps.record_open_position(family=hedge_instr.family, symbol=hedge_symbol, risk_dollars=200.0)
        lead_market = self._last_close["MGC"]
        hedge_market = self._last_close[hedge_symbol]
        lead_entry_side, hedge_entry_side = _pair_entry_sides(str(plan.side))
        lead_price = _fill_price_with_slippage(
            price=lead_market,
            side=lead_entry_side,
            symbol="MGC",
            instrument=lead_instr,
            atr_14_1m_price=self._last_atr_1m_price.get("MGC", 0.0),
        )
        hedge_price = _fill_price_with_slippage(
            price=hedge_market,
            side=hedge_entry_side,
            symbol=hedge_symbol,
            instrument=hedge_instr,
            atr_14_1m_price=self._last_atr_1m_price.get(hedge_symbol, 0.0),
        )
        entry_commission = (lead_instr.commission_rt * lead_qty * 0.5) + (hedge_instr.commission_rt * hedge_qty * 0.5)
        position = _OpenPair(
            position_id=utc_timestamp_id("paper_pair_MGC_SIL"),
            module_id=StrategyModule.STRAT_D_PAIR.value,
            lead_symbol="MGC",
            hedge_symbol=hedge_symbol,
            side=str(plan.side),
            lead_qty=lead_qty,
            hedge_qty=hedge_qty,
            beta=plan.pair_beta,
            stop_proxy=plan.pair_stop_proxy,
            entry_spread=lead_price - (plan.pair_beta * hedge_price),
            flatten_by=plan.flatten_by,
            lead_entry=lead_price,
            hedge_entry=hedge_price,
            initial_risk_dollars=400.0,
            realized_pnl=-entry_commission,
        )
        self._open_pair[("MGC", hedge_symbol)] = position
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "pair_entry_filled",
                "strategy": StrategyModule.STRAT_D_PAIR.value,
                "position_id": position.position_id,
                "lead_symbol": position.lead_symbol,
                "hedge_symbol": position.hedge_symbol,
                "lead_qty": position.lead_qty,
                "hedge_qty": position.hedge_qty,
                "side": position.side,
                "lead_entry_price": position.lead_entry,
                "hedge_entry_price": position.hedge_entry,
                "entry_spread": position.entry_spread,
                "initial_risk_dollars": position.initial_risk_dollars,
            }
        )

    def _update_open_pairs(self, row: _MarketRow) -> None:
        keys = list(self._open_pair.keys())
        for key in keys:
            pos = self._open_pair[key]
            if pos.lead_symbol not in self._last_close or pos.hedge_symbol not in self._last_close:
                continue
            lead = self._last_close[pos.lead_symbol]
            hedge = self._last_close[pos.hedge_symbol]
            spread = lead - (pos.beta * hedge)
            if row.ts >= pos.flatten_by:
                self._close_pair(pos=pos, ts=row.ts, reason="FLATTEN_CUTOFF")
                continue
            if pos.side == "short_spread":
                if spread <= pos.entry_spread or spread >= pos.entry_spread + pos.stop_proxy:
                    reason = "PAIR_TP_EXIT" if spread <= pos.entry_spread else "PAIR_STOP_EXIT"
                    self._close_pair(pos=pos, ts=row.ts, reason=reason)
            else:
                if spread >= pos.entry_spread or spread <= pos.entry_spread - pos.stop_proxy:
                    reason = "PAIR_TP_EXIT" if spread >= pos.entry_spread else "PAIR_STOP_EXIT"
                    self._close_pair(pos=pos, ts=row.ts, reason=reason)

    def _close_pair(self, *, pos: _OpenPair, ts: datetime, reason: str) -> None:
        lead_instr = self._instrument_or_synthetic(pos.lead_symbol, Family.METALS)
        hedge_instr = self._instrument_or_synthetic(pos.hedge_symbol, Family.METALS)
        lead_market = self._last_close[pos.lead_symbol]
        hedge_market = self._last_close[pos.hedge_symbol]
        lead_exit_side, hedge_exit_side = _pair_exit_sides(pos.side)
        lead_exit = _fill_price_with_slippage(
            price=lead_market,
            side=lead_exit_side,
            symbol=pos.lead_symbol,
            instrument=lead_instr,
            atr_14_1m_price=self._last_atr_1m_price.get(pos.lead_symbol, 0.0),
        )
        hedge_exit = _fill_price_with_slippage(
            price=hedge_market,
            side=hedge_exit_side,
            symbol=pos.hedge_symbol,
            instrument=hedge_instr,
            atr_14_1m_price=self._last_atr_1m_price.get(pos.hedge_symbol, 0.0),
        )
        if pos.side == "short_spread":
            lead_pnl = (pos.lead_entry - lead_exit) * pos.lead_qty * lead_instr.point_value
            hedge_pnl = (hedge_exit - pos.hedge_entry) * pos.hedge_qty * hedge_instr.point_value
        else:
            lead_pnl = (lead_exit - pos.lead_entry) * pos.lead_qty * lead_instr.point_value
            hedge_pnl = (pos.hedge_entry - hedge_exit) * pos.hedge_qty * hedge_instr.point_value
        gross_pair_pnl = lead_pnl + hedge_pnl
        exit_commission = (lead_instr.commission_rt * pos.lead_qty * 0.5) + (
            hedge_instr.commission_rt * pos.hedge_qty * 0.5
        )
        net_delta = gross_pair_pnl - exit_commission
        pos.realized_pnl += net_delta
        self._session_realized_pnl += net_delta
        self._halt.update_realized_pnl(realized_pnl=self._session_realized_pnl)
        self._cooldowns.record_closed_trade(
            module_id=pos.module_id,
            symbol=pos.lead_symbol,
            net_realized_pnl_after_costs=pos.realized_pnl,
            closed_at=ts,
        )
        self._cooldowns.record_closed_trade(
            module_id=pos.module_id,
            symbol=pos.hedge_symbol,
            net_realized_pnl_after_costs=pos.realized_pnl,
            closed_at=ts,
        )
        self._caps.record_close_position(symbol=pos.lead_symbol)
        self._caps.record_close_position(symbol=pos.hedge_symbol)
        self._log.write(
            {
                "ts": ts.isoformat(),
                "event": "pair_position_closed",
                "strategy": StrategyModule.STRAT_D_PAIR.value,
                "position_id": pos.position_id,
                "lead_symbol": pos.lead_symbol,
                "hedge_symbol": pos.hedge_symbol,
                "reason": reason,
                "exit_reason": reason,
                "side": pos.side,
                "lead_qty": pos.lead_qty,
                "hedge_qty": pos.hedge_qty,
                "lead_entry_price": pos.lead_entry,
                "hedge_entry_price": pos.hedge_entry,
                "lead_exit_price": lead_exit,
                "hedge_exit_price": hedge_exit,
                "entry_spread": pos.entry_spread,
                "exit_spread": lead_exit - (pos.beta * hedge_exit),
                "gross_pnl": gross_pair_pnl,
                "commission_delta": exit_commission,
                "realized_pnl": pos.realized_pnl,
                "initial_risk_dollars": pos.initial_risk_dollars,
            }
        )
        self._open_pair.pop((pos.lead_symbol, pos.hedge_symbol), None)

    def _global_gate(self, *, row: _MarketRow, module_id: str, symbol: str, family: Family) -> str | None:
        if row.lockout:
            return "TIER1_LOCKOUT_ACTIVE"
        if not row.data_ok or not row.quote_ok:
            return "DATA_NOT_OK"
        if not row.trade_eligible:
            return "TRADE_NOT_ELIGIBLE"
        if row.atr_14_5m <= 0.0:
            return "ATR_KILL_SWITCH"
        if self._cooldowns.is_in_cooldown(module_id=module_id, symbol=symbol, now=row.ts):
            return "COOLDOWN_ACTIVE"
        if self._family_freeze.get(family, False):
            return "FAMILY_FREEZE_ACTIVE"
        halt = self._halt.can_open_new_entry()
        if not halt.approved:
            return halt.reason_code
        return None

    def _currently_open_symbols(self) -> set[str]:
        symbols = set(self._open_single.keys()) | set(self._pending.keys())
        for lead, hedge in self._open_pair:
            symbols.add(lead)
            symbols.add(hedge)
        return symbols

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

    def _roll_session_if_needed(self, ts: datetime) -> None:
        day = ts.date()
        if self._session_day == day:
            return
        self._session_day = day
        self._session_realized_pnl = 0.0
        self._halt.reset_session(session_start_equity=100_000.0)


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


def _load_rows(path: str | Path) -> list[_MarketRow]:
    rows: list[_MarketRow] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            rows.append(_parse_row(raw))
    rows.sort(key=lambda r: (r.ts, r.symbol))
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


def _estimate_slippage(*, symbol: str, atr_14_1m_price: float, tick_size: float) -> float:
    if tick_size <= 0.0:
        return 0.0
    atr_ticks = atr_14_1m_price / tick_size
    return estimate_slippage_ticks(symbol, atr_ticks).slippage_est_ticks


def _fill_price_with_slippage(
    *,
    price: float,
    side: OrderSide,
    symbol: str,
    instrument: InstrumentMeta,
    atr_14_1m_price: float,
) -> float:
    slip_ticks = _estimate_slippage(
        symbol=symbol,
        atr_14_1m_price=atr_14_1m_price,
        tick_size=instrument.tick_size,
    )
    slip_px = slip_ticks * instrument.tick_size
    if side is OrderSide.BUY:
        return price + slip_px
    return price - slip_px


def _pair_entry_sides(side: str) -> tuple[OrderSide, OrderSide]:
    if side == "short_spread":
        return OrderSide.SELL, OrderSide.BUY
    return OrderSide.BUY, OrderSide.SELL


def _pair_exit_sides(side: str) -> tuple[OrderSide, OrderSide]:
    if side == "short_spread":
        return OrderSide.BUY, OrderSide.SELL
    return OrderSide.SELL, OrderSide.BUY


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
