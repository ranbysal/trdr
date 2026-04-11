from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.enums import Family, StrategyModule
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.corrected_orchestrator import (
    AcceptedSignalOutput,
    CorrectedSignalOrchestrator,
    DailyHaltRejectedSignalOutput,
    EvaluationStage,
    GoldEvaluationRequest,
    NQEvaluationRequest,
    RejectedSignalOutput,
    RiskRejectedSignalOutput,
    YMEvaluationRequest,
)
from futures_bot.risk.models import OpenPositionMtmSnapshot

ET = ZoneInfo("America/New_York")


def _bars(start: datetime, *, count: int, base: float, slope: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i in range(count):
        close = base + slope * i
        rows.append(
            {
                "ts": start + timedelta(minutes=i),
                "open": close - 0.2,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1000.0 + i,
            }
        )
    return pd.DataFrame(rows)


def _instrument(symbol: str, *, family: Family, tick_size: float, tick_value: float) -> InstrumentMeta:
    return InstrumentMeta(
        symbol=symbol,
        root_symbol=symbol,
        family=family,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=tick_value / tick_size,
        commission_rt=4.8,
        symbol_type="future",
        micro_equivalent=symbol,
        contract_units=1.0,
    )


def _orchestrator() -> CorrectedSignalOrchestrator:
    return CorrectedSignalOrchestrator(
        nq_config=NQStrategyConfig(hard_risk_per_trade_dollars=750.0, daily_halt_loss_dollars=1_500.0),
        ym_config=YMStrategyConfig(hard_risk_per_trade_dollars=500.0, daily_halt_loss_dollars=1_500.0),
        gold_config=GoldStrategyConfig(hard_risk_per_trade_dollars=400.0, daily_halt_loss_dollars=1_200.0),
    )


def test_evaluation_order_is_deterministic() -> None:
    orchestrator = _orchestrator()
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=20_480.0, slope=0.45)

    out = orchestrator.evaluate_nq(
        NQEvaluationRequest(
            bars_1m=bars,
            instrument=_instrument("NQ", family=Family.EQUITIES, tick_size=0.25, tick_value=5.0),
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=20_520.0,
            structure_break_price=20_525.0,
            order_block_low=20_518.0,
            order_block_high=20_525.0,
        )
    )

    assert isinstance(out, AcceptedSignalOutput)
    assert tuple(event.stage for event in out.stage_events) == (
        EvaluationStage.MARKET_SESSION_STATE_UPDATE,
        EvaluationStage.ANCHORED_SESSION_SELECTION,
        EvaluationStage.INDICATOR_UPDATE,
        EvaluationStage.LIQUIDITY_NEWS_GATING,
        EvaluationStage.INSTRUMENT_SIGNAL_EVALUATION,
        EvaluationStage.HARD_RISK_CAP_SIZING,
        EvaluationStage.DAILY_HALT_CHECK,
        EvaluationStage.FINAL_SIGNAL_OUTPUT,
    )


def test_accepted_and_rejected_reasons_are_stable() -> None:
    orchestrator = _orchestrator()
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=42_200.0, slope=0.0)
    request = YMEvaluationRequest(
        bars_1m=bars,
        instrument=_instrument("YM", family=Family.EQUITIES, tick_size=1.0, tick_value=5.0),
        session_start_equity=100_000.0,
        realized_pnl=0.0,
        open_positions=(),
        liquidity_ok=True,
        macro_blocked=False,
    )

    first = orchestrator.evaluate_ym(request)
    second = orchestrator.evaluate_ym(request)

    assert isinstance(first, RejectedSignalOutput)
    assert isinstance(second, RejectedSignalOutput)
    assert first.rejection_reason == second.rejection_reason
    assert tuple(event.reason for event in first.stage_events) == tuple(event.reason for event in second.stage_events)


def test_daily_halt_blocks_signals_regardless_of_realized_only_state() -> None:
    orchestrator = CorrectedSignalOrchestrator(
        nq_config=NQStrategyConfig(hard_risk_per_trade_dollars=750.0, daily_halt_loss_dollars=1_500.0),
        ym_config=YMStrategyConfig(hard_risk_per_trade_dollars=500.0, daily_halt_loss_dollars=1_500.0),
        gold_config=GoldStrategyConfig(
            hard_risk_per_trade_dollars=400.0,
            daily_halt_loss_dollars=1_200.0,
            symbol="MGC",
        ),
    )
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=2_640.0, slope=0.0)
    positions = (
        OpenPositionMtmSnapshot(
            ts=datetime(2026, 1, 5, 15, 0, tzinfo=ET),
            symbol="NQ",
            quantity=1,
            avg_entry_price=20_000.0,
            mark_price=19_990.0,
            point_value=20.0,
        ),
    )

    out = orchestrator.evaluate_gold(
        GoldEvaluationRequest(
            bars_1m=bars,
            instrument=_instrument("MGC", family=Family.METALS, tick_size=0.1, tick_value=1.0),
            session_start_equity=100_000.0,
            realized_pnl=-1_400.0,
            open_positions=positions,
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=2_640.0,
            structure_break_price=2_640.0,
            order_block_low=2_639.8,
            order_block_high=2_640.2,
        )
    )

    assert isinstance(out, DailyHaltRejectedSignalOutput)
    assert out.rejection_reason == "DAILY_LOSS_HALT"


def test_hard_risk_cap_blocks_oversize_trades() -> None:
    orchestrator = CorrectedSignalOrchestrator(
        nq_config=NQStrategyConfig(hard_risk_per_trade_dollars=150.0, daily_halt_loss_dollars=1_500.0),
        ym_config=YMStrategyConfig(hard_risk_per_trade_dollars=500.0, daily_halt_loss_dollars=1_500.0),
        gold_config=GoldStrategyConfig(hard_risk_per_trade_dollars=400.0, daily_halt_loss_dollars=1_200.0),
    )
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=20_480.0, slope=0.45)

    out = orchestrator.evaluate_nq(
        NQEvaluationRequest(
            bars_1m=bars,
            instrument=_instrument("NQ", family=Family.EQUITIES, tick_size=0.25, tick_value=5.0),
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
            pullback_price=20_520.0,
            structure_break_price=20_525.0,
            order_block_low=20_300.0,
            order_block_high=20_525.0,
        )
    )

    assert isinstance(out, RiskRejectedSignalOutput)
    assert out.rejection_reason == "HARD_RISK_CAP_EXCEEDED"


def test_incremental_indicator_updates_match_fresh_full_history_evaluation() -> None:
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=20_480.0, slope=0.45)
    instrument = _instrument("NQ", family=Family.EQUITIES, tick_size=0.25, tick_value=5.0)
    request_kwargs = {
        "instrument": instrument,
        "session_start_equity": 100_000.0,
        "realized_pnl": 0.0,
        "open_positions": (),
        "liquidity_ok": True,
        "macro_blocked": False,
        "pullback_price": 20_520.0,
        "structure_break_price": 20_525.0,
        "order_block_low": 20_518.0,
        "order_block_high": 20_525.0,
    }
    incremental_orchestrator = _orchestrator()
    incremental_output = None
    for end in range(1, len(bars.index) + 1):
        incremental_output = incremental_orchestrator.evaluate_nq(
            NQEvaluationRequest(
                bars_1m=bars.iloc[:end],
                **request_kwargs,
            )
        )

    fresh_output = _orchestrator().evaluate_nq(
        NQEvaluationRequest(
            bars_1m=bars,
            **request_kwargs,
        )
    )

    assert incremental_output is not None
    assert type(incremental_output) is type(fresh_output)
    assert tuple(event.reason for event in incremental_output.stage_events) == tuple(
        event.reason for event in fresh_output.stage_events
    )
    if isinstance(incremental_output, AcceptedSignalOutput) and isinstance(fresh_output, AcceptedSignalOutput):
        assert incremental_output.signal.side == fresh_output.signal.side
        assert incremental_output.signal.score == fresh_output.signal.score
        assert incremental_output.sizing.contracts == fresh_output.sizing.contracts


def test_no_stage_silently_mutates_instrument_architecture_assumptions() -> None:
    orchestrator = _orchestrator()
    bars = _bars(datetime(2026, 1, 5, 9, 30, tzinfo=ET), count=130, base=42_180.0, slope=0.25)

    out = orchestrator.evaluate_ym(
        YMEvaluationRequest(
            bars_1m=bars,
            instrument=_instrument("YM", family=Family.EQUITIES, tick_size=1.0, tick_value=5.0),
            session_start_equity=100_000.0,
            realized_pnl=0.0,
            open_positions=(),
            liquidity_ok=True,
            macro_blocked=False,
        )
    )

    assert orchestrator.nq_config.symbol == "NQ"
    assert orchestrator.ym_config.symbol == "YM"
    assert orchestrator.gold_config.symbol == "GC"
    assert isinstance(out, AcceptedSignalOutput | RejectedSignalOutput)
    if isinstance(out, AcceptedSignalOutput):
        assert out.signal.symbol == "YM"
        assert out.signal.strategy is StrategyModule.STRAT_YM_SIGNAL
