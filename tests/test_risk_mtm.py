from __future__ import annotations

from datetime import datetime, timezone

from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta
from futures_bot.risk.daily_halt import DailyHaltManager, build_account_risk_state
from futures_bot.risk.models import OpenPositionMtmSnapshot, SingleLegSizingRequest
from futures_bot.risk.sizing_single import size_single_leg_with_hard_risk_cap


def _instrument(symbol: str) -> InstrumentMeta:
    tick_size = 0.25 if symbol == "NQ" else 1.0
    tick_value = 5.0 if symbol == "NQ" else 10.0
    return InstrumentMeta(
        symbol=symbol,
        root_symbol=symbol,
        family=Family.EQUITIES if symbol == "NQ" else Family.METALS,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=tick_value / tick_size,
        commission_rt=4.8,
        symbol_type="future",
        micro_equivalent=symbol,
        contract_units=1.0,
    )


def test_hard_risk_cap_skips_when_one_contract_exceeds_limit() -> None:
    request = SingleLegSizingRequest(
        instrument=_instrument("NQ"),
        equity=100_000.0,
        risk_pct=0.01,
        entry_price=20_000.0,
        stop_price=19_980.0,
        atr_14_1m_price=10.0,
    )

    decision = size_single_leg_with_hard_risk_cap(request, hard_max_risk_dollars=300.0)

    assert decision.approved is False
    assert decision.reason_code == "HARD_RISK_CAP_EXCEEDED"
    assert decision.contracts == 0


def test_daily_mtm_halt_calculation() -> None:
    positions = [
        OpenPositionMtmSnapshot(
            ts=datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc),
            symbol="NQ",
            quantity=1,
            avg_entry_price=20_000.0,
            mark_price=19_990.0,
            point_value=20.0,
        )
    ]

    state = build_account_risk_state(
        session_start_equity=100_000.0,
        realized_pnl=-1_000.0,
        open_positions=positions,
        daily_loss_halt_pct=0.015,
    )

    assert state.realized_pnl == -1_000.0
    assert state.unrealized_pnl == -200.0
    assert state.daily_pnl == -1_200.0
    assert state.is_daily_halt is False

    halt = DailyHaltManager(realized_loss_halt_pct=0.015)
    halt.reset_session(session_start_equity=100_000.0)
    halt.update_realized_pnl(realized_pnl=-1_400.0)
    halt.update_open_positions(open_positions=positions)

    decision = halt.can_open_new_entry()

    assert decision.approved is False
    assert decision.reason_code == "DAILY_LOSS_HALT"
