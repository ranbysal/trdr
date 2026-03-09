from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family, Regime
from futures_bot.core.reason_codes import DATA_NOT_OK
from futures_bot.core.types import Bar1m, InstrumentMeta
from futures_bot.data.calendar_store import LockoutStatus
from futures_bot.execution.paper_engine import StrategyAPaperEngine
from futures_bot.pipeline.orb_pipeline import ORBFeatureSnapshot, ORBRiskVaultState, ORBSymbolSnapshot
from futures_bot.regime.models import FamilyRegimeState
from futures_bot.risk.cooldowns import ConsecutiveLossCooldownManager
from futures_bot.risk.daily_halt import DailyHaltManager
from futures_bot.risk.portfolio_caps import PortfolioCapsManager
from futures_bot.strategies.strategy_a_orb import StrategyAORB

ET = ZoneInfo("America/New_York")


def _instrument(symbol: str, micro: str) -> InstrumentMeta:
    tick_size = 0.25 if symbol in {"NQ", "MNQ"} else 1.0
    tick_value = 5.0 if symbol == "NQ" else 0.5 if symbol == "MNQ" else 5.0
    return InstrumentMeta(
        symbol=symbol,
        root_symbol=symbol,
        family=Family.EQUITIES,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=tick_value / tick_size,
        commission_rt=4.8 if symbol == "NQ" else 1.2,
        symbol_type="future",
        micro_equivalent=micro,
        contract_units=1.0,
    )


def _bar(ts: datetime, close: float, *, high: float | None = None, low: float | None = None, symbol: str = "NQ") -> Bar1m:
    h = close + 0.5 if high is None else high
    l = close - 0.5 if low is None else low
    return Bar1m(ts=ts, symbol=symbol, open=close, high=h, low=l, close=close, volume=1000)


def _build_or(strategy: StrategyAORB, day: datetime, symbol: str = "NQ") -> None:
    for i in range(15):
        ts = day.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(minutes=i)
        b = Bar1m(ts=ts, symbol=symbol, open=100.0, high=102.0 + i * 0.1, low=98.0 - i * 0.1, close=100.0, volume=1000)
        strategy.update_or_state(b)
    strategy.update_or_state(
        Bar1m(
            ts=day.replace(hour=9, minute=45, second=0, microsecond=0),
            symbol=symbol,
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1000,
        )
    )


def _risk_state(risk_pct: float = 0.003) -> ORBRiskVaultState:
    caps = PortfolioCapsManager(equity=100_000.0)
    cooldown = ConsecutiveLossCooldownManager()
    halt = DailyHaltManager()
    halt.reset_session(session_start_equity=100_000.0)
    instruments = {
        "NQ": _instrument("NQ", "MNQ"),
        "MNQ": _instrument("MNQ", "MNQ"),
    }
    return ORBRiskVaultState(
        module_id="strat_a_orb",
        equity=100_000.0,
        risk_pct=risk_pct,
        instruments_by_symbol=instruments,
        caps_manager=caps,
        cooldown_manager=cooldown,
        daily_halt_manager=halt,
    )


def _feat() -> ORBFeatureSnapshot:
    return ORBFeatureSnapshot(
        session_vwap=100.0,
        ema9_5m=106.0,
        ema21_5m=103.0,
        atr_14_5m=4.0,
        vol_strong_1m=True,
        rvol_3bar_aggregate_5m=1.2,
        exec_quality=1.0,
        ema9_1m=105.0,
    )


def _family_state() -> FamilyRegimeState:
    return FamilyRegimeState(family=Family.EQUITIES, raw_regime=Regime.TREND, confidence=1.0, is_weak_neutral=False)


def _read_events(path: Path) -> list[dict[str, object]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def test_paper_engine_submits_and_fills_entry_with_slippage(tmp_path: Path) -> None:
    log_path = tmp_path / "trade_logs.json"
    engine = StrategyAPaperEngine(trade_log_path=log_path)
    strategy = StrategyAORB()
    day = datetime(2026, 1, 12, tzinfo=ET)
    _build_or(strategy, day=day)
    risk = _risk_state()

    submit = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6, high=104.8, low=104.0),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )
    assert submit.submitted_entry

    fill = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=1), close=105.0, high=110.0, low=104.2),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )
    assert fill.filled_entry

    events = _read_events(log_path)
    assert any(e.get("event") == "order_submitted" for e in events)
    filled = [e for e in events if e.get("event") == "entry_filled"]
    assert filled
    assert float(filled[0]["slippage_ticks"]) > 0.0


def test_paper_engine_rejects_when_data_not_ok(tmp_path: Path) -> None:
    log_path = tmp_path / "trade_logs.json"
    engine = StrategyAPaperEngine(trade_log_path=log_path)
    strategy = StrategyAORB()
    day = datetime(2026, 1, 12, tzinfo=ET)
    _build_or(strategy, day=day)
    risk = _risk_state(risk_pct=0.001)

    out = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=False,
        quote_ok=True,
    )

    assert out.reason_code == DATA_NOT_OK
    events = _read_events(log_path)
    assert any(e.get("code") == DATA_NOT_OK for e in events)


def test_paper_engine_cancels_resting_entry_on_lockout(tmp_path: Path) -> None:
    log_path = tmp_path / "trade_logs.json"
    engine = StrategyAPaperEngine(trade_log_path=log_path)
    strategy = StrategyAORB()
    day = datetime(2026, 1, 12, tzinfo=ET)
    _build_or(strategy, day=day)
    risk = _risk_state(risk_pct=0.001)

    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6, high=104.8, low=104.0),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )

    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=1), close=104.7, high=104.9, low=104.3),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=True, cancel_resting_entries=True, code="CALENDAR_LOCKOUT"),
        risk_state=risk,
        data_ok=False,
        quote_ok=True,
    )

    events = _read_events(log_path)
    assert any(e.get("code") == "PENDING_ENTRY_CANCELLED_LOCKOUT" for e in events)


def test_paper_engine_partial_exits_and_position_close(tmp_path: Path) -> None:
    log_path = tmp_path / "trade_logs.json"
    engine = StrategyAPaperEngine(trade_log_path=log_path)
    strategy = StrategyAORB()
    day = datetime(2026, 1, 12, tzinfo=ET)
    _build_or(strategy, day=day)
    risk = _risk_state(risk_pct=0.001)

    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6, high=104.8, low=104.0),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )
    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=1), close=105.0, high=110.0, low=104.2),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )

    # Hit TP1 and TP2 without stopping out remaining TP3 tranche.
    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=2), close=106.0, high=130.0, low=105.0),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )

    # Drop into trailing stop for TP3.
    _ = engine.step(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=3), close=100.0, high=101.0, low=90.0),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=_feat(),
        family_regime_state=_family_state(),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
        data_ok=True,
        quote_ok=True,
    )

    events = _read_events(log_path)
    updates = [e for e in events if e.get("event") == "position_update"]
    assert len(updates) >= 1
    assert any(
        e.get("exit_reason") in {"TP1_EXIT", "TP2_EXIT", "TP3_TRAIL_EXIT", "TP1_STOP_EXIT", "TP2_STOP_EXIT"}
        for e in updates
    )
    assert any(e.get("event") == "position_closed" for e in events)
