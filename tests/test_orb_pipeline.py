from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from futures_bot.core.enums import Family, Regime
from futures_bot.core.types import Bar1m, InstrumentMeta
from futures_bot.data.calendar_store import LockoutStatus
from futures_bot.pipeline.orb_pipeline import (
    ORBFeatureSnapshot,
    ORBRiskVaultState,
    ORBSymbolSnapshot,
    run_strategy_a_orb_pipeline,
)
from futures_bot.regime.models import FamilyRegimeState
from futures_bot.risk.cooldowns import ConsecutiveLossCooldownManager
from futures_bot.risk.daily_halt import DailyHaltManager
from futures_bot.risk.portfolio_caps import PortfolioCapsManager
from futures_bot.strategies.strategy_a_orb import StrategyAORB

ET = ZoneInfo("America/New_York")


def _instrument(symbol: str, micro: str) -> InstrumentMeta:
    return InstrumentMeta(
        symbol=symbol,
        root_symbol=symbol,
        family=Family.EQUITIES,
        tick_size=0.25 if symbol in {"NQ", "MNQ"} else 1.0,
        tick_value=5.0 if symbol == "NQ" else 0.5 if symbol == "MNQ" else 5.0,
        point_value=20.0,
        commission_rt=4.8 if symbol == "NQ" else 1.2,
        symbol_type="future",
        micro_equivalent=micro,
        contract_units=1.0,
    )


def _bar(ts: datetime, close: float, symbol: str = "NQ") -> Bar1m:
    return Bar1m(ts=ts, symbol=symbol, open=close, high=close + 0.5, low=close - 0.5, close=close, volume=1000)


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


def _risk_state() -> ORBRiskVaultState:
    caps = PortfolioCapsManager(equity=100_000.0)
    cooldown = ConsecutiveLossCooldownManager()
    halt = DailyHaltManager()
    halt.reset_session(session_start_equity=100_000.0)
    instruments = {
        "NQ": _instrument("NQ", "MNQ"),
        "MNQ": _instrument("MNQ", "MNQ"),
        "YM": _instrument("YM", "MYM"),
    }
    return ORBRiskVaultState(
        module_id="strat_a_orb",
        equity=100_000.0,
        risk_pct=0.003,
        instruments_by_symbol=instruments,
        caps_manager=caps,
        cooldown_manager=cooldown,
        daily_halt_manager=halt,
    )


def test_pipeline_approval_path() -> None:
    strategy = StrategyAORB()
    day = datetime(2026, 1, 7, tzinfo=ET)
    _build_or(strategy, day=day)

    snap = ORBSymbolSnapshot(
        bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6),
        instrument=_instrument("NQ", "MNQ"),
        atr_14_1m_price=2.0,
    )
    feat = ORBFeatureSnapshot(
        session_vwap=100.0,
        ema9_5m=106.0,
        ema21_5m=103.0,
        atr_14_5m=4.0,
        vol_strong_1m=True,
        rvol_3bar_aggregate_5m=1.2,
        exec_quality=1.0,
    )
    family_state = FamilyRegimeState(family=Family.EQUITIES, raw_regime=Regime.TREND, confidence=1.0, is_weak_neutral=False)
    lockout = LockoutStatus(is_locked_out=False, cancel_resting_entries=False)

    out = run_strategy_a_orb_pipeline(
        strategy=strategy,
        symbol_snapshot=snap,
        feature_snapshot=feat,
        family_regime_state=family_state,
        lockout_state=lockout,
        risk_state=_risk_state(),
    )

    assert out.approved is True
    assert out.signal is not None
    assert out.signal.score >= 80.0
    assert out.sizing is not None and out.sizing.approved is True


def test_pipeline_rejection_on_lockout() -> None:
    strategy = StrategyAORB()
    day = datetime(2026, 1, 7, tzinfo=ET)
    _build_or(strategy, day=day)

    out = run_strategy_a_orb_pipeline(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=ORBFeatureSnapshot(
            session_vwap=100.0,
            ema9_5m=106.0,
            ema21_5m=103.0,
            atr_14_5m=4.0,
            vol_strong_1m=True,
            rvol_3bar_aggregate_5m=1.2,
        ),
        family_regime_state=FamilyRegimeState(family=Family.EQUITIES, raw_regime=Regime.TREND, confidence=1.0, is_weak_neutral=False),
        lockout_state=LockoutStatus(is_locked_out=True, cancel_resting_entries=True, code="CALENDAR_LOCKOUT"),
        risk_state=_risk_state(),
    )

    assert out.approved is False
    assert out.reason_code == "TIER1_LOCKOUT_ACTIVE"


def test_pipeline_rejection_on_cooldown() -> None:
    strategy = StrategyAORB()
    day = datetime(2026, 1, 7, tzinfo=ET)
    _build_or(strategy, day=day)

    risk = _risk_state()
    t0 = datetime(2026, 1, 7, 14, 40, tzinfo=timezone.utc)
    risk.cooldown_manager.record_closed_trade(module_id="strat_a_orb", symbol="NQ", net_realized_pnl_after_costs=-1.0, closed_at=t0)
    risk.cooldown_manager.record_closed_trade(module_id="strat_a_orb", symbol="NQ", net_realized_pnl_after_costs=-1.0, closed_at=t0 + timedelta(minutes=1))
    risk.cooldown_manager.record_closed_trade(module_id="strat_a_orb", symbol="NQ", net_realized_pnl_after_costs=-1.0, closed_at=t0 + timedelta(minutes=2))

    out = run_strategy_a_orb_pipeline(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=ORBFeatureSnapshot(
            session_vwap=100.0,
            ema9_5m=106.0,
            ema21_5m=103.0,
            atr_14_5m=4.0,
            vol_strong_1m=True,
            rvol_3bar_aggregate_5m=1.2,
        ),
        family_regime_state=FamilyRegimeState(family=Family.EQUITIES, raw_regime=Regime.TREND, confidence=1.0, is_weak_neutral=False),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
    )

    assert out.approved is False
    assert out.reason_code == "COOLDOWN_ACTIVE"


def test_pipeline_rejection_on_caps() -> None:
    strategy = StrategyAORB()
    day = datetime(2026, 1, 7, tzinfo=ET)
    _build_or(strategy, day=day)

    risk = _risk_state()
    risk.caps_manager.record_open_position(family=Family.EQUITIES, symbol="YM", risk_dollars=740.0)

    out = run_strategy_a_orb_pipeline(
        strategy=strategy,
        symbol_snapshot=ORBSymbolSnapshot(
            bar_1m=_bar(day.replace(hour=10, minute=0), close=104.6),
            instrument=_instrument("NQ", "MNQ"),
            atr_14_1m_price=2.0,
        ),
        feature_snapshot=ORBFeatureSnapshot(
            session_vwap=100.0,
            ema9_5m=106.0,
            ema21_5m=103.0,
            atr_14_5m=4.0,
            vol_strong_1m=True,
            rvol_3bar_aggregate_5m=1.2,
        ),
        family_regime_state=FamilyRegimeState(family=Family.EQUITIES, raw_regime=Regime.TREND, confidence=1.0, is_weak_neutral=False),
        lockout_state=LockoutStatus(is_locked_out=False, cancel_resting_entries=False),
        risk_state=risk,
    )

    assert out.approved is False
    assert out.reason_code == "FAMILY_OPEN_RISK_CAP"
