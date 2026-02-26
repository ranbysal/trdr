from __future__ import annotations

from futures_bot.core.enums import Family, Regime
from futures_bot.regime.engine import RegimeEngine, classify_symbol_candidate
from futures_bot.regime.models import RegimeEngineState, SymbolFeatureSnapshot, SymbolRegimeState


def _snap(
    symbol: str,
    *,
    adx: float,
    er: float,
    slope: float,
    atr_pct: float,
    rvol_5m: float,
    data_ok: bool = True,
) -> SymbolFeatureSnapshot:
    return SymbolFeatureSnapshot(
        symbol=symbol,
        adx_14=adx,
        er_20=er,
        vwap_slope_norm=slope,
        atr_pct_rank=atr_pct,
        rvol_tod_5m=rvol_5m,
        data_ok=data_ok,
    )


def test_candidate_classification_rules() -> None:
    trend = classify_symbol_candidate(
        _snap("NQ", adx=22.0, er=0.35, slope=0.15, atr_pct=30.0, rvol_5m=1.0)
    )
    chop = classify_symbol_candidate(
        _snap("YM", adx=18.0, er=0.25, slope=0.08, atr_pct=8.0, rvol_5m=1.0)
    )
    neutral = classify_symbol_candidate(
        _snap("NQ", adx=21.9, er=0.35, slope=0.15, atr_pct=30.0, rvol_5m=1.0)
    )

    assert trend is Regime.TREND
    assert chop is Regime.CHOP
    assert neutral is Regime.NEUTRAL


def test_hysteresis_requires_two_consecutive_candidates() -> None:
    engine = RegimeEngine()
    state = engine.initialize_state()

    snapshots = {
        "NQ": _snap("NQ", adx=25.0, er=0.50, slope=0.25, atr_pct=50.0, rvol_5m=1.0),
    }

    state_1, _ = engine.step(state=state, snapshots=snapshots)
    state_2, _ = engine.step(state=state_1, snapshots=snapshots)

    assert state_1.symbol_states["NQ"].regime is Regime.NEUTRAL
    assert state_1.symbol_states["NQ"].pending_candidate is Regime.TREND
    assert state_1.symbol_states["NQ"].pending_count == 1

    assert state_2.symbol_states["NQ"].regime is Regime.TREND
    assert state_2.symbol_states["NQ"].pending_count == 0


def test_family_weak_neutral_on_disagreement_and_missing_symbol() -> None:
    engine = RegimeEngine()
    base = engine.initialize_state()

    disagreement_state = RegimeEngineState(
        symbol_states={
            **base.symbol_states,
            "NQ": SymbolRegimeState(symbol="NQ", regime=Regime.TREND),
            "YM": SymbolRegimeState(symbol="YM", regime=Regime.CHOP),
        },
        family_states=base.family_states,
    )

    snapshots = {
        "NQ": _snap("NQ", adx=24.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.9),
        "YM": _snap("YM", adx=16.0, er=0.2, slope=0.04, atr_pct=40.0, rvol_5m=0.9),
    }
    out_disagree, _ = engine.step(state=disagreement_state, snapshots=snapshots)
    family_disagree = out_disagree.family_states[Family.EQUITIES]

    assert family_disagree.raw_regime is Regime.NEUTRAL
    assert family_disagree.confidence == 0.6
    assert family_disagree.is_weak_neutral is True

    missing_snapshots = {
        "NQ": _snap("NQ", adx=24.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.9),
    }
    out_missing, _ = engine.step(state=disagreement_state, snapshots=missing_snapshots)
    family_missing = out_missing.family_states[Family.EQUITIES]

    assert family_missing.raw_regime is Regime.NEUTRAL
    assert family_missing.is_weak_neutral is True


def test_low_volume_trend_streak_updates_without_downgrading_trend() -> None:
    engine = RegimeEngine()
    base = engine.initialize_state()
    seeded = RegimeEngineState(
        symbol_states={
            **base.symbol_states,
            "NQ": SymbolRegimeState(symbol="NQ", regime=Regime.TREND),
            "YM": SymbolRegimeState(symbol="YM", regime=Regime.TREND),
        },
        family_states=base.family_states,
    )

    low_rvol = {
        "NQ": _snap("NQ", adx=24.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.60),
        "YM": _snap("YM", adx=23.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.65),
    }
    step_1, _ = engine.step(state=seeded, snapshots=low_rvol)
    step_2, _ = engine.step(state=step_1, snapshots=low_rvol)

    fam_1 = step_1.family_states[Family.EQUITIES]
    fam_2 = step_2.family_states[Family.EQUITIES]

    assert fam_1.raw_regime is Regime.TREND
    assert fam_1.low_volume_trend_streak_5m == 1
    assert fam_2.raw_regime is Regime.TREND
    assert fam_2.low_volume_trend_streak_5m == 2

    high_rvol = {
        "NQ": _snap("NQ", adx=24.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.9),
        "YM": _snap("YM", adx=23.0, er=0.4, slope=0.2, atr_pct=50.0, rvol_5m=0.85),
    }
    step_3, _ = engine.step(state=step_2, snapshots=high_rvol)
    fam_3 = step_3.family_states[Family.EQUITIES]

    assert fam_3.raw_regime is Regime.TREND
    assert fam_3.low_volume_trend_streak_5m == 0
