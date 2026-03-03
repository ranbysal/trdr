"""5m regime engine implementation with hysteresis and family consensus."""

from __future__ import annotations

import math
from dataclasses import replace
from typing import Mapping

from futures_bot.core.enums import Family, Regime
from futures_bot.regime.models import (
    FamilyRegimeState,
    QualifiedTrendInputs,
    RegimeEngineState,
    SymbolFeatureSnapshot,
    SymbolRegimeState,
)

FAMILY_SYMBOLS: dict[Family, tuple[str, str]] = {
    Family.EQUITIES: ("NQ", "YM"),
    Family.METALS: ("MGC", "SIL"),
}


def classify_symbol_candidate(snapshot: SymbolFeatureSnapshot) -> Regime:
    """Classify a symbol-level candidate regime from 5m features."""
    adx = snapshot.adx_14
    er = snapshot.er_20
    slope_abs = abs(snapshot.vwap_slope_norm)
    atr_pct = snapshot.atr_pct_rank

    if adx >= 22.0 and er >= 0.35 and slope_abs >= 0.15 and 30.0 <= atr_pct <= 95.0:
        return Regime.TREND
    if adx <= 18.0 and er <= 0.25 and slope_abs <= 0.08 and 8.0 <= atr_pct <= 95.0:
        return Regime.CHOP
    return Regime.NEUTRAL


def build_qualified_trend_for_breakout_inputs(
    *,
    family_state: FamilyRegimeState,
    trigger_rvol_tod_1m: float | None,
    trigger_vol_strong_1m: bool,
) -> QualifiedTrendInputs:
    """Package breakout-qualification inputs for downstream 1m signal checks."""
    return QualifiedTrendInputs(
        family_raw_regime=family_state.raw_regime,
        low_volume_trend_streak_5m=family_state.low_volume_trend_streak_5m,
        trigger_rvol_tod_1m=trigger_rvol_tod_1m,
        trigger_vol_strong_1m=trigger_vol_strong_1m,
    )


def qualified_trend_for_breakout(inputs: QualifiedTrendInputs) -> bool:
    """Return True when TREND is qualified for 1m breakout entries."""
    return inputs.family_raw_regime is Regime.TREND and (
        inputs.low_volume_trend_streak_5m < 3 or inputs.trigger_vol_strong_1m
    )


class RegimeEngine:
    """Deterministic 5m regime engine without strategy entry logic."""

    def __init__(
        self,
        *,
        family_symbols: Mapping[Family, tuple[str, str]] | None = None,
    ) -> None:
        self._family_symbols = dict(family_symbols or FAMILY_SYMBOLS)

    def initialize_state(self) -> RegimeEngineState:
        symbol_states: dict[str, SymbolRegimeState] = {}
        family_states: dict[Family, FamilyRegimeState] = {}
        for family, symbols in self._family_symbols.items():
            family_states[family] = FamilyRegimeState(family=family)
            for symbol in symbols:
                symbol_states[symbol] = SymbolRegimeState(symbol=symbol)
        return RegimeEngineState(symbol_states=symbol_states, family_states=family_states)

    def step(
        self,
        *,
        state: RegimeEngineState,
        snapshots: Mapping[str, SymbolFeatureSnapshot],
    ) -> tuple[RegimeEngineState, list[dict[str, object]]]:
        """Apply one 5m update and return new state plus structured logs."""
        logs: list[dict[str, object]] = []

        next_symbol_states = dict(state.symbol_states)
        for symbol, snapshot in snapshots.items():
            prev = next_symbol_states.get(symbol, SymbolRegimeState(symbol=symbol))
            updated, event = _apply_hysteresis(prev, candidate=classify_symbol_candidate(snapshot))
            next_symbol_states[symbol] = updated
            if event is not None:
                logs.append({"code": "SYMBOL_REGIME_UPDATED", "symbol": symbol, **event})

        next_family_states = dict(state.family_states)
        for family, members in self._family_symbols.items():
            prev_family = next_family_states.get(family, FamilyRegimeState(family=family))
            family_state, event = _resolve_family_state(
                family=family,
                members=members,
                symbol_states=next_symbol_states,
                snapshots=snapshots,
                prev_family=prev_family,
            )
            next_family_states[family] = family_state
            logs.append({"code": "FAMILY_REGIME_EVALUATED", "family": family.value, **event})

        return RegimeEngineState(
            symbol_states=next_symbol_states,
            family_states=next_family_states,
        ), logs


def _apply_hysteresis(
    prev: SymbolRegimeState,
    *,
    candidate: Regime,
) -> tuple[SymbolRegimeState, dict[str, object] | None]:
    if candidate == prev.regime:
        return replace(prev, pending_candidate=candidate, pending_count=0), None

    if prev.pending_candidate == candidate:
        next_count = prev.pending_count + 1
    else:
        next_count = 1

    if next_count >= 2:
        updated = replace(
            prev,
            regime=candidate,
            pending_candidate=candidate,
            pending_count=0,
        )
        return updated, {"from": prev.regime.value, "to": candidate.value}

    updated = replace(prev, pending_candidate=candidate, pending_count=next_count)
    return updated, None


def _resolve_family_state(
    *,
    family: Family,
    members: tuple[str, str],
    symbol_states: Mapping[str, SymbolRegimeState],
    snapshots: Mapping[str, SymbolFeatureSnapshot],
    prev_family: FamilyRegimeState,
) -> tuple[FamilyRegimeState, dict[str, object]]:
    left, right = members

    left_state = symbol_states.get(left)
    right_state = symbol_states.get(right)

    left_snap = snapshots.get(left)
    right_snap = snapshots.get(right)

    both_present = left_state is not None and right_state is not None and left_snap is not None and right_snap is not None
    both_data_ok = bool(both_present and left_snap.data_ok and right_snap.data_ok)

    if both_data_ok and left_state.regime == right_state.regime:
        raw_regime = left_state.regime
        confidence = 1.0
        is_weak_neutral = False
    else:
        raw_regime = Regime.NEUTRAL
        confidence = 0.6
        is_weak_neutral = True

    family_rvol = _family_rvol_tod_5m(members=members, snapshots=snapshots)
    if raw_regime is Regime.TREND and family_rvol is not None and family_rvol < 0.70:
        next_streak = prev_family.low_volume_trend_streak_5m + 1
    else:
        next_streak = 0

    updated = replace(
        prev_family,
        raw_regime=raw_regime,
        confidence=confidence,
        is_weak_neutral=is_weak_neutral,
        low_volume_trend_streak_5m=next_streak,
        rvol_tod_5m=family_rvol,
    )

    event = {
        "raw_regime": raw_regime.value,
        "confidence": confidence,
        "is_weak_neutral": is_weak_neutral,
        "low_volume_trend_streak_5m": next_streak,
    }
    return updated, event


def _family_rvol_tod_5m(
    *,
    members: tuple[str, str],
    snapshots: Mapping[str, SymbolFeatureSnapshot],
) -> float | None:
    values: list[float] = []
    for symbol in members:
        snap = snapshots.get(symbol)
        if snap is None or snap.rvol_tod_5m is None:
            continue
        if math.isfinite(snap.rvol_tod_5m):
            values.append(float(snap.rvol_tod_5m))

    if not values:
        return None
    return sum(values) / len(values)
