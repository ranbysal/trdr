"""Typed models for regime engine inputs and state."""

from __future__ import annotations

from dataclasses import dataclass, field

from futures_bot.core.enums import Family, Regime


@dataclass(frozen=True, slots=True)
class SymbolFeatureSnapshot:
    symbol: str
    adx_14: float
    er_20: float
    vwap_slope_norm: float
    atr_pct_rank: float
    rvol_tod_5m: float | None
    data_ok: bool


@dataclass(frozen=True, slots=True)
class SymbolRegimeState:
    symbol: str
    regime: Regime = Regime.NEUTRAL
    pending_candidate: Regime = Regime.NEUTRAL
    pending_count: int = 0


@dataclass(frozen=True, slots=True)
class FamilyRegimeState:
    family: Family
    raw_regime: Regime = Regime.NEUTRAL
    confidence: float = 0.6
    is_weak_neutral: bool = True
    low_volume_trend_streak_5m: int = 0
    rvol_tod_5m: float | None = None


@dataclass(frozen=True, slots=True)
class RegimeEngineState:
    symbol_states: dict[str, SymbolRegimeState] = field(default_factory=dict)
    family_states: dict[Family, FamilyRegimeState] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QualifiedTrendInputs:
    family_raw_regime: Regime
    low_volume_trend_streak_5m: int
    trigger_rvol_tod_1m: float | None
    trigger_vol_strong_1m: bool
