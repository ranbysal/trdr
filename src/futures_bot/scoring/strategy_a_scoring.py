"""Scoring model for Strategy A ORB."""

from __future__ import annotations

import math
from dataclasses import dataclass

from futures_bot.utils.math_utils import clip01


@dataclass(frozen=True, slots=True)
class StrategyAScoringConfig:
    target_ratio: float = 2.0
    tolerance_ratio: float = 1.0
    weight_regime_quality: float = 20.0
    weight_session_quality: float = 10.0
    weight_vwap_align: float = 15.0
    weight_pattern_quality: float = 20.0
    weight_volume_quality: float = 15.0
    weight_vol_sanity: float = 10.0
    weight_exec_quality: float = 10.0


@dataclass(frozen=True, slots=True)
class StrategyAScoreBreakdown:
    regime_quality: float
    session_quality: float
    vwap_align: float
    pattern_quality: float
    volume_quality: float
    vol_sanity: float
    exec_quality: float
    final_score: float
    action: str
    risk_fraction: float


def compute_pattern_quality(
    *,
    or_width: float,
    atr_14_5m: float,
    target_ratio: float,
    tolerance_ratio: float,
) -> float:
    """Score OR width pattern quality against target OR/ATR ratio."""
    if atr_14_5m <= 0.0 or tolerance_ratio <= 0.0:
        return 0.0

    ratio = or_width / atr_14_5m
    return clip01(1.0 - abs(ratio - target_ratio) / tolerance_ratio)


def compute_strategy_a_score(
    *,
    regime_quality: float,
    session_quality: float,
    vwap_align: float,
    pattern_quality: float,
    volume_quality: float,
    vol_sanity: float,
    exec_quality: float,
    config: StrategyAScoringConfig | None = None,
) -> StrategyAScoreBreakdown:
    """Compute final Strategy A score and action threshold."""
    cfg = config or StrategyAScoringConfig()

    components = {
        "regime_quality": _validate_component(regime_quality, "regime_quality"),
        "session_quality": _validate_component(session_quality, "session_quality"),
        "vwap_align": _validate_component(vwap_align, "vwap_align"),
        "pattern_quality": _validate_component(pattern_quality, "pattern_quality"),
        "volume_quality": _validate_component(volume_quality, "volume_quality"),
        "vol_sanity": _validate_component(vol_sanity, "vol_sanity"),
        "exec_quality": _validate_component(exec_quality, "exec_quality"),
    }

    final = (
        components["regime_quality"] * cfg.weight_regime_quality
        + components["session_quality"] * cfg.weight_session_quality
        + components["vwap_align"] * cfg.weight_vwap_align
        + components["pattern_quality"] * cfg.weight_pattern_quality
        + components["volume_quality"] * cfg.weight_volume_quality
        + components["vol_sanity"] * cfg.weight_vol_sanity
        + components["exec_quality"] * cfg.weight_exec_quality
    )
    final_score = round(final, 2)

    if final_score >= 80.0:
        action = "full_risk"
        risk_fraction = 1.0
    elif final_score >= 70.0:
        action = "half_risk"
        risk_fraction = 0.5
    else:
        action = "reject"
        risk_fraction = 0.0

    return StrategyAScoreBreakdown(
        regime_quality=components["regime_quality"],
        session_quality=components["session_quality"],
        vwap_align=components["vwap_align"],
        pattern_quality=components["pattern_quality"],
        volume_quality=components["volume_quality"],
        vol_sanity=components["vol_sanity"],
        exec_quality=components["exec_quality"],
        final_score=final_score,
        action=action,
        risk_fraction=risk_fraction,
    )


def _validate_component(value: float, name: str) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return clip01(float(value))
