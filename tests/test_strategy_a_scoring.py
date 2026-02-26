from __future__ import annotations

from futures_bot.scoring.strategy_a_scoring import (
    StrategyAScoringConfig,
    compute_pattern_quality,
    compute_strategy_a_score,
)


def test_score_computation_deterministic() -> None:
    cfg = StrategyAScoringConfig(target_ratio=2.0, tolerance_ratio=1.0)
    pattern = compute_pattern_quality(or_width=4.0, atr_14_5m=2.0, target_ratio=2.0, tolerance_ratio=1.0)

    out = compute_strategy_a_score(
        regime_quality=1.0,
        session_quality=0.8,
        vwap_align=1.0,
        pattern_quality=pattern,
        volume_quality=1.0,
        vol_sanity=1.0,
        exec_quality=0.9,
        config=cfg,
    )

    assert pattern == 1.0
    assert out.final_score == 97.0
    assert out.action == "full_risk"


def test_threshold_behavior() -> None:
    hi = compute_strategy_a_score(
        regime_quality=1.0,
        session_quality=1.0,
        vwap_align=1.0,
        pattern_quality=1.0,
        volume_quality=1.0,
        vol_sanity=1.0,
        exec_quality=1.0,
    )
    mid = compute_strategy_a_score(
        regime_quality=0.7,
        session_quality=0.7,
        vwap_align=0.7,
        pattern_quality=0.7,
        volume_quality=0.7,
        vol_sanity=0.7,
        exec_quality=0.7,
    )
    lo = compute_strategy_a_score(
        regime_quality=0.5,
        session_quality=0.5,
        vwap_align=0.5,
        pattern_quality=0.5,
        volume_quality=0.5,
        vol_sanity=0.5,
        exec_quality=0.5,
    )

    assert hi.action == "full_risk"
    assert 70.0 <= mid.final_score < 80.0
    assert mid.action == "half_risk"
    assert lo.final_score < 70.0
    assert lo.action == "reject"
