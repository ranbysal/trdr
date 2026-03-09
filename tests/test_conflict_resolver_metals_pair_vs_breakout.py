from __future__ import annotations

from futures_bot.core.enums import Family, StrategyModule
from futures_bot.pipeline.portfolio_orchestrator import StrategyCandidate, resolve_strategy_conflicts


def test_metals_pair_breakout_conflict_prefers_lower_slippage_within_5_points() -> None:
    candidates = [
        StrategyCandidate(
            strategy=StrategyModule.STRAT_C_METALS_ORB,
            family=Family.METALS,
            symbols=("MGC",),
            score=90.0,
            slippage_est_ticks=2.5,
        ),
        StrategyCandidate(
            strategy=StrategyModule.STRAT_D_PAIR,
            family=Family.METALS,
            symbols=("MGC", "SIL"),
            score=87.0,
            slippage_est_ticks=0.4,
        ),
    ]
    selected, decisions = resolve_strategy_conflicts(candidates=candidates, open_symbols=set())

    assert len(selected) == 1
    assert selected[0].strategy is StrategyModule.STRAT_D_PAIR
    assert any((not d.accepted) and d.reason_code == "METALS_PAIR_CONFLICT" for d in decisions)

