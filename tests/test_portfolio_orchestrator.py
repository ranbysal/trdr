from __future__ import annotations

from futures_bot.core.enums import Family, StrategyModule
from futures_bot.pipeline.portfolio_orchestrator import StrategyCandidate, resolve_strategy_conflicts


def test_conflict_resolution_prioritizes_and_blocks_overlap() -> None:
    cands = [
        StrategyCandidate(strategy=StrategyModule.STRAT_A_ORB, family=Family.EQUITIES, symbols=("NQ",), score=90.0),
        StrategyCandidate(strategy=StrategyModule.STRAT_B_VWAP_REV, family=Family.EQUITIES, symbols=("NQ",), score=95.0),
        StrategyCandidate(strategy=StrategyModule.STRAT_D_PAIR, family=Family.METALS, symbols=("MGC", "SIL"), score=85.0),
        StrategyCandidate(strategy=StrategyModule.STRAT_C_METALS_ORB, family=Family.METALS, symbols=("MGC",), score=88.0),
    ]
    selected, decisions = resolve_strategy_conflicts(candidates=cands, open_symbols=set())

    selected_strats = {c.strategy for c in selected}
    # A outranks B on same symbol by module priority.
    assert StrategyModule.STRAT_A_ORB in selected_strats
    assert StrategyModule.STRAT_B_VWAP_REV not in selected_strats
    # C vs D overlap in metals family conflict: C has higher priority.
    assert StrategyModule.STRAT_C_METALS_ORB in selected_strats
    assert StrategyModule.STRAT_D_PAIR not in selected_strats
    assert any(d.reason_code in {"SYMBOL_CONFLICT", "METALS_PAIR_CONFLICT"} for d in decisions if not d.accepted)


def test_open_symbol_gate_rejects_candidates() -> None:
    cands = [
        StrategyCandidate(strategy=StrategyModule.STRAT_C_METALS_ORB, family=Family.METALS, symbols=("MGC",), score=80.0),
    ]
    selected, decisions = resolve_strategy_conflicts(candidates=cands, open_symbols={"MGC"})
    assert selected == []
    assert decisions[0].accepted is False
    assert decisions[0].reason_code == "SYMBOL_ALREADY_OPEN"

