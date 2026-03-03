"""Portfolio-level multi-strategy candidate orchestration and conflict resolution."""

from __future__ import annotations

from dataclasses import dataclass

from futures_bot.core.enums import Family, StrategyModule


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    strategy: StrategyModule
    family: Family
    symbols: tuple[str, ...]
    score: float


@dataclass(frozen=True, slots=True)
class CandidateDecision:
    accepted: bool
    reason_code: str
    candidate: StrategyCandidate


_PRIORITY: dict[StrategyModule, int] = {
    StrategyModule.STRAT_A_ORB: 100,
    StrategyModule.STRAT_B_VWAP_REV: 90,
    StrategyModule.STRAT_C_METALS_ORB: 80,
    StrategyModule.STRAT_D_PAIR: 70,
}


def resolve_strategy_conflicts(
    *,
    candidates: list[StrategyCandidate],
    open_symbols: set[str],
) -> tuple[list[StrategyCandidate], list[CandidateDecision]]:
    decisions: list[CandidateDecision] = []
    selected: list[StrategyCandidate] = []
    selected_symbols: set[str] = set()

    ordered = sorted(
        candidates,
        key=lambda c: (_PRIORITY.get(c.strategy, 0), c.score),
        reverse=True,
    )

    for cand in ordered:
        if any(sym in open_symbols for sym in cand.symbols):
            decisions.append(CandidateDecision(accepted=False, reason_code="SYMBOL_ALREADY_OPEN", candidate=cand))
            continue

        if any(sym in selected_symbols for sym in cand.symbols):
            decisions.append(CandidateDecision(accepted=False, reason_code="SYMBOL_CONFLICT", candidate=cand))
            continue

        if _metals_breakout_pair_conflict(candidate=cand, selected=selected):
            decisions.append(CandidateDecision(accepted=False, reason_code="METALS_PAIR_CONFLICT", candidate=cand))
            continue

        selected.append(cand)
        selected_symbols.update(cand.symbols)
        decisions.append(CandidateDecision(accepted=True, reason_code="APPROVED", candidate=cand))

    return selected, decisions


def _metals_breakout_pair_conflict(*, candidate: StrategyCandidate, selected: list[StrategyCandidate]) -> bool:
    for other in selected:
        if {candidate.strategy, other.strategy} == {StrategyModule.STRAT_C_METALS_ORB, StrategyModule.STRAT_D_PAIR}:
            if candidate.family is Family.METALS or other.family is Family.METALS:
                return True
    return False

