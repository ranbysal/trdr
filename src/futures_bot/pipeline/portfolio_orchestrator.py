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
    slippage_est_ticks: float = 0.0


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
    max_new_positions: int | None = None,
) -> tuple[list[StrategyCandidate], list[CandidateDecision]]:
    decisions: list[CandidateDecision] = []
    selected: list[StrategyCandidate] = []
    selected_symbols: set[str] = set()

    ordered = sorted(candidates, key=lambda c: (c.score, -c.slippage_est_ticks), reverse=True)

    for cand in ordered:
        if max_new_positions is not None and len(selected) >= max_new_positions:
            decisions.append(CandidateDecision(accepted=False, reason_code="PORTFOLIO_CAP_REACHED", candidate=cand))
            continue

        if any(sym in open_symbols for sym in cand.symbols):
            decisions.append(CandidateDecision(accepted=False, reason_code="SYMBOL_ALREADY_OPEN", candidate=cand))
            continue

        conflicting = [existing for existing in selected if _is_conflict(candidate=cand, other=existing)]
        if conflicting:
            best_existing = conflicting[0]
            if _candidate_wins(left=cand, right=best_existing):
                selected.remove(best_existing)
                selected_symbols = {sym for item in selected for sym in item.symbols}
                reason = (
                    "METALS_PAIR_CONFLICT" if _is_metals_pair_conflict(cand, best_existing) else "SYMBOL_CONFLICT"
                )
                decisions.append(CandidateDecision(accepted=False, reason_code=reason, candidate=best_existing))
            else:
                reason = "METALS_PAIR_CONFLICT" if _is_metals_pair_conflict(cand, best_existing) else "SYMBOL_CONFLICT"
                decisions.append(CandidateDecision(accepted=False, reason_code=reason, candidate=cand))
                continue

        selected.append(cand)
        selected_symbols.update(cand.symbols)
        decisions.append(CandidateDecision(accepted=True, reason_code="APPROVED", candidate=cand))

    return selected, decisions


def _is_conflict(*, candidate: StrategyCandidate, other: StrategyCandidate) -> bool:
    return bool(set(candidate.symbols) & set(other.symbols)) or _is_metals_pair_conflict(candidate, other)


def _is_metals_pair_conflict(candidate: StrategyCandidate, other: StrategyCandidate) -> bool:
    if {candidate.strategy, other.strategy} != {StrategyModule.STRAT_C_METALS_ORB, StrategyModule.STRAT_D_PAIR}:
        return False
    return candidate.family is Family.METALS or other.family is Family.METALS


def _candidate_wins(*, left: StrategyCandidate, right: StrategyCandidate) -> bool:
    score_gap = abs(left.score - right.score)
    if score_gap <= 5.0:
        if left.slippage_est_ticks != right.slippage_est_ticks:
            return left.slippage_est_ticks < right.slippage_est_ticks
    if left.score != right.score:
        return left.score > right.score
    return _PRIORITY.get(left.strategy, 0) > _PRIORITY.get(right.strategy, 0)
