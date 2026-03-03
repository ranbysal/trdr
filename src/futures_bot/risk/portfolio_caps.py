"""Portfolio cap manager for open-risk and symbol concurrency limits."""

from __future__ import annotations

from collections import Counter

from futures_bot.core.enums import Family
from futures_bot.policy import cro_policy
from futures_bot.risk.models import OpenRiskEntry, RiskDecision


class PortfolioCapsManager:
    """Stateful manager enforcing family/total/symbol caps."""

    def __init__(
        self,
        *,
        equity: float,
        family_max_open_risk_pct: float = cro_policy.family_open_risk_cap,
        total_max_open_risk_pct: float = cro_policy.total_open_risk_cap,
        max_positions_per_symbol: int = cro_policy.max_positions_per_symbol,
    ) -> None:
        self._equity = float(equity)
        self._family_max_open_risk_pct = family_max_open_risk_pct
        self._total_max_open_risk_pct = total_max_open_risk_pct
        self._max_positions_per_symbol = max_positions_per_symbol
        self._open_entries: list[OpenRiskEntry] = []

    def check_new_position(self, *, family: Family, symbol: str, proposed_risk_dollars: float) -> RiskDecision:
        by_symbol = Counter(entry.symbol for entry in self._open_entries)
        if by_symbol[symbol] >= self._max_positions_per_symbol:
            return RiskDecision(approved=False, reason_code="SYMBOL_POSITION_CAP")

        family_open = sum(e.risk_dollars for e in self._open_entries if e.family is family)
        family_limit = self._equity * self._family_max_open_risk_pct
        if (family_open + proposed_risk_dollars) > family_limit:
            return RiskDecision(
                approved=False,
                reason_code="FAMILY_OPEN_RISK_CAP",
                details={"family_open": family_open, "family_limit": family_limit},
            )

        total_open = sum(e.risk_dollars for e in self._open_entries)
        total_limit = self._equity * self._total_max_open_risk_pct
        if (total_open + proposed_risk_dollars) > total_limit:
            return RiskDecision(
                approved=False,
                reason_code="TOTAL_OPEN_RISK_CAP",
                details={"total_open": total_open, "total_limit": total_limit},
            )

        return RiskDecision(approved=True, reason_code="APPROVED")

    def record_open_position(self, *, family: Family, symbol: str, risk_dollars: float) -> None:
        self._open_entries.append(OpenRiskEntry(symbol=symbol, family=family, risk_dollars=float(risk_dollars)))

    def record_close_position(self, *, symbol: str) -> None:
        for idx, entry in enumerate(self._open_entries):
            if entry.symbol == symbol:
                del self._open_entries[idx]
                break

    def has_open_position(self, *, symbol: str) -> bool:
        return any(entry.symbol == symbol for entry in self._open_entries)
