from __future__ import annotations

from futures_bot.core.enums import Family
from futures_bot.risk.portfolio_caps import PortfolioCapsManager


def test_family_cap_rejection() -> None:
    caps = PortfolioCapsManager(equity=100_000.0)
    caps.record_open_position(family=Family.EQUITIES, symbol="NQ", risk_dollars=500.0)

    decision = caps.check_new_position(family=Family.EQUITIES, symbol="YM", proposed_risk_dollars=300.0)

    assert decision.approved is False
    assert decision.reason_code == "FAMILY_OPEN_RISK_CAP"


def test_total_cap_rejection() -> None:
    caps = PortfolioCapsManager(equity=100_000.0)
    caps.record_open_position(family=Family.EQUITIES, symbol="NQ", risk_dollars=700.0)
    caps.record_open_position(family=Family.METALS, symbol="MGC", risk_dollars=450.0)

    decision = caps.check_new_position(family=Family.METALS, symbol="SIL", proposed_risk_dollars=100.0)

    assert decision.approved is False
    assert decision.reason_code == "TOTAL_OPEN_RISK_CAP"


def test_symbol_position_cap_rejection() -> None:
    caps = PortfolioCapsManager(equity=100_000.0)
    caps.record_open_position(family=Family.EQUITIES, symbol="NQ", risk_dollars=300.0)

    decision = caps.check_new_position(family=Family.EQUITIES, symbol="NQ", proposed_risk_dollars=100.0)

    assert decision.approved is False
    assert decision.reason_code == "SYMBOL_POSITION_CAP"
