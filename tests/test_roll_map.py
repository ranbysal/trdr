from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from futures_bot.data.integrity import IntegrityError
from futures_bot.data.roll_map import RollMapStore

ET = ZoneInfo("America/New_York")


def test_roll_map_immutable_within_window_and_active_intraday() -> None:
    store = RollMapStore()
    generated = datetime(2026, 1, 5, 17, 0, tzinfo=ET)

    store.set_daily_map(generated_at_et=generated, mapping={"ES": "ESH6"})

    eligible_1, code_1 = store.trade_eligibility(
        at_et=datetime(2026, 1, 6, 10, 0, tzinfo=ET),
        root_symbol="ES",
        contract_symbol="ESH6",
    )
    eligible_2, code_2 = store.trade_eligibility(
        at_et=datetime(2026, 1, 6, 15, 59, tzinfo=ET),
        root_symbol="ES",
        contract_symbol="ESH6",
    )

    assert eligible_1 is True
    assert code_1 is None
    assert eligible_2 is True
    assert code_2 is None

    with pytest.raises(IntegrityError, match="immutable"):
        store.set_daily_map(generated_at_et=generated, mapping={"ES": "ESM6"})


def test_roll_inactive_when_contract_not_active() -> None:
    store = RollMapStore()
    generated = datetime(2026, 1, 5, 17, 0, tzinfo=ET)
    store.set_daily_map(generated_at_et=generated, mapping={"GC": "GCG6"})

    eligible, code = store.trade_eligibility(
        at_et=datetime(2026, 1, 6, 11, 0, tzinfo=ET),
        root_symbol="ES",
        contract_symbol="ESH6",
    )

    assert eligible is False
    assert code == "ROLL_INACTIVE"
