"""Daily roll-map store with 17:00 ET immutability windows."""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from futures_bot.data.integrity import IntegrityError

ET = ZoneInfo("America/New_York")


class RollMapStore:
    """Stores root->active contract mappings by 17:00 ET roll windows."""

    def __init__(self) -> None:
        self._daily_maps: dict[datetime, dict[str, str]] = {}

    def set_daily_map(self, *, generated_at_et: datetime, mapping: dict[str, str]) -> None:
        ts = _to_et(generated_at_et)
        if (ts.hour, ts.minute, ts.second, ts.microsecond) != (17, 0, 0, 0):
            raise IntegrityError("Roll map generation time must be exactly 17:00:00 ET")

        existing = self._daily_maps.get(ts)
        if existing is None:
            self._daily_maps[ts] = dict(mapping)
            return

        if existing != mapping:
            raise IntegrityError("Roll map is immutable within the active 17:00 ET window")

    def trade_eligibility(
        self,
        *,
        at_et: datetime,
        root_symbol: str,
        contract_symbol: str,
    ) -> tuple[bool, str | None]:
        window_start = active_window_start(at_et)
        mapping = self._daily_maps.get(window_start)
        if mapping is None:
            return False, "ROLL_INACTIVE"

        active_contract = mapping.get(root_symbol)
        if active_contract != contract_symbol:
            return False, "ROLL_INACTIVE"

        return True, None


def active_window_start(at_et: datetime) -> datetime:
    ts = _to_et(at_et)
    today_roll = ts.replace(hour=17, minute=0, second=0, microsecond=0)
    if ts >= today_roll:
        return today_roll
    return today_roll - timedelta(days=1)


def _to_et(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=ET)
    return ts.astimezone(ET)
