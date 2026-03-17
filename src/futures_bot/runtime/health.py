"""Runtime status tracking for the live signal bot."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.runtime.schedule import in_daily_halt, market_is_open

ET = ZoneInfo("America/New_York")


@dataclass(slots=True)
class RuntimeStatus:
    signals_active: bool
    market_open: bool
    in_daily_halt: bool
    feed_connected: bool
    last_bar_timestamp: str | None
    active_ideas: int
    strategies_enabled: list[str]
    output_path: str


class RuntimeHealth:
    def __init__(
        self,
        *,
        out_dir: str | Path,
        enabled_strategies: set[str],
        signals_active: bool = True,
    ) -> None:
        self._out_dir = Path(out_dir)
        self._enabled_strategies = sorted(enabled_strategies)
        self._signals_active = signals_active
        self._feed_connected = False
        self._last_bar_by_symbol: dict[str, datetime] = {}

    @property
    def signals_active(self) -> bool:
        return self._signals_active

    def set_signals_active(self, value: bool) -> None:
        self._signals_active = value

    def set_feed_connected(self, value: bool) -> None:
        self._feed_connected = value

    def mark_bar(self, symbol: str, ts: datetime) -> None:
        self._last_bar_by_symbol[symbol] = ts

    def last_bar_by_symbol(self) -> dict[str, datetime]:
        return dict(self._last_bar_by_symbol)

    def load_last_bars(self, payload: dict[str, datetime]) -> None:
        self._last_bar_by_symbol = dict(payload)

    def snapshot(self, *, now_et: datetime, active_ideas: int) -> RuntimeStatus:
        now_et = now_et.astimezone(ET)
        last_bar = max(self._last_bar_by_symbol.values(), default=None)
        return RuntimeStatus(
            signals_active=self._signals_active,
            market_open=market_is_open(now_et),
            in_daily_halt=in_daily_halt(now_et),
            feed_connected=self._feed_connected,
            last_bar_timestamp=last_bar.isoformat() if last_bar is not None else None,
            active_ideas=active_ideas,
            strategies_enabled=list(self._enabled_strategies),
            output_path=str(self._out_dir),
        )
