from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from futures_bot.alerts.telegram import TelegramNotifier
from futures_bot.core.enums import StrategyModule
from futures_bot.signals.models import SignalIdea
from futures_bot.signals.state import AlertStateManager


def test_alert_state_dedupes_and_tracks_updates(tmp_path: Path) -> None:
    manager = AlertStateManager(out_dir=tmp_path, notifier=TelegramNotifier())
    idea = SignalIdea(
        idea_id="strat_a_orb:NQ:BUY",
        strategy=StrategyModule.STRAT_A_ORB,
        symbol="NQ",
        symbol_display="NQ",
        side="BUY",
        entry_low=100.0,
        entry_high=100.5,
        stop_loss=99.0,
        tp1=101.0,
        tp2=102.0,
        invalidation="thesis fails below 99.00",
        partial_profit_guidance="trim into strength at 101.00",
        timestamp=datetime(2026, 1, 12, 10, 0),
        flatten_by=datetime(2026, 1, 12, 11, 30),
        regime="trend",
        confidence=0.9,
        strategy_context="strat_a_orb",
        last_price=100.2,
    )

    manager.register(idea)
    manager.register(idea)
    manager.process_market(
        ts=datetime(2026, 1, 12, 10, 1),
        symbol="NQ",
        high=100.4,
        low=100.1,
        close=100.3,
        regime="trend",
        confidence=0.9,
        latest_prices={"NQ": 100.3},
    )
    manager.process_market(
        ts=datetime(2026, 1, 12, 10, 2),
        symbol="NQ",
        high=101.2,
        low=100.2,
        close=101.0,
        regime="trend",
        confidence=0.9,
        latest_prices={"NQ": 101.0},
    )
    manager.process_market(
        ts=datetime(2026, 1, 12, 10, 3),
        symbol="NQ",
        high=102.1,
        low=101.4,
        close=102.1,
        regime="trend",
        confidence=0.95,
        latest_prices={"NQ": 102.1},
    )
    manager.process_market(
        ts=idea.flatten_by + timedelta(minutes=1),
        symbol="NQ",
        high=102.2,
        low=101.8,
        close=102.0,
        regime="trend",
        confidence=0.95,
        latest_prices={"NQ": 102.0},
    )
    manager.flush()

    events_path = tmp_path / "signal_events.ndjson"
    snapshot_path = tmp_path / "active_ideas.json"
    assert events_path.exists()
    assert snapshot_path.exists()

    events = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    states = [event["state"] for event in events]
    assert states.count("NEW_SIGNAL") == 1
    assert "ENTRY_ZONE_ACTIVE" in states
    assert "PARTIAL_TAKE_SUGGESTED" in states
    assert "STOP_TO_BREAKEVEN_SUGGESTED" in states
    assert "TP_EXTENSION_SUGGESTED" in states or "CLOSE_SIGNAL" in states
    assert json.loads(snapshot_path.read_text(encoding="utf-8")) == []
