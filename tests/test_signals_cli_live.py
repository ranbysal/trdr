from __future__ import annotations

import asyncio
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from bot_trader_v1.cli import _resolve_live_signal_settings
from bot_trader_v1.cli import main
from futures_bot.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
    DEFAULT_DATABENTO_SYMBOLS,
)
from futures_bot.config.loader import load_instruments
from futures_bot.core.enums import StrategyModule
from futures_bot.live.feed_models import FeedMessage
from futures_bot.live.live_runner import run_live_signals

ET = ZoneInfo("America/New_York")


def test_live_signal_settings_require_databento_and_telegram() -> None:
    args = Namespace(
        databento_api_key=None,
        databento_dataset=DEFAULT_DATABENTO_DATASET,
        databento_schema=DEFAULT_DATABENTO_SCHEMA,
        databento_stype_in=DEFAULT_DATABENTO_STYPE_IN,
        databento_symbols="YM.v.0,NQ.v.0",
        telegram_token=None,
        telegram_chat_id=None,
    )

    with pytest.raises(
        ValueError,
        match="DATABENTO_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID",
    ):
        _resolve_live_signal_settings(args)


def test_live_signal_settings_validate_bars_only_continuous_env() -> None:
    args = Namespace(
        databento_api_key="db-key",
        databento_dataset=DEFAULT_DATABENTO_DATASET,
        databento_schema=DEFAULT_DATABENTO_SCHEMA,
        databento_stype_in=DEFAULT_DATABENTO_STYPE_IN,
        databento_symbols="",
        telegram_token="tg-token",
        telegram_chat_id="12345",
    )

    with pytest.raises(ValueError, match="DATABENTO_SYMBOLS"):
        _resolve_live_signal_settings(args)


def test_signals_cli_starts_live_signal_runner_without_execution(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    async def fake_run_live_signals(**kwargs):
        captured.update(kwargs)

    monkeypatch.setenv("DATABENTO_API_KEY", "db-key")
    monkeypatch.setenv("DATABENTO_DATASET", DEFAULT_DATABENTO_DATASET)
    monkeypatch.setenv("DATABENTO_SCHEMA", DEFAULT_DATABENTO_SCHEMA)
    monkeypatch.setenv("DATABENTO_STYPE_IN", DEFAULT_DATABENTO_STYPE_IN)
    monkeypatch.setenv("DATABENTO_SYMBOLS", ",".join(DEFAULT_DATABENTO_SYMBOLS))
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tg-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setattr("bot_trader_v1.cli.run_live_signals", fake_run_live_signals)

    rc = main(
        [
            "signals",
            "--config-dir",
            "configs/trader_v1",
            "--out",
            str(tmp_path / "signals_live"),
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert rc == 0
    assert captured["databento_api_key"] == "db-key"
    assert captured["databento_dataset"] == DEFAULT_DATABENTO_DATASET
    assert captured["databento_schema"] == DEFAULT_DATABENTO_SCHEMA
    assert captured["databento_stype_in"] == DEFAULT_DATABENTO_STYPE_IN
    assert captured["databento_symbols"] == DEFAULT_DATABENTO_SYMBOLS
    assert captured["state_dir"] == str(tmp_path / "state")
    notifier = captured["notifier"]
    assert getattr(notifier, "enabled") is True
    assert notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[TRADER-V1] ")


def test_signal_runner_uses_bars_only_continuous_databento_defaults(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, object] = {}

    class FakeDatabentoClient:
        quote_schema_enabled = False

        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def messages(self):
            if False:
                yield

    async def scenario() -> None:
        monkeypatch.setattr("futures_bot.live.live_runner.DatabentoLiveClient", FakeDatabentoClient)
        await run_live_signals(
            out_dir=tmp_path / "bars_only_live",
            instruments_by_symbol=load_instruments("configs"),
            enabled_strategies={StrategyModule.STRAT_A_ORB},
            databento_api_key="db-key",
            max_messages=0,
        )

    asyncio.run(scenario())
    assert captured["dataset"] == DEFAULT_DATABENTO_DATASET
    assert captured["schema"] == DEFAULT_DATABENTO_SCHEMA
    assert captured["stype_in"] == DEFAULT_DATABENTO_STYPE_IN
    assert captured["symbols"] == DEFAULT_DATABENTO_SYMBOLS


def test_signal_runner_starts_in_signal_only_bars_mode(tmp_path: Path) -> None:
    class FakeFeedClient:
        quote_schema_enabled = False

        def __init__(self) -> None:
            start = datetime(2026, 1, 12, 9, 30, tzinfo=ET)
            self._messages = [
                FeedMessage(
                    type="bar_1m",
                    timestamp_et=start,
                    symbol="NQ",
                    payload={"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 100.0},
                ),
                FeedMessage(
                    type="bar_1m",
                    timestamp_et=start + timedelta(minutes=1),
                    symbol="NQ",
                    payload={"open": 10.5, "high": 11.5, "low": 10.0, "close": 11.0, "volume": 120.0},
                ),
            ]

        async def start(self) -> None:
            return None

        async def stop(self) -> None:
            return None

        async def messages(self):
            for message in self._messages:
                yield message

    async def scenario() -> None:
        out_dir = tmp_path / "bars_only_live"
        await run_live_signals(
            out_dir=out_dir,
            instruments_by_symbol=load_instruments("configs"),
            enabled_strategies={StrategyModule.STRAT_A_ORB},
            feed_client=FakeFeedClient(),
            max_messages=2,
        )
        assert out_dir.exists()

    asyncio.run(scenario())
