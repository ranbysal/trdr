from __future__ import annotations

import importlib
from argparse import Namespace
from pathlib import Path

from bot_prop_v2.cli import build_runtime
from bot_prop_v2.cli import main as prop_v2_main
from futures_bot.cli import main as legacy_main


def test_shared_modules_import_cleanly() -> None:
    modules = [
        "shared.alerts.telegram",
        "shared.alerts.error_forwarder",
        "shared.alerts.eod_summary",
        "shared.runtime.schedule",
        "shared.runtime.state_store",
        "shared.runtime.stale_data",
        "shared.live.databento_adapter",
        "shared.live.feed_models",
    ]

    for module_name in modules:
        assert importlib.import_module(module_name) is not None


def test_prop_v2_scaffold_bootstrap_initializes_independently(tmp_path: Path) -> None:
    runtime = build_runtime(
        Namespace(
            config_dir="configs/prop_v2",
            out=str(tmp_path / "out"),
            state_dir=str(tmp_path / "state"),
            telegram_token=None,
            telegram_chat_id=None,
            bot_alert_tag="[PROP-V2]",
        )
    )

    assert runtime.config.architecture == "atr_normalized_smc"
    assert runtime.out_dir.exists()
    assert runtime.state_dir.exists()
    assert runtime.notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[PROP-V2] ")


def test_prop_v2_cli_bootstrap_returns_zero(tmp_path: Path) -> None:
    rc = prop_v2_main(
        [
            "bootstrap",
            "--config-dir",
            "configs/prop_v2",
            "--out",
            str(tmp_path / "out"),
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert rc == 0


def test_legacy_futures_bot_entrypoint_still_routes_to_trader_v1(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    async def fake_run_live_signals(**kwargs):
        captured.update(kwargs)

    monkeypatch.setenv("DATABENTO_API_KEY", "db-key")
    monkeypatch.setenv("DATABENTO_DATASET", "GLBX.MDP3")
    monkeypatch.setenv("DATABENTO_SCHEMA", "ohlcv-1m")
    monkeypatch.setenv("DATABENTO_STYPE_IN", "continuous")
    monkeypatch.setenv("DATABENTO_SYMBOLS", "YM.v.0,NQ.v.0")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tg-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setattr("bot_trader_v1.cli.run_live_signals", fake_run_live_signals)

    rc = legacy_main(
        [
            "signals",
            "--config-dir",
            "configs/trader_v1",
            "--out",
            str(tmp_path / "out"),
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert rc == 0
    notifier = captured["notifier"]
    assert notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[TRADER-V1] ")
