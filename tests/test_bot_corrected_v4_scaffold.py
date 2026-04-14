from __future__ import annotations

import importlib
import sys
from argparse import Namespace
from pathlib import Path

from bot_corrected_v4.cli import build_runtime, main


def test_corrected_v4_modules_import_cleanly() -> None:
    modules = [
        "bot_corrected_v4",
        "bot_corrected_v4.__main__",
        "bot_corrected_v4.cli",
        "bot_corrected_v4.config",
        "bot_corrected_v4.live",
    ]

    for module_name in modules:
        assert importlib.import_module(module_name) is not None


def test_shared_databento_adapter_import_does_not_boot_live_runner() -> None:
    modules_to_clear = [
        "shared.live.databento_adapter",
        "shared.live.databento_primitives",
        "futures_bot.live",
        "futures_bot.live.live_runner",
        "bot_exec_v3",
        "bot_exec_v3.live",
    ]
    for module_name in modules_to_clear:
        sys.modules.pop(module_name, None)

    adapter = importlib.import_module("shared.live.databento_adapter")

    assert adapter.DatabentoLiveClient is not None
    assert "futures_bot.live.live_runner" not in sys.modules
    assert "bot_exec_v3.live" not in sys.modules


def test_corrected_v4_bootstrap_initializes_independently(tmp_path: Path) -> None:
    runtime = build_runtime(
        Namespace(
            config_dir="configs/corrected_v4",
            out=str(tmp_path / "out"),
            state_dir=str(tmp_path / "state"),
            telegram_token=None,
            telegram_chat_id=None,
            bot_alert_tag="[CORR-V4]",
        )
    )

    assert runtime.config.architecture == "corrected_futures_orchestrator"
    assert runtime.out_dir.exists()
    assert runtime.state_dir.exists()
    assert runtime.notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[CORR-V4] ")
    assert {"NQ", "YM", "MGC"} <= set(runtime.instruments_by_symbol)


def test_corrected_v4_cli_bootstrap_returns_zero(tmp_path: Path) -> None:
    rc = main(
        [
            "bootstrap",
            "--config-dir",
            "configs/corrected_v4",
            "--out",
            str(tmp_path / "out"),
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert rc == 0


def test_corrected_v4_cli_signals_bootstraps_without_circular_import(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    async def fake_run_live_signals(**kwargs) -> None:
        captured.update(kwargs)

    monkeypatch.setenv("DATABENTO_API_KEY", "db-key")
    monkeypatch.setenv("DATABENTO_DATASET", "GLBX.MDP3")
    monkeypatch.setenv("DATABENTO_SCHEMA", "ohlcv-1m")
    monkeypatch.setenv("DATABENTO_STYPE_IN", "continuous")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tg-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")
    monkeypatch.setattr("bot_corrected_v4.cli.run_live_signals", fake_run_live_signals)

    rc = main(
        [
            "signals",
            "--config-dir",
            "configs/corrected_v4",
            "--out",
            str(tmp_path / "out"),
            "--state-dir",
            str(tmp_path / "state"),
        ]
    )

    assert rc == 0
    assert captured["databento_api_key"] == "db-key"
    assert captured["databento_dataset"] == "GLBX.MDP3"
    assert captured["databento_schema"] == "ohlcv-1m"
    assert captured["databento_stype_in"] == "continuous"
    assert captured["state_dir"] == tmp_path / "state"
