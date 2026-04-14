from __future__ import annotations

import importlib
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
