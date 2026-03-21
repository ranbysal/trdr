from __future__ import annotations

from argparse import Namespace
from pathlib import Path

from bot_prop_v2.cli import build_runtime, main as prop_v2_main
from bot_prop_v2.config import load_prop_v2_config
from bot_prop_v2.pipeline import SignalEngine, build_pipeline


def test_bot_prop_v2_pipeline_wraps_signal_engine(tmp_path: Path) -> None:
    config = load_prop_v2_config("configs/prop_v2")
    pipeline = build_pipeline(config, out_dir=tmp_path / "out")

    assert isinstance(pipeline.engine, SignalEngine)
    assert pipeline.log_dir.exists()
    assert pipeline.report_dir.exists()
    assert pipeline.describe() == "prop_v2:atr_normalized_smc:scaffold"


def test_bot_prop_v2_runtime_uses_prop_alert_tag_by_default(tmp_path: Path) -> None:
    runtime = build_runtime(
        Namespace(
            config_dir="configs/prop_v2",
            out=str(tmp_path / "out"),
            state_dir=str(tmp_path / "state"),
            telegram_token=None,
            telegram_chat_id=None,
            bot_alert_tag=None,
        )
    )

    assert runtime.engine is runtime.pipeline.engine
    assert runtime.notifier.prepare_text(text="<b>HEARTBEAT</b>").startswith("[PROP-V2] ")


def test_bot_prop_v2_cli_bootstrap_command_returns_zero(tmp_path: Path) -> None:
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
