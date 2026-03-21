"""CLI scaffold for the Prop V2 bot."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from bot_prop_v2.config import PropV2Config, load_prop_v2_config
from bot_prop_v2.pipeline import PropV2Pipeline, build_pipeline
from shared.alerts.telegram import TelegramNotifier


@dataclass(frozen=True, slots=True)
class PropV2Runtime:
    config: PropV2Config
    pipeline: PropV2Pipeline
    out_dir: Path
    state_dir: Path
    notifier: TelegramNotifier

    @property
    def engine(self):
        return self.pipeline.engine


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="prop-v2", description="Prop V2 scaffold CLI")
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate-config", help="Validate Bot 2 scaffold config")
    validate.add_argument("--config-dir", default="configs/prop_v2", help="Configuration directory")

    bootstrap = subparsers.add_parser("bootstrap", help="Initialize the Bot 2 scaffold runtime")
    bootstrap.add_argument("--config-dir", default="configs/prop_v2", help="Configuration directory")
    bootstrap.add_argument("--out", default="out/prop_v2", help="Output directory")
    bootstrap.add_argument("--state-dir", default="state/prop_v2", help="State directory")
    bootstrap.add_argument("--telegram-token", default=os.getenv("TELEGRAM_BOT_TOKEN"), help="Telegram bot token")
    bootstrap.add_argument("--telegram-chat-id", default=os.getenv("TELEGRAM_CHAT_ID"), help="Telegram chat id")
    bootstrap.add_argument(
        "--bot-alert-tag",
        default=os.getenv("BOT_ALERT_TAG"),
        help="Telegram prefix tag for Bot 2 alerts",
    )

    return parser


def build_runtime(args: argparse.Namespace) -> PropV2Runtime:
    config = load_prop_v2_config(args.config_dir)
    out_dir = Path(args.out)
    state_dir = Path(args.state_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    pipeline = build_pipeline(config, out_dir=out_dir)
    notifier = TelegramNotifier(
        token=getattr(args, "telegram_token", None),
        chat_id=getattr(args, "telegram_chat_id", None),
        alert_tag=getattr(args, "bot_alert_tag", None) or config.alert_tag,
    )
    return PropV2Runtime(
        config=config,
        pipeline=pipeline,
        out_dir=out_dir,
        state_dir=state_dir,
        notifier=notifier,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "validate-config":
        load_prop_v2_config(args.config_dir)
        return 0
    if args.command == "bootstrap":
        build_runtime(args)
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
