"""CLI scaffold for the Prop V2 bot."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from bot_prop_v2.config import PropV2Config, load_prop_v2_config
from bot_prop_v2.live import run_live_signals
from bot_prop_v2.pipeline import PropV2Pipeline, build_pipeline
from shared.alerts.telegram import TelegramNotifier
from shared.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
)


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


@dataclass(frozen=True, slots=True)
class LiveSignalSettings:
    databento_api_key: str
    databento_dataset: str
    databento_schema: str
    databento_stype_in: str
    databento_symbols: tuple[str, ...]
    telegram_token: str
    telegram_chat_id: str


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

    signals = subparsers.add_parser("signals", help="Run the Bot 2 live signal watcher and Telegram alerts")
    signals.add_argument("--config-dir", default="configs/prop_v2", help="Configuration directory")
    signals.add_argument("--out", default="out/prop_v2", help="Output directory")
    signals.add_argument("--state-dir", default="state/prop_v2", help="State directory")
    signals.add_argument(
        "--databento-api-key",
        default=os.getenv("DATABENTO_API_KEY"),
        help="Databento API key",
    )
    signals.add_argument(
        "--databento-dataset",
        default=os.getenv("DATABENTO_DATASET", DEFAULT_DATABENTO_DATASET),
        help="Databento dataset",
    )
    signals.add_argument(
        "--databento-schema",
        default=os.getenv("DATABENTO_SCHEMA", DEFAULT_DATABENTO_SCHEMA),
        help="Databento schema",
    )
    signals.add_argument(
        "--databento-stype-in",
        default=os.getenv("DATABENTO_STYPE_IN", DEFAULT_DATABENTO_STYPE_IN),
        help="Databento input symbology type",
    )
    signals.add_argument(
        "--databento-symbols",
        default=os.getenv("DATABENTO_SYMBOLS"),
        help="Comma-separated Databento live symbols",
    )
    signals.add_argument("--telegram-token", default=os.getenv("TELEGRAM_BOT_TOKEN"), help="Telegram bot token")
    signals.add_argument("--telegram-chat-id", default=os.getenv("TELEGRAM_CHAT_ID"), help="Telegram chat id")
    signals.add_argument(
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
    if args.command == "signals":
        runtime = build_runtime(args)
        try:
            live_settings = _resolve_live_signal_settings(args, runtime.config)
        except ValueError as exc:
            parser.error(str(exc))
        asyncio.run(
            run_live_signals(
                config=runtime.config,
                engine=runtime.engine,
                out_dir=runtime.out_dir,
                state_dir=runtime.state_dir,
                notifier=runtime.notifier,
                databento_api_key=live_settings.databento_api_key,
                databento_dataset=live_settings.databento_dataset,
                databento_schema=live_settings.databento_schema,
                databento_stype_in=live_settings.databento_stype_in,
                databento_symbols=live_settings.databento_symbols,
            )
        )
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


def _resolve_live_signal_settings(args: argparse.Namespace, config: PropV2Config) -> LiveSignalSettings:
    missing: list[str] = []
    if not getattr(args, "databento_api_key", None):
        missing.append("DATABENTO_API_KEY")
    if not getattr(args, "databento_dataset", None):
        missing.append("DATABENTO_DATASET")
    if not getattr(args, "databento_schema", None):
        missing.append("DATABENTO_SCHEMA")
    if not getattr(args, "databento_stype_in", None):
        missing.append("DATABENTO_STYPE_IN")
    if not getattr(args, "telegram_token", None):
        missing.append("TELEGRAM_BOT_TOKEN")
    if not getattr(args, "telegram_chat_id", None):
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"live signals require environment variables or flags for: {joined}")

    raw_symbols = getattr(args, "databento_symbols", None)
    if raw_symbols:
        symbols = tuple(symbol.strip() for symbol in str(raw_symbols).split(",") if symbol.strip())
    else:
        symbols = config.databento_symbols
    if not symbols:
        raise ValueError("live signals require at least one Databento symbol")
    if str(args.databento_schema) != DEFAULT_DATABENTO_SCHEMA:
        raise ValueError(f"live signals currently support only Databento schema {DEFAULT_DATABENTO_SCHEMA}")
    if str(args.databento_stype_in) != DEFAULT_DATABENTO_STYPE_IN:
        raise ValueError(f"live signals currently require Databento stype_in={DEFAULT_DATABENTO_STYPE_IN}")

    return LiveSignalSettings(
        databento_api_key=args.databento_api_key,
        databento_dataset=args.databento_dataset,
        databento_schema=args.databento_schema,
        databento_stype_in=args.databento_stype_in,
        databento_symbols=symbols,
        telegram_token=args.telegram_token,
        telegram_chat_id=args.telegram_chat_id,
    )


if __name__ == "__main__":
    raise SystemExit(main())
