"""CLI for the corrected V4 live signal runner."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from bot_corrected_v4.config import CorrectedV4Config, load_corrected_v4_config
from bot_corrected_v4.live import run_live_signals
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.corrected_orchestrator import CorrectedSignalOrchestrator
from shared.alerts.telegram import TelegramNotifier

DEFAULT_DATABENTO_DATASET = "GLBX.MDP3"
DEFAULT_DATABENTO_SCHEMA = "ohlcv-1m"
DEFAULT_DATABENTO_STYPE_IN = "continuous"


@dataclass(frozen=True, slots=True)
class CorrectedV4Runtime:
    config: CorrectedV4Config
    orchestrator: CorrectedSignalOrchestrator
    instruments_by_symbol: dict[str, InstrumentMeta]
    out_dir: Path
    state_dir: Path
    notifier: TelegramNotifier


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
    parser = argparse.ArgumentParser(prog="corrected-v4", description="Corrected V4 live signal runner")
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate-config", help="Validate Corrected V4 configuration")
    validate.add_argument("--config-dir", default="configs/corrected_v4", help="Configuration directory")

    bootstrap = subparsers.add_parser("bootstrap", help="Initialize the Corrected V4 runtime")
    bootstrap.add_argument("--config-dir", default="configs/corrected_v4", help="Configuration directory")
    bootstrap.add_argument("--out", default="out/corrected_v4", help="Output directory")
    bootstrap.add_argument("--state-dir", default="state/corrected_v4", help="State directory")
    bootstrap.add_argument("--telegram-token", default=os.getenv("TELEGRAM_BOT_TOKEN"), help="Telegram bot token")
    bootstrap.add_argument("--telegram-chat-id", default=os.getenv("TELEGRAM_CHAT_ID"), help="Telegram chat id")
    bootstrap.add_argument(
        "--bot-alert-tag",
        default=os.getenv("BOT_ALERT_TAG"),
        help="Telegram prefix tag for Corrected V4 alerts",
    )

    signals = subparsers.add_parser("signals", help="Run the Corrected V4 live signal watcher")
    signals.add_argument("--config-dir", default="configs/corrected_v4", help="Configuration directory")
    signals.add_argument("--out", default="out/corrected_v4", help="Output directory")
    signals.add_argument("--state-dir", default="state/corrected_v4", help="State directory")
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
        help="Telegram prefix tag for Corrected V4 alerts",
    )

    return parser


def build_runtime(args: argparse.Namespace) -> CorrectedV4Runtime:
    config = load_corrected_v4_config(args.config_dir)
    out_dir = Path(args.out)
    state_dir = Path(args.state_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    notifier = TelegramNotifier(
        token=getattr(args, "telegram_token", None),
        chat_id=getattr(args, "telegram_chat_id", None),
        alert_tag=getattr(args, "bot_alert_tag", None) or config.alert_tag,
    )
    return CorrectedV4Runtime(
        config=config,
        orchestrator=config.build_orchestrator(),
        instruments_by_symbol=config.load_instruments(args.config_dir),
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
        runtime = build_runtime(
            argparse.Namespace(
                config_dir=args.config_dir,
                out="out/corrected_v4",
                state_dir="state/corrected_v4",
                telegram_token=None,
                telegram_chat_id=None,
                bot_alert_tag=None,
            )
        )
        assert runtime.config is not None
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
                orchestrator=runtime.orchestrator,
                instruments_by_symbol=runtime.instruments_by_symbol,
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


def _resolve_live_signal_settings(args: argparse.Namespace, config: CorrectedV4Config) -> LiveSignalSettings:
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
