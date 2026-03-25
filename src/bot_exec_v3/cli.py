"""CLI scaffold for Bot 3 paper executor."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import ExecutorConfig, SizingConfig
from bot_exec_v3.live import run_live_signals
from bot_exec_v3.risk import PaperRiskSizer
from shared.alerts.telegram import TelegramNotifier
from shared.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
)


@dataclass(frozen=True, slots=True)
class ExecutorV3Runtime:
    config: ExecutorConfig
    journal: PaperTradeJournal
    executor: PaperExecutor
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
    parser = argparse.ArgumentParser(prog="executor-v3", description="Bot 3 paper executor scaffold")
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate-config", help="Validate Bot 3 config")
    validate.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

    bootstrap = subparsers.add_parser("bootstrap", help="Initialize Bot 3 paper journal and directories")
    bootstrap.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

    init_db = subparsers.add_parser("init-db", help="Initialize the Bot 3 SQLite journal")
    init_db.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

    signals = subparsers.add_parser("signals", help="Run the Bot 3 live paper execution watcher")
    signals.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")
    signals.add_argument("--out", default="out/executor_v3", help="Output directory")
    signals.add_argument("--state-dir", default="state/executor_v3", help="State directory")
    signals.add_argument(
        "--databento-api-key",
        default=os.getenv("DATABENTO_API_KEY"),
        help="Databento API key",
    )
    signals.add_argument(
        "--databento-dataset",
        default=os.getenv("DATABENTO_DATASET"),
        help="Databento dataset",
    )
    signals.add_argument(
        "--databento-schema",
        default=os.getenv("DATABENTO_SCHEMA"),
        help="Databento schema",
    )
    signals.add_argument(
        "--databento-stype-in",
        default=os.getenv("DATABENTO_STYPE_IN"),
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
        help="Telegram prefix tag for Bot 3 alerts",
    )

    return parser


def load_executor_v3_config(config_dir: str | Path) -> ExecutorConfig:
    config_path = Path(config_dir) / "bot.yaml"
    if not config_path.exists():
        raise ValueError(f"Bot 3 config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Bot 3 config root must be a mapping: {config_path}")
    bot = payload.get("bot")
    if not isinstance(bot, dict):
        raise ValueError("bot.yaml: key 'bot' must be a mapping")
    runtime = payload.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise ValueError("bot.yaml: key 'runtime' must be a mapping when present")
    sizing = payload.get("sizing") or {}
    if not isinstance(sizing, dict):
        raise ValueError("bot.yaml: key 'sizing' must be a mapping when present")
    contracts_by_instrument = sizing.get("contracts_by_instrument") or {}
    point_value_by_instrument = sizing.get("point_value_by_instrument") or {}
    raw_symbols = runtime.get("databento_symbols", ["NQ.v.0", "YM.v.0", "GC.v.0", "SI.v.0"])
    if not isinstance(contracts_by_instrument, dict):
        raise ValueError("bot.yaml: contracts_by_instrument must be a mapping when present")
    if not isinstance(point_value_by_instrument, dict):
        raise ValueError("bot.yaml: point_value_by_instrument must be a mapping when present")
    if isinstance(raw_symbols, str):
        databento_symbols = tuple(symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip())
    elif isinstance(raw_symbols, list):
        databento_symbols = tuple(str(symbol).strip() for symbol in raw_symbols if str(symbol).strip())
    else:
        raise ValueError("bot.yaml: databento_symbols must be a string or list when present")
    config = ExecutorConfig(
        enabled=bool(bot.get("enabled", True)),
        source_bot=str(bot.get("source_bot", "prop_v2")),
        signal_queue_path=Path(str(bot.get("signal_queue_path", "out/prop_v2/execution_signals.ndjson"))),
        sqlite_path=Path(str(bot.get("sqlite_path", "state/executor_v3/paper_ledger.db"))),
        freshness_seconds=int(bot.get("freshness_seconds", 180)),
        paper_mode=bool(bot.get("paper_mode", True)),
        sizing=SizingConfig(
            default_contracts=int(sizing.get("default_contracts", 1)),
            contracts_by_instrument={str(key).upper(): int(value) for key, value in contracts_by_instrument.items()},
            risk_per_trade_percent=(
                None
                if sizing.get("risk_per_trade_percent") in {None, ""}
                else float(sizing["risk_per_trade_percent"])
            ),
            account_size=(
                None
                if sizing.get("account_size") in {None, ""}
                else float(sizing["account_size"])
            ),
            point_value_by_instrument={
                str(key).upper(): float(value) for key, value in point_value_by_instrument.items()
            },
        ),
        alert_tag=str(bot.get("alert_tag", "[EXEC-V3]")),
        dataset=str(bot.get("dataset", DEFAULT_DATABENTO_DATASET)),
        schema=str(bot.get("schema", DEFAULT_DATABENTO_SCHEMA)),
        stype_in=str(bot.get("stype_in", DEFAULT_DATABENTO_STYPE_IN)),
        databento_symbols=databento_symbols,
        heartbeat_interval_hours=float(runtime.get("heartbeat_interval_hours", 4.0)),
        bars_stale_after_s=float(runtime.get("bars_stale_after_s", 180.0)),
    )
    if not config.paper_mode:
        raise ValueError("Bot 3 only supports paper_mode=true in this pass")
    if config.sizing.default_contracts <= 0:
        raise ValueError("default_contracts must be positive")
    if config.freshness_seconds <= 0:
        raise ValueError("freshness_seconds must be positive")
    if not config.databento_symbols:
        raise ValueError("databento_symbols must include at least one symbol")
    return config


def build_runtime(args: argparse.Namespace) -> ExecutorV3Runtime:
    config = load_executor_v3_config(args.config_dir)
    out_dir = Path(getattr(args, "out", "out/executor_v3"))
    state_dir = Path(getattr(args, "state_dir", "state/executor_v3"))
    out_dir.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = (
        config.sqlite_path
        if config.sqlite_path.is_absolute()
        else state_dir / config.sqlite_path.name
    )
    queue_path = (
        config.signal_queue_path
        if config.signal_queue_path.is_absolute()
        else Path(config.signal_queue_path)
    )
    config = ExecutorConfig(
        enabled=config.enabled,
        source_bot=config.source_bot,
        signal_queue_path=queue_path,
        sqlite_path=sqlite_path,
        freshness_seconds=config.freshness_seconds,
        paper_mode=config.paper_mode,
        sizing=config.sizing,
        alert_tag=config.alert_tag,
        dataset=config.dataset,
        schema=config.schema,
        stype_in=config.stype_in,
        databento_symbols=config.databento_symbols,
        heartbeat_interval_hours=config.heartbeat_interval_hours,
        bars_stale_after_s=config.bars_stale_after_s,
    )
    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    config.signal_queue_path.parent.mkdir(parents=True, exist_ok=True)
    journal = PaperTradeJournal(config.sqlite_path)
    risk_sizer = PaperRiskSizer(config.sizing)
    executor = PaperExecutor(config=config, journal=journal, risk_sizer=risk_sizer)
    notifier = TelegramNotifier(
        token=getattr(args, "telegram_token", None),
        chat_id=getattr(args, "telegram_chat_id", None),
        alert_tag=getattr(args, "bot_alert_tag", None) or config.alert_tag,
    )
    return ExecutorV3Runtime(
        config=config,
        journal=journal,
        executor=executor,
        out_dir=out_dir,
        state_dir=state_dir,
        notifier=notifier,
    )


def _resolve_live_signal_settings(args: argparse.Namespace, config: ExecutorConfig) -> LiveSignalSettings:
    missing: list[str] = []
    databento_api_key = getattr(args, "databento_api_key", None)
    databento_dataset = getattr(args, "databento_dataset", None) or config.dataset
    databento_schema = getattr(args, "databento_schema", None) or config.schema
    databento_stype_in = getattr(args, "databento_stype_in", None) or config.stype_in
    telegram_token = getattr(args, "telegram_token", None)
    telegram_chat_id = getattr(args, "telegram_chat_id", None)

    if not databento_api_key:
        missing.append("DATABENTO_API_KEY")
    if not databento_dataset:
        missing.append("DATABENTO_DATASET")
    if not databento_schema:
        missing.append("DATABENTO_SCHEMA")
    if not databento_stype_in:
        missing.append("DATABENTO_STYPE_IN")
    if not telegram_token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not telegram_chat_id:
        missing.append("TELEGRAM_CHAT_ID")
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"live signals require environment variables or flags for: {joined}")

    raw_symbols = getattr(args, "databento_symbols", None)
    if raw_symbols:
        databento_symbols = tuple(symbol.strip() for symbol in str(raw_symbols).split(",") if symbol.strip())
    else:
        databento_symbols = config.databento_symbols
    if not databento_symbols:
        raise ValueError("live signals require at least one Databento symbol")
    if databento_schema != DEFAULT_DATABENTO_SCHEMA:
        raise ValueError(f"live signals currently support only Databento schema {DEFAULT_DATABENTO_SCHEMA}")
    if databento_stype_in != DEFAULT_DATABENTO_STYPE_IN:
        raise ValueError(f"live signals currently require Databento stype_in={DEFAULT_DATABENTO_STYPE_IN}")

    return LiveSignalSettings(
        databento_api_key=databento_api_key,
        databento_dataset=databento_dataset,
        databento_schema=databento_schema,
        databento_stype_in=databento_stype_in,
        databento_symbols=databento_symbols,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    if args.command == "validate-config":
        load_executor_v3_config(args.config_dir)
        return 0
    if args.command in {"bootstrap", "init-db"}:
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
                executor=runtime.executor,
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


if __name__ == "__main__":
    raise SystemExit(main())
