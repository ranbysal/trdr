"""CLI scaffold for Bot 3 paper executor."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import yaml

from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import ExecutorConfig, SizingConfig
from bot_exec_v3.risk import PaperRiskSizer


@dataclass(frozen=True, slots=True)
class ExecutorV3Runtime:
    config: ExecutorConfig
    journal: PaperTradeJournal
    executor: PaperExecutor


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="executor-v3", description="Bot 3 paper executor scaffold")
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate-config", help="Validate Bot 3 config")
    validate.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

    bootstrap = subparsers.add_parser("bootstrap", help="Initialize Bot 3 paper journal and directories")
    bootstrap.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

    init_db = subparsers.add_parser("init-db", help="Initialize the Bot 3 SQLite journal")
    init_db.add_argument("--config-dir", default="configs/executor_v3", help="Configuration directory")

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
    sizing = payload.get("sizing") or {}
    if not isinstance(sizing, dict):
        raise ValueError("bot.yaml: key 'sizing' must be a mapping when present")
    contracts_by_instrument = sizing.get("contracts_by_instrument") or {}
    point_value_by_instrument = sizing.get("point_value_by_instrument") or {}
    if not isinstance(contracts_by_instrument, dict):
        raise ValueError("bot.yaml: contracts_by_instrument must be a mapping when present")
    if not isinstance(point_value_by_instrument, dict):
        raise ValueError("bot.yaml: point_value_by_instrument must be a mapping when present")
    config = ExecutorConfig(
        enabled=bool(bot.get("enabled", True)),
        source_bot=str(bot.get("source_bot", "prop_v2")),
        signal_queue_path=Path(str(bot.get("signal_queue_path", "out/prop_v2/signal_queue.ndjson"))),
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
    )
    if not config.paper_mode:
        raise ValueError("Bot 3 only supports paper_mode=true in this pass")
    if config.sizing.default_contracts <= 0:
        raise ValueError("default_contracts must be positive")
    if config.freshness_seconds <= 0:
        raise ValueError("freshness_seconds must be positive")
    return config


def build_runtime(args: argparse.Namespace) -> ExecutorV3Runtime:
    config = load_executor_v3_config(args.config_dir)
    config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    config.signal_queue_path.parent.mkdir(parents=True, exist_ok=True)
    journal = PaperTradeJournal(config.sqlite_path)
    risk_sizer = PaperRiskSizer(config.sizing)
    executor = PaperExecutor(config=config, journal=journal, risk_sizer=risk_sizer)
    return ExecutorV3Runtime(config=config, journal=journal, executor=executor)


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
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
