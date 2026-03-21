"""CLI for the Trader V1 bot."""

from __future__ import annotations

import argparse
import asyncio
import os
from collections.abc import Sequence
from dataclasses import dataclass

from bot_trader_v1.config import load_all_configs, load_instruments
from bot_trader_v1.live import run_live_signals
from bot_trader_v1.pipeline import run_multistrategy_signal_loop
from bot_trader_v1.policy import cro_policy
from futures_bot.alerts.telegram import TelegramNotifier
from futures_bot.backtest.replay_runner import run_replay_backtest
from futures_bot.config.policy_guard import enforce_policy_guard
from futures_bot.core.enums import StrategyModule
from futures_bot.live.databento_adapter import (
    DEFAULT_DATABENTO_DATASET,
    DEFAULT_DATABENTO_SCHEMA,
    DEFAULT_DATABENTO_STYPE_IN,
    DEFAULT_DATABENTO_SYMBOLS,
)


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
    parser = argparse.ArgumentParser(prog="trader-v1", description="Trader V1 bot CLI")
    subparsers = parser.add_subparsers(dest="command")

    backtest = subparsers.add_parser("backtest", help="Run deterministic historical replay backtest")
    backtest.add_argument("--config-dir", default="configs/trader_v1", help="Configuration directory")
    backtest.add_argument("--data", required=False, help="1m CSV input for replay")
    backtest.add_argument("--out", default="out/trader_v1/backtest", help="Output directory")
    backtest.add_argument(
        "--strategies",
        default="A",
        help="Comma-separated strategy set from A,B,C,D",
    )
    backtest.add_argument(
        "--allow-policy-drift",
        action="store_true",
        help="Bypass strict config-vs-policy startup guard",
    )

    validate = subparsers.add_parser("validate-config", help="Validate configuration files")
    validate.add_argument("--config-dir", default="configs/trader_v1", help="Configuration directory")
    validate.add_argument(
        "--allow-policy-drift",
        action="store_true",
        help="Bypass strict config-vs-policy startup guard",
    )

    signals = subparsers.add_parser("signals", help="Run the live signal watcher and Telegram alerts")
    signals.add_argument("--data", required=False, help="CSV input for deterministic signal replay")
    signals.add_argument("--config-dir", default="configs/trader_v1", help="Configuration directory")
    signals.add_argument("--out", default="out/trader_v1", help="Output directory")
    signals.add_argument("--state-dir", default="state/trader_v1", help="State directory")
    signals.add_argument(
        "--strategies",
        default="A",
        help="Comma-separated strategy set from A,B,C,D",
    )
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
        default=os.getenv("DATABENTO_SYMBOLS", ",".join(DEFAULT_DATABENTO_SYMBOLS)),
        help="Comma-separated Databento live symbols",
    )
    signals.add_argument("--telegram-token", default=os.getenv("TELEGRAM_BOT_TOKEN"), help="Telegram bot token")
    signals.add_argument("--telegram-chat-id", default=os.getenv("TELEGRAM_CHAT_ID"), help="Telegram chat id")
    signals.add_argument(
        "--bot-alert-tag",
        default=os.getenv("BOT_ALERT_TAG", "[TRADER-V1]"),
        help="Telegram prefix tag for Bot 1 alerts",
    )
    signals.add_argument(
        "--allow-policy-drift",
        action="store_true",
        help="Bypass strict config-vs-policy startup guard",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    if args.command in {"validate-config", "backtest", "signals"}:
        configs = load_all_configs(args.config_dir)
        risk_cfg = configs.get("risk", {})
        enforce_policy_guard(
            risk_cfg,
            cro_policy,
            allow_policy_drift=args.allow_policy_drift,
        )
        if args.command == "backtest":
            if not args.data:
                parser.error("--data is required for backtest mode")
            try:
                enabled = _parse_strategies(args.strategies)
            except ValueError as exc:
                parser.error(str(exc))
            instruments = load_instruments(args.config_dir)
            try:
                run_replay_backtest(
                    data_path=args.data,
                    out_dir=args.out,
                    instruments_by_symbol=instruments,
                    enabled_strategies=enabled,
                    config_snapshot=configs,
                )
            except (FileNotFoundError, RuntimeError, ValueError) as exc:
                parser.exit(2, f"backtest failed: {exc}\n")
        if args.command == "signals":
            try:
                enabled = _parse_strategies(args.strategies)
            except ValueError as exc:
                parser.error(str(exc))
            instruments = load_instruments(args.config_dir)
            notifier = TelegramNotifier(
                token=args.telegram_token,
                chat_id=args.telegram_chat_id,
                alert_tag=args.bot_alert_tag,
            )
            if args.data:
                run_multistrategy_signal_loop(
                    data_path=args.data,
                    out_dir=args.out,
                    state_dir=args.state_dir,
                    instruments_by_symbol=instruments,
                    enabled_strategies=enabled,
                    notifier=notifier,
                )
            else:
                try:
                    live_settings = _resolve_live_signal_settings(args)
                except ValueError as exc:
                    parser.error(str(exc))
                asyncio.run(
                    run_live_signals(
                        out_dir=args.out,
                        state_dir=args.state_dir,
                        instruments_by_symbol=instruments,
                        enabled_strategies=enabled,
                        notifier=notifier,
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


def _parse_strategies(raw: str) -> set[StrategyModule]:
    mapping = {
        "A": StrategyModule.STRAT_A_ORB,
        "B": StrategyModule.STRAT_B_VWAP_REV,
        "C": StrategyModule.STRAT_C_METALS_ORB,
        "D": StrategyModule.STRAT_D_PAIR,
    }
    enabled: set[StrategyModule] = set()
    for item in raw.split(","):
        key = item.strip().upper()
        if not key:
            continue
        if key not in mapping:
            raise ValueError(f"Unsupported strategy flag: {key}")
        enabled.add(mapping[key])
    if not enabled:
        raise ValueError("At least one strategy must be enabled")
    return enabled


def _resolve_live_signal_settings(args: argparse.Namespace) -> LiveSignalSettings:
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
    symbols = tuple(symbol.strip() for symbol in str(getattr(args, "databento_symbols", "")).split(",") if symbol.strip())
    if not symbols:
        raise ValueError("live signals require at least one Databento symbol via DATABENTO_SYMBOLS or --databento-symbols")
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

