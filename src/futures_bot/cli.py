"""Command-line interface for futures_bot."""

from __future__ import annotations

import asyncio
import argparse
from collections.abc import Sequence

from futures_bot.config.loader import load_all_configs
from futures_bot.config.loader import load_instruments
from futures_bot.backtest.replay_runner import run_replay_backtest
from futures_bot.config.policy_guard import enforce_policy_guard
from futures_bot.core.enums import StrategyModule
from futures_bot.live.live_runner import run_live_paper
from futures_bot.pipeline.multistrategy_paper import run_multistrategy_paper_loop
from futures_bot.policy import cro_policy


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="futures-bot", description="Futures bot CLI scaffold")
    subparsers = parser.add_subparsers(dest="command")

    backtest = subparsers.add_parser("backtest", help="Run historical backtest (scaffold)")
    backtest.add_argument("--config-dir", default="configs", help="Configuration directory")
    backtest.add_argument("--data", required=False, help="1m CSV input for replay")
    backtest.add_argument("--out", default="backtest_out", help="Output directory")
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

    paper = subparsers.add_parser("paper", help="Run paper trading loop (scaffold)")
    paper.add_argument("--config-dir", default="configs", help="Configuration directory")
    paper.add_argument("--data", required=False, help="CSV input for paper loop")
    paper.add_argument("--out", default="paper_out", help="Output directory")
    paper.add_argument(
        "--strategies",
        default="A",
        help="Comma-separated strategy set from A,B,C,D",
    )
    paper.add_argument(
        "--allow-policy-drift",
        action="store_true",
        help="Bypass strict config-vs-policy startup guard",
    )

    validate = subparsers.add_parser("validate-config", help="Validate configuration files")
    validate.add_argument("--config-dir", default="configs", help="Configuration directory")
    validate.add_argument(
        "--allow-policy-drift",
        action="store_true",
        help="Bypass strict config-vs-policy startup guard",
    )

    live = subparsers.add_parser("live", help="Run live websocket ingestion loop")
    live.add_argument("--ws-url", required=False, help="Websocket URL")
    live.add_argument("--config-dir", default="configs", help="Configuration directory")
    live.add_argument("--out", default="live_out", help="Output directory")
    live.add_argument(
        "--strategies",
        default="A",
        help="Comma-separated strategy set from A,B,C,D",
    )
    live.add_argument("--paper", action="store_true", help="Run paper execution only")
    live.add_argument(
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

    if args.command in {"validate-config", "backtest", "paper", "live"}:
        configs = load_all_configs(args.config_dir)
        risk_cfg = configs.get("risk", {})
        enforce_policy_guard(
            risk_cfg,
            cro_policy,
            allow_policy_drift=args.allow_policy_drift,
        )
        if args.command == "paper":
            if not args.data:
                parser.error("--data is required for paper mode")
            try:
                enabled = _parse_strategies(args.strategies)
            except ValueError as exc:
                parser.error(str(exc))
            instruments = load_instruments(args.config_dir)
            run_multistrategy_paper_loop(
                data_path=args.data,
                out_dir=args.out,
                instruments_by_symbol=instruments,
                enabled_strategies=enabled,
            )
        if args.command == "backtest":
            if not args.data:
                parser.error("--data is required for backtest mode")
            try:
                enabled = _parse_strategies(args.strategies)
            except ValueError as exc:
                parser.error(str(exc))
            instruments = load_instruments(args.config_dir)
            run_replay_backtest(
                data_path=args.data,
                out_dir=args.out,
                instruments_by_symbol=instruments,
                enabled_strategies=enabled,
            )
        if args.command == "live":
            if not args.ws_url:
                parser.error("--ws-url is required for live mode")
            if not args.paper:
                parser.error("--paper is required in live mode")
            try:
                enabled = _parse_strategies(args.strategies)
            except ValueError as exc:
                parser.error(str(exc))
            instruments = load_instruments(args.config_dir)
            asyncio.run(
                run_live_paper(
                    ws_url=args.ws_url,
                    out_dir=args.out,
                    instruments_by_symbol=instruments,
                    enabled_strategies=enabled,
                    paper=True,
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


if __name__ == "__main__":
    raise SystemExit(main())
