"""Command-line interface for futures_bot."""

from __future__ import annotations

import argparse
from collections.abc import Sequence


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="futures-bot", description="Futures bot CLI scaffold")
    subparsers = parser.add_subparsers(dest="command")

    backtest = subparsers.add_parser("backtest", help="Run historical backtest (scaffold)")
    backtest.add_argument("--config-dir", default="configs", help="Configuration directory")

    paper = subparsers.add_parser("paper", help="Run paper trading loop (scaffold)")
    paper.add_argument("--config-dir", default="configs", help="Configuration directory")

    validate = subparsers.add_parser("validate-config", help="Validate configuration files")
    validate.add_argument("--config-dir", default="configs", help="Configuration directory")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "validate-config":
        return 0

    if args.command in {"backtest", "paper"}:
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
