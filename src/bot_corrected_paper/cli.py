"""CLI for the isolated CORR-V4 corrected paper executor."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from bot_corrected_paper.config import CorrectedPaperConfig, load_corrected_paper_config
from bot_corrected_paper.engine import CorrectedPaperEngine


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="corrected-paper", description="CORR-V4 isolated paper executor")
    subparsers = parser.add_subparsers(dest="command")

    validate = subparsers.add_parser("validate-config", help="Validate corrected paper config")
    validate.add_argument("--config-dir", default="configs/corrected_paper", help="Configuration directory")

    bootstrap = subparsers.add_parser("bootstrap", help="Create corrected paper directories and reports")
    bootstrap.add_argument("--config-dir", default="configs/corrected_paper", help="Configuration directory")

    run_once = subparsers.add_parser("run-once", help="Process new corrected execution queue records once")
    run_once.add_argument("--config-dir", default="configs/corrected_paper", help="Configuration directory")
    run_once.add_argument("--source-queue", default=None, help="Override CORR-V4 execution queue path")
    run_once.add_argument("--state-path", default=None, help="Override corrected paper state path")
    run_once.add_argument("--journal-path", default=None, help="Override corrected paper journal path")
    run_once.add_argument("--events-path", default=None, help="Override corrected paper event path")
    run_once.add_argument("--reports-dir", default=None, help="Override corrected paper reports directory")

    watch = subparsers.add_parser("watch", help="Continuously process corrected execution queue records")
    watch.add_argument("--config-dir", default="configs/corrected_paper", help="Configuration directory")
    watch.add_argument("--source-queue", default=None, help="Override CORR-V4 execution queue path")
    watch.add_argument("--state-path", default=None, help="Override corrected paper state path")
    watch.add_argument("--journal-path", default=None, help="Override corrected paper journal path")
    watch.add_argument("--events-path", default=None, help="Override corrected paper event path")
    watch.add_argument("--reports-dir", default=None, help="Override corrected paper reports directory")
    watch.add_argument("--poll-interval", type=float, default=5.0, help="Queue polling interval in seconds")
    watch.add_argument("--max-iterations", type=int, default=None, help="Optional finite loop count for smoke tests")

    status = subparsers.add_parser("status", help="Print corrected paper status")
    status.add_argument("--config-dir", default="configs/corrected_paper", help="Configuration directory")
    status.add_argument("--source-queue", default=None, help="Override CORR-V4 execution queue path")
    status.add_argument("--state-path", default=None, help="Override corrected paper state path")
    status.add_argument("--journal-path", default=None, help="Override corrected paper journal path")
    status.add_argument("--events-path", default=None, help="Override corrected paper event path")
    status.add_argument("--reports-dir", default=None, help="Override corrected paper reports directory")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 2

    config = _apply_overrides(load_corrected_paper_config(args.config_dir), args)
    if args.command == "validate-config":
        print("corrected paper config ok")
        return 0

    engine = CorrectedPaperEngine(config=config)
    if args.command == "bootstrap":
        engine.write_reports()
        print(f"state_path={config.state_path}")
        print(f"journal_path={config.journal_path}")
        print(f"events_path={config.events_path}")
        print(f"reports_dir={config.reports_dir}")
        return 0
    if args.command == "run-once":
        result = engine.process_queue()
        print(
            "processed_records={processed} opened_positions={opened} closed_positions={closed} skipped_records={skipped}".format(
                processed=result.processed_records,
                opened=result.opened_positions,
                closed=result.closed_positions,
                skipped=result.skipped_records,
            )
        )
        return 0
    if args.command == "watch":
        return _watch(engine=engine, poll_interval=float(args.poll_interval), max_iterations=args.max_iterations)
    if args.command == "status":
        print(json.dumps(engine.summary(), indent=2, sort_keys=True))
        return 0
    parser.print_help()
    return 2


def _apply_overrides(config: CorrectedPaperConfig, args: argparse.Namespace) -> CorrectedPaperConfig:
    return CorrectedPaperConfig(
        enabled=config.enabled,
        source_queue_path=Path(args.source_queue) if getattr(args, "source_queue", None) else config.source_queue_path,
        state_path=Path(args.state_path) if getattr(args, "state_path", None) else config.state_path,
        journal_path=Path(args.journal_path) if getattr(args, "journal_path", None) else config.journal_path,
        events_path=Path(args.events_path) if getattr(args, "events_path", None) else config.events_path,
        reports_dir=Path(args.reports_dir) if getattr(args, "reports_dir", None) else config.reports_dir,
        default_point_values=config.default_point_values,
    )


def _watch(*, engine: CorrectedPaperEngine, poll_interval: float, max_iterations: int | None) -> int:
    if poll_interval <= 0.0:
        raise ValueError("poll_interval must be positive")
    engine.write_runtime_event("CORRECTED_PAPER_STARTUP", engine.summary())
    print("corrected_paper_startup", flush=True)
    iterations = 0
    while max_iterations is None or iterations < max_iterations:
        result = engine.process_queue()
        status = {
            "processed_records": result.processed_records,
            "opened_positions": result.opened_positions,
            "closed_positions": result.closed_positions,
            "skipped_records": result.skipped_records,
            **engine.summary(),
        }
        engine.write_runtime_event("CORRECTED_PAPER_STATUS", status)
        print(
            "corrected_paper_status processed_records={processed} opened_positions={opened} closed_positions={closed} skipped_records={skipped} open_positions={open_positions} closed_trades={closed_trades} realized_pnl={realized_pnl:.2f}".format(
                processed=result.processed_records,
                opened=result.opened_positions,
                closed=result.closed_positions,
                skipped=result.skipped_records,
                open_positions=status["open_positions"],
                closed_trades=status["closed_trades"],
                realized_pnl=float(status["realized_pnl"]),
            ),
            flush=True,
        )
        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            break
        time.sleep(poll_interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
