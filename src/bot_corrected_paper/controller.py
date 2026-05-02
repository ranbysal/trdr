"""Read-only command controller for corrected paper visibility."""

from __future__ import annotations

from pathlib import Path

from bot_corrected_paper.config import load_corrected_paper_config
from bot_corrected_paper.engine import CorrectedPaperEngine, CorrectedPaperPosition, CorrectedPaperTrade


class CorrectedPaperCommandController:
    def __init__(self, *, config_dir: str | Path = "configs/corrected_paper") -> None:
        self._engine = CorrectedPaperEngine(config=load_corrected_paper_config(config_dir))

    def handle_command(self, text: str) -> str | None:
        parts = text.strip().split()
        if not parts:
            return None
        command = parts[0].lower()
        if command == "/corrected_paper_open":
            return self._format_open(self._engine.open_positions())
        if command == "/corrected_paper_recent":
            limit = _parse_limit(parts[1] if len(parts) > 1 else "10")
            return self._format_recent(self._engine.recent_trades(limit=limit))
        if command == "/corrected_paper_pnl":
            summary = self._engine.summary()
            return (
                "[CORR-PAPER] Paper PnL\n"
                f"realized={summary['realized_pnl']:.2f} | open_positions={summary['open_positions']} | "
                f"closed_trades={summary['closed_trades']} | win/loss={summary['wins']}/{summary['losses']}"
            )
        if command == "/corrected_paper_stats":
            return self._format_stats(self._engine.summary()["instrument_stats"])
        return None

    def _format_open(self, positions: tuple[CorrectedPaperPosition, ...]) -> str:
        if not positions:
            return "[CORR-PAPER] Open Positions\nNo open corrected paper positions."
        lines = ["[CORR-PAPER] Open Positions"]
        for position in positions:
            lines.append(
                f"{position.position_id} | {position.symbol} {position.direction} | qty={position.quantity} | "
                f"entry={position.entry_price:.2f} stop={position.stop_price:.2f} tp1={position.tp1_price:.2f} | "
                f"opened={position.opened_at_et}"
            )
        return "\n".join(lines)

    def _format_recent(self, trades: tuple[CorrectedPaperTrade, ...]) -> str:
        if not trades:
            return "[CORR-PAPER] Recent Trades\nNo closed corrected paper trades."
        lines = ["[CORR-PAPER] Recent Trades"]
        for trade in trades:
            lines.append(
                f"{trade.position_id} | {trade.symbol} {trade.direction} | outcome={trade.closure_outcome} | "
                f"pnl={trade.realized_pnl:.2f} | win_loss={trade.win_loss} | closed={trade.closed_at_et}"
            )
        return "\n".join(lines)

    def _format_stats(self, instrument_stats: object) -> str:
        if not isinstance(instrument_stats, dict) or not instrument_stats:
            return "[CORR-PAPER] Basic Stats\nNo corrected paper stats yet."
        lines = ["[CORR-PAPER] Basic Stats"]
        for symbol, stats in sorted(instrument_stats.items()):
            if not isinstance(stats, dict):
                continue
            lines.append(
                f"{symbol}: realized={float(stats.get('realized_pnl', 0.0)):.2f} | "
                f"opened={int(stats.get('opened', 0))} | closed={int(stats.get('closed', 0))} | "
                f"win/loss={int(stats.get('wins', 0))}/{int(stats.get('losses', 0))}"
            )
        return "\n".join(lines)


def _parse_limit(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError:
        return 10
    return max(1, min(value, 100))
