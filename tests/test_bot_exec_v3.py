from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from bot_exec_v3.cli import load_executor_v3_config
from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import Direction, ExecutorConfig, MarketBar, SignalEvent, SizingConfig
from bot_exec_v3.risk import PaperRiskSizer

ET = ZoneInfo("America/New_York")


def _config(db_path: Path) -> ExecutorConfig:
    return ExecutorConfig(
        enabled=True,
        source_bot="prop_v2",
        signal_queue_path=Path("out/prop_v2/signal_queue.ndjson"),
        sqlite_path=db_path,
        freshness_seconds=180,
        paper_mode=True,
        sizing=SizingConfig(
            default_contracts=2,
            contracts_by_instrument={"NQ": 2},
            risk_per_trade_percent=None,
            account_size=150_000.0,
            point_value_by_instrument={"NQ": 1.0},
        ),
    )


def _executor(db_path: Path) -> PaperExecutor:
    config = _config(db_path)
    journal = PaperTradeJournal(config.sqlite_path)
    risk_sizer = PaperRiskSizer(config.sizing)
    return PaperExecutor(config=config, journal=journal, risk_sizer=risk_sizer)


def _signal(*, signal_id: str = "sig-1", formed_at: datetime | None = None) -> SignalEvent:
    formed = formed_at or datetime(2026, 3, 25, 9, 30, tzinfo=ET)
    return SignalEvent(
        signal_id=signal_id,
        source_bot="prop_v2",
        instrument="NQ",
        direction=Direction.LONG,
        setup_type="smc_breakout",
        session="ny_open",
        confluence=0.92,
        formed_timestamp_et=formed,
        entry=100.0,
        stop=95.0,
        tp1=105.0,
        tp2=110.0,
        tp3=115.0,
        notes="typed Bot 2 signal",
        freshness_seconds=180,
    )


def test_duplicate_signal_rejection(tmp_path: Path) -> None:
    db_path = tmp_path / "paper_ledger.db"
    executor = _executor(db_path)
    signal = _signal()
    received_at = datetime(2026, 3, 25, 9, 31, tzinfo=ET)

    first = executor.submit_signal(signal, received_at_et=received_at)
    second = executor.submit_signal(signal, received_at_et=received_at)

    assert first.accepted
    assert not second.accepted
    assert second.reason == "duplicate signal_id"

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    assert count == 1


def test_stale_signal_rejection(tmp_path: Path) -> None:
    db_path = tmp_path / "paper_ledger.db"
    executor = _executor(db_path)
    signal = _signal(formed_at=datetime(2026, 3, 25, 9, 20, tzinfo=ET))

    result = executor.submit_signal(signal, received_at_et=datetime(2026, 3, 25, 9, 31, tzinfo=ET))

    assert not result.accepted
    assert result.reason == "stale signal"

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT status, rejection_reason FROM signals WHERE signal_id = ?", (signal.signal_id,)).fetchone()
    assert row == ("rejected", "stale signal")


def test_sqlite_journal_creation(tmp_path: Path) -> None:
    db_path = tmp_path / "paper_ledger.db"
    PaperTradeJournal(db_path)

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        }
    assert tables == {"fills", "orders", "pnl_snapshots", "positions", "signals"}


def test_pending_order_creation(tmp_path: Path) -> None:
    db_path = tmp_path / "paper_ledger.db"
    executor = _executor(db_path)
    signal = _signal()

    result = executor.submit_signal(signal, received_at_et=datetime(2026, 3, 25, 9, 31, tzinfo=ET))

    assert result.accepted
    assert result.order_id == "ord_sig-1"
    assert result.position_size == 2

    with sqlite3.connect(db_path) as conn:
        order = conn.execute(
            "SELECT status, quantity, tp1_quantity, tp2_quantity, tp3_quantity FROM orders WHERE order_id = ?",
            (result.order_id,),
        ).fetchone()
    assert order == ("pending", 2, 1, 0, 1)


def test_basic_fill_and_close_lifecycle(tmp_path: Path) -> None:
    db_path = tmp_path / "paper_ledger.db"
    executor = _executor(db_path)
    signal = _signal()
    executor.submit_signal(signal, received_at_et=datetime(2026, 3, 25, 9, 31, tzinfo=ET))

    fill_result = executor.on_market_bar(
        MarketBar(
            instrument="NQ",
            timestamp_et=datetime(2026, 3, 25, 9, 32, tzinfo=ET),
            open=99.0,
            high=100.5,
            low=99.0,
            close=100.0,
        )
    )
    close_result = executor.on_market_bar(
        MarketBar(
            instrument="NQ",
            timestamp_et=datetime(2026, 3, 25, 9, 33, tzinfo=ET),
            open=100.0,
            high=116.0,
            low=99.5,
            close=115.0,
        )
    )

    assert fill_result.filled_order_ids == ("ord_sig-1",)
    assert close_result.closed_position_ids == ("pos_sig-1",)

    with sqlite3.connect(db_path) as conn:
        fills = conn.execute(
            "SELECT fill_type, quantity, price, realized_pnl FROM fills WHERE signal_id = ? ORDER BY fill_timestamp_et, fill_id",
            (signal.signal_id,),
        ).fetchall()
        position = conn.execute(
            "SELECT status, quantity_open, realized_pnl FROM positions WHERE position_id = ?",
            ("pos_sig-1",),
        ).fetchone()
        signal_status = conn.execute("SELECT status FROM signals WHERE signal_id = ?", (signal.signal_id,)).fetchone()
    assert fills == [
        ("entry", 2, 100.0, 0.0),
        ("tp1", 1, 105.0, 5.0),
        ("tp3", 1, 115.0, 15.0),
    ]
    assert position == ("closed", 0, 20.0)
    assert signal_status == ("position_closed",)


def test_executor_v3_config_defaults() -> None:
    config = load_executor_v3_config("configs/executor_v3")

    assert config.enabled is True
    assert config.source_bot == "prop_v2"
    assert config.sqlite_path == Path("state/executor_v3/paper_ledger.db")
