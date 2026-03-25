from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from bot_exec_v3.controller import PaperCommandController
from bot_exec_v3.executor import PaperExecutor
from bot_exec_v3.journal import PaperTradeJournal
from bot_exec_v3.models import Direction, ExecutorConfig, MarketBar, SignalEvent, SizingConfig
from bot_exec_v3.query import PaperTradeQueries
from bot_exec_v3.summary import PaperDailySummaryManager
from bot_exec_v3.risk import PaperRiskSizer
from futures_bot.alerts.telegram import TelegramDelivery, TelegramNotifier
from futures_bot.alerts.telegram_listener import TelegramCommandListener

ET = ZoneInfo("America/New_York")


class FakeTelegramNotifier(TelegramNotifier):
    def __init__(self) -> None:
        super().__init__(token="token", chat_id="12345")
        self.texts: list[str] = []

    def send_text(self, *, text: str) -> TelegramDelivery:
        prepared = self.prepare_text(text=text)
        self.texts.append(prepared)
        return TelegramDelivery(delivered=True, message=prepared, response_code=200)


def _seed_paper_ledger(tmp_path: Path) -> tuple[Path, Path]:
    db_path = tmp_path / "state" / "executor_v3" / "paper_ledger.db"
    reports_dir = tmp_path / "out" / "executor_v3" / "reports"
    config = ExecutorConfig(
        enabled=True,
        source_bot="prop_v2",
        signal_queue_path=tmp_path / "out" / "prop_v2" / "execution_signals.ndjson",
        sqlite_path=db_path,
        freshness_seconds=180,
        paper_mode=True,
        sizing=SizingConfig(
            default_contracts=1,
            contracts_by_instrument={"NQ": 1, "YM": 1},
            point_value_by_instrument={"NQ": 1.0, "YM": 1.0},
        ),
    )
    executor = PaperExecutor(
        config=config,
        journal=PaperTradeJournal(db_path),
        risk_sizer=PaperRiskSizer(config.sizing),
    )

    closed_signal = SignalEvent(
        signal_id="sig-closed",
        source_bot="prop_v2",
        instrument="NQ",
        direction=Direction.LONG,
        setup_type="ict_smc",
        session="ny_open",
        confluence=4.0,
        formed_timestamp_et=datetime(2026, 3, 25, 10, 0, tzinfo=ET),
        entry=100.0,
        stop=95.0,
        tp1=105.0,
        tp2=110.0,
        tp3=115.0,
        notes="closed winner",
        freshness_seconds=180,
    )
    open_signal = SignalEvent(
        signal_id="sig-open",
        source_bot="prop_v2",
        instrument="YM",
        direction=Direction.LONG,
        setup_type="ict_smc",
        session="ny_open",
        confluence=4.0,
        formed_timestamp_et=datetime(2026, 3, 25, 11, 0, tzinfo=ET),
        entry=200.0,
        stop=195.0,
        tp1=205.0,
        tp2=210.0,
        tp3=215.0,
        notes="open trade",
        freshness_seconds=180,
    )

    executor.submit_signal(closed_signal, received_at_et=datetime(2026, 3, 25, 10, 1, tzinfo=ET))
    executor.on_market_bar(
        MarketBar(
            instrument="NQ",
            timestamp_et=datetime(2026, 3, 25, 10, 2, tzinfo=ET),
            open=99.5,
            high=100.5,
            low=99.0,
            close=100.0,
        )
    )
    executor.on_market_bar(
        MarketBar(
            instrument="NQ",
            timestamp_et=datetime(2026, 3, 25, 10, 3, tzinfo=ET),
            open=100.0,
            high=106.0,
            low=99.5,
            close=105.0,
        )
    )

    executor.submit_signal(open_signal, received_at_et=datetime(2026, 3, 25, 11, 1, tzinfo=ET))
    executor.on_market_bar(
        MarketBar(
            instrument="YM",
            timestamp_et=datetime(2026, 3, 25, 11, 2, tzinfo=ET),
            open=199.5,
            high=200.5,
            low=199.0,
            close=200.0,
        )
    )
    return db_path, reports_dir


def _build_listener(db_path: Path, reports_dir: Path) -> tuple[TelegramCommandListener, FakeTelegramNotifier]:
    notifier = FakeTelegramNotifier()
    controller = PaperCommandController(
        sqlite_path=db_path,
        reports_dir=reports_dir,
        now_provider=lambda: datetime(2026, 3, 25, 12, 0, tzinfo=ET),
    )

    async def set_signals_active(_: bool) -> None:
        return None

    listener = TelegramCommandListener(
        notifier=notifier,
        status_provider=lambda: "<b>STATUS</b>\n<b>signals_active:</b> true",
        set_signals_active=set_signals_active,
        command_handler=controller.handle_command,
    )
    return listener, notifier


def test_paper_open_command(tmp_path: Path) -> None:
    db_path, reports_dir = _seed_paper_ledger(tmp_path)
    listener, notifier = _build_listener(db_path, reports_dir)

    asyncio.run(
        listener._handle_update({"update_id": 1, "message": {"chat": {"id": "12345"}, "text": "/paper_open"}})
    )

    assert notifier.texts[0].startswith("[EXEC-V3] Open Paper Positions")
    assert "YM LONG" in notifier.texts[0]
    assert "status=open" in notifier.texts[0]


def test_paper_pnl_command(tmp_path: Path) -> None:
    db_path, reports_dir = _seed_paper_ledger(tmp_path)
    listener, notifier = _build_listener(db_path, reports_dir)

    asyncio.run(
        listener._handle_update({"update_id": 1, "message": {"chat": {"id": "12345"}, "text": "/paper_pnl"}})
    )

    assert notifier.texts[0].startswith("[EXEC-V3] Paper PnL")
    assert "Today: realized=5.00" in notifier.texts[0]
    assert "open_positions=1" in notifier.texts[0]


def test_paper_trade_command(tmp_path: Path) -> None:
    db_path, reports_dir = _seed_paper_ledger(tmp_path)
    listener, notifier = _build_listener(db_path, reports_dir)

    asyncio.run(
        listener._handle_update(
            {"update_id": 1, "message": {"chat": {"id": "12345"}, "text": "/paper_trade pos_sig-closed"}}
        )
    )

    assert notifier.texts[0].startswith("[EXEC-V3] Trade pos_sig-closed")
    assert "ENTRY qty=1" in notifier.texts[0]
    assert "TP1 qty=1" in notifier.texts[0]


def test_daily_summary_formatting(tmp_path: Path) -> None:
    db_path, _ = _seed_paper_ledger(tmp_path)
    manager = PaperDailySummaryManager(queries=PaperTradeQueries(db_path))

    summary = manager.build_summary(now_et=datetime(2026, 3, 25, 18, 0, tzinfo=ET))
    message = manager.format_message(summary)

    assert "PAPER DAILY SUMMARY" in message
    assert "Trades Opened" in message
    assert "Trades Closed" in message
    assert "Win/Loss" in message
    assert "Realized PnL" in message


def test_csv_export_creation(tmp_path: Path) -> None:
    db_path, reports_dir = _seed_paper_ledger(tmp_path)
    controller = PaperCommandController(
        sqlite_path=db_path,
        reports_dir=reports_dir,
        now_provider=lambda: datetime(2026, 3, 25, 12, 0, tzinfo=ET),
    )

    export_path = controller.export_recent_trades_csv(limit=10)

    assert export_path.exists()
    content = export_path.read_text(encoding="utf-8")
    assert "trade_id,signal_id,order_id,instrument,direction,entry,exit,size,open_quantity,realized_pnl,status,opened_at_et,closed_at_et" in content
    assert "pos_sig-closed" in content
