from __future__ import annotations

import json
from pathlib import Path

from bot_corrected_paper.config import CorrectedPaperConfig, load_corrected_paper_config
from bot_corrected_paper.controller import CorrectedPaperCommandController
from bot_corrected_paper.cli import main
from bot_corrected_paper.engine import CorrectedPaperEngine
from futures_bot.live.live_runner import LiveSignalRunner


def _config(tmp_path: Path) -> CorrectedPaperConfig:
    return CorrectedPaperConfig(
        enabled=True,
        source_queue_path=tmp_path / "out" / "corrected_v4" / "execution_events.ndjson",
        state_path=tmp_path / "state" / "corrected_paper" / "corrected_paper_state.json",
        journal_path=tmp_path / "out" / "corrected_paper" / "corrected_paper_journal.ndjson",
        events_path=tmp_path / "out" / "corrected_paper" / "events.ndjson",
        reports_dir=tmp_path / "out" / "corrected_paper" / "reports",
        default_point_values={"NQ": 1.0},
    )


def _write_queue(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "event": "CORRECTED_EXECUTION_SIGNAL",
            "paper_only": True,
            "source": "CORR-V4",
            "signal_id": "corr-v4-test",
            "timestamp_et": "2026-04-18T09:30:00-04:00",
            "symbol": "NQ",
            "strategy": "strat_nq_signal",
            "setup": "structural_continuation",
            "direction": "LONG",
            "score": 92.0,
            "contracts": 2,
            "point_value": 1.0,
            "entry_price": 100.0,
            "stop_price": 95.0,
            "tp1_price": 105.0,
            "risk_dollars": 10.0,
        },
        {
            "event": "CORRECTED_MARKET_BAR",
            "timestamp_et": "2026-04-18T09:31:00-04:00",
            "symbol": "NQ",
            "open": 100.0,
            "high": 105.5,
            "low": 99.5,
            "close": 105.0,
            "volume": 1000.0,
        },
    ]
    path.write_text("\n".join(json.dumps(record) for record in records) + "\n", encoding="utf-8")


def test_corrected_paper_engine_opens_closes_and_persists_outputs(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_queue(config.source_queue_path)
    engine = CorrectedPaperEngine(config=config)

    result = engine.process_queue()

    assert result.processed_records == 2
    assert result.opened_positions == 1
    assert result.closed_positions == 1
    assert engine.open_positions() == ()
    assert engine.summary()["realized_pnl"] == 10.0
    assert engine.summary()["wins"] == 1
    state = json.loads(config.state_path.read_text(encoding="utf-8"))
    journal_lines = config.journal_path.read_text(encoding="utf-8").strip().splitlines()
    report = json.loads((config.reports_dir / "summary.json").read_text(encoding="utf-8"))
    trades_csv = (config.reports_dir / "closed_trades.csv").read_text(encoding="utf-8")
    assert state["closed_trades"][0]["entry_price"] == 100.0
    assert state["closed_trades"][0]["stop_price"] == 95.0
    assert state["closed_trades"][0]["tp1_price"] == 105.0
    assert state["closed_trades"][0]["closure_outcome"] == "tp1"
    assert state["closed_trades"][0]["win_loss"] == "win"
    assert any(json.loads(line)["event"] == "POSITION_OPENED" for line in journal_lines)
    assert any(json.loads(line)["event"] == "POSITION_CLOSED" for line in journal_lines)
    assert report["instrument_stats"]["NQ"]["wins"] == 1
    assert "position_id,signal_id,symbol" in trades_csv
    assert "corr-paper-corr-v4-test" in trades_csv


def test_corrected_paper_engine_uses_conservative_stop_first_when_same_bar_hits_both(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.source_queue_path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "event": "CORRECTED_EXECUTION_SIGNAL",
            "signal_id": "corr-v4-stop-first",
            "timestamp_et": "2026-04-18T09:30:00-04:00",
            "symbol": "NQ",
            "strategy": "strat_nq_signal",
            "setup": "structural_continuation",
            "direction": "LONG",
            "contracts": 1,
            "entry_price": 100.0,
            "stop_price": 95.0,
            "tp1_price": 105.0,
        },
        {
            "event": "CORRECTED_MARKET_BAR",
            "timestamp_et": "2026-04-18T09:31:00-04:00",
            "symbol": "NQ",
            "open": 100.0,
            "high": 106.0,
            "low": 94.0,
            "close": 101.0,
            "volume": 1000.0,
        },
    ]
    config.source_queue_path.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")

    engine = CorrectedPaperEngine(config=config)
    engine.process_queue()

    trade = engine.recent_trades(limit=1)[0]
    assert trade.closure_outcome == "stop"
    assert trade.realized_pnl == -5.0
    assert trade.win_loss == "loss"


def test_corrected_paper_controller_read_only_commands(tmp_path: Path) -> None:
    config = _config(tmp_path)
    _write_queue(config.source_queue_path)
    CorrectedPaperEngine(config=config).process_queue()
    config_dir = tmp_path / "configs" / "corrected_paper"
    config_dir.mkdir(parents=True)
    (config_dir / "bot.yaml").write_text(
        "\n".join(
            [
                "bot:",
                f"  source_queue_path: {config.source_queue_path}",
                "storage:",
                f"  state_path: {config.state_path}",
                f"  journal_path: {config.journal_path}",
                f"  events_path: {config.events_path}",
                f"  reports_dir: {config.reports_dir}",
                "point_values:",
                "  NQ: 1.0",
            ]
        ),
        encoding="utf-8",
    )
    controller = CorrectedPaperCommandController(config_dir=config_dir)

    pnl = controller.handle_command("/corrected_paper_pnl")
    recent = controller.handle_command("/corrected_paper_recent 5")
    stats = controller.handle_command("/corrected_paper_stats")
    open_positions = controller.handle_command("/corrected_paper_open")

    assert pnl is not None and pnl.startswith("[CORR-PAPER] Paper PnL")
    assert "realized=10.00" in pnl
    assert recent is not None and "outcome=tp1" in recent
    assert stats is not None and "NQ: realized=10.00" in stats
    assert open_positions == "[CORR-PAPER] Open Positions\nNo open corrected paper positions."


def test_corrected_paper_watch_cli_writes_runtime_events(tmp_path: Path, capsys) -> None:
    config = _config(tmp_path)
    _write_queue(config.source_queue_path)

    rc = main(
        [
            "watch",
            "--config-dir",
            "configs/corrected_paper",
            "--source-queue",
            str(config.source_queue_path),
            "--state-path",
            str(config.state_path),
            "--journal-path",
            str(config.journal_path),
            "--events-path",
            str(config.events_path),
            "--reports-dir",
            str(config.reports_dir),
            "--poll-interval",
            "0.01",
            "--max-iterations",
            "1",
        ]
    )

    output = capsys.readouterr().out
    event_lines = config.events_path.read_text(encoding="utf-8").strip().splitlines()
    events = [json.loads(line)["event"] for line in event_lines]
    assert rc == 0
    assert "corrected_paper_startup" in output
    assert "corrected_paper_status processed_records=2" in output
    assert "CORRECTED_PAPER_STARTUP" in events
    assert "CORRECTED_PAPER_STATUS" in events


def test_live_runner_routes_existing_paper_commands_before_corrected_paper_commands() -> None:
    class FakeController:
        def __init__(self, mapping: dict[str, str]) -> None:
            self._mapping = mapping

        def handle_command(self, text: str) -> str | None:
            return self._mapping.get(text)

    runner = object.__new__(LiveSignalRunner)
    runner._paper_controller = FakeController({"/paper_pnl": "exec-v3-paper"})
    runner._corrected_paper_controller = FakeController({"/corrected_paper_pnl": "corrected-paper"})

    assert runner._handle_read_only_command("/paper_pnl") == "exec-v3-paper"
    assert runner._handle_read_only_command("/corrected_paper_pnl") == "corrected-paper"
    assert runner._handle_read_only_command("/unknown") is None


def test_corrected_paper_vps_artifacts_are_present() -> None:
    service = Path("services/corrected_paper.service")
    launcher = Path("scripts/run_corrected_paper.sh")
    deployer = Path("scripts/deploy_corrected_paper_vps.sh")
    verifier = Path("scripts/verify_corrected_paper_vps.sh")
    runbook = Path("docs/runbooks/corrected_paper_vps.md")

    assert service.exists()
    assert launcher.exists()
    assert deployer.exists()
    assert verifier.exists()
    assert runbook.exists()
    assert "corrected-paper\" watch" in launcher.read_text(encoding="utf-8")
    deployer_text = deployer.read_text(encoding="utf-8")
    assert "VPS_TARGET is required" in deployer_text
    assert "scripts/verify_corrected_paper_vps.sh" in deployer_text
    verifier_text = verifier.read_text(encoding="utf-8")
    assert "systemctl is-active" in verifier_text
    assert "out/corrected_paper/reports/summary.json" in verifier_text
    assert "/corrected_paper_pnl" in verifier_text
    assert "CorrectedPaperCommandController" in verifier_text
    service_text = service.read_text(encoding="utf-8")
    assert "Restart=always" in service_text
    assert "out/corrected_paper/logs/systemd_stdout.log" in service_text


def test_corrected_paper_config_defaults() -> None:
    config = load_corrected_paper_config("configs/corrected_paper")

    assert config.source_queue_path == Path("out/corrected_v4/execution_events.ndjson")
    assert config.state_path == Path("state/corrected_paper/corrected_paper_state.json")
    assert config.default_point_values["MGC"] == 10.0
