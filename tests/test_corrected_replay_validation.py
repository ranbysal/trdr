from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from futures_bot.backtest.corrected_replay import run_corrected_validation_replay
from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.enums import Family
from futures_bot.core.types import InstrumentMeta

ET = ZoneInfo("America/New_York")
REPO_ROOT = Path(__file__).resolve().parents[1]


def _instrument(symbol: str, *, family: Family, tick_size: float, tick_value: float) -> InstrumentMeta:
    return InstrumentMeta(
        symbol=symbol,
        root_symbol=symbol,
        family=family,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=tick_value / tick_size,
        commission_rt=4.8,
        symbol_type="future",
        micro_equivalent=symbol,
        contract_units=1.0,
    )


def _write_validation_csv(path: Path) -> None:
    fields = [
        "timestamp_et",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "liquidity_ok",
        "macro_blocked",
        "choch_confirmed",
        "fvg_present",
        "intermarket_confirmed",
        "pullback_price",
        "structure_break_price",
        "order_block_low",
        "order_block_high",
        "session_start_equity",
        "realized_pnl",
        "open_position_symbol",
        "open_position_quantity",
        "open_position_avg_entry_price",
        "open_position_mark_price",
        "open_position_point_value",
    ]
    start = datetime(2026, 1, 5, 9, 30, tzinfo=ET)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        for i in range(130):
            ts = start + timedelta(minutes=i)
            close_nq = 20_480.0 + 0.45 * i
            writer.writerow(
                {
                    "timestamp_et": ts.isoformat(),
                    "symbol": "NQ",
                    "open": f"{close_nq - 0.2:.2f}",
                    "high": f"{close_nq + 0.5:.2f}",
                    "low": f"{close_nq - 0.5:.2f}",
                    "close": f"{close_nq:.2f}",
                    "volume": "1000",
                    "liquidity_ok": "true",
                    "macro_blocked": "false",
                    "pullback_price": "20520.0",
                    "structure_break_price": "20525.0",
                    "order_block_low": "20518.0",
                    "order_block_high": "20525.0",
                    "session_start_equity": "100000",
                    "realized_pnl": "0",
                }
            )

            close_ym = 42_180.0 + 0.25 * i
            writer.writerow(
                {
                    "timestamp_et": ts.isoformat(),
                    "symbol": "YM",
                    "open": f"{close_ym - 0.2:.2f}",
                    "high": f"{close_ym + 0.5:.2f}",
                    "low": f"{close_ym - 0.5:.2f}",
                    "close": f"{close_ym:.2f}",
                    "volume": "900",
                    "liquidity_ok": "true",
                    "macro_blocked": "false",
                    "session_start_equity": "100000",
                    "realized_pnl": "0",
                }
            )

            close_mgc = 2_640.0
            writer.writerow(
                {
                    "timestamp_et": ts.isoformat(),
                    "symbol": "MGC",
                    "open": f"{close_mgc - 0.2:.2f}",
                    "high": f"{close_mgc + 0.5:.2f}",
                    "low": f"{close_mgc - 0.5:.2f}",
                    "close": f"{close_mgc:.2f}",
                    "volume": "800",
                    "liquidity_ok": "true",
                    "macro_blocked": "false",
                    "pullback_price": "2640.0",
                    "structure_break_price": "2640.0",
                    "order_block_low": "2639.8",
                    "order_block_high": "2640.2",
                    "session_start_equity": "100000",
                    "realized_pnl": "-1400",
                    "open_position_symbol": "NQ",
                    "open_position_quantity": "1",
                    "open_position_avg_entry_price": "20000",
                    "open_position_mark_price": "19990",
                    "open_position_point_value": "20",
                }
            )

        pre_anchor = datetime(2026, 1, 6, 9, 0, tzinfo=ET)
        writer.writerow(
            {
                "timestamp_et": pre_anchor.isoformat(),
                "symbol": "YM",
                "open": "42200.00",
                "high": "42200.50",
                "low": "42199.50",
                "close": "42200.00",
                "volume": "900",
                "liquidity_ok": "true",
                "macro_blocked": "false",
                "session_start_equity": "100000",
                "realized_pnl": "0",
            }
        )


def _configs() -> tuple[NQStrategyConfig, YMStrategyConfig, GoldStrategyConfig]:
    return (
        NQStrategyConfig(hard_risk_per_trade_dollars=750.0, daily_halt_loss_dollars=1_500.0),
        YMStrategyConfig(hard_risk_per_trade_dollars=5.0, daily_halt_loss_dollars=1_500.0),
        GoldStrategyConfig(
            hard_risk_per_trade_dollars=400.0,
            daily_halt_loss_dollars=1_200.0,
            symbol="MGC",
        ),
    )


def _instruments() -> dict[str, InstrumentMeta]:
    return {
        "NQ": _instrument("NQ", family=Family.EQUITIES, tick_size=0.25, tick_value=5.0),
        "YM": _instrument("YM", family=Family.EQUITIES, tick_size=1.0, tick_value=5.0),
        "MGC": _instrument("MGC", family=Family.METALS, tick_size=0.1, tick_value=1.0),
    }


def test_replay_determinism_and_identical_inputs_produce_identical_outputs(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_a = tmp_path / "out_a"
    out_b = tmp_path / "out_b"
    _write_validation_csv(data)
    nq, ym, gold = _configs()

    result_a = run_corrected_validation_replay(
        data_path=data,
        out_dir=out_a,
        instruments_by_symbol=_instruments(),
        nq_config=nq,
        ym_config=ym,
        gold_config=gold,
    )
    result_b = run_corrected_validation_replay(
        data_path=data,
        out_dir=out_b,
        instruments_by_symbol=_instruments(),
        nq_config=nq,
        ym_config=ym,
        gold_config=gold,
    )

    assert Path(result_a.paths["events_path"]).read_text(encoding="utf-8") == Path(result_b.paths["events_path"]).read_text(
        encoding="utf-8"
    )
    assert json.loads(Path(result_a.paths["summary_path"]).read_text(encoding="utf-8")) == json.loads(
        Path(result_b.paths["summary_path"]).read_text(encoding="utf-8")
    )
    assert result_a.paths["summary_path"].name == "summary.json"


def test_corrected_replay_cli_writes_reports_and_out_dir(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_dir = tmp_path / "cli_out"
    _write_validation_csv(data)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "futures_bot.backtest.corrected_replay",
            "--data",
            str(data),
            "--out",
            str(out_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert out_dir.exists()
    assert (out_dir / "summary.json").exists()
    assert (out_dir / "accepted_signals.csv").exists()
    assert (out_dir / "rejected_signals.csv").exists()
    assert (out_dir / "validation_events.ndjson").exists()
    assert "processed" in completed.stderr


def test_rejection_reason_counts_are_stable(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_dir = tmp_path / "out"
    _write_validation_csv(data)
    nq, ym, gold = _configs()

    result = run_corrected_validation_replay(
        data_path=data,
        out_dir=out_dir,
        instruments_by_symbol=_instruments(),
        nq_config=nq,
        ym_config=ym,
        gold_config=gold,
    )

    reasons = {item["rejection_reason"]: item["count"] for item in result.summary["rejections_by_reason"]}
    assert reasons["HARD_RISK_CAP_EXCEEDED"] >= 1
    assert reasons["DAILY_LOSS_HALT"] >= 1


def test_anchored_vwap_and_session_behavior_remains_correct_in_replay(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_dir = tmp_path / "out"
    _write_validation_csv(data)
    nq, ym, gold = _configs()

    result = run_corrected_validation_replay(
        data_path=data,
        out_dir=out_dir,
        instruments_by_symbol=_instruments(),
        nq_config=nq,
        ym_config=ym,
        gold_config=gold,
    )

    pre_anchor_record = next(
        record for record in result.records if record.symbol == "YM" and record.ts == datetime(2026, 1, 6, 9, 0, tzinfo=ET)
    )
    assert pre_anchor_record.effective_anchor_ts == datetime(2026, 1, 5, 9, 30, tzinfo=ET)


def test_mark_to_market_daily_halt_behavior_remains_correct_in_replay(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_dir = tmp_path / "out"
    _write_validation_csv(data)
    nq, ym, gold = _configs()

    result = run_corrected_validation_replay(
        data_path=data,
        out_dir=out_dir,
        instruments_by_symbol=_instruments(),
        nq_config=nq,
        ym_config=ym,
        gold_config=gold,
    )

    assert any(record.outcome.value == "rejected_due_to_daily_halt" for record in result.records)
    assert result.summary["daily_halt_occurrences"][0]["daily_halt_occurrences"] >= 1


def test_corrected_replay_cli_writes_required_reports(tmp_path: Path) -> None:
    data = tmp_path / "validation.csv"
    out_dir = tmp_path / "out"
    _write_validation_csv(data)

    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "futures_bot.backtest.corrected_replay",
            "--data",
            str(data),
            "--out",
            str(out_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    expected_files = {
        "summary.json",
        "accepted_signals.csv",
        "rejected_signals.csv",
        "rejection_reason_counts.csv",
        "signal_frequency_by_instrument.csv",
        "daily_halt_events.csv",
    }
    assert expected_files <= {path.name for path in out_dir.iterdir()}

    summary = json.loads((out_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["accepted_signal_count"] >= 1
    assert summary["risk_skip_count"] >= 1
    assert summary["daily_halt_event_count"] >= 1


def test_corrected_replay_cli_fails_loudly_when_required_columns_are_missing(tmp_path: Path) -> None:
    data = tmp_path / "missing_columns.csv"
    out_dir = tmp_path / "out"
    data.write_text(
        "\n".join(
            [
                "timestamp_et,symbol,open,high,low,volume",
                "2026-01-05T09:30:00-05:00,NQ,20480,20481,20479,1000",
            ]
        ),
        encoding="utf-8",
    )

    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "futures_bot.backtest.corrected_replay",
            "--data",
            str(data),
            "--out",
            str(out_dir),
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 2
    assert "Replay CSV missing required columns" in completed.stderr
