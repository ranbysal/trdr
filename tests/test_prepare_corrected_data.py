from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
import pytest

from futures_bot.backtest.corrected_replay import _load_replay_rows
from futures_bot.backtest.prepare_corrected_data import (
    OUTPUT_COLUMNS,
    main,
    prepare_corrected_replay_csv,
)


def _write_databento_csv(path: Path, rows: list[dict[str, object]], *, include_symbol: bool = True) -> None:
    fieldnames = [
        "ts_event",
        "rtype",
        "publisher_id",
        "instrument_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    if include_symbol:
        fieldnames.append("symbol")

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _raw_row(
    *,
    ts_event: str,
    symbol: str,
    instrument_id: int,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: int,
) -> dict[str, object]:
    return {
        "ts_event": ts_event,
        "rtype": 33,
        "publisher_id": 1,
        "instrument_id": instrument_id,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "symbol": symbol,
    }


def test_prepare_cli_filters_combined_multi_symbol_csv_and_maps_to_replay_symbols(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw.csv"
    out_path = tmp_path / "corrected_bars.csv"
    _write_databento_csv(
        raw_path,
        [
            _raw_row(
                ts_event="2026-03-23T13:31:00.000000000Z",
                symbol="NQM6",
                instrument_id=102,
                open_price=20481.0,
                high_price=20482.0,
                low_price=20480.5,
                close_price=20481.5,
                volume=220,
            ),
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="YMM6",
                instrument_id=101,
                open_price=42180.0,
                high_price=42182.0,
                low_price=42179.0,
                close_price=42181.0,
                volume=120,
            ),
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="GCM6-GCV6",
                instrument_id=999,
                open_price=-12.0,
                high_price=-12.0,
                low_price=-12.5,
                close_price=-12.5,
                volume=4,
            ),
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="GCM6",
                instrument_id=103,
                open_price=3040.0,
                high_price=3041.5,
                low_price=3039.5,
                close_price=3041.0,
                volume=180,
            ),
        ],
    )

    assert (
        main(
            [
                "--input",
                str(raw_path),
                "--contracts",
                "YMM6",
                "NQM6",
                "GCM6",
                "--out",
                str(out_path),
            ]
        )
        == 0
    )

    prepared = pd.read_csv(out_path)
    assert prepared.columns.tolist() == OUTPUT_COLUMNS
    assert prepared.to_dict(orient="records") == [
        {
            "timestamp_et": "2026-03-23T09:30:00-04:00",
            "symbol": "GOLD",
            "open": 3040.0,
            "high": 3041.5,
            "low": 3039.5,
            "close": 3041.0,
            "volume": 180,
        },
        {
            "timestamp_et": "2026-03-23T09:30:00-04:00",
            "symbol": "YM",
            "open": 42180.0,
            "high": 42182.0,
            "low": 42179.0,
            "close": 42181.0,
            "volume": 120,
        },
        {
            "timestamp_et": "2026-03-23T09:31:00-04:00",
            "symbol": "NQ",
            "open": 20481.0,
            "high": 20482.0,
            "low": 20480.5,
            "close": 20481.5,
            "volume": 220,
        },
    ]


def test_prepare_cli_merges_multiple_csv_inputs_deterministically(tmp_path: Path) -> None:
    file_a = tmp_path / "a.csv"
    file_b = tmp_path / "b.csv"
    out_a = tmp_path / "out_a.csv"
    out_b = tmp_path / "out_b.csv"
    _write_databento_csv(
        file_a,
        [
            _raw_row(
                ts_event="2026-03-23T13:31:00.000000000Z",
                symbol="YMM6",
                instrument_id=201,
                open_price=42181.0,
                high_price=42182.0,
                low_price=42180.0,
                close_price=42181.5,
                volume=100,
            ),
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="NQM6",
                instrument_id=202,
                open_price=20480.0,
                high_price=20481.0,
                low_price=20479.0,
                close_price=20480.5,
                volume=200,
            ),
        ],
    )
    _write_databento_csv(
        file_b,
        [
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="YMM6",
                instrument_id=203,
                open_price=42180.0,
                high_price=42181.0,
                low_price=42179.5,
                close_price=42180.5,
                volume=90,
            ),
            _raw_row(
                ts_event="2026-03-23T13:31:00.000000000Z",
                symbol="NQM6",
                instrument_id=204,
                open_price=20481.0,
                high_price=20482.0,
                low_price=20480.0,
                close_price=20481.5,
                volume=210,
            ),
        ],
    )

    assert (
        main(
            [
                "--inputs",
                str(file_a),
                str(file_b),
                "--contracts",
                "YMM6",
                "NQM6",
                "--out",
                str(out_a),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--inputs",
                str(file_b),
                str(file_a),
                "--contracts",
                "YMM6",
                "NQM6",
                "--out",
                str(out_b),
            ]
        )
        == 0
    )

    assert out_a.read_text(encoding="utf-8") == out_b.read_text(encoding="utf-8")
    prepared = pd.read_csv(out_a)
    assert prepared[["timestamp_et", "symbol"]].to_dict(orient="records") == [
        {"timestamp_et": "2026-03-23T09:30:00-04:00", "symbol": "NQ"},
        {"timestamp_et": "2026-03-23T09:30:00-04:00", "symbol": "YM"},
        {"timestamp_et": "2026-03-23T09:31:00-04:00", "symbol": "NQ"},
        {"timestamp_et": "2026-03-23T09:31:00-04:00", "symbol": "YM"},
    ]


def test_prepare_fails_loudly_when_required_raw_columns_are_missing(tmp_path: Path) -> None:
    raw_path = tmp_path / "missing_symbol.csv"
    _write_databento_csv(
        raw_path,
        [
            {
                "ts_event": "2026-03-23T13:30:00.000000000Z",
                "rtype": 33,
                "publisher_id": 1,
                "instrument_id": 301,
                "open": 20480.0,
                "high": 20481.0,
                "low": 20479.0,
                "close": 20480.5,
                "volume": 200,
            }
        ],
        include_symbol=False,
    )

    with pytest.raises(ValueError, match="missing Databento OHLCV-1m required columns"):
        prepare_corrected_replay_csv(
            input_paths=[raw_path],
            contracts=["NQM6"],
            out_path=tmp_path / "out.csv",
        )


def test_prepare_fails_loudly_when_requested_contract_is_missing(tmp_path: Path) -> None:
    raw_path = tmp_path / "raw.csv"
    _write_databento_csv(
        raw_path,
        [
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="YMM6",
                instrument_id=401,
                open_price=42180.0,
                high_price=42181.0,
                low_price=42179.0,
                close_price=42180.5,
                volume=80,
            )
        ],
    )

    with pytest.raises(ValueError, match="Requested contracts not found in raw data: \\['NQM6'\\]"):
        prepare_corrected_replay_csv(
            input_paths=[raw_path],
            contracts=["YMM6", "NQM6"],
            out_path=tmp_path / "out.csv",
        )


def test_prepare_maps_gc_and_mgc_contracts_to_gold(tmp_path: Path) -> None:
    raw_path = tmp_path / "gold.csv"
    out_path = tmp_path / "prepared.csv"
    _write_databento_csv(
        raw_path,
        [
            _raw_row(
                ts_event="2026-03-23T13:30:00.000000000Z",
                symbol="GCM6",
                instrument_id=501,
                open_price=3040.0,
                high_price=3041.0,
                low_price=3039.0,
                close_price=3040.5,
                volume=90,
            ),
            _raw_row(
                ts_event="2026-03-23T13:31:00.000000000Z",
                symbol="MGCM6",
                instrument_id=502,
                open_price=3041.0,
                high_price=3042.0,
                low_price=3040.5,
                close_price=3041.5,
                volume=110,
            ),
        ],
    )

    prepare_corrected_replay_csv(
        input_paths=[raw_path],
        contracts=["GCM6", "MGCM6"],
        out_path=out_path,
    )

    prepared = pd.read_csv(out_path)
    assert prepared["symbol"].tolist() == ["GOLD", "GOLD"]


def test_corrected_replay_loader_accepts_prepared_gold_symbol(tmp_path: Path) -> None:
    prepared_path = tmp_path / "prepared.csv"
    prepared_path.write_text(
        "\n".join(
            [
                "timestamp_et,symbol,open,high,low,close,volume",
                "2026-03-23T09:30:00-04:00,GOLD,3040.0,3041.0,3039.0,3040.5,90",
            ]
        ),
        encoding="utf-8",
    )

    loaded = _load_replay_rows(prepared_path)
    assert loaded["symbol"].tolist() == ["MGC"]
