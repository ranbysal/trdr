"""Prepare Databento OHLCV-1m CSVs for corrected replay."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

import pandas as pd

RAW_REQUIRED_COLUMNS = {
    "ts_event",
    "rtype",
    "publisher_id",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "symbol",
}
OUTPUT_COLUMNS = ["timestamp_et", "symbol", "open", "high", "low", "close", "volume"]
_ET = ZoneInfo("America/New_York")


def prepare_corrected_replay_csv(
    *,
    input_paths: Sequence[str | Path],
    contracts: Sequence[str],
    out_path: str | Path,
) -> pd.DataFrame:
    normalized_inputs = tuple(Path(path) for path in input_paths)
    if not normalized_inputs:
        raise ValueError("At least one input CSV is required.")

    normalized_contracts = tuple(_normalize_contract(contract) for contract in contracts)
    if not normalized_contracts:
        raise ValueError("At least one contract must be supplied with --contracts.")

    frames = [_load_raw_csv(path) for path in normalized_inputs]
    raw = pd.concat(frames, ignore_index=True)
    if raw.empty:
        raise ValueError("No Databento rows were found in the supplied input files.")

    raw = raw.copy()
    raw["symbol"] = raw["symbol"].astype(str).str.strip().str.upper()

    requested_contracts = set(normalized_contracts)
    available_contracts = set(raw["symbol"].unique())
    missing_contracts = sorted(requested_contracts.difference(available_contracts))
    if missing_contracts:
        raise ValueError(f"Requested contracts not found in raw data: {missing_contracts}")

    filtered = raw.loc[raw["symbol"].isin(requested_contracts)].copy()
    filtered["ts"] = pd.to_datetime(filtered["ts_event"], errors="raise", utc=True).dt.tz_convert(_ET)
    for column in ("open", "high", "low", "close", "volume"):
        filtered[column] = pd.to_numeric(filtered[column], errors="raise")

    filtered["symbol"] = filtered["symbol"].map(_canonical_replay_symbol)
    filtered = filtered.sort_values(["ts", "symbol"], kind="mergesort").reset_index(drop=True)
    filtered["timestamp_et"] = filtered["ts"].map(lambda value: value.isoformat())

    output = filtered.loc[:, OUTPUT_COLUMNS].copy()
    duplicate_mask = output.duplicated(subset=["timestamp_et", "symbol"], keep=False)
    if duplicate_mask.any():
        sample = output.loc[duplicate_mask, ["timestamp_et", "symbol"]].head(5).to_dict(orient="records")
        raise ValueError(f"Prepared replay data has duplicate timestamp/symbol rows: {sample}")

    destination = Path(out_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(destination, index=False)
    return output


def _load_raw_csv(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    missing = RAW_REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"{path}: missing Databento OHLCV-1m required columns: {sorted(missing)}")
    return frame


def _normalize_contract(contract: str) -> str:
    normalized = str(contract).strip().upper()
    if not normalized:
        raise ValueError("Contracts must be non-empty strings.")
    return normalized


def _canonical_replay_symbol(contract: str) -> str:
    normalized = _normalize_contract(contract)
    if normalized.startswith("YM"):
        return "YM"
    if normalized.startswith("NQ"):
        return "NQ"
    if normalized.startswith("MGC") or normalized.startswith("GC"):
        return "GOLD"
    raise ValueError(f"Unsupported corrected replay contract mapping: {normalized}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare Databento OHLCV-1m CSVs into one corrected replay CSV."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", help="Single Databento CSV input.")
    group.add_argument("--inputs", nargs="+", help="Multiple Databento CSV inputs.")
    parser.add_argument("--contracts", nargs="+", required=True, help="Exact contracts to keep from the raw symbol column.")
    parser.add_argument("--out", required=True, help="Output path for the replay-ready CSV.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    input_paths = [args.input] if args.input is not None else list(args.inputs)
    try:
        prepare_corrected_replay_csv(
            input_paths=input_paths,
            contracts=args.contracts,
            out_path=args.out,
        )
    except ValueError as exc:
        parser.exit(2, f"error: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
