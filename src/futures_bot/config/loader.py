"""Load configuration files from disk with explicit validation errors."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from futures_bot.config.validators import validate_instrument_record
from futures_bot.core.errors import ConfigurationError
from futures_bot.core.types import InstrumentMeta


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load YAML as a mapping with explicit schema errors."""
    yaml_path = Path(path)
    if not yaml_path.exists():
        raise ConfigurationError(f"Config file not found: {yaml_path}")

    with yaml_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ConfigurationError(f"YAML root must be a mapping: {yaml_path}")
    return payload


def load_all_configs(config_dir: str | Path) -> dict[str, dict[str, Any]]:
    """Load all YAML configs from a directory keyed by filename stem."""
    base_dir = Path(config_dir)
    if not base_dir.exists() or not base_dir.is_dir():
        raise ConfigurationError(f"Config directory not found: {base_dir}")

    configs: dict[str, dict[str, Any]] = {}
    for path in sorted(base_dir.glob("*.yaml")):
        configs[path.stem] = load_yaml(path)
    return configs


def load_instruments(config_dir: str | Path) -> dict[str, InstrumentMeta]:
    """Load and validate instrument metadata from instruments.yaml."""
    config_path = Path(config_dir) / "instruments.yaml"
    payload = load_yaml(config_path)

    records = payload.get("instruments")
    if not isinstance(records, list):
        raise ConfigurationError("instruments.yaml: key 'instruments' must be a list")

    by_symbol: dict[str, InstrumentMeta] = {}
    for idx, raw in enumerate(records):
        if not isinstance(raw, dict):
            raise ConfigurationError(f"instruments[{idx}]: each entry must be a mapping")
        item = validate_instrument_record(raw, idx)
        if item.symbol in by_symbol:
            raise ConfigurationError(f"Duplicate instrument symbol: {item.symbol}")
        by_symbol[item.symbol] = item

    return by_symbol
