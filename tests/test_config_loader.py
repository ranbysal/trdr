from __future__ import annotations

from pathlib import Path

import pytest

from futures_bot.config.loader import load_all_configs, load_instruments, load_yaml
from futures_bot.core.enums import Family
from futures_bot.core.errors import ConfigurationError


def test_load_yaml_success(tmp_path: Path) -> None:
    path = tmp_path / "sample.yaml"
    path.write_text("version: 1\nname: test\n", encoding="utf-8")

    payload = load_yaml(path)

    assert payload["version"] == 1
    assert payload["name"] == "test"


def test_load_all_configs_reads_yaml_files(tmp_path: Path) -> None:
    (tmp_path / "a.yaml").write_text("version: 1\n", encoding="utf-8")
    (tmp_path / "b.yaml").write_text("enabled: true\n", encoding="utf-8")

    configs = load_all_configs(tmp_path)

    assert set(configs) == {"a", "b"}


def test_load_instruments_success() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    instruments = load_instruments(repo_root / "configs")

    assert "ES" in instruments
    assert instruments["ES"].family is Family.EQUITIES
    assert instruments["GC"].family is Family.METALS


def test_load_instruments_missing_required_key(tmp_path: Path) -> None:
    bad = tmp_path / "instruments.yaml"
    bad.write_text(
        "instruments:\n"
        "  - symbol: ES\n"
        "    root_symbol: ES\n"
        "    family: equities\n"
        "    tick_size: 0.25\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="missing required key 'tick_value'"):
        load_instruments(tmp_path)


def test_load_instruments_invalid_family(tmp_path: Path) -> None:
    bad = tmp_path / "instruments.yaml"
    bad.write_text(
        "instruments:\n"
        "  - symbol: ES\n"
        "    root_symbol: ES\n"
        "    family: crypto\n"
        "    tick_size: 0.25\n"
        "    tick_value: 12.5\n"
        "    point_value: 50\n"
        "    commission_rt: 4.8\n"
        "    symbol_type: future\n"
        "    micro_equivalent: MES\n"
        "    contract_units: 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="must be one of"):
        load_instruments(tmp_path)


def test_load_instruments_duplicate_symbol(tmp_path: Path) -> None:
    bad = tmp_path / "instruments.yaml"
    bad.write_text(
        "instruments:\n"
        "  - symbol: ES\n"
        "    root_symbol: ES\n"
        "    family: equities\n"
        "    tick_size: 0.25\n"
        "    tick_value: 12.5\n"
        "    point_value: 50\n"
        "    commission_rt: 4.8\n"
        "    symbol_type: future\n"
        "    micro_equivalent: MES\n"
        "    contract_units: 1\n"
        "  - symbol: ES\n"
        "    root_symbol: ES\n"
        "    family: equities\n"
        "    tick_size: 0.25\n"
        "    tick_value: 12.5\n"
        "    point_value: 50\n"
        "    commission_rt: 4.8\n"
        "    symbol_type: future\n"
        "    micro_equivalent: MES\n"
        "    contract_units: 1\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="Duplicate instrument symbol: ES"):
        load_instruments(tmp_path)
