"""Schema validation for configuration documents."""

from __future__ import annotations

from typing import Any

from futures_bot.core.enums import Family
from futures_bot.core.errors import ConfigurationError
from futures_bot.core.types import InstrumentMeta


def _require_key(data: dict[str, Any], key: str, context: str) -> Any:
    if key not in data:
        raise ConfigurationError(f"{context}: missing required key '{key}'")
    return data[key]


def _as_str(value: Any, key: str, context: str) -> str:
    if not isinstance(value, str) or value.strip() == "":
        raise ConfigurationError(f"{context}: key '{key}' must be a non-empty string")
    return value


def _as_float(value: Any, key: str, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{context}: key '{key}' must be a number")
    return float(value)


def validate_instrument_record(record: dict[str, Any], index: int) -> InstrumentMeta:
    context = f"instruments[{index}]"

    symbol = _as_str(_require_key(record, "symbol", context), "symbol", context)
    root_symbol = _as_str(_require_key(record, "root_symbol", context), "root_symbol", context)

    family_raw = _as_str(_require_key(record, "family", context), "family", context)
    try:
        family = Family(family_raw)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in Family)
        raise ConfigurationError(
            f"{context}: key 'family' must be one of [{allowed}], got '{family_raw}'"
        ) from exc

    tick_size = _as_float(_require_key(record, "tick_size", context), "tick_size", context)
    tick_value = _as_float(_require_key(record, "tick_value", context), "tick_value", context)
    point_value = _as_float(_require_key(record, "point_value", context), "point_value", context)
    commission_rt = _as_float(_require_key(record, "commission_rt", context), "commission_rt", context)
    symbol_type = _as_str(_require_key(record, "symbol_type", context), "symbol_type", context)
    micro_equivalent = _as_str(
        _require_key(record, "micro_equivalent", context), "micro_equivalent", context
    )
    contract_units = _as_float(
        _require_key(record, "contract_units", context), "contract_units", context
    )

    return InstrumentMeta(
        symbol=symbol,
        root_symbol=root_symbol,
        family=family,
        tick_size=tick_size,
        tick_value=tick_value,
        point_value=point_value,
        commission_rt=commission_rt,
        symbol_type=symbol_type,
        micro_equivalent=micro_equivalent,
        contract_units=contract_units,
    )
