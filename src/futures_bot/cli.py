"""Compatibility CLI shim that routes the legacy command to Trader V1."""

from bot_trader_v1.cli import LiveSignalSettings, _build_parser, _parse_strategies, _resolve_live_signal_settings, main

__all__ = [
    "LiveSignalSettings",
    "_build_parser",
    "_parse_strategies",
    "_resolve_live_signal_settings",
    "main",
]
