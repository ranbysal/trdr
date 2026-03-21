"""Config loader for the Prop V2 scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True, slots=True)
class PropV2Config:
    name: str
    architecture: str
    mode: str
    dataset: str
    schema: str
    stype_in: str


def load_prop_v2_config(config_dir: str | Path) -> PropV2Config:
    config_path = Path(config_dir) / "bot.yaml"
    if not config_path.exists():
        raise ValueError(f"Bot 2 config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Bot 2 config root must be a mapping: {config_path}")
    bot = payload.get("bot")
    if not isinstance(bot, dict):
        raise ValueError("bot.yaml: key 'bot' must be a mapping")
    return PropV2Config(
        name=str(bot.get("name", "prop_v2")),
        architecture=str(bot.get("architecture", "atr_normalized_smc")),
        mode=str(bot.get("mode", "scaffold")),
        dataset=str(bot.get("dataset", "GLBX.MDP3")),
        schema=str(bot.get("schema", "ohlcv-1m")),
        stype_in=str(bot.get("stype_in", "continuous")),
    )

