"""ATR-normalized SMC scaffold for Bot 2."""

from __future__ import annotations

from dataclasses import dataclass

from bot_prop_v2.config import PropV2Config


@dataclass(slots=True)
class PropV2Pipeline:
    config: PropV2Config

    def describe(self) -> str:
        return f"{self.config.name}:{self.config.architecture}:{self.config.mode}"


def build_pipeline(config: PropV2Config) -> PropV2Pipeline:
    return PropV2Pipeline(config=config)

