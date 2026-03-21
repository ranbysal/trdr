"""Bot 2 pipeline compatibility wrapper around the pure signal engine."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from bot_prop_v2.config import PropV2Config
from bot_prop_v2.pipeline.signal_engine import SignalEngine, TradeLogger


@dataclass(slots=True, frozen=True)
class PropV2Pipeline:
    config: PropV2Config
    engine: SignalEngine
    log_dir: Path
    report_dir: Path

    def describe(self) -> str:
        return f"{self.config.name}:{self.config.architecture}:{self.config.mode}"


def build_pipeline(config: PropV2Config, *, out_dir: str | Path | None = None) -> PropV2Pipeline:
    base_out_dir = Path(out_dir) if out_dir is not None else Path("out") / config.name
    log_dir = base_out_dir / config.log_dirname
    report_dir = base_out_dir / config.report_dirname
    log_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    engine = SignalEngine(config.build_risk_parameters())
    engine.logger = TradeLogger(log_dir=str(log_dir))

    return PropV2Pipeline(
        config=config,
        engine=engine,
        log_dir=log_dir,
        report_dir=report_dir,
    )
