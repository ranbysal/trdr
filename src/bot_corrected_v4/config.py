"""Config loader for the corrected V4 live signal runner."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from futures_bot.config.loader import load_instruments
from futures_bot.config.models import GoldStrategyConfig, NQStrategyConfig, YMStrategyConfig
from futures_bot.core.types import InstrumentMeta
from futures_bot.pipeline.corrected_orchestrator import CorrectedSignalOrchestrator


@dataclass(frozen=True, slots=True)
class CorrectedV4Config:
    name: str
    architecture: str
    mode: str
    dataset: str
    schema: str
    stype_in: str
    databento_symbols: tuple[str, ...]
    alert_tag: str
    heartbeat_interval_hours: float
    bars_stale_after_s: float
    starting_equity: float
    nq_hard_risk_per_trade_dollars: float
    nq_daily_halt_loss_dollars: float
    ym_hard_risk_per_trade_dollars: float
    ym_daily_halt_loss_dollars: float
    gold_hard_risk_per_trade_dollars: float
    gold_daily_halt_loss_dollars: float
    gold_symbol: str

    def build_orchestrator(self) -> CorrectedSignalOrchestrator:
        return CorrectedSignalOrchestrator(
            nq_config=NQStrategyConfig(
                hard_risk_per_trade_dollars=self.nq_hard_risk_per_trade_dollars,
                daily_halt_loss_dollars=self.nq_daily_halt_loss_dollars,
            ),
            ym_config=YMStrategyConfig(
                hard_risk_per_trade_dollars=self.ym_hard_risk_per_trade_dollars,
                daily_halt_loss_dollars=self.ym_daily_halt_loss_dollars,
            ),
            gold_config=GoldStrategyConfig(
                hard_risk_per_trade_dollars=self.gold_hard_risk_per_trade_dollars,
                daily_halt_loss_dollars=self.gold_daily_halt_loss_dollars,
                symbol=self.gold_symbol,
            ),
        )

    def load_instruments(self, config_dir: str | Path) -> dict[str, InstrumentMeta]:
        instruments = load_instruments(config_dir)
        required = {"NQ", "YM", self.gold_symbol}
        missing = sorted(symbol for symbol in required if symbol not in instruments)
        if missing:
            joined = ", ".join(missing)
            raise ValueError(f"corrected_v4 instruments.yaml missing required symbols: {joined}")
        return instruments


def load_corrected_v4_config(config_dir: str | Path) -> CorrectedV4Config:
    config_path = Path(config_dir) / "bot.yaml"
    if not config_path.exists():
        raise ValueError(f"Corrected V4 config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Corrected V4 config root must be a mapping: {config_path}")

    bot = payload.get("bot")
    if not isinstance(bot, dict):
        raise ValueError("bot.yaml: key 'bot' must be a mapping")
    runtime = payload.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise ValueError("bot.yaml: key 'runtime' must be a mapping when present")
    risk = payload.get("risk") or {}
    if not isinstance(risk, dict):
        raise ValueError("bot.yaml: key 'risk' must be a mapping when present")
    nq_risk = risk.get("nq") or {}
    ym_risk = risk.get("ym") or {}
    gold_risk = risk.get("gold") or {}
    if not isinstance(nq_risk, dict) or not isinstance(ym_risk, dict) or not isinstance(gold_risk, dict):
        raise ValueError("bot.yaml: risk.nq, risk.ym, and risk.gold must be mappings")

    raw_symbols = runtime.get("databento_symbols", bot.get("databento_symbols", ["NQ.v.0", "YM.v.0", "GC.v.0"]))
    if isinstance(raw_symbols, str):
        databento_symbols = tuple(symbol.strip() for symbol in raw_symbols.split(",") if symbol.strip())
    elif isinstance(raw_symbols, list):
        databento_symbols = tuple(str(symbol).strip() for symbol in raw_symbols if str(symbol).strip())
    else:
        raise ValueError("bot.yaml: databento_symbols must be a string or list when present")
    if not databento_symbols:
        raise ValueError("bot.yaml: databento_symbols must include at least one symbol")

    gold_symbol = str(bot.get("gold_symbol", "MGC")).strip().upper()
    return CorrectedV4Config(
        name=str(bot.get("name", "corrected_v4")),
        architecture=str(bot.get("architecture", "corrected_futures_orchestrator")),
        mode=str(bot.get("mode", "live_signals_only")),
        dataset=str(bot.get("dataset", "GLBX.MDP3")),
        schema=str(bot.get("schema", "ohlcv-1m")),
        stype_in=str(bot.get("stype_in", "continuous")),
        databento_symbols=databento_symbols,
        alert_tag=str(bot.get("alert_tag", "[CORR-V4]")),
        heartbeat_interval_hours=float(runtime.get("heartbeat_interval_hours", 4.0)),
        bars_stale_after_s=float(runtime.get("bars_stale_after_s", 180.0)),
        starting_equity=float(bot.get("starting_equity", 100_000.0)),
        nq_hard_risk_per_trade_dollars=float(nq_risk.get("hard_risk_per_trade_dollars", 750.0)),
        nq_daily_halt_loss_dollars=float(nq_risk.get("daily_halt_loss_dollars", 1_500.0)),
        ym_hard_risk_per_trade_dollars=float(ym_risk.get("hard_risk_per_trade_dollars", 500.0)),
        ym_daily_halt_loss_dollars=float(ym_risk.get("daily_halt_loss_dollars", 1_500.0)),
        gold_hard_risk_per_trade_dollars=float(gold_risk.get("hard_risk_per_trade_dollars", 400.0)),
        gold_daily_halt_loss_dollars=float(gold_risk.get("daily_halt_loss_dollars", 1_200.0)),
        gold_symbol=gold_symbol,
    )
