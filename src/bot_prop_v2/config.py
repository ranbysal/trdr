"""Config loader for the Prop V2 scaffold."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from bot_prop_v2.pipeline.signal_engine import RiskParameters


@dataclass(frozen=True, slots=True)
class PropV2Config:
    name: str
    architecture: str
    mode: str
    dataset: str
    schema: str
    stype_in: str
    alert_tag: str
    log_dirname: str
    report_dirname: str
    account_size: float
    max_risk_per_trade: float
    max_daily_loss: float
    max_concurrent: int
    min_rr_ratio: float
    partial_tp_r: float
    trail_activate_r: float
    breakeven_r: float
    atr_stop_multiplier: float
    fvg_fill_tolerance: float
    ob_displacement_atr: float
    ob_volume_ratio: float
    choch_body_atr: float
    min_gap_atr_ratio: float
    momentum_vol_ratio: float

    def build_risk_parameters(self) -> "RiskParameters":
        from bot_prop_v2.pipeline.signal_engine import RiskParameters

        return RiskParameters(
            account_size=self.account_size,
            max_risk_per_trade=self.max_risk_per_trade,
            max_daily_loss=self.max_daily_loss,
            max_concurrent=self.max_concurrent,
            min_rr_ratio=self.min_rr_ratio,
            partial_tp_r=self.partial_tp_r,
            trail_activate_r=self.trail_activate_r,
            breakeven_r=self.breakeven_r,
            atr_stop_multiplier=self.atr_stop_multiplier,
            fvg_fill_tolerance=self.fvg_fill_tolerance,
            ob_displacement_atr=self.ob_displacement_atr,
            ob_volume_ratio=self.ob_volume_ratio,
            choch_body_atr=self.choch_body_atr,
            min_gap_atr_ratio=self.min_gap_atr_ratio,
            momentum_vol_ratio=self.momentum_vol_ratio,
        )


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
    runtime = payload.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise ValueError("bot.yaml: key 'runtime' must be a mapping when present")
    risk = payload.get("risk") or {}
    if not isinstance(risk, dict):
        raise ValueError("bot.yaml: key 'risk' must be a mapping when present")
    return PropV2Config(
        name=str(bot.get("name", "prop_v2")),
        architecture=str(bot.get("architecture", "atr_normalized_smc")),
        mode=str(bot.get("mode", "scaffold")),
        dataset=str(bot.get("dataset", "GLBX.MDP3")),
        schema=str(bot.get("schema", "ohlcv-1m")),
        stype_in=str(bot.get("stype_in", "continuous")),
        alert_tag=str(bot.get("alert_tag", "[PROP-V2]")),
        log_dirname=str(runtime.get("log_dirname", "logs")),
        report_dirname=str(runtime.get("report_dirname", "reports")),
        account_size=float(risk.get("account_size", 150_000.0)),
        max_risk_per_trade=float(risk.get("max_risk_per_trade", 0.01)),
        max_daily_loss=float(risk.get("max_daily_loss", 0.02)),
        max_concurrent=int(risk.get("max_concurrent", 3)),
        min_rr_ratio=float(risk.get("min_rr_ratio", 2.5)),
        partial_tp_r=float(risk.get("partial_tp_r", 1.0)),
        trail_activate_r=float(risk.get("trail_activate_r", 1.5)),
        breakeven_r=float(risk.get("breakeven_r", 1.0)),
        atr_stop_multiplier=float(risk.get("atr_stop_multiplier", 1.25)),
        fvg_fill_tolerance=float(risk.get("fvg_fill_tolerance", 0.25)),
        ob_displacement_atr=float(risk.get("ob_displacement_atr", 1.5)),
        ob_volume_ratio=float(risk.get("ob_volume_ratio", 1.5)),
        choch_body_atr=float(risk.get("choch_body_atr", 0.5)),
        min_gap_atr_ratio=float(risk.get("min_gap_atr_ratio", 0.3)),
        momentum_vol_ratio=float(risk.get("momentum_vol_ratio", 1.5)),
    )
