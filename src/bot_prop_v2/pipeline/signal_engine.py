"""
================================================================================
  SIGNAL ENGINE — NQ / YM / SILVER / GOLD
  Funded Account: $100k–$150k
  Architecture: 6-Layer confluence-gated SMC + Mean Reversion + Momentum
  Author:  [YOUR NAME]
  Version: 1.0.0 skeleton
  Target:  Codex 5.4 — fill every TODO to complete the implementation
================================================================================

DIRECTORY LAYOUT (create these files as you expand):
  signal_engine.py        ← this file (core engine, start here)
  data_feed.py            ← TODO: broker/exchange data connector
  broker_interface.py     ← TODO: order execution adapter
  backtester.py           ← TODO: historical replay harness
  config.yaml             ← TODO: all tuneable params live here, not in code
  logs/                   ← auto-created by TradeLogger
  reports/                ← auto-created by TradeLogger

CODEX INSTRUCTIONS:
  1. Read every docstring and every TODO comment before writing a single line.
  2. Never hardcode a pip/tick value. Every threshold is expressed as ATR multiple.
  3. Every method that makes a trade decision must return a typed dataclass or None.
     No bare dicts escaping public method boundaries.
  4. Keep broker calls isolated in broker_interface.py. This file is pure logic.
  5. Run the built-in self-tests at the bottom before wiring to live data.
"""

from __future__ import annotations

import logging
import math
import csv
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from enum import Enum, auto
from typing import Optional
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Logging setup — every rejection reason is logged. Never silently skip.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("signal_engine")


# ===========================================================================
# SECTION 1 — ENUMS & CONSTANTS
# ===========================================================================

class Instrument(Enum):
    NQ     = "NQ"
    YM     = "YM"
    SILVER = "SILVER"
    GOLD   = "GOLD"


class Direction(Enum):
    LONG  = auto()
    SHORT = auto()


class SignalType(Enum):
    ICT_SMC        = "ict_smc"         # Order block + FVG + CHoCH
    MEAN_REVERSION = "mean_reversion"  # VWAP stretch + RSI div + BB
    MOMENTUM_BO    = "momentum_bo"     # Range expansion + volume surge + EMA cross


class SessionWindow(Enum):
    LONDON_OPEN   = "london_open"    # 03:00–05:00 EST
    NY_OPEN       = "ny_open"        # 09:30–11:00 EST
    LONDON_CLOSE  = "london_close"   # 10:00–11:00 EST
    DEAD_ZONE     = "dead_zone"      # 11:00–13:00 EST — NO TRADING
    AFTERNOON     = "afternoon"      # 13:00–16:00 EST
    CLOSED        = "closed"


# Per-instrument ATR scalar for adaptive swing lookback (tune in backtesting)
SWING_ATR_SCALAR: dict[Instrument, float] = {
    Instrument.NQ:     0.002,
    Instrument.YM:     0.003,
    Instrument.GOLD:   0.004,
    Instrument.SILVER: 0.005,
}

# Point value per full contract (USD per 1.0 point move)
POINT_VALUE: dict[Instrument, float] = {
    Instrument.NQ:     20.0,   # Micro NQ = $2.00 — set to micro values for funded acct
    Instrument.YM:     5.0,    # Micro YM = $0.50
    Instrument.GOLD:   10.0,   # Gold futures 100 oz × $0.10 tick
    Instrument.SILVER: 50.0,   # Silver futures 5000 oz
}

# Correlation pairs — do not hold both simultaneously in same direction
CORRELATED_PAIRS: list[tuple[Instrument, Instrument]] = [
    (Instrument.NQ, Instrument.YM),       # >0.90 correlation
    (Instrument.GOLD, Instrument.SILVER),  # >0.85 correlation
]


# ===========================================================================
# SECTION 2 — DATA STRUCTURES (dataclasses)
# All public method returns use these. No bare dicts leave a method boundary.
# ===========================================================================

@dataclass
class Candle:
    """Single OHLCV bar."""
    timestamp: datetime
    open:      float
    high:      float
    low:       float
    close:     float
    volume:    float
    instrument: Instrument

    @property
    def body_size(self) -> float:
        return abs(self.close - self.open)

    @property
    def total_range(self) -> float:
        return self.high - self.low

    @property
    def upper_wick(self) -> float:
        return self.high - max(self.open, self.close)

    @property
    def lower_wick(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def body_dominance(self) -> float:
        """Body size as fraction of total range. >0.70 = clean displacement."""
        if self.total_range == 0:
            return 0.0
        return self.body_size / self.total_range

    @property
    def is_bullish(self) -> bool:
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        return self.close < self.open


@dataclass
class SwingPoint:
    timestamp: datetime
    price:     float
    direction: Direction  # LONG = swing high, SHORT = swing low
    lookback:  int        # How many bars were used to confirm it


@dataclass
class FairValueGap:
    top:           float
    bottom:        float
    midpoint:      float
    invalidation:  float   # Price level that fully voids the FVG
    gap_size:      float
    gap_atr_ratio: float   # gap_size / ATR at formation time
    direction:     Direction
    formed_at:     datetime
    is_valid:      bool = True


@dataclass
class OrderBlock:
    ob_high:              float
    ob_low:               float
    ob_mid:               float
    invalidation:         float   # Close below ob_low = OB voided
    direction:            Direction
    displacement_strength: float  # displacement_body / ATR
    volume_ratio:         float   # displacement_volume / 20-bar vol MA
    formed_at:            datetime
    is_valid:             bool = True


@dataclass
class CHoCH:
    direction:       Direction   # Direction of the NEW trend (post-break)
    broken_level:    float
    break_candle:    Candle
    strength:        float       # break_body / ATR — higher = more conviction
    formed_at:       datetime


@dataclass
class StructureState:
    """Running market structure for one instrument on one timeframe."""
    instrument:    Instrument
    trend:         Optional[Direction]  # None = no clear trend
    swing_highs:   list[SwingPoint] = field(default_factory=list)
    swing_lows:    list[SwingPoint] = field(default_factory=list)
    last_choch:    Optional[CHoCH]  = None
    last_bos:      Optional[CHoCH]  = None  # Break of structure (trend continuation)


@dataclass
class HTFBias:
    instrument: Instrument
    direction:  Direction
    strength:   float           # 0.0–1.0 score
    reason:     str             # Human-readable string for the log
    is_premium: bool            # True = price in premium zone (short bias)
    is_discount: bool           # True = price in discount zone (long bias)


@dataclass
class Signal:
    """A fully validated, confluence-gated trade signal."""
    instrument:       Instrument
    direction:        Direction
    signal_type:      SignalType
    entry_price:      float
    stop_loss:        float
    take_profit_1:    float     # 1R target — take 50% off here
    take_profit_2:    float     # 2.5R target — trail remainder
    take_profit_3:    float     # 4R target — full runner exit
    risk_amount_usd:  float
    position_size:    float     # In contracts
    confluence_score: int       # How many of 5 gates passed (min 3 to fire)
    signal_type_name: str
    session:          SessionWindow
    formed_at:        datetime
    notes:            str = ""  # Audit trail — why this signal was generated


@dataclass
class RiskParameters:
    """Loaded from config.yaml at startup. Never hardcode these."""
    account_size:        float = 150_000.0
    max_risk_per_trade:  float = 0.01       # 1% of account
    max_daily_loss:      float = 0.02       # 2% daily drawdown limit
    max_concurrent:      int   = 3          # Max open positions
    min_rr_ratio:        float = 2.5        # Minimum reward:risk to fire signal
    partial_tp_r:        float = 1.0        # Take 50% off at 1R
    trail_activate_r:    float = 1.5        # Start trailing at 1.5R
    breakeven_r:         float = 1.0        # Move stop to BE at 1R
    atr_stop_multiplier: float = 1.25       # Stop = entry ± (ATR × this)
    fvg_fill_tolerance:  float = 0.25       # 25% of gap can be filled
    ob_displacement_atr: float = 1.5        # Min displacement body in ATR
    ob_volume_ratio:     float = 1.5        # Min volume vs 20-bar MA
    choch_body_atr:      float = 0.5        # Min CHoCH break body in ATR
    min_gap_atr_ratio:   float = 0.3        # FVG must be >= 0.3x ATR
    momentum_vol_ratio:  float = 1.5        # Breakout volume vs MA


# ===========================================================================
# SECTION 3 — INDICATOR CALCULATIONS
# Pure math. No side effects. All inputs explicit. All outputs typed.
# ===========================================================================

class Indicators:
    """
    Stateless indicator library.
    All methods are @staticmethod — no instance needed.
    Inputs are plain Python lists of floats (not pandas, not numpy).
    Codex: you may swap internals for numpy vectorisation but keep signatures.
    """

    @staticmethod
    def atr(candles: list[Candle], period: int = 14) -> float:
        """
        Average True Range over `period` bars.
        True Range = max(H-L, |H-prev_C|, |L-prev_C|)
        Returns 0.0 if insufficient data.
        """
        if len(candles) < period + 1:
            logger.warning("ATR: insufficient candles (%d < %d)", len(candles), period + 1)
            return 0.0

        true_ranges: list[float] = []
        for i in range(1, len(candles)):
            prev_close = candles[i - 1].close
            tr = max(
                candles[i].high - candles[i].low,
                abs(candles[i].high - prev_close),
                abs(candles[i].low  - prev_close),
            )
            true_ranges.append(tr)

        # Use the most recent `period` true ranges
        recent = true_ranges[-period:]
        return sum(recent) / len(recent)

    @staticmethod
    def ema(values: list[float], period: int) -> list[float]:
        """
        Exponential Moving Average.
        Returns list of same length as input (first `period-1` values are None).
        TODO: Codex — implement EMA calculation
        """
        if period <= 0:
            raise ValueError("EMA period must be positive")
        if not values:
            return []
        ema_values: list[Optional[float]] = [None] * len(values)
        if len(values) < period:
            return ema_values

        seed = sum(values[:period]) / period
        multiplier = 2.0 / (period + 1)
        ema_values[period - 1] = seed
        running = seed
        for i in range(period, len(values)):
            running = ((values[i] - running) * multiplier) + running
            ema_values[i] = running
        return ema_values

    @staticmethod
    def vwap(candles: list[Candle]) -> float:
        """
        Volume-Weighted Average Price for the current session.
        Reset at session open. Pass only session candles.
        TODO: Codex — implement VWAP (sum(typical_price × vol) / sum(vol))
        """
        total_volume = sum(c.volume for c in candles)
        if total_volume <= 0:
            return 0.0
        weighted = sum((((c.high + c.low + c.close) / 3.0) * c.volume) for c in candles)
        return weighted / total_volume

    @staticmethod
    def vwap_std_dev(candles: list[Candle], vwap_value: float, band: int = 2) -> tuple[float, float]:
        """
        Returns (upper_band, lower_band) at `band` standard deviations from VWAP.
        TODO: Codex — implement VWAP std dev bands
        """
        total_volume = sum(c.volume for c in candles)
        if total_volume <= 0:
            return vwap_value, vwap_value
        variance = sum(
            c.volume * ((((c.high + c.low + c.close) / 3.0) - vwap_value) ** 2)
            for c in candles
        ) / total_volume
        std = math.sqrt(max(variance, 0.0))
        return vwap_value + (std * band), vwap_value - (std * band)

    @staticmethod
    def rsi(candles: list[Candle], period: int = 14) -> float:
        """
        Relative Strength Index.
        TODO: Codex — implement Wilder's RSI (smoothed RS method)
        """
        if period <= 0 or len(candles) < period + 1:
            return 0.0

        closes = [c.close for c in candles]
        gains: list[float] = []
        losses: list[float] = []
        for i in range(1, len(closes)):
            change = closes[i] - closes[i - 1]
            gains.append(max(change, 0.0))
            losses.append(max(-change, 0.0))

        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        for i in range(period, len(gains)):
            avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
            avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period

        if avg_loss == 0:
            return 100.0 if avg_gain > 0 else 50.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def rsi_divergence(
        candles: list[Candle],
        rsi_values: list[float],
        lookback: int = 10
    ) -> Optional[Direction]:
        """
        Detects bullish or bearish RSI divergence over the last `lookback` bars.
        Bullish:  price makes lower low, RSI makes higher low  → return Direction.LONG
        Bearish:  price makes higher high, RSI makes lower high → return Direction.SHORT
        None if no divergence detected.
        TODO: Codex — implement divergence detection
        """
        if len(candles) < lookback or len(rsi_values) < lookback:
            return None

        recent_candles = candles[-lookback:]
        recent_rsi = rsi_values[-lookback:]
        valid = [(c, r) for c, r in zip(recent_candles, recent_rsi) if r is not None]
        if len(valid) < 4:
            return None

        lows = sorted(valid, key=lambda pair: pair[0].low)[:2]
        lows.sort(key=lambda pair: pair[0].timestamp)
        if lows[1][0].low < lows[0][0].low and lows[1][1] > lows[0][1]:
            return Direction.LONG

        highs = sorted(valid, key=lambda pair: pair[0].high, reverse=True)[:2]
        highs.sort(key=lambda pair: pair[0].timestamp)
        if highs[1][0].high > highs[0][0].high and highs[1][1] < highs[0][1]:
            return Direction.SHORT

        return None

    @staticmethod
    def volume_ma(candles: list[Candle], period: int = 20) -> float:
        """Simple moving average of volume over `period` bars."""
        if len(candles) < period:
            return 0.0
        recent = candles[-period:]
        return sum(c.volume for c in recent) / period

    @staticmethod
    def bollinger_bands(
        candles: list[Candle], period: int = 20, std_dev: float = 2.0
    ) -> tuple[float, float, float]:
        """
        Returns (upper_band, middle_band, lower_band).
        TODO: Codex — implement Bollinger Bands
        """
        if period <= 0 or len(candles) < period:
            return 0.0, 0.0, 0.0
        closes = [c.close for c in candles[-period:]]
        middle = sum(closes) / period
        variance = sum((c - middle) ** 2 for c in closes) / period
        std = math.sqrt(variance)
        return middle + (std * std_dev), middle, middle - (std * std_dev)

    @staticmethod
    def stochastic(
        candles: list[Candle], k_period: int = 14, d_period: int = 3
    ) -> tuple[float, float]:
        """
        Returns (%K, %D).
        TODO: Codex — implement Stochastic Oscillator
        """
        if k_period <= 0 or d_period <= 0 or len(candles) < k_period:
            return 0.0, 0.0

        def calc_k(window: list[Candle]) -> float:
            highest = max(c.high for c in window)
            lowest = min(c.low for c in window)
            if math.isclose(highest, lowest):
                return 50.0
            close = window[-1].close
            return ((close - lowest) / (highest - lowest)) * 100.0

        current_k = calc_k(candles[-k_period:])
        k_values = [current_k]
        history_needed = min(d_period - 1, len(candles) - k_period)
        for offset in range(1, history_needed + 1):
            start = len(candles) - k_period - offset
            end = len(candles) - offset
            k_values.append(calc_k(candles[start:end]))
        d_value = sum(k_values) / len(k_values)
        return current_k, d_value

    @staticmethod
    def swing_lookback(
        atr: float,
        price: float,
        instrument: Instrument,
        base: int = 5,
    ) -> int:
        """
        Adaptive swing detection lookback — scales with volatility.
        Higher ATR relative to price = wider lookback to avoid noise.
        Hard clamped: [3, 15] bars.
        """
        if price == 0:
            return base
        scalar = SWING_ATR_SCALAR[instrument]
        volatility_ratio = atr / price
        lookback = int(base + (volatility_ratio / scalar))
        return max(3, min(lookback, 15))


# ===========================================================================
# SECTION 4 — MARKET STRUCTURE ENGINE
# Tracks swing highs/lows, CHoCH, BOS per instrument per timeframe.
# ===========================================================================

class StructureEngine:
    """
    Maintains a StructureState and updates it as new candles arrive.
    One instance per (instrument × timeframe) pair.

    Usage:
        engine = StructureEngine(Instrument.NQ)
        for candle in historical_candles:
            engine.update(candle, atr)
        signal = engine.get_choch()
    """

    def __init__(self, instrument: Instrument):
        self.instrument = instrument
        self.state = StructureState(instrument=instrument, trend=None)
        self._log = logging.getLogger(f"structure.{instrument.value}")

    def update(self, candles: list[Candle], atr: float) -> StructureState:
        """
        Feed the latest candle window. Updates swing points and structure.
        Call this on every new closed candle.

        Args:
            candles: sliding window of recent candles (at least 31 bars)
            atr:     current ATR value (pre-calculated)

        Returns:
            Updated StructureState
        """
        if len(candles) < 5:
            return self.state

        lookback = Indicators.swing_lookback(
            atr=atr,
            price=candles[-1].close,
            instrument=self.instrument,
        )

        swing_high = self._detect_swing_high(candles, lookback)
        swing_low  = self._detect_swing_low(candles, lookback)

        if swing_high:
            self.state.swing_highs.append(swing_high)
            self._prune_swings()
            self._log.debug("New swing high: %.2f at %s", swing_high.price, swing_high.timestamp)

        if swing_low:
            self.state.swing_lows.append(swing_low)
            self._prune_swings()
            self._log.debug("New swing low: %.2f at %s", swing_low.price, swing_low.timestamp)

        self._update_trend()
        self._check_for_choch(candles, atr)

        return self.state

    def _detect_swing_high(
        self, candles: list[Candle], lookback: int
    ) -> Optional[SwingPoint]:
        """
        A swing high is the highest high with `lookback` lower highs
        on each side. We check the pivot bar at index -(lookback+1).

        Returns SwingPoint or None.
        TODO: Codex — implement pivot detection (see docstring above for logic)
        """
        if len(candles) < (lookback * 2) + 1:
            return None
        pivot_idx = len(candles) - lookback - 1
        pivot = candles[pivot_idx]
        start = pivot_idx - lookback
        end = pivot_idx + lookback + 1
        if start < 0 or end > len(candles):
            return None
        window = candles[start:end]
        if all(pivot.high > c.high for i, c in enumerate(window) if i != lookback):
            if self.state.swing_highs and self.state.swing_highs[-1].timestamp == pivot.timestamp:
                return None
            return SwingPoint(
                timestamp=pivot.timestamp,
                price=pivot.high,
                direction=Direction.LONG,
                lookback=lookback,
            )
        return None

    def _detect_swing_low(
        self, candles: list[Candle], lookback: int
    ) -> Optional[SwingPoint]:
        """
        Mirror of _detect_swing_high for lows.
        TODO: Codex — implement pivot low detection
        """
        if len(candles) < (lookback * 2) + 1:
            return None
        pivot_idx = len(candles) - lookback - 1
        pivot = candles[pivot_idx]
        start = pivot_idx - lookback
        end = pivot_idx + lookback + 1
        if start < 0 or end > len(candles):
            return None
        window = candles[start:end]
        if all(pivot.low < c.low for i, c in enumerate(window) if i != lookback):
            if self.state.swing_lows and self.state.swing_lows[-1].timestamp == pivot.timestamp:
                return None
            return SwingPoint(
                timestamp=pivot.timestamp,
                price=pivot.low,
                direction=Direction.SHORT,
                lookback=lookback,
            )
        return None

    def _update_trend(self) -> None:
        """
        Sets self.state.trend based on last two swing highs and swing lows.
        Uptrend  = HH + HL (each swing high/low higher than previous)
        Downtrend = LH + LL
        No trend (None) if mixed or insufficient swings.
        TODO: Codex — implement trend classification
        """
        if len(self.state.swing_highs) < 2 or len(self.state.swing_lows) < 2:
            self.state.trend = None
            return

        prev_high, last_high = self.state.swing_highs[-2], self.state.swing_highs[-1]
        prev_low, last_low = self.state.swing_lows[-2], self.state.swing_lows[-1]

        higher_high = last_high.price > prev_high.price
        higher_low = last_low.price > prev_low.price
        lower_high = last_high.price < prev_high.price
        lower_low = last_low.price < prev_low.price

        if higher_high and higher_low:
            self.state.trend = Direction.LONG
        elif lower_high and lower_low:
            self.state.trend = Direction.SHORT
        else:
            self.state.trend = None

    def _check_for_choch(self, candles: list[Candle], atr: float) -> None:
        """
        Checks the most recent closed candle for a CHoCH or BOS event.

        CHoCH (Change of Character) — first structural break against trend:
          In uptrend: close BELOW the most recent confirmed Higher Low
          In downtrend: close ABOVE the most recent confirmed Lower High

        BOS (Break of Structure) — structural break WITH trend:
          In uptrend: close ABOVE the most recent confirmed Higher High
          In downtrend: close BELOW the most recent confirmed Lower Low

        Conditions (ALL must be true to register):
          1. Candle body close past the level (not just wick)
          2. Body size > atr * CHOCH_BODY_ATR_THRESHOLD (0.5 default)
          3. Body dominance > 0.55 (not a doji/indecision candle)
          4. For CHoCH only: the PRIOR swing level is NOT also broken
             (if prior is broken too, it's a BOS continuation, not a CHoCH)

        Sets self.state.last_choch or self.state.last_bos on detection.
        TODO: Codex — implement this using the parameters above
        """
        if not candles or atr <= 0:
            return
        latest = candles[-1]
        if latest.body_size <= atr * RiskParameters().choch_body_atr:
            return
        if latest.body_dominance <= 0.55:
            return

        if self.state.trend == Direction.LONG and self.state.swing_lows and self.state.swing_highs:
            last_hl = self.state.swing_lows[-1]
            last_hh = self.state.swing_highs[-1]
            prior_hl = self.state.swing_lows[-2] if len(self.state.swing_lows) >= 2 else None
            if latest.close < last_hl.price:
                if prior_hl and latest.close < prior_hl.price:
                    self.state.last_bos = CHoCH(
                        direction=Direction.SHORT,
                        broken_level=last_hl.price,
                        break_candle=latest,
                        strength=latest.body_size / atr,
                        formed_at=latest.timestamp,
                    )
                else:
                    self.state.last_choch = CHoCH(
                        direction=Direction.SHORT,
                        broken_level=last_hl.price,
                        break_candle=latest,
                        strength=latest.body_size / atr,
                        formed_at=latest.timestamp,
                    )
            elif latest.close > last_hh.price:
                self.state.last_bos = CHoCH(
                    direction=Direction.LONG,
                    broken_level=last_hh.price,
                    break_candle=latest,
                    strength=latest.body_size / atr,
                    formed_at=latest.timestamp,
                )
        elif self.state.trend == Direction.SHORT and self.state.swing_highs and self.state.swing_lows:
            last_lh = self.state.swing_highs[-1]
            last_ll = self.state.swing_lows[-1]
            prior_lh = self.state.swing_highs[-2] if len(self.state.swing_highs) >= 2 else None
            if latest.close > last_lh.price:
                if prior_lh and latest.close > prior_lh.price:
                    self.state.last_bos = CHoCH(
                        direction=Direction.LONG,
                        broken_level=last_lh.price,
                        break_candle=latest,
                        strength=latest.body_size / atr,
                        formed_at=latest.timestamp,
                    )
                else:
                    self.state.last_choch = CHoCH(
                        direction=Direction.LONG,
                        broken_level=last_lh.price,
                        break_candle=latest,
                        strength=latest.body_size / atr,
                        formed_at=latest.timestamp,
                    )
            elif latest.close < last_ll.price:
                self.state.last_bos = CHoCH(
                    direction=Direction.SHORT,
                    broken_level=last_ll.price,
                    break_candle=latest,
                    strength=latest.body_size / atr,
                    formed_at=latest.timestamp,
                )

    def _prune_swings(self, max_keep: int = 10) -> None:
        """Keep only the most recent N swing points to avoid memory growth."""
        if len(self.state.swing_highs) > max_keep:
            self.state.swing_highs = self.state.swing_highs[-max_keep:]
        if len(self.state.swing_lows) > max_keep:
            self.state.swing_lows = self.state.swing_lows[-max_keep:]

    def get_trend(self) -> Optional[Direction]:
        return self.state.trend

    def get_latest_choch(self) -> Optional[CHoCH]:
        return self.state.last_choch

    def clear_choch(self) -> None:
        """Call this after a CHoCH has been consumed by a signal."""
        self.state.last_choch = None


# ===========================================================================
# SECTION 5 — SMC PATTERN DETECTORS
# FVG, Order Block detection with all objective gates from the architecture.
# ===========================================================================

class SMCDetector:
    """
    Detects Fair Value Gaps and Order Blocks with full objective validation.
    All thresholds come from RiskParameters — never hardcoded.

    Usage:
        detector = SMCDetector(params)
        fvg = detector.detect_fvg(last_three_candles, atr)
        ob  = detector.detect_order_block(ob_candle, displacement_candle,
                                           all_candles, atr, vol_ma)
    """

    def __init__(self, params: RiskParameters):
        self.params = params
        self._log = logging.getLogger("smc_detector")

    # ------------------------------------------------------------------
    # FAIR VALUE GAP
    # ------------------------------------------------------------------

    def detect_fvg(
        self,
        c1: Candle,
        c2: Candle,
        c3: Candle,
        atr: float,
    ) -> Optional[FairValueGap]:
        """
        Detects a Bullish or Bearish Fair Value Gap in the three-candle sequence.

        Bullish FVG:  c1.high < c3.low   (gap between C1's top and C3's bottom)
        Bearish FVG:  c1.low  > c3.high  (gap between C1's bottom and C3's top)

        Validation gates (ALL must pass):
          Gate 1: Gap size >= atr * min_gap_atr_ratio (not noise)
          Gate 2: C2 is a displacement candle — body_dominance > 0.65
          Gate 3: Gap not already fully mitigated (checked externally via is_fvg_valid)

        The returned FairValueGap.invalidation is the price level at which
        (fill_tolerance × gap_size) of the gap has been covered.
        A subsequent candle closing past invalidation voids the FVG.

        TODO: Codex — implement using the logic described above
        """
        if atr <= 0:
            return None

        direction: Optional[Direction] = None
        top = bottom = invalidation = 0.0

        if c1.high < c3.low:
            direction = Direction.LONG
            bottom = c1.high
            top = c3.low
            gap_size = top - bottom
            invalidation = top - (gap_size * self.params.fvg_fill_tolerance)
        elif c1.low > c3.high:
            direction = Direction.SHORT
            top = c1.low
            bottom = c3.high
            gap_size = top - bottom
            invalidation = bottom + (gap_size * self.params.fvg_fill_tolerance)
        else:
            return None

        if gap_size < atr * self.params.min_gap_atr_ratio:
            return None
        if c2.body_dominance <= 0.65:
            return None

        return FairValueGap(
            top=top,
            bottom=bottom,
            midpoint=(top + bottom) / 2.0,
            invalidation=invalidation,
            gap_size=gap_size,
            gap_atr_ratio=gap_size / atr,
            direction=direction,
            formed_at=c3.timestamp,
        )

    def is_fvg_valid(self, fvg: FairValueGap, candles_since: list[Candle]) -> bool:
        """
        Re-validates an existing FVG against subsequent price action.
        Returns False and marks fvg.is_valid=False if price has closed
        past fvg.invalidation level.
        TODO: Codex — implement validation check
        """
        for candle in candles_since:
            if fvg.direction == Direction.LONG and candle.close <= fvg.invalidation:
                fvg.is_valid = False
                return False
            if fvg.direction == Direction.SHORT and candle.close >= fvg.invalidation:
                fvg.is_valid = False
                return False
        return True

    # ------------------------------------------------------------------
    # ORDER BLOCK
    # ------------------------------------------------------------------

    def detect_order_block(
        self,
        ob_candle:           Candle,   # The last down-close candle before the move
        displacement_candle: Candle,   # The first big candle of the impulse
        all_candles:         list[Candle],
        atr:                 float,
        volume_ma:           float,
    ) -> Optional[OrderBlock]:
        """
        Validates and returns an OrderBlock if ALL five gates pass.

        Gate 1 — Displacement body > atr * ob_displacement_atr (1.5 default)
            Confirms institutional aggression. Drift moves are rejected.

        Gate 2 — Displacement candle body dominance > 0.70
            Upper/lower wick must each be < 30% of total range.
            Rejection wicks signal a contested move — not clean displacement.

        Gate 3 — Displacement candle volume > volume_ma * ob_volume_ratio (1.5x)
            No volume = no institutional participation = fake order block.

        Gate 4 — OB candle body dominance > 0.55
            The OB itself should not be an indecision/doji candle.

        Gate 5 — OB has NOT been previously mitigated (price tapped through ob_low)
            Check all candles AFTER ob_candle.timestamp. If any candle's low
            has already traded through the OB body, discard it.

        Returns OrderBlock with invalidation = ob_low for longs (ob_high for shorts).
        TODO: Codex — implement all five gates
        """
        if atr <= 0 or volume_ma <= 0:
            return None

        direction = Direction.LONG if displacement_candle.is_bullish else Direction.SHORT
        displacement_strength = displacement_candle.body_size / atr
        if displacement_strength < self.params.ob_displacement_atr:
            return None
        if displacement_candle.body_dominance <= 0.70:
            return None
        if (displacement_candle.volume / volume_ma) < self.params.ob_volume_ratio:
            return None
        if ob_candle.body_dominance <= 0.55:
            return None

        later_candles = [c for c in all_candles if c.timestamp > ob_candle.timestamp]
        ob_high = max(ob_candle.open, ob_candle.close)
        ob_low = min(ob_candle.open, ob_candle.close)
        if direction == Direction.LONG:
            mitigated = any(c.low < ob_low for c in later_candles)
            invalidation = ob_low
        else:
            mitigated = any(c.high > ob_high for c in later_candles)
            invalidation = ob_high
        if mitigated:
            return None

        return OrderBlock(
            ob_high=ob_high,
            ob_low=ob_low,
            ob_mid=(ob_high + ob_low) / 2.0,
            invalidation=invalidation,
            direction=direction,
            displacement_strength=displacement_strength,
            volume_ratio=displacement_candle.volume / volume_ma,
            formed_at=displacement_candle.timestamp,
        )

    def is_ob_valid(self, ob: OrderBlock, candles_since: list[Candle]) -> bool:
        """
        Re-checks an existing OB on each new candle.
        Invalidates if price closes beyond ob.invalidation.
        TODO: Codex — implement
        """
        for candle in candles_since:
            if ob.direction == Direction.LONG and candle.close <= ob.invalidation:
                ob.is_valid = False
                return False
            if ob.direction == Direction.SHORT and candle.close >= ob.invalidation:
                ob.is_valid = False
                return False
        return True


# ===========================================================================
# SECTION 6 — SESSION & HTF BIAS FILTERS
# ===========================================================================

class SessionFilter:
    """
    Determines the current trading session and whether the bot is allowed
    to trade. Hard blocks: dead zone, news window, outside market hours.
    """

    # EST session windows (hour, minute) tuples — (open_time, close_time)
    SESSION_WINDOWS: dict[SessionWindow, tuple[time, time]] = {
        SessionWindow.LONDON_OPEN:  (time(3, 0),  time(5, 0)),
        SessionWindow.NY_OPEN:      (time(9, 30), time(11, 0)),
        SessionWindow.LONDON_CLOSE: (time(10, 0), time(11, 0)),
        SessionWindow.DEAD_ZONE:    (time(11, 0), time(13, 0)),
        SessionWindow.AFTERNOON:    (time(13, 0), time(16, 0)),
    }

    # Instruments that are active in London open
    LONDON_INSTRUMENTS = {Instrument.GOLD, Instrument.SILVER}

    # Instruments that are active in NY open
    NY_INSTRUMENTS = {Instrument.NQ, Instrument.YM, Instrument.GOLD, Instrument.SILVER}

    def __init__(self):
        self._news_blackout_windows: list[tuple[datetime, datetime]] = []
        self._log = logging.getLogger("session_filter")

    def get_current_session(self, now: datetime) -> SessionWindow:
        """
        Returns the current SessionWindow for the given UTC datetime.
        TODO: Codex — convert `now` to EST, then match against SESSION_WINDOWS.
              Return SessionWindow.CLOSED if outside all defined windows.
        """
        est_now = now.astimezone(ZoneInfo("America/New_York"))
        est_time = est_now.time()
        for session, (start, end) in self.SESSION_WINDOWS.items():
            if start <= est_time < end:
                return session
        return SessionWindow.CLOSED

    def is_tradeable(self, instrument: Instrument, now: datetime) -> tuple[bool, str]:
        """
        Returns (True, reason) or (False, reason).

        Blocks trading when:
          - Current session is DEAD_ZONE
          - Current session is CLOSED
          - Within 15 minutes before or after a scheduled news event
          - Instrument not active in the current session
            (Gold/Silver do not trade NY equity hours by default —
             configure in config.yaml to override)

        TODO: Codex — implement all four checks, log the block reason
        """
        session = self.get_current_session(now)
        if session == SessionWindow.DEAD_ZONE:
            reason = "dead_zone"
            self._log.info("Trade blocked for %s: %s", instrument.value, reason)
            return False, reason
        if session == SessionWindow.CLOSED:
            reason = "session_closed"
            self._log.info("Trade blocked for %s: %s", instrument.value, reason)
            return False, reason

        for start, end in self._news_blackout_windows:
            buffer_start = start - timedelta(minutes=15)
            buffer_end = end + timedelta(minutes=15)
            if buffer_start <= now <= buffer_end:
                reason = "news_blackout"
                self._log.info("Trade blocked for %s: %s", instrument.value, reason)
                return False, reason

        allowed: set[Instrument]
        if session == SessionWindow.LONDON_OPEN:
            allowed = self.LONDON_INSTRUMENTS
        elif session in {SessionWindow.NY_OPEN, SessionWindow.LONDON_CLOSE, SessionWindow.AFTERNOON}:
            allowed = self.NY_INSTRUMENTS
        else:
            allowed = set()
        if instrument not in allowed:
            reason = f"{instrument.value}_inactive_in_{session.value}"
            self._log.info("Trade blocked for %s: %s", instrument.value, reason)
            return False, reason
        return True, "ok"

    def add_news_blackout(self, start: datetime, end: datetime) -> None:
        """Register a news blackout window (load from economic calendar)."""
        self._news_blackout_windows.append((start, end))
        self._log.info("News blackout added: %s → %s", start, end)

    def clear_old_blackouts(self, now: datetime) -> None:
        """Remove expired blackout windows."""
        self._news_blackout_windows = [
            (s, e) for s, e in self._news_blackout_windows if e > now
        ]


class HTFBiasEngine:
    """
    Computes the higher-timeframe directional bias for each instrument.
    Inputs: Daily and Weekly candle data.
    Output: HTFBias with direction, strength score, and premium/discount flag.

    The bot ONLY takes longs when HTF bias is LONG (or neutral).
    The bot ONLY takes shorts when HTF bias is SHORT (or neutral).
    Conflicting bias = no trade.
    """

    def __init__(self):
        self._log = logging.getLogger("htf_bias")

    def compute_bias(
        self,
        instrument:     Instrument,
        daily_candles:  list[Candle],
        weekly_candles: list[Candle],
        current_price:  float,
    ) -> HTFBias:
        """
        Computes HTF directional bias from three factors:

        Factor 1 — Daily trend (0.4 weight):
            Use StructureEngine on daily candles. HH+HL = bullish, LH+LL = bearish.

        Factor 2 — Weekly trend (0.4 weight):
            Same as above on weekly candles. Overrides daily if conflicting.

        Factor 3 — Premium / Discount (0.2 weight):
            Find the last significant swing range (swing_high – swing_low).
            Equilibrium = midpoint of that range.
            Price above equilibrium = Premium zone → SHORT bias.
            Price below equilibrium = Discount zone → LONG bias.
            This is the ICT premium/discount framework.

        Combine weighted scores into a final Direction and strength (0.0–1.0).

        TODO: Codex — implement all three factors and weighted combination.
              Use StructureEngine internally for trend detection.
              Return HTFBias with full reason string for the log.
        """
        def trend_score(candles: list[Candle], label: str) -> tuple[float, Optional[Direction], StructureState]:
            engine = StructureEngine(instrument)
            atr = Indicators.atr(candles, period=min(14, max(1, len(candles) - 1)))
            if atr <= 0:
                atr = max((candles[-1].high - candles[-1].low), 1e-9) if candles else 1e-9
            for i in range(5, len(candles) + 1):
                engine.update(candles[:i], atr)
            direction = engine.get_trend()
            score = 0.4 if direction is not None else 0.0
            return score, direction, engine.state

        daily_weight, daily_dir, daily_state = trend_score(daily_candles, "daily")
        weekly_weight, weekly_dir, weekly_state = trend_score(weekly_candles, "weekly")

        premium = discount = False
        pd_dir: Optional[Direction] = None
        equilibrium = self._find_equilibrium(weekly_state.swing_highs or daily_state.swing_highs,
                                             weekly_state.swing_lows or daily_state.swing_lows)
        pd_weight = 0.0
        if equilibrium is not None:
            if current_price > equilibrium:
                premium = True
                pd_dir = Direction.SHORT
                pd_weight = 0.2
            elif current_price < equilibrium:
                discount = True
                pd_dir = Direction.LONG
                pd_weight = 0.2

        long_score = 0.0
        short_score = 0.0
        if daily_dir == Direction.LONG:
            long_score += daily_weight
        elif daily_dir == Direction.SHORT:
            short_score += daily_weight
        if weekly_dir == Direction.LONG:
            long_score += weekly_weight
        elif weekly_dir == Direction.SHORT:
            short_score += weekly_weight
        if pd_dir == Direction.LONG:
            long_score += pd_weight
        elif pd_dir == Direction.SHORT:
            short_score += pd_weight

        if weekly_dir and daily_dir and weekly_dir != daily_dir:
            if weekly_dir == Direction.LONG:
                long_score += 0.1
                short_score = max(0.0, short_score - 0.1)
            else:
                short_score += 0.1
                long_score = max(0.0, long_score - 0.1)

        if long_score >= short_score:
            direction = Direction.LONG
            strength = min(1.0, long_score)
        else:
            direction = Direction.SHORT
            strength = min(1.0, short_score)

        if equilibrium is not None:
            reason = (
                f"daily={daily_dir.name if daily_dir else 'NONE'}, "
                f"weekly={weekly_dir.name if weekly_dir else 'NONE'}, "
                f"equilibrium={equilibrium:.2f}"
            )
        else:
            reason = (
                f"daily={daily_dir.name if daily_dir else 'NONE'}, "
                f"weekly={weekly_dir.name if weekly_dir else 'NONE'}, "
                "equilibrium=None"
            )
        return HTFBias(
            instrument=instrument,
            direction=direction,
            strength=strength,
            reason=reason,
            is_premium=premium,
            is_discount=discount,
        )

    def _find_equilibrium(
        self, swing_highs: list[SwingPoint], swing_lows: list[SwingPoint]
    ) -> Optional[float]:
        """
        Returns the midpoint between the most recent significant swing high
        and swing low, or None if insufficient data.
        TODO: Codex — implement
        """
        if not swing_highs or not swing_lows:
            return None
        high = swing_highs[-1].price
        low = swing_lows[-1].price
        return (high + low) / 2.0


# ===========================================================================
# SECTION 7 — SIGNAL GENERATORS
# Three independent strategies. Each returns Signal or None.
# ===========================================================================

class ICTSignalGenerator:
    """
    Strategy 1: ICT / Smart Money Concepts
    Primary instruments: NQ, YM
    Timeframe: 1M–5M entry, 15M structure, 1H HTF

    Signal fires when ALL of the following are present:
      1. HTF bias aligns with trade direction
      2. CHoCH detected on the 15M (structure confirmation)
      3. Valid, unmitigated Order Block in the direction of HTF bias
      4. Price is currently inside or returning to the Order Block range
      5. A Bullish/Bearish FVG exists between the OB and the CHoCH origin
      6. Entry is within an active kill zone session
      7. Confluence score >= 3 of 5 (confluence gate check)
    """

    def __init__(self, params: RiskParameters, smc: SMCDetector):
        self.params = params
        self.smc    = smc
        self._log   = logging.getLogger("signal.ict")

    def generate(
        self,
        instrument:    Instrument,
        candles_1m:    list[Candle],
        candles_15m:   list[Candle],
        structure:     StructureState,
        htf_bias:      HTFBias,
        session:       SessionWindow,
        atr:           float,
        volume_ma:     float,
    ) -> Optional[Signal]:
        """
        Main entry point. Evaluates all conditions and returns a Signal or None.

        Step 1: Check HTF bias direction. If None or conflicting → return None.
        Step 2: Check for recent CHoCH on structure. If none → return None.
        Step 3: Find the most recent valid OB in bias direction → validate.
        Step 4: Check if current price is at OB. Price must be within OB range.
        Step 5: Check for FVG confirmation (optional but raises confluence score).
        Step 6: Count confluence score. Fire only if score >= 3.
        Step 7: Build Signal with RiskEngine.size_position() for exact sizing.

        Log the rejection reason at every step that returns None.
        TODO: Codex — implement step by step. Follow the order above exactly.
        """
        if htf_bias.direction not in {Direction.LONG, Direction.SHORT}:
            self._log.info("%s ICT reject: no HTF bias", instrument.value)
            return None
        choch = structure.last_choch
        if choch is None:
            self._log.info("%s ICT reject: no CHoCH", instrument.value)
            return None
        if choch.direction != htf_bias.direction:
            self._log.info("%s ICT reject: CHoCH conflicts with HTF bias", instrument.value)
            return None
        if len(candles_15m) < 2:
            self._log.info("%s ICT reject: insufficient 15m candles", instrument.value)
            return None

        direction = htf_bias.direction
        ob_candle = candles_15m[-2]
        displacement = candles_15m[-1]
        if direction == Direction.LONG and ob_candle.close >= ob_candle.open:
            self._log.info("%s ICT reject: latest OB seed is not bearish", instrument.value)
            return None
        if direction == Direction.SHORT and ob_candle.close <= ob_candle.open:
            self._log.info("%s ICT reject: latest OB seed is not bullish", instrument.value)
            return None

        ob = self.smc.detect_order_block(ob_candle, displacement, candles_15m, atr, volume_ma)
        if ob is None or ob.direction != direction:
            self._log.info("%s ICT reject: no valid OB", instrument.value)
            return None

        current_price = candles_1m[-1].close if candles_1m else candles_15m[-1].close
        at_key_level = ob.ob_low <= current_price <= ob.ob_high
        if not at_key_level:
            self._log.info("%s ICT reject: price not inside OB", instrument.value)
            return None

        fvg = None
        if len(candles_15m) >= 3:
            fvg = self.smc.detect_fvg(candles_15m[-3], candles_15m[-2], candles_15m[-1], atr)
            if fvg and not self.smc.is_fvg_valid(fvg, []):
                fvg = None

        htf_aligned = True
        volume_confirm = displacement.volume > volume_ma if volume_ma > 0 else False
        in_session = session != SessionWindow.CLOSED and session != SessionWindow.DEAD_ZONE
        fvg_present = fvg is not None and fvg.direction == direction
        confluence = self._score_confluence(
            htf_aligned=htf_aligned,
            at_key_level=at_key_level,
            volume_confirm=volume_confirm,
            in_session=in_session,
            fvg_present=fvg_present,
        )
        if confluence < 3:
            self._log.info("%s ICT reject: confluence=%d", instrument.value, confluence)
            return None

        stop_price = ob.invalidation - (atr * 0.1) if direction == Direction.LONG else ob.invalidation + (atr * 0.1)
        contracts, risk_usd = RiskEngine(self.params).size_position(
            instrument=instrument,
            entry_price=current_price,
            stop_price=stop_price,
            atr=atr,
            direction=direction,
        )
        tp1, tp2, tp3 = RiskEngine(self.params).calculate_targets(current_price, stop_price, direction)
        return Signal(
            instrument=instrument,
            direction=direction,
            signal_type=SignalType.ICT_SMC,
            entry_price=current_price,
            stop_loss=stop_price,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            risk_amount_usd=risk_usd,
            position_size=contracts,
            confluence_score=confluence,
            signal_type_name=SignalType.ICT_SMC.value,
            session=session,
            formed_at=candles_15m[-1].timestamp,
            notes=f"CHoCH {choch.direction.name}; OB {ob.ob_low:.2f}-{ob.ob_high:.2f}",
        )

    def _score_confluence(
        self,
        htf_aligned:    bool,
        at_key_level:   bool,
        volume_confirm: bool,
        in_session:     bool,
        fvg_present:    bool,
    ) -> int:
        """
        Counts how many of the 5 confluence conditions are True.
        Minimum 3 required to fire. Returns raw count.
        """
        return sum([htf_aligned, at_key_level, volume_confirm, in_session, fvg_present])


class MeanReversionSignalGenerator:
    """
    Strategy 2: VWAP Mean Reversion
    Primary instruments: GOLD, SILVER
    Secondary: NQ, YM (afternoon session only)
    Timeframe: 5M entry, 15M confirmation

    Signal fires when:
      1. Price is >= 2 standard deviations from VWAP (stretched)
      2. RSI divergence present (price extreme ≠ RSI extreme)
      3. Stochastic in oversold (<20) for longs / overbought (>80) for shorts
      4. HTF bias is neutral or aligned (does not trade against strong HTF trend)
      5. Price at a known S/R level (previous day high/low, round number, OB)
    """

    def __init__(self, params: RiskParameters):
        self.params = params
        self._log   = logging.getLogger("signal.mean_rev")

    def generate(
        self,
        instrument:   Instrument,
        candles_5m:   list[Candle],
        htf_bias:     HTFBias,
        session:      SessionWindow,
        atr:          float,
        vwap:         float,
        vwap_upper:   float,
        vwap_lower:   float,
        rsi:          float,
        stoch_k:      float,
        stoch_d:      float,
    ) -> Optional[Signal]:
        """
        TODO: Codex — implement mean reversion signal using conditions above.
              Entry price = current close (market order at bar close).
              Stop = beyond the extreme wick by 0.5× ATR.
              Target 1 = VWAP midline.
              Target 2 = VWAP opposite band.
        """
        if not candles_5m:
            return None
        latest = candles_5m[-1]
        prices = [c.close for c in candles_5m]
        rsi_series = []
        for i in range(2, len(candles_5m) + 1):
            rsi_series.append(Indicators.rsi(candles_5m[:i]))
        divergence = Indicators.rsi_divergence(candles_5m, rsi_series, lookback=min(10, len(candles_5m)))
        direction: Optional[Direction] = None
        if latest.close <= vwap_lower and divergence == Direction.LONG and stoch_k < 20 and stoch_d < 25:
            direction = Direction.LONG
        elif latest.close >= vwap_upper and divergence == Direction.SHORT and stoch_k > 80 and stoch_d > 75:
            direction = Direction.SHORT
        else:
            self._log.info("%s mean-rev reject: stretch/divergence/stoch missing", instrument.value)
            return None

        if htf_bias.direction != direction and htf_bias.strength > 0.65:
            self._log.info("%s mean-rev reject: strong HTF conflict", instrument.value)
            return None

        if direction == Direction.LONG:
            stop_price = latest.low - (0.5 * atr)
            tp1 = vwap
            tp2 = vwap_upper
        else:
            stop_price = latest.high + (0.5 * atr)
            tp1 = vwap
            tp2 = vwap_lower
        tp3 = tp2
        contracts, risk_usd = RiskEngine(self.params).size_position(
            instrument=instrument,
            entry_price=latest.close,
            stop_price=stop_price,
            atr=atr,
            direction=direction,
        )
        return Signal(
            instrument=instrument,
            direction=direction,
            signal_type=SignalType.MEAN_REVERSION,
            entry_price=latest.close,
            stop_loss=stop_price,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            risk_amount_usd=risk_usd,
            position_size=contracts,
            confluence_score=4,
            signal_type_name=SignalType.MEAN_REVERSION.value,
            session=session,
            formed_at=latest.timestamp,
            notes=f"VWAP stretch with RSI={rsi:.1f} stoch=({stoch_k:.1f},{stoch_d:.1f})",
        )


class MomentumSignalGenerator:
    """
    Strategy 3: Range Expansion / Momentum Breakout
    All instruments — only in clear trending regimes.
    Timeframe: 15M entry, 1H confirmation

    Signal fires when:
      1. HTF bias is strong (strength > 0.65) — no weak trend breakouts
      2. Price has been in compression (BB squeeze: bandwidth < 20-period low)
      3. Breakout bar body > 1.5× ATR (not a fake break)
      4. Volume on breakout bar > volume_ma * momentum_vol_ratio (1.5x)
      5. 9 EMA crossed above/below 21 EMA in the last 3 bars
      6. Not trading into a major HTF resistance/supply zone
    """

    def __init__(self, params: RiskParameters):
        self.params = params
        self._log   = logging.getLogger("signal.momentum")

    def generate(
        self,
        instrument:  Instrument,
        candles_15m: list[Candle],
        htf_bias:    HTFBias,
        session:     SessionWindow,
        atr:         float,
        volume_ma:   float,
        bb_upper:    float,
        bb_lower:    float,
        bb_width:    float,
        ema_9:       float,
        ema_21:      float,
    ) -> Optional[Signal]:
        """
        TODO: Codex — implement momentum breakout signal using conditions above.
              Entry = breakout candle close.
              Stop = breakout candle low (for longs) or high (for shorts).
              Target 1 = stop_distance × 1.0 (1R)
              Target 2 = stop_distance × 2.5 (2.5R)
              Target 3 = stop_distance × 4.0 (4R)
        """
        if len(candles_15m) < 21:
            return None
        if htf_bias.strength <= 0.65:
            self._log.info("%s momentum reject: weak HTF bias", instrument.value)
            return None

        latest = candles_15m[-1]
        close_series = [c.close for c in candles_15m]
        bb_widths: list[float] = []
        for i in range(20, len(candles_15m) + 1):
            upper, _, lower = Indicators.bollinger_bands(candles_15m[:i], 20, 2.0)
            bb_widths.append(upper - lower)
        prior_low = min(bb_widths[:-1]) if len(bb_widths) > 1 else bb_width
        compression = bb_width <= prior_low
        if not compression:
            self._log.info("%s momentum reject: no squeeze", instrument.value)
            return None

        breakout_strength = latest.body_size / atr if atr > 0 else 0.0
        if breakout_strength <= 1.5:
            self._log.info("%s momentum reject: breakout too small", instrument.value)
            return None
        if volume_ma <= 0 or latest.volume <= volume_ma * self.params.momentum_vol_ratio:
            self._log.info("%s momentum reject: volume missing", instrument.value)
            return None

        ema9_series = Indicators.ema(close_series, 9)
        ema21_series = Indicators.ema(close_series, 21)
        direction = htf_bias.direction
        crossed = False
        for i in range(max(1, len(close_series) - 3), len(close_series)):
            if ema9_series[i] is None or ema21_series[i] is None or ema9_series[i - 1] is None or ema21_series[i - 1] is None:
                continue
            if direction == Direction.LONG and ema9_series[i - 1] <= ema21_series[i - 1] and ema9_series[i] > ema21_series[i]:
                crossed = True
            if direction == Direction.SHORT and ema9_series[i - 1] >= ema21_series[i - 1] and ema9_series[i] < ema21_series[i]:
                crossed = True
        if not crossed:
            self._log.info("%s momentum reject: no EMA cross", instrument.value)
            return None

        stop_price = latest.low if direction == Direction.LONG else latest.high
        contracts, risk_usd = RiskEngine(self.params).size_position(
            instrument=instrument,
            entry_price=latest.close,
            stop_price=stop_price,
            atr=atr,
            direction=direction,
        )
        tp1, tp2, tp3 = RiskEngine(self.params).calculate_targets(latest.close, stop_price, direction)
        return Signal(
            instrument=instrument,
            direction=direction,
            signal_type=SignalType.MOMENTUM_BO,
            entry_price=latest.close,
            stop_loss=stop_price,
            take_profit_1=tp1,
            take_profit_2=tp2,
            take_profit_3=tp3,
            risk_amount_usd=risk_usd,
            position_size=contracts,
            confluence_score=5,
            signal_type_name=SignalType.MOMENTUM_BO.value,
            session=session,
            formed_at=latest.timestamp,
            notes=f"Breakout width={bb_width:.4f} EMA9={ema_9:.2f} EMA21={ema_21:.2f}",
        )


# ===========================================================================
# SECTION 8 — RISK ENGINE
# Position sizing, daily loss tracking, correlation exposure management.
# ===========================================================================

class RiskEngine:
    """
    Handles all money management for the funded account.
    This class is the last line of defence before an order hits the broker.

    Key rules:
      - Max 1% risk per trade
      - Max 2% daily loss (hard halt)
      - Max 3 concurrent positions
      - Never hold correlated positions in the same direction
      - Stop is always ATR-based (never fixed ticks)
    """

    def __init__(self, params: RiskParameters):
        self.params          = params
        self.daily_pnl       = 0.0
        self.open_positions: dict[Instrument, Direction] = {}
        self._log            = logging.getLogger("risk_engine")

    def size_position(
        self,
        instrument:   Instrument,
        entry_price:  float,
        stop_price:   float,
        atr:          float,
        direction:    Direction,
    ) -> tuple[float, float]:
        """
        Calculates position size in contracts and exact USD risk.

        Formula:
            risk_usd       = account_size × max_risk_per_trade
            stop_distance  = abs(entry_price - stop_price)  [in points]
            risk_per_lot   = stop_distance × point_value[instrument]
            contracts      = floor(risk_usd / risk_per_lot)

        Hard minimum: 1 contract.
        Hard maximum: risk_usd must not exceed max_risk_per_trade × account_size.

        Returns:
            (contracts, actual_risk_usd)

        TODO: Codex — implement using the formula above.
              Log the sizing breakdown at DEBUG level.
        """
        risk_usd = self.params.account_size * self.params.max_risk_per_trade
        stop_distance = abs(entry_price - stop_price)
        if stop_distance <= 0:
            fallback = max(atr * self.params.atr_stop_multiplier, 1e-9)
            stop_distance = fallback
        risk_per_lot = stop_distance * POINT_VALUE[instrument]
        if risk_per_lot <= 0:
            return 1.0, 0.0
        contracts = max(1, math.floor(risk_usd / risk_per_lot))
        actual_risk_usd = min(risk_usd, contracts * risk_per_lot)
        self._log.debug(
            "Position sizing %s %s: entry=%.2f stop=%.2f dist=%.4f risk/lot=%.2f contracts=%d actual_risk=%.2f",
            instrument.value, direction.name, entry_price, stop_price, stop_distance, risk_per_lot, contracts, actual_risk_usd
        )
        return float(contracts), actual_risk_usd

    def calculate_targets(
        self,
        entry_price:   float,
        stop_price:    float,
        direction:     Direction,
    ) -> tuple[float, float, float]:
        """
        Returns (tp1, tp2, tp3) based on R-multiples from params.
            tp1 = entry ± (stop_distance × partial_tp_r)       default 1R
            tp2 = entry ± (stop_distance × min_rr_ratio)       default 2.5R
            tp3 = entry ± (stop_distance × 4.0)                fixed 4R runner

        TODO: Codex — implement. Direction determines ± sign.
        """
        stop_distance = abs(entry_price - stop_price)
        sign = 1 if direction == Direction.LONG else -1
        tp1 = entry_price + (sign * stop_distance * self.params.partial_tp_r)
        tp2 = entry_price + (sign * stop_distance * self.params.min_rr_ratio)
        tp3 = entry_price + (sign * stop_distance * 4.0)
        return tp1, tp2, tp3

    def can_take_trade(
        self,
        instrument: Instrument,
        direction:  Direction,
    ) -> tuple[bool, str]:
        """
        Final pre-flight check before a signal is passed to the broker.

        Blocks if ANY of the following:
          1. Daily PnL <= -(account_size × max_daily_loss)  → halt for the day
          2. len(open_positions) >= max_concurrent           → max positions reached
          3. Instrument already has an open position         → no doubling up
          4. A correlated instrument has a position in the SAME direction
             e.g. long NQ already open → block long YM

        Returns (True, "ok") or (False, "reason string")
        TODO: Codex — implement all four checks
        """
        daily_loss_limit = self.params.account_size * self.params.max_daily_loss
        if self.daily_pnl <= -daily_loss_limit:
            return False, "daily_loss_limit_hit"
        if len(self.open_positions) >= self.params.max_concurrent:
            return False, "max_concurrent_positions"
        if instrument in self.open_positions:
            return False, "instrument_already_open"
        for left, right in CORRELATED_PAIRS:
            if instrument == left and self.open_positions.get(right) == direction:
                return False, f"correlated_{right.value}_{direction.name.lower()}_open"
            if instrument == right and self.open_positions.get(left) == direction:
                return False, f"correlated_{left.value}_{direction.name.lower()}_open"
        return True, "ok"

    def update_pnl(self, pnl_usd: float, instrument: Instrument) -> None:
        """
        Called by the broker interface when a position closes.
        Updates daily_pnl and removes instrument from open_positions.
        Logs a warning if daily loss limit is approaching (>= 75% of limit).
        """
        self.daily_pnl += pnl_usd
        if instrument in self.open_positions:
            del self.open_positions[instrument]

        daily_loss_limit = self.params.account_size * self.params.max_daily_loss
        if self.daily_pnl <= -(daily_loss_limit * 0.75):
            self._log.warning(
                "Approaching daily loss limit: PnL=%.2f / Limit=%.2f",
                self.daily_pnl, -daily_loss_limit,
            )

        if self.daily_pnl <= -daily_loss_limit:
            self._log.critical(
                "DAILY LOSS LIMIT HIT. PnL=%.2f. No further trades today.", self.daily_pnl
            )

    def register_position(self, instrument: Instrument, direction: Direction) -> None:
        """Call this when a position is confirmed open by the broker."""
        self.open_positions[instrument] = direction

    def reset_daily(self) -> None:
        """Call at the start of each new trading day (midnight EST)."""
        self._log.info("Daily PnL reset. Previous day: %.2f", self.daily_pnl)
        self.daily_pnl = 0.0


# ===========================================================================
# SECTION 9 — TRADE MANAGER
# Handles open position lifecycle: partial TP, trail stop, break-even, time exit.
# ===========================================================================

@dataclass
class OpenTrade:
    """Represents an active position being managed."""
    instrument:    Instrument
    direction:     Direction
    entry_price:   float
    stop_loss:     float
    take_profit_1: float
    take_profit_2: float
    take_profit_3: float
    contracts:     float
    risk_amount:   float
    opened_at:     datetime
    atr_at_entry:  float
    partial_taken: bool = False
    breakeven_set: bool = False
    trail_active:  bool = False


class TradeManager:
    """
    Manages open trades through their lifecycle.
    Called on every new candle close for each open trade.

    Rules:
      - At 1R profit:     move stop to break-even
      - At 1R profit:     close 50% of position (partial TP)
      - At 1.5R profit:   activate trailing stop (trail by 1× ATR)
      - At 2.5R profit:   close next 25% (second partial)
      - At 4R profit:     close remaining position (full exit)
      - Time exit:        if position has been open for > max_bars with no progress,
                          exit at market (prevents dead capital)
    """

    MAX_BARS_STALL = 40   # Candles before time-based exit triggers

    def __init__(self, params: RiskParameters):
        self.params = params
        self._log   = logging.getLogger("trade_manager")

    def update(
        self,
        trade:  OpenTrade,
        candle: Candle,
        atr:    float,
    ) -> dict:
        """
        Evaluates the current candle against the open trade and returns
        an action dict describing what the broker should do.

        Return format:
        {
            "action":       "none" | "partial_close" | "move_stop" | "full_close",
            "close_pct":    float,   # 0.0–1.0 fraction of position to close
            "new_stop":     float,   # New stop price (if action is move_stop)
            "reason":       str,     # Audit trail
        }

        TODO: Codex — implement lifecycle management using the rules above.
              The action dict is consumed by broker_interface.py.
              Always log the action taken at DEBUG level.
        """
        action = {"action": "none", "close_pct": 0.0, "new_stop": trade.stop_loss, "reason": ""}
        current_r = self._current_r(trade, candle.close)

        if (trade.direction == Direction.LONG and candle.low <= trade.stop_loss) or (
            trade.direction == Direction.SHORT and candle.high >= trade.stop_loss
        ):
            action = {"action": "full_close", "close_pct": 1.0, "new_stop": trade.stop_loss, "reason": "stop_hit"}
        elif current_r >= 4.0:
            action = {"action": "full_close", "close_pct": 1.0, "new_stop": trade.stop_loss, "reason": "tp3_hit"}
        elif current_r >= self.params.min_rr_ratio and trade.partial_taken:
            action = {"action": "partial_close", "close_pct": 0.25, "new_stop": trade.stop_loss, "reason": "tp2_hit"}
        elif current_r >= self.params.trail_activate_r:
            new_stop = self._trail_stop(trade, candle.close, atr)
            trade.trail_active = True
            trade.stop_loss = new_stop
            action = {"action": "move_stop", "close_pct": 0.0, "new_stop": new_stop, "reason": "trail_active"}
        elif current_r >= self.params.breakeven_r and not trade.breakeven_set:
            trade.breakeven_set = True
            trade.stop_loss = trade.entry_price
            action = {"action": "move_stop", "close_pct": 0.0, "new_stop": trade.entry_price, "reason": "breakeven"}
        elif current_r >= self.params.partial_tp_r and not trade.partial_taken:
            trade.partial_taken = True
            action = {"action": "partial_close", "close_pct": 0.5, "new_stop": trade.stop_loss, "reason": "tp1_hit"}
        elif candle.timestamp >= trade.opened_at + timedelta(minutes=self.MAX_BARS_STALL):
            action = {"action": "full_close", "close_pct": 1.0, "new_stop": trade.stop_loss, "reason": "time_exit"}

        self._log.debug("Trade action %s: %s", trade.instrument.value, action)
        return action

    def _current_r(self, trade: OpenTrade, current_price: float) -> float:
        """
        Returns current R-multiple. 1.0 = 1R in profit, -1.0 = stopped out.
        TODO: Codex — implement. Account for direction (LONG vs SHORT).
        """
        risk = abs(trade.entry_price - trade.stop_loss)
        if risk <= 0:
            return 0.0
        if trade.direction == Direction.LONG:
            return (current_price - trade.entry_price) / risk
        return (trade.entry_price - current_price) / risk

    def _trail_stop(self, trade: OpenTrade, current_price: float, atr: float) -> float:
        """
        Returns the new trailing stop price.
        Trail distance = 1.0 × ATR behind current price in direction of trade.
        Stop only moves in the profitable direction — never against.
        TODO: Codex — implement
        """
        if trade.direction == Direction.LONG:
            return max(trade.stop_loss, current_price - atr)
        return min(trade.stop_loss, current_price + atr)


# ===========================================================================
# SECTION 10 — TRADE LOGGER
# Every signal considered, every rejection, every trade — logged to disk.
# ===========================================================================

class TradeLogger:
    """
    Structured logging for backtesting analysis and live performance review.
    Writes CSV rows so you can open the log in Excel/Python for analysis.

    Log files:
      logs/signals_YYYYMMDD.csv    — every signal considered (fired + rejected)
      logs/trades_YYYYMMDD.csv     — every trade opened and closed
      logs/rejections_YYYYMMDD.csv — every rejected signal with reason
    """

    SIGNAL_COLUMNS = [
        "timestamp", "instrument", "direction", "signal_type",
        "confluence_score", "htf_aligned", "at_key_level", "vol_confirm",
        "in_session", "pattern_present", "fired", "rejection_reason",
    ]

    TRADE_COLUMNS = [
        "opened_at", "closed_at", "instrument", "direction",
        "entry", "stop", "tp1", "tp2", "tp3",
        "contracts", "risk_usd", "pnl_usd", "r_multiple",
        "exit_reason",
    ]

    def __init__(self, log_dir: str = "logs"):
        import os
        os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self._log = logging.getLogger("trade_logger")

    def log_signal_considered(
        self,
        signal:           Optional[Signal],
        instrument:       Instrument,
        direction:        Direction,
        signal_type:      SignalType,
        confluence_score: int,
        rejection_reason: str,
        timestamp:        datetime,
    ) -> None:
        """
        Logs every signal evaluation — whether it fired or not.
        This is your most important debugging tool.
        TODO: Codex — implement CSV append to signals_YYYYMMDD.csv
        """
        file_path = f"{self.log_dir}/signals_{timestamp.strftime('%Y%m%d')}.csv"
        file_exists = False
        try:
            with open(file_path, "r", newline=""):
                file_exists = True
        except FileNotFoundError:
            file_exists = False

        with open(file_path, "a", newline="") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(self.SIGNAL_COLUMNS)
            writer.writerow([
                timestamp.isoformat(),
                instrument.value,
                direction.name,
                signal_type.value,
                confluence_score,
                int(signal is not None and signal.direction == direction),
                int(signal is not None),
                "",
                int(signal is not None and signal.session not in {SessionWindow.CLOSED, SessionWindow.DEAD_ZONE}),
                int(signal is not None),
                int(signal is not None),
                rejection_reason,
            ])

    def log_trade_closed(
        self,
        trade:       OpenTrade,
        close_price: float,
        close_time:  datetime,
        pnl_usd:     float,
        r_multiple:  float,
        exit_reason: str,
    ) -> None:
        """
        TODO: Codex — implement CSV append to trades_YYYYMMDD.csv
        """
        file_path = f"{self.log_dir}/trades_{close_time.strftime('%Y%m%d')}.csv"
        file_exists = False
        try:
            with open(file_path, "r", newline=""):
                file_exists = True
        except FileNotFoundError:
            file_exists = False

        with open(file_path, "a", newline="") as fh:
            writer = csv.writer(fh)
            if not file_exists:
                writer.writerow(self.TRADE_COLUMNS)
            writer.writerow([
                trade.opened_at.isoformat(),
                close_time.isoformat(),
                trade.instrument.value,
                trade.direction.name,
                trade.entry_price,
                trade.stop_loss,
                trade.take_profit_1,
                trade.take_profit_2,
                trade.take_profit_3,
                trade.contracts,
                trade.risk_amount,
                pnl_usd,
                r_multiple,
                exit_reason,
            ])


# ===========================================================================
# SECTION 11 — MAIN SIGNAL ENGINE (ORCHESTRATOR)
# Wires all components together. This is what your main loop calls.
# ===========================================================================

class SignalEngine:
    """
    Top-level orchestrator. One instance per trading session.

    Initialisation:
        params  = RiskParameters()        # Load from config.yaml in production
        engine  = SignalEngine(params)

    On each new candle (from data_feed.py):
        signal = engine.on_candle(candle, historical_candles, daily_candles, weekly_candles)
        if signal:
            broker_interface.place_order(signal)

    On position close (callback from broker_interface.py):
        engine.on_position_closed(instrument, pnl_usd)
    """

    def __init__(self, params: RiskParameters):
        self.params  = params

        # Core components
        self.risk    = RiskEngine(params)
        self.session = SessionFilter()
        self.htf     = HTFBiasEngine()
        self.smc     = SMCDetector(params)
        self.logger  = TradeLogger()

        # Per-instrument structure engines (15M timeframe)
        self.structure: dict[Instrument, StructureEngine] = {
            inst: StructureEngine(inst) for inst in Instrument
        }

        # Signal generators
        self.ict_gen  = ICTSignalGenerator(params, self.smc)
        self.mr_gen   = MeanReversionSignalGenerator(params)
        self.mo_gen   = MomentumSignalGenerator(params)

        # Active trade management
        self.open_trades: dict[Instrument, OpenTrade] = {}
        self.trade_mgr    = TradeManager(params)

        self._log = logging.getLogger("signal_engine.main")

    def on_candle(
        self,
        candle:          Candle,
        candles_1m:      list[Candle],
        candles_5m:      list[Candle],
        candles_15m:     list[Candle],
        daily_candles:   list[Candle],
        weekly_candles:  list[Candle],
    ) -> Optional[Signal]:
        """
        Main entry point. Called on every new closed candle.

        Execution order (do not reorder — each step feeds the next):

        Step 1: Session check
            session_filter.is_tradeable(instrument, now)
            → If not tradeable, update open trades and return None.

        Step 2: Indicator calculation
            atr       = Indicators.atr(candles_15m)
            volume_ma = Indicators.volume_ma(candles_15m)
            vwap      = Indicators.vwap(session_candles)
            rsi       = Indicators.rsi(candles_5m)
            etc.

        Step 3: Structure update
            structure = self.structure[instrument].update(candles_15m, atr)

        Step 4: HTF bias
            htf_bias = self.htf.compute_bias(instrument, daily_candles, weekly_candles, price)

        Step 5: Signal generation (try all three generators)
            sig = ict_gen.generate(...) or mr_gen.generate(...) or mo_gen.generate(...)

        Step 6: Risk pre-flight
            can_trade, reason = self.risk.can_take_trade(instrument, direction)
            If not can_trade → log rejection and return None.

        Step 7: Active trade management
            For each open trade, call trade_mgr.update(trade, candle, atr)
            and execute the returned action via broker_interface.

        Step 8: Return signal (or None)

        TODO: Codex — implement this orchestration using the steps above.
              Import broker_interface at the top of broker_interface.py
              and call it here for trade management actions.
        """
        instrument = candle.instrument
        now = candle.timestamp

        managed_atr = Indicators.atr(candles_15m)
        open_trade = self.open_trades.get(instrument)
        if open_trade:
            self.trade_mgr.update(open_trade, candle, managed_atr)

        tradeable, reason = self.session.is_tradeable(instrument, now)
        if not tradeable:
            self._log.info("%s blocked by session filter: %s", instrument.value, reason)
            return None

        atr = managed_atr
        volume_ma = Indicators.volume_ma(candles_15m)
        session_candles = [c for c in candles_5m if self.session.get_current_session(c.timestamp) == self.session.get_current_session(now)]
        session_candles = session_candles or candles_5m
        vwap = Indicators.vwap(session_candles)
        vwap_upper, vwap_lower = Indicators.vwap_std_dev(session_candles, vwap, 2)
        rsi = Indicators.rsi(candles_5m)
        stoch_k, stoch_d = Indicators.stochastic(candles_5m)
        bb_upper, _, bb_lower = Indicators.bollinger_bands(candles_15m)
        bb_width = bb_upper - bb_lower
        closes_15m = [c.close for c in candles_15m]
        ema_9_series = Indicators.ema(closes_15m, 9)
        ema_21_series = Indicators.ema(closes_15m, 21)
        ema_9 = ema_9_series[-1] if ema_9_series else 0.0
        ema_21 = ema_21_series[-1] if ema_21_series else 0.0

        structure = self.structure[instrument].update(candles_15m, atr)
        htf_bias = self.htf.compute_bias(instrument, daily_candles, weekly_candles, candle.close)
        session = self.session.get_current_session(now)

        signal = (
            self.ict_gen.generate(instrument, candles_1m, candles_15m, structure, htf_bias, session, atr, volume_ma)
            or self.mr_gen.generate(instrument, candles_5m, htf_bias, session, atr, vwap, vwap_upper, vwap_lower, rsi, stoch_k, stoch_d)
            or self.mo_gen.generate(instrument, candles_15m, htf_bias, session, atr, volume_ma, bb_upper, bb_lower, bb_width, ema_9, ema_21)
        )
        if signal is None:
            return None

        can_trade, reason = self.risk.can_take_trade(instrument, signal.direction)
        if not can_trade:
            self.logger.log_signal_considered(
                signal=None,
                instrument=instrument,
                direction=signal.direction,
                signal_type=signal.signal_type,
                confluence_score=signal.confluence_score,
                rejection_reason=reason,
                timestamp=now,
            )
            return None

        self.logger.log_signal_considered(
            signal=signal,
            instrument=instrument,
            direction=signal.direction,
            signal_type=signal.signal_type,
            confluence_score=signal.confluence_score,
            rejection_reason="",
            timestamp=now,
        )
        self.risk.register_position(instrument, signal.direction)
        self.open_trades[instrument] = OpenTrade(
            instrument=instrument,
            direction=signal.direction,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            take_profit_1=signal.take_profit_1,
            take_profit_2=signal.take_profit_2,
            take_profit_3=signal.take_profit_3,
            contracts=signal.position_size,
            risk_amount=signal.risk_amount_usd,
            opened_at=signal.formed_at,
            atr_at_entry=atr,
        )
        return signal

    def on_position_closed(
        self,
        instrument:  Instrument,
        pnl_usd:     float,
        close_price: float,
        close_time:  datetime,
        exit_reason: str,
    ) -> None:
        """
        Callback from broker_interface when a position is fully closed.
        Updates risk engine PnL, removes from open_trades, logs the trade.
        TODO: Codex — implement
        """
        trade = self.open_trades.pop(instrument, None)
        self.risk.update_pnl(pnl_usd, instrument)
        if trade is None:
            return
        r_multiple = self.trade_mgr._current_r(trade, close_price)
        self.logger.log_trade_closed(
            trade=trade,
            close_price=close_price,
            close_time=close_time,
            pnl_usd=pnl_usd,
            r_multiple=r_multiple,
            exit_reason=exit_reason,
        )

    def add_news_blackout(self, start: datetime, end: datetime) -> None:
        """Proxy to SessionFilter. Call from your economic calendar loader."""
        self.session.add_news_blackout(start, end)

    def reset_daily(self) -> None:
        """Call at midnight EST each day to reset daily PnL tracking."""
        self.risk.reset_daily()
        self.session.clear_old_blackouts(datetime.utcnow())
        self._log.info("Daily reset complete.")


# ===========================================================================
# SECTION 12 — SELF-TESTS
# Run these before connecting to live data. They test the pure math only.
# ===========================================================================

def _make_candle(
    ts:     str,
    o:      float,
    h:      float,
    l:      float,
    c:      float,
    vol:    float = 1000.0,
    inst:   Instrument = Instrument.NQ,
) -> Candle:
    return Candle(
        timestamp=datetime.fromisoformat(ts),
        open=o, high=h, low=l, close=c, volume=vol,
        instrument=inst,
    )


def test_atr():
    """ATR should return a positive float for 15 bars of data."""
    candles = [
        _make_candle(f"2024-01-01 09:{i:02d}:00", 100+i, 102+i, 99+i, 101+i)
        for i in range(20)
    ]
    atr = Indicators.atr(candles, period=14)
    assert atr > 0, f"ATR should be > 0, got {atr}"
    print(f"  [PASS] ATR = {atr:.4f}")


def test_candle_properties():
    """Candle property calculations."""
    c = _make_candle("2024-01-01 09:30:00", o=100, h=110, l=95, c=108)
    assert c.body_size     == 8.0,  f"Expected body 8.0, got {c.body_size}"
    assert c.total_range   == 15.0, f"Expected range 15.0, got {c.total_range}"
    assert c.upper_wick    == 2.0,  f"Expected upper wick 2.0, got {c.upper_wick}"
    assert c.lower_wick    == 5.0,  f"Expected lower wick 5.0, got {c.lower_wick}"
    assert c.is_bullish    is True
    assert c.body_dominance == pytest_approx(8.0 / 15.0, abs=1e-9)
    print("  [PASS] Candle properties correct")


def pytest_approx(val, abs=1e-9):
    """Minimal approx helper (no pytest dependency required)."""
    class _Approx:
        def __eq__(self, other):
            return math.isclose(other, val, abs_tol=abs)
    return _Approx()


def test_swing_lookback():
    """Lookback should increase with higher ATR/price ratio."""
    lb_low_vol  = Indicators.swing_lookback(atr=10,  price=20000, instrument=Instrument.NQ)
    lb_high_vol = Indicators.swing_lookback(atr=200, price=20000, instrument=Instrument.NQ)
    assert lb_high_vol > lb_low_vol, "Higher ATR should give wider lookback"
    assert 3 <= lb_low_vol  <= 15, f"Lookback out of bounds: {lb_low_vol}"
    assert 3 <= lb_high_vol <= 15, f"Lookback out of bounds: {lb_high_vol}"
    print(f"  [PASS] Swing lookback: low_vol={lb_low_vol}, high_vol={lb_high_vol}")


def run_self_tests():
    print("\n=== Running self-tests ===")
    test_atr()
    test_candle_properties()
    test_swing_lookback()
    print("=== All implemented tests passed ===\n")


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    run_self_tests()

    # Minimal usage demonstration (wiring to data_feed.py not shown here)
    params = RiskParameters(account_size=150_000.0)
    engine = SignalEngine(params)
    print("SignalEngine initialised. Wire data_feed.py to engine.on_candle() to begin.")
    print(f"Account size:     ${params.account_size:,.0f}")
    print(f"Max risk/trade:   {params.max_risk_per_trade*100:.1f}%  "
          f"(${params.account_size * params.max_risk_per_trade:,.0f})")
    print(f"Daily loss limit: {params.max_daily_loss*100:.1f}%  "
          f"(${params.account_size * params.max_daily_loss:,.0f})")
    print(f"Min R:R ratio:    {params.min_rr_ratio}R")
