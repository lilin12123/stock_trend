from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from datetime import datetime
from typing import Dict, Optional, Tuple

from ..features.indicators import RollingATR, RollingMean, RollingWindow, VWAPState, RollingRSI


@dataclass
class SymbolState:
    atr: RollingATR
    vol_mean: RollingMean
    atr_window: RollingWindow
    vwap: VWAPState
    rsi: RollingRSI
    last_signal_ts: Dict[str, datetime] = field(default_factory=dict)
    bars: deque = field(default_factory=lambda: deque(maxlen=20))


class StateStore:
    def __init__(self, atr_period: int, vol_avg_period: int, atr_lookback: int) -> None:
        self.atr_period = atr_period
        self.vol_avg_period = vol_avg_period
        self.atr_lookback = atr_lookback
        self.states: Dict[str, SymbolState] = {}
        self.rsi_period = 14
        self.prev_day: Dict[str, Tuple[float, float]] = {}

    def get(self, key: str) -> SymbolState:
        if key not in self.states:
            self.states[key] = SymbolState(
                atr=RollingATR(self.atr_period),
                vol_mean=RollingMean(self.vol_avg_period),
                atr_window=RollingWindow(self.atr_lookback),
                vwap=VWAPState(),
                rsi=RollingRSI(self.rsi_period),
            )
        return self.states[key]

    def update_periods(self, atr_period: int, vol_avg_period: int, atr_lookback: int, rsi_period: int) -> None:
        self.atr_period = atr_period
        self.vol_avg_period = vol_avg_period
        self.atr_lookback = atr_lookback
        self.rsi_period = rsi_period
        # Existing states keep their rolling windows to avoid losing context.

    def set_prev_day(self, symbol: str, high: float, low: float) -> None:
        self.prev_day[symbol] = (high, low)

    def get_prev_day(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        if symbol in self.prev_day:
            return self.prev_day[symbol]
        return None, None
