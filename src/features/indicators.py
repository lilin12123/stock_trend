from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class RollingATR:
    period: int
    trs: Deque[float]
    prev_close: Optional[float] = None

    def __init__(self, period: int) -> None:
        self.period = period
        self.trs = deque(maxlen=period)
        self.prev_close = None

    def update(self, high: float, low: float, close: float) -> Optional[float]:
        # 计算 True Range 并返回滚动ATR
        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - self.prev_close), abs(low - self.prev_close))
        self.trs.append(tr)
        self.prev_close = close
        if len(self.trs) < self.period:
            return None
        return sum(self.trs) / len(self.trs)


@dataclass
class RollingMean:
    period: int
    values: Deque[float]

    def __init__(self, period: int) -> None:
        self.period = period
        self.values = deque(maxlen=period)

    def update(self, value: float) -> Optional[float]:
        # 计算滚动均值（用于成交量均值）
        self.values.append(value)
        if len(self.values) < self.period:
            return None
        return sum(self.values) / len(self.values)


@dataclass
class RollingRSI:
    period: int
    gains: Deque[float]
    losses: Deque[float]
    prev_close: Optional[float] = None

    def __init__(self, period: int) -> None:
        self.period = period
        self.gains = deque(maxlen=period)
        self.losses = deque(maxlen=period)
        self.prev_close = None

    def update(self, close: float) -> Optional[float]:
        if self.prev_close is None:
            self.prev_close = close
            return None
        change = close - self.prev_close
        self.prev_close = close
        self.gains.append(max(change, 0.0))
        self.losses.append(max(-change, 0.0))
        if len(self.gains) < self.period:
            return None
        avg_gain = sum(self.gains) / len(self.gains)
        avg_loss = sum(self.losses) / len(self.losses)
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


@dataclass
class RollingWindow:
    size: int
    values: Deque[float]

    def __init__(self, size: int) -> None:
        self.size = size
        self.values = deque(maxlen=size)

    def update(self, value: float) -> None:
        self.values.append(value)

    def quantile(self, q: float) -> Optional[float]:
        # 简单分位数（非线性插值）
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        idx = int(q * (len(sorted_vals) - 1))
        return sorted_vals[idx]

    def latest(self) -> Optional[float]:
        if not self.values:
            return None
        return self.values[-1]


@dataclass
class VWAPState:
    cum_pv: float = 0.0
    cum_vol: float = 0.0
    session_date: Optional[str] = None

    def reset(self) -> None:
        self.cum_pv = 0.0
        self.cum_vol = 0.0

    def update(self, price: float, volume: float, session_date: str) -> Optional[float]:
        # 按交易日滚动VWAP
        if self.session_date != session_date:
            self.session_date = session_date
            self.reset()
        self.cum_pv += price * volume
        self.cum_vol += volume
        if self.cum_vol <= 0:
            return None
        return self.cum_pv / self.cum_vol
