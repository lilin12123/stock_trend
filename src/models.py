from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class Bar:
    symbol: str
    timeframe: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class Trigger:
    name: str
    direction: str  # "up" | "down" | "neutral"
    message: str


@dataclass(frozen=True)
class Signal:
    symbol: str
    timeframe: str
    ts: datetime
    rule: str
    message: str
    strength: Optional[float] = None
    level: Optional[str] = None
    symbol_name: Optional[str] = None
    context: Optional[dict] = None
    direction: Optional[str] = None
    triggers: Optional[List[Trigger]] = None
