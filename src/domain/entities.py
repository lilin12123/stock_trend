from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


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
    signal_id: Optional[str] = None
    source_rule: Optional[str] = None
    scope: str = "global"
    owner_user_id: Optional[int] = None
    dedupe_key: Optional[str] = None
    context_snapshot: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class User:
    id: int
    username: str
    role: str
    is_active: bool
    created_at: str


@dataclass(frozen=True)
class SymbolSubscription:
    symbol: str
    symbol_name: Optional[str] = None
    enabled: bool = True
    source: str = "global"
    owner_user_id: Optional[int] = None


@dataclass(frozen=True)
class NotificationSetting:
    owner_user_id: int
    mode: str
    telegram_token: str = ""
    telegram_chat_id: str = ""
    bell_on_alert: bool = False


@dataclass(frozen=True)
class SignalTriggerRecord:
    signal_id: str
    name: str
    direction: str
    message: str


@dataclass(frozen=True)
class RuntimeStatus:
    started: bool
    active_symbols: List[str] = field(default_factory=list)
    active_timeframes: List[str] = field(default_factory=list)
    subscriptions_count: int = 0
    profiles_count: int = 0
    last_warmup_at: Optional[str] = None
    warmup_total_bars: int = 0
    last_error: Optional[str] = None
    recent_events: List[Dict[str, Any]] = field(default_factory=list)
