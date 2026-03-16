from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Iterable, Tuple
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class MarketProfile:
    code: str
    timezone: str
    sessions: Tuple[Tuple[time, time], ...]

    @property
    def primary_open(self) -> time:
        return self.sessions[0][0]


_MARKET_PROFILES = {
    "HK": MarketProfile(
        code="HK",
        timezone="Asia/Hong_Kong",
        sessions=((time(9, 30), time(12, 0)), (time(13, 0), time(16, 0))),
    ),
    "US": MarketProfile(
        code="US",
        timezone="America/New_York",
        sessions=((time(9, 30), time(16, 0)),),
    ),
    "CN": MarketProfile(
        code="CN",
        timezone="Asia/Shanghai",
        sessions=((time(9, 30), time(11, 30)), (time(13, 0), time(15, 0))),
    ),
}


def detect_market_code(symbol: str | None) -> str:
    text = str(symbol or "").upper()
    if text.startswith("US."):
        return "US"
    if text.startswith(("SH.", "SZ.", "BJ.")):
        return "CN"
    return "HK"


def get_market_profile(symbol: str | None, default_tz: str = "Asia/Hong_Kong") -> MarketProfile:
    code = detect_market_code(symbol)
    if code in _MARKET_PROFILES:
        return _MARKET_PROFILES[code]
    return MarketProfile(code=code, timezone=default_tz, sessions=((time(9, 30), time(16, 0)),))


def market_timezone(symbol: str | None, default_tz: str = "Asia/Hong_Kong") -> str:
    return get_market_profile(symbol, default_tz).timezone


def normalize_market_datetime(symbol: str | None, value: datetime, default_tz: str = "Asia/Hong_Kong") -> datetime:
    tz = ZoneInfo(market_timezone(symbol, default_tz))
    if value.tzinfo is None:
        return value.replace(tzinfo=tz)
    return value.astimezone(tz)


def market_trading_session(symbol: str | None, ts: datetime, default_tz: str = "Asia/Hong_Kong") -> bool:
    local_ts = normalize_market_datetime(symbol, ts, default_tz)
    current = local_ts.time()
    return any(start <= current < end for start, end in get_market_profile(symbol, default_tz).sessions)


def market_primary_session_open(symbol: str | None, ts: datetime, default_tz: str = "Asia/Hong_Kong") -> datetime:
    local_ts = normalize_market_datetime(symbol, ts, default_tz)
    start = get_market_profile(symbol, default_tz).primary_open
    return local_ts.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)


def market_day_close(symbol: str | None, ts: datetime, default_tz: str = "Asia/Hong_Kong") -> datetime:
    local_ts = normalize_market_datetime(symbol, ts, default_tz)
    end = get_market_profile(symbol, default_tz).sessions[-1][1]
    return local_ts.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)


def market_in_primary_session(symbol: str | None, ts: datetime, default_tz: str = "Asia/Hong_Kong") -> bool:
    local_ts = normalize_market_datetime(symbol, ts, default_tz)
    start, end = get_market_profile(symbol, default_tz).sessions[0]
    current = local_ts.time()
    return start <= current < end


def market_today(symbol: str | None, default_tz: str = "Asia/Hong_Kong", now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now(ZoneInfo(market_timezone(symbol, default_tz)))
    else:
        now = normalize_market_datetime(symbol, now, default_tz)
    return now.date().isoformat()


def market_yesterday(symbol: str | None, default_tz: str = "Asia/Hong_Kong", now: datetime | None = None) -> str:
    if now is None:
        current = datetime.now(ZoneInfo(market_timezone(symbol, default_tz)))
    else:
        current = normalize_market_datetime(symbol, now, default_tz)
    return (current.date() - timedelta(days=1)).isoformat()


def market_is_trading_day(symbol: str | None, value: date | datetime, default_tz: str = "Asia/Hong_Kong") -> bool:
    current_date = value.date() if isinstance(value, datetime) else value
    return current_date.weekday() < 5


def market_previous_trading_date(
    symbol: str | None,
    default_tz: str = "Asia/Hong_Kong",
    now: datetime | None = None,
    reference_date: date | None = None,
) -> str:
    if reference_date is None:
        if now is None:
            current = datetime.now(ZoneInfo(market_timezone(symbol, default_tz)))
        else:
            current = normalize_market_datetime(symbol, now, default_tz)
        reference_date = current.date()
    current_date = reference_date - timedelta(days=1)
    while not market_is_trading_day(symbol, current_date, default_tz):
        current_date -= timedelta(days=1)
    return current_date.isoformat()


def market_effective_trade_date(symbol: str | None, default_tz: str = "Asia/Hong_Kong", now: datetime | None = None) -> str:
    if now is None:
        current = datetime.now(ZoneInfo(market_timezone(symbol, default_tz)))
    else:
        current = normalize_market_datetime(symbol, now, default_tz)
    if not market_is_trading_day(symbol, current, default_tz):
        return market_previous_trading_date(symbol, default_tz, reference_date=current.date())
    if current.time() < get_market_profile(symbol, default_tz).primary_open:
        return market_previous_trading_date(symbol, default_tz, reference_date=current.date())
    return current.date().isoformat()
