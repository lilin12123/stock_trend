from __future__ import annotations

import threading
from dataclasses import asdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..application.market_profile import (
    detect_market_code,
    market_effective_trade_date,
    market_previous_trading_date,
    market_trading_session,
    normalize_market_datetime,
)
from ..application.notification_policy import sanitize_notification_setting
from ..application.rule_runtime import parse_time_key
from ..application.subscriptions import ProfileRuntime, SubscriptionPlanner
from ..domain import Bar, RuntimeStatus, Signal
from ..infrastructure import NotificationDispatcher, OpenDGateway, SqliteStore


class MonitoringService:
    STATUS_EVENT_TYPES = [
        "runtime_started",
        "config_applied",
        "subscriptions_updated",
        "warmup_completed",
        "config_apply_failed",
    ]

    def __init__(
        self,
        store: SqliteStore,
        gateway: OpenDGateway,
        dispatcher: NotificationDispatcher,
        planner: SubscriptionPlanner,
        app_cfg: Dict[str, Any],
    ) -> None:
        self.store = store
        self.gateway = gateway
        self.dispatcher = dispatcher
        self.planner = planner
        self.app_cfg = app_cfg
        self._tz = ZoneInfo(app_cfg.get("tz", "Asia/Hong_Kong"))
        self._timeframes = list(app_cfg.get("timeframes", ["1m", "5m"]))
        self._profiles: List[ProfileRuntime] = []
        self._profiles_lock = threading.Lock()
        self._day_bars: Dict[str, List[Dict[str, Any]]] = {}
        self._vwap_state: Dict[str, Dict[str, Any]] = {}
        self._signal_cleanup_days: Dict[str, str] = {}
        self._status = RuntimeStatus(started=False, active_timeframes=self._timeframes)
        self._status_lock = threading.Lock()

    def start(self) -> None:
        self.apply_config(startup=True)

    def stop(self) -> None:
        self.gateway.close()
        self.dispatcher.close()

    def apply_config(self, startup: bool = False) -> Dict[str, Any]:
        try:
            profiles = self._load_profiles()
            symbols = self.planner.active_symbols(profiles)
            with self._profiles_lock:
                self._profiles = profiles
            warmup_total = self._warmup(symbols)
            payload = {
                "symbols": symbols,
                "symbol_count": len(symbols),
                "timeframes": self._timeframes,
                "timeframe_count": len(self._timeframes),
                "profiles_count": len(profiles),
                "warmup_total_bars": warmup_total,
                "startup": startup,
            }
            if startup:
                self.gateway.start(symbols, self._timeframes, self.on_bar)
            else:
                self.gateway.apply_subscriptions(symbols, self._timeframes)
            self._set_status(
                started=True,
                active_symbols=symbols,
                active_timeframes=self._timeframes,
                subscriptions_count=len(symbols) * len(self._timeframes),
                profiles_count=len(profiles),
                last_warmup_at=datetime.now(self._tz).isoformat(),
                warmup_total_bars=warmup_total,
                last_error=None,
                recent_events=self.store.list_runtime_events(10, self.STATUS_EVENT_TYPES),
            )
            self.store.add_runtime_event(
                "info",
                "warmup_completed",
                f"Warmup completed with {warmup_total} bars.",
                payload,
            )
            self.store.add_runtime_event(
                "info",
                "config_applied",
                f"Applied runtime config for {len(symbols)} symbols.",
                payload,
            )
            self.store.add_runtime_event(
                "info",
                "runtime_started" if startup else "subscriptions_updated",
                "Runtime started and waiting for new bars." if startup else "Subscriptions refreshed without restarting the app.",
                payload,
            )
        except Exception as exc:
            self.store.add_runtime_event(
                "error",
                "config_apply_failed",
                f"Failed to apply runtime config: {exc}",
                {"error": str(exc), "startup": startup},
            )
            self._set_status(last_error=str(exc), recent_events=self.store.list_runtime_events(10, self.STATUS_EVENT_TYPES))
        return asdict(self.get_status())

    def get_status(self) -> RuntimeStatus:
        with self._status_lock:
            current = asdict(self._status)
        current["recent_events"] = self.store.list_runtime_events(10, self.STATUS_EVENT_TYPES)
        return RuntimeStatus(**current)

    def get_day_bars(self, symbol: str, timeframe: str, since: Optional[str] = None) -> List[Dict[str, Any]]:
        key = f"{symbol}:{timeframe}"
        items = list(self._day_bars.get(key, []))
        if since:
            items = [item for item in items if str(item.get("ts", "")) > since]
        return items

    def on_bar(self, bar: Bar) -> None:
        if bar.ts.tzinfo is None:
            bar = Bar(
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                ts=normalize_market_datetime(bar.symbol, bar.ts, self._tz.key),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
        else:
            bar = Bar(
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                ts=normalize_market_datetime(bar.symbol, bar.ts, self._tz.key),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
        if not market_trading_session(bar.symbol, bar.ts, self._tz.key):
            return
        self._purge_old_market_signals(bar)
        self._track_bar(bar)
        self._process_bar(bar, emit=True)

    def _load_profiles(self) -> List[ProfileRuntime]:
        users = [self.store.get_user_by_id(user.id) for user in self.store.list_users()]
        users = [user for user in users if user]
        global_symbols = self.store.list_global_symbols()
        user_symbols_by_user = {int(user["id"]): self.store.list_user_symbols(int(user["id"])) for user in users}
        user_rule_overrides = {int(user["id"]): self.store.get_user_rule_overrides(int(user["id"])) for user in users}
        notifications = {
            int(user["id"]): sanitize_notification_setting(
                str(user.get("role", "user")),
                self.store.get_notification_settings(int(user["id"])),
            )
            for user in users
        }
        default_rules = self.store.get_default_rule_config()
        return self.planner.build_profiles(default_rules, users, global_symbols, user_symbols_by_user, user_rule_overrides, notifications)

    def _warmup(self, symbols: List[str]) -> int:
        total = 0
        with self._profiles_lock:
            profiles = list(self._profiles)
        for symbol in symbols:
            today = market_effective_trade_date(symbol, self._tz.key)
            yesterday = market_previous_trading_date(symbol, self._tz.key, reference_date=datetime.fromisoformat(today).date())
            prev_day = self.gateway.request_prev_day(symbol, yesterday)
            if prev_day:
                for profile in profiles:
                    if symbol in profile.symbols:
                        profile.engine.store.set_prev_day(symbol, float(prev_day["high"]), float(prev_day["low"]))
            for timeframe in self._timeframes:
                for row in self.gateway.request_history(symbol, today, today, timeframe):
                    ts = parse_time_key(row["time_key"])
                    bar = Bar(
                        symbol=row["code"],
                        timeframe=timeframe,
                        ts=normalize_market_datetime(row["code"], ts, self._tz.key),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                    if not market_trading_session(bar.symbol, bar.ts, self._tz.key):
                        continue
                    self._purge_old_market_signals(bar)
                    self._track_bar(bar)
                    self._process_bar(bar, emit=False)
                    total += 1
        return total

    def _process_bar(self, bar: Bar, emit: bool) -> None:
        with self._profiles_lock:
            profiles = list(self._profiles)
        for profile in profiles:
            if bar.symbol not in profile.symbols:
                continue
            signals = profile.engine.on_bar(bar)
            if not emit:
                continue
            for raw_signal in signals:
                symbol_name = profile.symbol_names.get(raw_signal.symbol)
                signal = Signal(
                    symbol=raw_signal.symbol,
                    symbol_name=symbol_name,
                    timeframe=raw_signal.timeframe,
                    ts=raw_signal.ts,
                    rule=raw_signal.rule,
                    message=raw_signal.message,
                    strength=raw_signal.strength,
                    direction=raw_signal.direction,
                    triggers=raw_signal.triggers,
                    scope=profile.scope,
                    owner_user_id=profile.owner_user_id,
                    source_rule=raw_signal.rule,
                    dedupe_key=self._dedupe_key(profile, raw_signal),
                    context_snapshot=raw_signal.context or {},
                    signal_id=uuid4().hex,
                )
                signal_id = self.store.save_signal(signal, raw_signal.triggers or [])
                self.store.add_runtime_event(
                    "info",
                    "signal_created",
                    f"{profile.username} {signal.symbol} {signal.rule}",
                    {"signal_id": signal_id, "scope": signal.scope, "owner_user_id": signal.owner_user_id},
                )
                if profile.scope == "user" and profile.notification:
                    head = f"{signal.symbol_name}({signal.symbol})" if signal.symbol_name else signal.symbol
                    text = f"{head} {signal.timeframe} {signal.rule}: {signal.message}"
                    self.dispatcher.dispatch(signal_id, profile.notification, text)

    def _track_bar(self, bar: Bar) -> None:
        key = f"{bar.symbol}:{bar.timeframe}"
        bars = self._day_bars.setdefault(key, [])
        ts = bar.ts.isoformat()
        day = ts.split("T")[0]
        state = self._vwap_state.get(key)
        if not state or state.get("day") != day:
            state = {"day": day, "cum_pv": 0.0, "cum_vol": 0.0}
            self._vwap_state[key] = state
            bars.clear()
        state["cum_pv"] += bar.close * bar.volume
        state["cum_vol"] += bar.volume
        payload = {
            "ts": ts,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "vwap": (state["cum_pv"] / state["cum_vol"]) if state["cum_vol"] else None,
        }
        if bars and bars[-1]["ts"] == ts:
            bars[-1] = payload
        else:
            bars.append(payload)

    def _dedupe_key(self, profile: ProfileRuntime, signal: Signal) -> str:
        minute = signal.ts.strftime("%Y-%m-%dT%H:%M")
        return f"{profile.scope}:{profile.owner_user_id}:{signal.symbol}:{signal.timeframe}:{minute}:{signal.rule}:{signal.direction}"

    def _purge_old_market_signals(self, bar: Bar) -> None:
        market = detect_market_code(bar.symbol)
        trade_date = bar.ts.date().isoformat()
        if self._signal_cleanup_days.get(market) == trade_date:
            return
        deleted = self.store.purge_signals_before_trade_date(market, trade_date)
        self._signal_cleanup_days[market] = trade_date
        if deleted > 0:
            self.store.add_runtime_event(
                "info",
                "signals_pruned",
                f"Purged {deleted} stale {market} signals before {trade_date}.",
                {"market": market, "trade_date": trade_date, "deleted": deleted},
            )

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            current = asdict(self._status)
            current.update({key: value for key, value in updates.items() if value is not None})
            self._status = RuntimeStatus(**current)
