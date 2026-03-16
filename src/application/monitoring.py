from __future__ import annotations

import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from ..application.market_profile import (
    detect_market_code,
    market_day_close,
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


@dataclass
class PendingSignalEvaluation:
    signal_id: str
    symbol: str
    timeframe: str
    trade_date: str
    signal_bar_ts: str
    base_close: float
    horizon_minutes: int
    target_bars: int
    deadline_ts: datetime
    observed_bars: int = 0
    last_observed_bar_ts: Optional[str] = None
    max_up: float = 0.0
    max_down: float = 0.0
    final_change: float = 0.0
    completed: bool = False


@dataclass
class WarmupResult:
    active_symbols: List[str]
    failed_symbols: List[Dict[str, str]]
    total_bars: int = 0


class MonitoringService:
    STATUS_EVENT_TYPES = [
        "runtime_started",
        "config_applied",
        "subscriptions_updated",
        "warmup_completed",
        "warmup_symbol_failed",
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
        self._market_snapshots: Dict[str, Dict[str, Any]] = {}
        self._market_reference_days: Dict[str, str] = {}
        self._pending_signal_evaluations: Dict[str, List[PendingSignalEvaluation]] = {}
        self._signal_cleanup_days: Dict[str, str] = {}
        self._forward_metrics_config = self.store.get_forward_metrics_config(app_cfg)
        self._status = RuntimeStatus(started=False, active_timeframes=self._timeframes)
        self._status_lock = threading.Lock()

    def start(self) -> None:
        self.apply_config(startup=True)

    def stop(self) -> None:
        self.gateway.close()
        self.dispatcher.close()

    def apply_config(self, startup: bool = False) -> Dict[str, Any]:
        try:
            self._forward_metrics_config = self.store.get_forward_metrics_config(self.app_cfg)
            profiles = self._load_profiles()
            symbols = self.planner.active_symbols(profiles)
            with self._profiles_lock:
                self._profiles = profiles
            warmup = self._warmup(symbols)
            active_symbols = warmup.active_symbols
            warmup_total = warmup.total_bars
            payload = {
                "symbols": active_symbols,
                "symbol_count": len(active_symbols),
                "timeframes": self._timeframes,
                "timeframe_count": len(self._timeframes),
                "profiles_count": len(profiles),
                "warmup_total_bars": warmup_total,
                "requested_symbols": symbols,
                "requested_symbol_count": len(symbols),
                "failed_symbols": warmup.failed_symbols,
                "startup": startup,
            }
            if startup:
                self.gateway.start(active_symbols, self._timeframes, self.on_bar)
            else:
                self.gateway.apply_subscriptions(active_symbols, self._timeframes)
            self._set_status(
                started=True,
                active_symbols=active_symbols,
                active_timeframes=self._timeframes,
                subscriptions_count=len(active_symbols) * len(self._timeframes),
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
                f"Applied runtime config for {len(active_symbols)} symbols.",
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

    def get_market_snapshots(self) -> Dict[str, Dict[str, Any]]:
        return {symbol: dict(snapshot) for symbol, snapshot in self._market_snapshots.items()}

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
        self._update_pending_signal_evaluations(bar)
        self._process_bar(bar, emit=True)
        self._finalize_pending_signal_evaluations_for_market_close(bar)

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

    def _warmup(self, symbols: List[str]) -> WarmupResult:
        total = 0
        active_symbols: List[str] = []
        failed_symbols: List[Dict[str, str]] = []
        with self._profiles_lock:
            profiles = list(self._profiles)
        for symbol in symbols:
            try:
                today = market_effective_trade_date(symbol, self._tz.key)
                yesterday = market_previous_trading_date(symbol, self._tz.key, reference_date=datetime.fromisoformat(today).date())
                prev_day = self.gateway.request_prev_day(symbol, yesterday)
                if prev_day:
                    self._market_reference_days[symbol] = today
                    self._market_snapshots[symbol] = {
                        "symbol": symbol,
                        "trade_date": today,
                        "prev_close": float(prev_day.get("close") or 0.0),
                        "last_price": None,
                        "change_pct": None,
                        "last_ts": None,
                        "timeframe": None,
                    }
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
            except Exception as exc:
                failed = {"symbol": symbol, "error": str(exc)}
                failed_symbols.append(failed)
                self.store.add_runtime_event(
                    "error",
                    "warmup_symbol_failed",
                    f"Warmup skipped for {symbol}: {exc}",
                    failed,
                )
                for timeframe in self._timeframes:
                    key = f"{symbol}:{timeframe}"
                    self._day_bars.pop(key, None)
                    self._vwap_state.pop(key, None)
                self._market_snapshots.pop(symbol, None)
                self._market_reference_days.pop(symbol, None)
                continue
            active_symbols.append(symbol)
        return WarmupResult(active_symbols=active_symbols, failed_symbols=failed_symbols, total_bars=total)

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
                context_snapshot = {
                    **(raw_signal.context or {}),
                    "forward_metrics": self._initial_forward_metrics(raw_signal.timeframe),
                }
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
                    context_snapshot=context_snapshot,
                    signal_id=uuid4().hex,
                )
                signal_id = self.store.save_signal(signal, raw_signal.triggers or [])
                self._register_signal_evaluation(signal_id, signal, bar.close)
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
        self._ensure_market_reference(bar.symbol, bar.ts.date().isoformat())
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
        self._update_market_snapshot(bar)

    def _dedupe_key(self, profile: ProfileRuntime, signal: Signal) -> str:
        minute = signal.ts.strftime("%Y-%m-%dT%H:%M")
        return f"{profile.scope}:{profile.owner_user_id}:{signal.symbol}:{signal.timeframe}:{minute}:{signal.rule}:{signal.direction}"

    def _initial_forward_metrics(self, timeframe: str) -> Dict[str, Any]:
        horizon_minutes, target_bars = self._evaluation_window(timeframe)
        return {
            "horizon_minutes": horizon_minutes,
            "target_bars": target_bars,
            "observed_bars": 0,
            "max_up": None,
            "max_down": None,
            "final_change": None,
            "completed": False,
        }

    @staticmethod
    def _bars_for_horizon(timeframe: str, horizon_minutes: int) -> int:
        tf = str(timeframe or "").strip().lower()
        tf_minutes = 1 if tf == "1m" else 5
        return max(1, int(horizon_minutes / tf_minutes))

    @staticmethod
    def _timeframe_minutes(timeframe: str) -> int:
        return 1 if str(timeframe or "").strip().lower() == "1m" else 5

    def _evaluation_window(self, timeframe: str) -> tuple[int, int]:
        tf = str(timeframe or "").strip().lower()
        if tf == "1m":
            horizon_minutes = int(self._forward_metrics_config.get("1m_horizon_minutes", 20))
        else:
            horizon_minutes = int(self._forward_metrics_config.get("5m_horizon_minutes", 60))
        return horizon_minutes, self._bars_for_horizon(tf, horizon_minutes)

    def _register_signal_evaluation(self, signal_id: str, signal: Signal, base_close: float) -> None:
        horizon_minutes, target_bars = self._evaluation_window(signal.timeframe)
        if base_close <= 0:
            return
        signal_ts = normalize_market_datetime(signal.symbol, signal.ts, self._tz.key)
        deadline_ts = min(
            signal_ts + timedelta(minutes=horizon_minutes),
            market_day_close(signal.symbol, signal_ts, self._tz.key),
        )
        key = f"{signal.symbol}:{signal.timeframe}"
        self._pending_signal_evaluations.setdefault(key, []).append(
            PendingSignalEvaluation(
                signal_id=signal_id,
                symbol=signal.symbol,
                timeframe=signal.timeframe,
                trade_date=signal_ts.date().isoformat(),
                signal_bar_ts=signal_ts.isoformat(),
                base_close=float(base_close),
                horizon_minutes=horizon_minutes,
                target_bars=target_bars,
                deadline_ts=deadline_ts,
            )
        )

    def _update_pending_signal_evaluations(self, bar: Bar) -> None:
        key = f"{bar.symbol}:{bar.timeframe}"
        pending = self._pending_signal_evaluations.get(key)
        if not pending:
            return
        remaining: List[PendingSignalEvaluation] = []
        current_trade_date = bar.ts.date().isoformat()
        bar_ts = bar.ts.isoformat()
        for item in pending:
            if item.completed or item.base_close <= 0:
                continue
            if item.trade_date != current_trade_date:
                self._complete_signal_evaluation(item, item.deadline_ts)
                continue
            if bar_ts == item.signal_bar_ts:
                remaining.append(item)
                continue
            if item.last_observed_bar_ts != bar_ts:
                item.observed_bars += 1
                item.last_observed_bar_ts = bar_ts
            item.max_up = max(item.max_up, (bar.high - item.base_close) / item.base_close * 100.0)
            item.max_down = max(item.max_down, (item.base_close - bar.low) / item.base_close * 100.0)
            item.final_change = (bar.close - item.base_close) / item.base_close * 100.0
            is_closing_bar = self._bar_interval_end(bar) >= market_day_close(bar.symbol, bar.ts, self._tz.key)
            if item.observed_bars >= item.target_bars or bar.ts >= item.deadline_ts or is_closing_bar:
                self._complete_signal_evaluation(item, bar.ts, completed_by_close=is_closing_bar and bar.ts < item.deadline_ts)
            else:
                remaining.append(item)
        if remaining:
            self._pending_signal_evaluations[key] = remaining
        else:
            self._pending_signal_evaluations.pop(key, None)

    def _finalize_pending_signal_evaluations_for_market_close(self, bar: Bar) -> None:
        if self._bar_interval_end(bar) < market_day_close(bar.symbol, bar.ts, self._tz.key):
            return
        key = f"{bar.symbol}:{bar.timeframe}"
        pending = self._pending_signal_evaluations.get(key)
        if not pending:
            return
        remaining: List[PendingSignalEvaluation] = []
        for item in pending:
            if item.completed or item.trade_date != bar.ts.date().isoformat():
                remaining.append(item)
                continue
            self._complete_signal_evaluation(item, bar.ts, completed_by_close=True)
        self._pending_signal_evaluations[key] = [item for item in remaining if not item.completed]
        if not self._pending_signal_evaluations[key]:
            self._pending_signal_evaluations.pop(key, None)

    def _complete_signal_evaluation(
        self,
        item: PendingSignalEvaluation,
        completed_at: datetime,
        completed_by_close: bool = False,
    ) -> None:
        item.completed = True
        signal_row = self.store.get_signal(item.signal_id)
        if not signal_row:
            return
        context_snapshot = signal_row.get("context_snapshot") or {}
        context_snapshot["forward_metrics"] = {
            "horizon_minutes": item.horizon_minutes,
            "target_bars": item.target_bars,
            "observed_bars": item.observed_bars,
            "max_up": item.max_up,
            "max_down": item.max_down,
            "final_change": item.final_change,
            "completed": True,
            "completed_at": completed_at.isoformat(),
            "completed_by_close": completed_by_close,
        }
        self.store.update_signal_context_snapshot(item.signal_id, context_snapshot)

    def _bar_interval_end(self, bar: Bar) -> datetime:
        return bar.ts + timedelta(minutes=self._timeframe_minutes(bar.timeframe))

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
                f"Archived {deleted} stale {market} signals before {trade_date}.",
                {"market": market, "trade_date": trade_date, "archived": deleted},
            )

    def _ensure_market_reference(self, symbol: str, trade_date: str) -> None:
        if self._market_reference_days.get(symbol) == trade_date:
            return
        yesterday = market_previous_trading_date(
            symbol,
            self._tz.key,
            reference_date=datetime.fromisoformat(trade_date).date(),
        )
        prev_day = self.gateway.request_prev_day(symbol, yesterday)
        prev_close = float(prev_day.get("close") or 0.0) if prev_day else 0.0
        snapshot = self._market_snapshots.get(symbol, {})
        snapshot.update(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "prev_close": prev_close if prev_close > 0 else snapshot.get("prev_close"),
                "last_price": None,
                "change_pct": None,
                "last_ts": None,
                "timeframe": None,
            }
        )
        self._market_snapshots[symbol] = snapshot
        self._market_reference_days[symbol] = trade_date

    def _update_market_snapshot(self, bar: Bar) -> None:
        trade_date = bar.ts.date().isoformat()
        snapshot = self._market_snapshots.get(bar.symbol, {})
        prev_close = snapshot.get("prev_close")
        if not prev_close or float(prev_close) <= 0:
            prev_close = None
        snapshot.update(
            {
                "symbol": bar.symbol,
                "trade_date": trade_date,
                "last_price": float(bar.close),
                "last_ts": bar.ts.isoformat(),
                "timeframe": bar.timeframe,
                "change_pct": ((float(bar.close) - float(prev_close)) / float(prev_close) * 100.0) if prev_close else None,
            }
        )
        self._market_snapshots[bar.symbol] = snapshot

    def _set_status(self, **updates: Any) -> None:
        with self._status_lock:
            current = asdict(self._status)
            current.update({key: value for key, value in updates.items() if value is not None})
            self._status = RuntimeStatus(**current)
