from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.application.auth_service import AuthService
from src.application.backtesting import BacktestRunner
from src.application.market_profile import (
    detect_market_code,
    get_market_profile,
    market_effective_trade_date,
    market_previous_trading_date,
    market_trading_session,
    normalize_market_datetime,
)
from src.application.monitoring import MonitoringService
from src.application.notification_policy import sanitize_notification_data
from src.application.query_service import SignalQueryService
from src.application.subscriptions import SubscriptionPlanner
from src.domain import Bar, Signal, Trigger
from src.infrastructure import PasswordHasher, SqliteStore
from src.presentation.web import LocalWebApp
from src.rules.indicators.open_range import OpenRangeState, open_range_breakout


class FakeGateway:
    def __init__(self) -> None:
        self.started = []
        self.applied = []
        self.prev_day_requests = []
        self.history_requests = []

    def start(self, symbols, timeframes, on_bar):
        self.started.append((list(symbols), list(timeframes)))

    def apply_subscriptions(self, symbols, timeframes):
        self.applied.append((list(symbols), list(timeframes)))

    def request_prev_day(self, symbol, day):
        self.prev_day_requests.append((symbol, day))
        return None

    def request_history(self, symbol, start, end, timeframe):
        self.history_requests.append((symbol, start, end, timeframe))
        return []

    def close(self):
        return None


class FakeDispatcher:
    def __init__(self) -> None:
        self.messages = []

    def dispatch(self, signal_id, settings, message):
        self.messages.append((signal_id, settings, message))

    def close(self):
        return None


class V2AppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "app.sqlite")
        self.cfg = {
            "app": {
                "tz": "Asia/Hong_Kong",
                "auth": {
                    "bootstrap_admin": {"username": "admin", "password": "admin"},
                    "public_registration": {"enabled": True, "max_users": 200},
                },
                "open_d": {"host": "127.0.0.1", "port": 11111},
                "timeframes": ["1m", "5m"],
                "symbols": ["HK.00700", "HK.00981"],
                "symbol_names": {"HK.00700": "腾讯控股", "HK.00981": "中芯国际"},
                "notify": {"mode": "local", "telegram": {"token": "", "chat_id": ""}},
                "storage": {"db_path": self.db_path, "dir": str(Path(self.tempdir.name) / "signals")},
            },
            "rules": {"cooldown_seconds": 60, "rsi_extreme": {"enabled": True, "period": 14, "overbought": 70, "oversold": 30}},
            "backtest": {},
        }
        self.store = SqliteStore(self.db_path)
        self.hasher = PasswordHasher()
        self.store.seed_from_config(self.cfg, self.hasher.hash_password("admin"))

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_seed_and_auth_login(self) -> None:
        auth = AuthService(self.store, self.hasher)
        result = auth.login("admin", "admin")
        self.assertTrue(result["ok"])
        user = auth.authenticate(result["session_id"])
        self.assertIsNotNone(user)
        self.assertEqual(user["username"], "admin")

    def test_auth_locks_after_five_failures(self) -> None:
        auth = AuthService(self.store, self.hasher)
        result = None
        for _ in range(5):
            result = auth.login("admin", "wrong")
        self.assertIsNotNone(result)
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "account_locked")
        locked_user = self.store.get_user_by_username("admin")
        self.assertIsNotNone(locked_user)
        self.assertIsNotNone(locked_user["locked_until"])
        blocked = auth.login("admin", "admin")
        self.assertFalse(blocked["ok"])
        self.assertEqual(blocked["error_code"], "account_locked")

    def test_successful_login_clears_failure_counter(self) -> None:
        auth = AuthService(self.store, self.hasher)
        auth.login("admin", "wrong")
        auth.login("admin", "wrong")
        result = auth.login("admin", "admin")
        self.assertTrue(result["ok"])
        user = self.store.get_user_by_username("admin")
        self.assertEqual(user["failed_login_attempts"], 0)
        self.assertIsNone(user["locked_until"])

    def test_seed_from_config_resets_bootstrap_admin_password_on_every_start(self) -> None:
        auth = AuthService(self.store, self.hasher)
        self.assertTrue(auth.login("admin", "admin")["ok"])
        auth.login("admin", "wrong")
        auth.login("admin", "wrong")
        self.store.update_user_password(1, self.hasher.hash_password("older-password"))

        new_cfg = {
            **self.cfg,
            "app": {
                **self.cfg["app"],
                "auth": {
                    **self.cfg["app"]["auth"],
                    "bootstrap_admin": {"username": "admin", "password": "new-secret"},
                },
            },
        }
        self.store.seed_from_config(new_cfg, self.hasher.hash_password("new-secret"))

        fresh_auth = AuthService(self.store, self.hasher)
        self.assertFalse(fresh_auth.login("admin", "older-password")["ok"])
        self.assertTrue(fresh_auth.login("admin", "new-secret")["ok"])
        user = self.store.get_user_by_username("admin")
        self.assertEqual(user["failed_login_attempts"], 0)
        self.assertIsNone(user["locked_until"])

    def test_inactive_user_cannot_login(self) -> None:
        auth = AuthService(self.store, self.hasher)
        user_id = self.store.create_user("carol", self.hasher.hash_password("pw"), "user")
        self.store.update_user_active_state(user_id, False)
        result = auth.login("carol", "pw")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "invalid_credentials")

    def test_disabling_user_clears_existing_sessions(self) -> None:
        auth = AuthService(self.store, self.hasher)
        user_id = self.store.create_user("dave", self.hasher.hash_password("pw"), "user")
        result = auth.login("dave", "pw")
        self.assertTrue(result["ok"])
        self.assertIsNotNone(auth.authenticate(result["session_id"]))
        self.store.update_user_active_state(user_id, False)
        self.assertIsNone(auth.authenticate(result["session_id"]))

    def test_subscription_planner_mixed_scope(self) -> None:
        user_id = self.store.create_user("alice", self.hasher.hash_password("pw"), "user")
        self.store.add_user_symbol(user_id, "HK.01810", "小米集团", True)
        planner = SubscriptionPlanner()
        users = [self.store.get_user_by_id(user.id) for user in self.store.list_users()]
        users = [user for user in users if user]
        profiles = planner.build_profiles(
            default_rules=self.store.get_default_rule_config(),
            users=users,
            global_symbols=self.store.list_global_symbols(),
            user_symbols_by_user={user_id: self.store.list_user_symbols(user_id)},
            user_rule_overrides={user_id: self.store.get_user_rule_overrides(user_id)},
            notifications={user_id: self.store.get_notification_settings(user_id)},
        )
        active = planner.active_symbols(profiles)
        self.assertIn("HK.01810", active)
        self.assertIn("HK.00700", active)
        user_profile = next(profile for profile in profiles if profile.scope == "user" and profile.owner_user_id == user_id)
        self.assertEqual(user_profile.symbols, {"HK.01810"})

    def test_signal_query_merges_same_minute(self) -> None:
        user_id = self.store.create_user("bob", self.hasher.hash_password("pw"), "user")
        ts = datetime.fromisoformat("2026-03-13T10:15:00+08:00")
        first = Signal(
            symbol="HK.00700",
            symbol_name="腾讯控股",
            timeframe="1m",
            ts=ts,
            rule="rsi_oversold",
            message="first",
            direction="up",
            scope="user",
            owner_user_id=user_id,
            source_rule="rsi_oversold",
            dedupe_key="a",
        )
        second = Signal(
            symbol="HK.00700",
            symbol_name="腾讯控股",
            timeframe="1m",
            ts=ts,
            rule="open_range_breakout",
            message="second",
            direction="up",
            scope="user",
            owner_user_id=user_id,
            source_rule="open_range_breakout",
            dedupe_key="b",
        )
        self.store.save_signal(first, [Trigger(name="rsi_oversold", direction="up", message="")])
        self.store.save_signal(second, [Trigger(name="open_range_breakout", direction="up", message="")])

        query = SignalQueryService(self.store)
        items = query.list_signals(owner_user_id=user_id, limit=20)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["level"], "Lv2")
        self.assertEqual(items[0]["direction"], "up")

    def test_signal_query_keeps_previous_trade_day_until_cleanup_runs(self) -> None:
        user_id = self.store.create_user("bob2", self.hasher.hash_password("pw"), "user")
        ts = datetime.fromisoformat("2026-03-13T15:55:00-04:00")
        signal = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="1m",
            ts=ts,
            rule="rsi_oversold",
            message="carry overnight",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="carry-us",
        )
        self.store.save_signal(signal, [Trigger(name="rsi_oversold", direction="up", message="")])

        query = SignalQueryService(self.store)
        items = query.list_signals(owner_user_id=user_id, limit=20, symbol="US.NVDA", timeframe="1m")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["message"], "carry overnight")

    def test_signal_query_offsets_opposite_directions(self) -> None:
        user_id = self.store.create_user("erin", self.hasher.hash_password("pw"), "user")
        ts = datetime.fromisoformat("2026-03-13T10:20:00-04:00")
        global_up = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="5m",
            ts=ts,
            rule="rsi_oversold",
            message="global up",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="g-up",
        )
        user_up = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="5m",
            ts=ts,
            rule="squeeze_breakout",
            message="user up",
            direction="up",
            scope="user",
            owner_user_id=user_id,
            source_rule="squeeze_breakout",
            dedupe_key="u-up",
        )
        user_down = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="5m",
            ts=ts,
            rule="rsi_overbought",
            message="user down",
            direction="down",
            scope="user",
            owner_user_id=user_id,
            source_rule="rsi_overbought",
            dedupe_key="u-down",
        )
        self.store.save_signal(global_up, [Trigger(name="rsi_oversold", direction="up", message="")])
        self.store.save_signal(user_up, [Trigger(name="squeeze_breakout", direction="up", message="")])
        self.store.save_signal(user_down, [Trigger(name="rsi_overbought", direction="down", message="")])

        query = SignalQueryService(self.store)
        items = query.list_signals(owner_user_id=user_id, limit=20, symbol="US.NVDA", timeframe="5m")

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["direction"], "up")
        self.assertEqual(items[0]["level"], "Lv1")
        self.assertEqual(items[0]["trigger_count"], 1)
        self.assertEqual({trigger["name"] for trigger in items[0]["triggers"]}, {"rsi_oversold", "squeeze_breakout"})

    def test_signal_query_hides_bar_when_directions_cancel_out(self) -> None:
        user_id = self.store.create_user("frank", self.hasher.hash_password("pw"), "user")
        ts = datetime.fromisoformat("2026-03-13T10:25:00-04:00")
        self.store.save_signal(
            Signal(
                symbol="US.NVDA",
                symbol_name="NVIDIA",
                timeframe="5m",
                ts=ts,
                rule="rsi_oversold",
                message="up",
                direction="up",
                scope="global",
                owner_user_id=None,
                source_rule="rsi_oversold",
                dedupe_key="cancel-up",
            ),
            [Trigger(name="rsi_oversold", direction="up", message="")],
        )
        self.store.save_signal(
            Signal(
                symbol="US.NVDA",
                symbol_name="NVIDIA",
                timeframe="5m",
                ts=ts,
                rule="rsi_overbought",
                message="down",
                direction="down",
                scope="global",
                owner_user_id=None,
                source_rule="rsi_overbought",
                dedupe_key="cancel-down",
            ),
            [Trigger(name="rsi_overbought", direction="down", message="")],
        )

        query = SignalQueryService(self.store)
        items = query.list_signals(owner_user_id=user_id, limit=20, symbol="US.NVDA", timeframe="5m")
        self.assertEqual(items, [])

    def test_monitoring_apply_config_builds_runtime_status(self) -> None:
        gateway = FakeGateway()
        dispatcher = FakeDispatcher()
        monitoring = MonitoringService(
            store=self.store,
            gateway=gateway,
            dispatcher=dispatcher,
            planner=SubscriptionPlanner(),
            app_cfg=self.cfg["app"],
        )
        monitoring.start()
        status = monitoring.get_status()
        self.assertTrue(status.started)
        self.assertIn("HK.00700", status.active_symbols)
        self.assertEqual(len(gateway.started), 1)

    def test_market_profiles_cover_us_and_cn(self) -> None:
        us_profile = get_market_profile("US.NVDA")
        cn_profile = get_market_profile("SH.600519")
        self.assertEqual(detect_market_code("US.NVDA"), "US")
        self.assertEqual(detect_market_code("SH.600519"), "CN")
        self.assertEqual(us_profile.timezone, "America/New_York")
        self.assertEqual(cn_profile.timezone, "Asia/Shanghai")

    def test_market_trading_session_respects_us_and_cn_hours(self) -> None:
        us_open = datetime(2026, 3, 13, 10, 0, tzinfo=ZoneInfo("America/New_York"))
        us_closed = datetime(2026, 3, 13, 16, 30, tzinfo=ZoneInfo("America/New_York"))
        cn_open = datetime(2026, 3, 13, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        cn_mid_break = datetime(2026, 3, 13, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        self.assertTrue(market_trading_session("US.NVDA", us_open))
        self.assertFalse(market_trading_session("US.NVDA", us_closed))
        self.assertTrue(market_trading_session("SH.600519", cn_open))
        self.assertFalse(market_trading_session("SH.600519", cn_mid_break))

    def test_effective_trade_date_rolls_back_before_open_and_on_weekends(self) -> None:
        self.assertEqual(
            market_effective_trade_date("US.NVDA", now=datetime(2026, 3, 16, 8, 0, tzinfo=ZoneInfo("America/New_York"))),
            "2026-03-13",
        )
        self.assertEqual(
            market_effective_trade_date("US.NVDA", now=datetime(2026, 3, 16, 10, 0, tzinfo=ZoneInfo("America/New_York"))),
            "2026-03-16",
        )
        self.assertEqual(
            market_effective_trade_date("US.NVDA", now=datetime(2026, 3, 14, 12, 0, tzinfo=ZoneInfo("America/New_York"))),
            "2026-03-13",
        )
        self.assertEqual(
            market_previous_trading_date("US.NVDA", reference_date=datetime(2026, 3, 16, 0, 0, tzinfo=ZoneInfo("America/New_York")).date()),
            "2026-03-13",
        )

    def test_store_purges_signals_by_market_and_trade_date(self) -> None:
        old_hk = Signal(
            symbol="HK.00700",
            symbol_name="腾讯控股",
            timeframe="1m",
            ts=datetime.fromisoformat("2026-03-12T10:00:00+08:00"),
            rule="rsi_oversold",
            message="old hk",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="old-hk",
        )
        new_hk = Signal(
            symbol="HK.00700",
            symbol_name="腾讯控股",
            timeframe="1m",
            ts=datetime.fromisoformat("2026-03-13T10:00:00+08:00"),
            rule="rsi_oversold",
            message="new hk",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="new-hk",
        )
        old_us = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="1m",
            ts=datetime.fromisoformat("2026-03-12T10:00:00-04:00"),
            rule="rsi_oversold",
            message="old us",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="old-us",
        )
        self.store.save_signal(old_hk, [Trigger(name="rsi_oversold", direction="up", message="")])
        self.store.save_signal(new_hk, [Trigger(name="rsi_oversold", direction="up", message="")])
        self.store.save_signal(old_us, [Trigger(name="rsi_oversold", direction="up", message="")])

        deleted = self.store.purge_signals_before_trade_date("HK", "2026-03-13")
        self.assertEqual(deleted, 1)

        remaining_hk = self.store.list_signals_raw(owner_user_id=None, include_global=True, limit=50, symbol="HK.00700")
        remaining_us = self.store.list_signals_raw(owner_user_id=None, include_global=True, limit=50, symbol="US.NVDA")
        self.assertEqual(len(remaining_hk), 1)
        self.assertEqual(remaining_hk[0]["message"], "new hk")
        self.assertEqual(len(remaining_us), 1)
        self.assertEqual(remaining_us[0]["message"], "old us")

    def test_monitoring_clears_previous_trade_day_signals_after_market_open(self) -> None:
        old_us = Signal(
            symbol="US.NVDA",
            symbol_name="NVIDIA",
            timeframe="1m",
            ts=datetime.fromisoformat("2026-03-12T15:55:00-04:00"),
            rule="rsi_oversold",
            message="old us",
            direction="up",
            scope="global",
            owner_user_id=None,
            source_rule="rsi_oversold",
            dedupe_key="old-us-market",
        )
        self.store.save_signal(old_us, [Trigger(name="rsi_oversold", direction="up", message="")])

        gateway = FakeGateway()
        dispatcher = FakeDispatcher()
        monitoring = MonitoringService(
            store=self.store,
            gateway=gateway,
            dispatcher=dispatcher,
            planner=SubscriptionPlanner(),
            app_cfg=self.cfg["app"],
        )
        bar = Bar(
            symbol="US.NVDA",
            timeframe="1m",
            ts=datetime(2026, 3, 13, 9, 31, tzinfo=ZoneInfo("America/New_York")),
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=1000.0,
        )
        monitoring.on_bar(bar)

        remaining_us = self.store.list_signals_raw(owner_user_id=None, include_global=True, limit=50, symbol="US.NVDA")
        self.assertEqual(remaining_us, [])

    def test_monitoring_warmup_uses_effective_trade_day_for_chart_data(self) -> None:
        gateway = FakeGateway()
        monitoring = MonitoringService(
            store=self.store,
            gateway=gateway,
            dispatcher=FakeDispatcher(),
            planner=SubscriptionPlanner(),
            app_cfg=self.cfg["app"],
        )

        from unittest.mock import patch

        with patch("src.application.monitoring.market_effective_trade_date", return_value="2026-03-13"):
            with patch("src.application.monitoring.market_previous_trading_date", return_value="2026-03-12"):
                monitoring._warmup(["US.NVDA"])

        self.assertIn(("US.NVDA", "2026-03-12"), gateway.prev_day_requests)
        self.assertIn(("US.NVDA", "2026-03-13", "2026-03-13", "1m"), gateway.history_requests)
        self.assertIn(("US.NVDA", "2026-03-13", "2026-03-13", "5m"), gateway.history_requests)

    def test_open_range_breakout_uses_market_primary_session(self) -> None:
        state = OpenRangeState()
        first_bar = Bar(
            symbol="US.NVDA",
            timeframe="1m",
            ts=normalize_market_datetime("US.NVDA", datetime(2026, 3, 13, 9, 35), "Asia/Hong_Kong"),
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.5,
            volume=1000.0,
        )
        range_close_bar = Bar(
            symbol="US.NVDA",
            timeframe="1m",
            ts=normalize_market_datetime("US.NVDA", datetime(2026, 3, 13, 9, 45), "Asia/Hong_Kong"),
            open=100.5,
            high=101.2,
            low=100.1,
            close=100.9,
            volume=1200.0,
        )
        second_bar = Bar(
            symbol="US.NVDA",
            timeframe="1m",
            ts=normalize_market_datetime("US.NVDA", datetime(2026, 3, 13, 9, 46), "Asia/Hong_Kong"),
            open=101.0,
            high=103.0,
            low=100.8,
            close=102.8,
            volume=2500.0,
        )
        self.assertIsNone(open_range_breakout(first_bar, state, 15, first_bar.volume, 800.0, 1.5))
        self.assertIsNone(open_range_breakout(range_close_bar, state, 15, range_close_bar.volume, 900.0, 1.5))
        trigger = open_range_breakout(second_bar, state, 15, second_bar.volume, 1000.0, 1.5)
        self.assertIsNotNone(trigger)
        self.assertEqual(trigger.direction, "up")

    def test_registration_state_uses_configured_user_limit(self) -> None:
        self.store.create_user("eva", self.hasher.hash_password("pw"), "user")
        self.store.create_user("fred", self.hasher.hash_password("pw"), "user")
        app = LocalWebApp(
            host="127.0.0.1",
            port=8088,
            store=self.store,
            auth_service=AuthService(self.store, self.hasher),
            query_service=SignalQueryService(self.store),
            monitoring_service=MonitoringService(
                store=self.store,
                gateway=FakeGateway(),
                dispatcher=FakeDispatcher(),
                planner=SubscriptionPlanner(),
                app_cfg=self.cfg["app"],
            ),
            backtest_runner=BacktestRunner(
                store=self.store,
                gateway=FakeGateway(),
                default_rules=self.store.get_default_rule_config(),
                backtest_cfg=self.cfg["backtest"],
                tz_name=self.cfg["app"]["tz"],
            ),
            app_cfg={"auth": {"public_registration": {"enabled": True, "max_users": 2}}},
        )
        registration = app._registration_state()
        self.assertTrue(registration["enabled"])
        self.assertEqual(registration["max_users"], 2)
        self.assertEqual(registration["registered_users"], 2)
        self.assertEqual(registration["remaining_slots"], 0)
        self.assertTrue(registration["limit_reached"])

    def test_duplicate_username_is_rejected(self) -> None:
        self.store.create_user("grace", self.hasher.hash_password("pw"), "user")
        with self.assertRaisesRegex(ValueError, "Username already exists"):
            self.store.create_user("grace", self.hasher.hash_password("pw"), "user")

    def test_personal_watchlist_is_limited_to_one_symbol(self) -> None:
        user_id = self.store.create_user("harry", self.hasher.hash_password("pw"), "user")
        self.store.add_user_symbol(user_id, "HK.00700", "腾讯控股", True)
        with self.assertRaisesRegex(ValueError, "limited to one symbol"):
            self.store.add_user_symbol(user_id, "HK.00981", "中芯国际", True)

    def test_updating_same_personal_symbol_still_allowed(self) -> None:
        user_id = self.store.create_user("ian", self.hasher.hash_password("pw"), "user")
        self.store.add_user_symbol(user_id, "HK.00700", "腾讯控股", True)
        self.store.add_user_symbol(user_id, "HK.00700", "腾讯控股更新", False)
        items = self.store.list_user_symbols(user_id)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["symbol"], "HK.00700")
        self.assertEqual(items[0]["symbol_name"], "腾讯控股更新")
        self.assertFalse(bool(items[0]["enabled"]))

    def test_notification_mode_can_be_disabled(self) -> None:
        user_id = self.store.create_user("jane", self.hasher.hash_password("pw"), "user")
        self.store.save_notification_settings(
            user_id,
            {"mode": "none", "telegram_token": "", "telegram_chat_id": "", "bell_on_alert": False},
        )
        settings = self.store.get_notification_settings_dict(user_id)
        self.assertEqual(settings["mode"], "none")

    def test_non_admin_notification_payload_strips_telegram(self) -> None:
        payload = sanitize_notification_data(
            "user",
            {
                "mode": "both",
                "telegram_token": "secret-token",
                "telegram_chat_id": "123456",
                "bell_on_alert": True,
            },
        )
        self.assertEqual(payload["mode"], "local")
        self.assertEqual(payload["telegram_token"], "")
        self.assertEqual(payload["telegram_chat_id"], "")
        self.assertTrue(payload["bell_on_alert"])

    def test_monitoring_ignores_telegram_settings_for_non_admin_users(self) -> None:
        user_id = self.store.create_user("kate", self.hasher.hash_password("pw"), "user")
        self.store.add_user_symbol(user_id, "US.NVDA", "NVIDIA", True)
        self.store.save_notification_settings(
            user_id,
            {
                "mode": "both",
                "telegram_token": "secret-token",
                "telegram_chat_id": "chat-id",
                "bell_on_alert": True,
            },
        )
        monitoring = MonitoringService(
            store=self.store,
            gateway=FakeGateway(),
            dispatcher=FakeDispatcher(),
            planner=SubscriptionPlanner(),
            app_cfg=self.cfg["app"],
        )

        profiles = monitoring._load_profiles()
        profile = next(item for item in profiles if item.scope == "user" and item.owner_user_id == user_id)

        self.assertIsNotNone(profile.notification)
        self.assertEqual(profile.notification.mode, "local")
        self.assertEqual(profile.notification.telegram_token, "")
        self.assertEqual(profile.notification.telegram_chat_id, "")

    def test_backtest_submit_requires_symbol(self) -> None:
        runner = BacktestRunner(
            store=self.store,
            gateway=FakeGateway(),
            default_rules=self.store.get_default_rule_config(),
            backtest_cfg=self.cfg["backtest"],
            tz_name=self.cfg["app"]["tz"],
        )
        with self.assertRaises(ValueError):
            runner.submit(owner_user_id=1, symbol="  ", trade_date="2026-03-13", rules={})

    def test_backtest_submit_requires_valid_date(self) -> None:
        runner = BacktestRunner(
            store=self.store,
            gateway=FakeGateway(),
            default_rules=self.store.get_default_rule_config(),
            backtest_cfg=self.cfg["backtest"],
            tz_name=self.cfg["app"]["tz"],
        )
        with self.assertRaises(ValueError):
            runner.submit(owner_user_id=1, symbol="HK.00700", trade_date="2026/03/13", rules={})

    def test_only_latest_backtest_persists_per_user(self) -> None:
        first_job = self.store.create_backtest_job(
            owner_user_id=1,
            symbol="HK.00700",
            trade_date="2026-03-12",
            profile_scope="user",
            profile_owner_user_id=1,
            template_version_id=None,
            params={"rules": {}},
        )
        self.store.replace_backtest_results(
            first_job,
            [{"signal_ts": "2026-03-12T09:35:00+08:00", "timeframe": "1m", "signal_text": "first", "result": {"passed": True}}],
        )

        second_job = self.store.create_backtest_job(
            owner_user_id=1,
            symbol="HK.00981",
            trade_date="2026-03-13",
            profile_scope="user",
            profile_owner_user_id=1,
            template_version_id=None,
            params={"rules": {}},
        )

        jobs = self.store.list_backtest_jobs(owner_user_id=1, limit=10)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["id"], second_job)
        self.assertIsNone(self.store.get_backtest_job(first_job))


if __name__ == "__main__":
    unittest.main()
