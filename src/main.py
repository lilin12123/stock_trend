from __future__ import annotations

import argparse

from .application import load_config
from .application.auth_service import AuthService
from .application.backtesting import BacktestRunner
from .application.monitoring import MonitoringService
from .application.query_service import SignalQueryService
from .application.subscriptions import SubscriptionPlanner
from .infrastructure import NotificationDispatcher, OpenDGateway, PasswordHasher, SqliteStore
from .presentation import LocalWebApp


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    app_cfg = cfg.get("app", {})
    storage_cfg = app_cfg.get("storage", {})
    db_path = storage_cfg.get("db_path", "data/app.sqlite")

    store = SqliteStore(db_path)
    hasher = PasswordHasher()
    bootstrap_password = app_cfg.get("auth", {}).get("bootstrap_admin", {}).get("password", "admin")
    store.seed_from_config(cfg, hasher.hash_password(bootstrap_password))

    auth_service = AuthService(store, hasher)
    gateway = OpenDGateway(
        host=app_cfg.get("open_d", {}).get("host", "127.0.0.1"),
        port=int(app_cfg.get("open_d", {}).get("port", 11111)),
    )
    dispatcher = NotificationDispatcher(
        event_logger=lambda level, event_type, payload: store.add_runtime_event(level, event_type, event_type, payload),
        delivery_logger=store.create_notification_event,
    )
    monitoring_service = MonitoringService(
        store=store,
        gateway=gateway,
        dispatcher=dispatcher,
        planner=SubscriptionPlanner(),
        app_cfg=app_cfg,
    )
    query_service = SignalQueryService(store)
    backtest_runner = BacktestRunner(
        store=store,
        gateway=gateway,
        default_rules=store.get_default_rule_config(),
        backtest_cfg=cfg.get("backtest", {}),
        tz_name=app_cfg.get("tz", "Asia/Hong_Kong"),
    )
    web_cfg = app_cfg.get("web", {})
    app = LocalWebApp(
        host=web_cfg.get("host", "127.0.0.1"),
        port=int(web_cfg.get("port", 8088)),
        store=store,
        auth_service=auth_service,
        query_service=query_service,
        monitoring_service=monitoring_service,
        backtest_runner=backtest_runner,
        app_cfg=app_cfg,
    )

    monitoring_service.start()
    try:
        app.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        monitoring_service.stop()


if __name__ == "__main__":
    main()
