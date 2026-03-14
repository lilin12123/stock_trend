from __future__ import annotations

import argparse
import json

from .application import deep_merge_dict, load_config
from .application.backtesting import BacktestRunner
from .infrastructure import OpenDGateway, SqliteStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--symbol", default="HK.00981")
    parser.add_argument("--date", required=True)
    parser.add_argument("--rules-json", default="")
    args = parser.parse_args()

    cfg = load_config(args.config)
    app_cfg = cfg.get("app", {})
    db_path = app_cfg.get("storage", {}).get("db_path", "data/app.sqlite")
    store = SqliteStore(db_path)
    gateway = OpenDGateway(
        host=app_cfg.get("open_d", {}).get("host", "127.0.0.1"),
        port=int(app_cfg.get("open_d", {}).get("port", 11111)),
    )
    rules = store.get_default_rule_config() or cfg.get("rules", {})
    if args.rules_json:
        rules = deep_merge_dict(rules, json.loads(args.rules_json))
    runner = BacktestRunner(
        store=store,
        gateway=gateway,
        default_rules=rules,
        backtest_cfg=cfg.get("backtest", {}),
        tz_name=app_cfg.get("tz", "Asia/Hong_Kong"),
    )
    payload = runner.run_once(args.symbol, args.date, rules)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
