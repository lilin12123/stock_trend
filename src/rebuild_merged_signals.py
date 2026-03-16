from __future__ import annotations

import argparse
import sqlite3

from .application import load_config
from .infrastructure import SqliteStore


def _count_rows(db_path: str, table: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return int(row[0] if row else 0)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild merged_signals from current live signals.")
    parser.add_argument("--config", default="config.yaml", help="Path to config yaml.")
    args = parser.parse_args()

    cfg = load_config(args.config)
    db_path = cfg.get("app", {}).get("storage", {}).get("db_path", "data/app.sqlite")

    before_signals = _count_rows(db_path, "signals")
    before_merged = _count_rows(db_path, "merged_signals")

    store = SqliteStore(db_path)
    store.rebuild_merged_signals()

    after_merged = _count_rows(db_path, "merged_signals")

    print(f"db_path={db_path}")
    print(f"signals={before_signals}")
    print(f"merged_signals_before={before_merged}")
    print(f"merged_signals_after={after_merged}")


if __name__ == "__main__":
    main()
