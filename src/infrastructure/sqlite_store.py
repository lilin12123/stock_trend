from __future__ import annotations

import json
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..domain import NotificationSetting, Signal, Trigger, User


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SqliteStore:
    LOGIN_MAX_ATTEMPTS = 5
    LOGIN_LOCK_MINUTES = 15

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
                    locked_until TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS global_symbols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    symbol_name TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS user_symbols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    UNIQUE(user_id, symbol),
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS rule_profiles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_user_id INTEGER,
                    scope TEXT NOT NULL,
                    profile_name TEXT NOT NULL,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(owner_user_id, scope),
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS notification_settings (
                    owner_user_id INTEGER PRIMARY KEY,
                    mode TEXT NOT NULL,
                    telegram_token TEXT NOT NULL DEFAULT '',
                    telegram_chat_id TEXT NOT NULL DEFAULT '',
                    bell_on_alert INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS signals (
                    id TEXT PRIMARY KEY,
                    owner_user_id INTEGER,
                    scope TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT,
                    timeframe TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    source_rule TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    message TEXT NOT NULL,
                    strength REAL,
                    direction TEXT,
                    dedupe_key TEXT NOT NULL,
                    context_snapshot TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS signal_triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    message TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS runtime_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rule_templates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT NOT NULL DEFAULT '',
                    created_by_user_id INTEGER,
                    is_system INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS rule_template_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    template_id INTEGER NOT NULL,
                    version_num INTEGER NOT NULL,
                    config_json TEXT NOT NULL,
                    created_by_user_id INTEGER,
                    created_at TEXT NOT NULL,
                    UNIQUE(template_id, version_num),
                    FOREIGN KEY(template_id) REFERENCES rule_templates(id) ON DELETE CASCADE,
                    FOREIGN KEY(created_by_user_id) REFERENCES users(id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS backtest_jobs (
                    id TEXT PRIMARY KEY,
                    owner_user_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    profile_scope TEXT NOT NULL,
                    profile_owner_user_id INTEGER,
                    template_version_id INTEGER,
                    status TEXT NOT NULL,
                    params_json TEXT NOT NULL DEFAULT '{}',
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    error_text TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    finished_at TEXT,
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY(profile_owner_user_id) REFERENCES users(id) ON DELETE SET NULL,
                    FOREIGN KEY(template_version_id) REFERENCES rule_template_versions(id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS backtest_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    signal_ts TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    signal_text TEXT NOT NULL,
                    signal_id TEXT,
                    result_json TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES backtest_jobs(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS notification_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    owner_user_id INTEGER,
                    channel TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_text TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signals(id) ON DELETE CASCADE,
                    FOREIGN KEY(owner_user_id) REFERENCES users(id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            self._ensure_column(conn, "users", "failed_login_attempts", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "users", "locked_until", "TEXT")

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def seed_from_config(self, cfg: Dict[str, Any], password_hash: str) -> None:
        app_cfg = cfg.get("app", {})
        auth_cfg = app_cfg.get("auth", {}).get("bootstrap_admin", {})
        username = auth_cfg.get("username", "admin")
        notify_cfg = app_cfg.get("notify", {})
        now = _utc_now()
        with self._connect() as conn:
            users_count = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
            admin_id: Optional[int] = None
            row = conn.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
            if row:
                admin_id = int(row["id"])
                conn.execute(
                    """
                    UPDATE users
                    SET password_hash = ?,
                        role = 'admin',
                        is_active = 1,
                        failed_login_attempts = 0,
                        locked_until = NULL,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (password_hash, now, admin_id),
                )
            elif users_count == 0:
                cursor = conn.execute(
                    """
                    INSERT INTO users
                    (username, password_hash, role, is_active, failed_login_attempts, locked_until, created_at, updated_at)
                    VALUES (?, ?, 'admin', 1, 0, NULL, ?, ?)
                    """,
                    (username, password_hash, now, now),
                )
                admin_id = int(cursor.lastrowid)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO users
                    (username, password_hash, role, is_active, failed_login_attempts, locked_until, created_at, updated_at)
                    VALUES (?, ?, 'admin', 1, 0, NULL, ?, ?)
                    """,
                    (username, password_hash, now, now),
                )
                admin_id = int(cursor.lastrowid)

            if conn.execute("SELECT COUNT(*) AS c FROM global_symbols").fetchone()["c"] == 0:
                symbol_names = app_cfg.get("symbol_names", {})
                for symbol in app_cfg.get("symbols", []):
                    conn.execute(
                        "INSERT OR IGNORE INTO global_symbols (symbol, symbol_name, enabled, created_at) VALUES (?, ?, 1, ?)",
                        (symbol, symbol_names.get(symbol), now),
                    )

            if conn.execute("SELECT COUNT(*) AS c FROM rule_profiles WHERE scope = 'global'").fetchone()["c"] == 0:
                conn.execute(
                    """
                    INSERT INTO rule_profiles (owner_user_id, scope, profile_name, config_json, created_at, updated_at)
                    VALUES (NULL, 'global', 'default', ?, ?, ?)
                    """,
                    (json.dumps(cfg.get("rules", {}), ensure_ascii=False), now, now),
                )

            if conn.execute("SELECT COUNT(*) AS c FROM app_settings WHERE key = 'forward_metrics'").fetchone()["c"] == 0:
                forward_cfg = app_cfg.get("forward_metrics", {})
                conn.execute(
                    """
                    INSERT INTO app_settings (key, value_json, updated_at)
                    VALUES ('forward_metrics', ?, ?)
                    """,
                    (
                        json.dumps(
                            {
                                "1m_horizon_minutes": int(forward_cfg.get("1m_horizon_minutes", 20)),
                                "5m_horizon_minutes": int(forward_cfg.get("5m_horizon_minutes", 60)),
                            },
                            ensure_ascii=False,
                        ),
                        now,
                    ),
                )

            if admin_id and conn.execute(
                "SELECT COUNT(*) AS c FROM notification_settings WHERE owner_user_id = ?",
                (admin_id,),
            ).fetchone()["c"] == 0:
                telegram_cfg = notify_cfg.get("telegram", {})
                conn.execute(
                    """
                    INSERT INTO notification_settings
                    (owner_user_id, mode, telegram_token, telegram_chat_id, bell_on_alert, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        admin_id,
                        notify_cfg.get("mode", "local"),
                        telegram_cfg.get("token", ""),
                        telegram_cfg.get("chat_id", ""),
                        0,
                        now,
                    ),
                )

            if conn.execute("SELECT COUNT(*) AS c FROM rule_templates").fetchone()["c"] == 0:
                cursor = conn.execute(
                    """
                    INSERT INTO rule_templates (name, description, created_by_user_id, is_system, created_at)
                    VALUES ('default', 'Seeded from config.yaml', ?, 1, ?)
                    """,
                    (admin_id, now),
                )
                template_id = int(cursor.lastrowid)
                conn.execute(
                    """
                    INSERT INTO rule_template_versions
                    (template_id, version_num, config_json, created_by_user_id, created_at)
                    VALUES (?, 1, ?, ?, ?)
                    """,
                    (template_id, json.dumps(cfg.get("rules", {}), ensure_ascii=False), admin_id, now),
                )

    def list_users(self) -> List[User]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, username, role, is_active, created_at FROM users ORDER BY id ASC"
            ).fetchall()
        return [
            User(
                id=int(row["id"]),
                username=row["username"],
                role=row["role"],
                is_active=bool(row["is_active"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def count_users(self, role: Optional[str] = None) -> int:
        with self._connect() as conn:
            if role:
                row = conn.execute("SELECT COUNT(*) AS c FROM users WHERE role = ?", (role,)).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return int(row["c"]) if row else 0

    def get_user_by_id(self, user_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, username, password_hash, role, is_active, failed_login_attempts, locked_until, created_at
                FROM users WHERE id = ?
                """,
                (user_id,),
            ).fetchone()
        return dict(row) if row else None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, username, password_hash, role, is_active, failed_login_attempts, locked_until, created_at
                FROM users WHERE username = ?
                """,
                (username,),
            ).fetchone()
        return dict(row) if row else None

    def create_user(self, username: str, password_hash: str, role: str = "user") -> int:
        username = username.strip()
        if not username:
            raise ValueError("Username is required")
        if not password_hash:
            raise ValueError("Password is required")
        if role not in {"user", "admin"}:
            raise ValueError("Role must be user or admin")
        now = _utc_now()
        with self._connect() as conn:
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO users
                    (username, password_hash, role, is_active, failed_login_attempts, locked_until, created_at, updated_at)
                    VALUES (?, ?, ?, 1, 0, NULL, ?, ?)
                    """,
                    (username, password_hash, role, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("Username already exists") from exc
            user_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO notification_settings (owner_user_id, mode, telegram_token, telegram_chat_id, bell_on_alert, updated_at)
                VALUES (?, 'local', '', '', 0, ?)
                """,
                (user_id, now),
            )
            conn.execute(
                """
                INSERT INTO rule_profiles (owner_user_id, scope, profile_name, config_json, created_at, updated_at)
                VALUES (?, 'user', 'personal', '{}', ?, ?)
                """,
                (user_id, now, now),
            )
        return user_id

    def update_user_password(self, user_id: int, password_hash: str) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET password_hash = ?, failed_login_attempts = 0, locked_until = NULL, updated_at = ?
                WHERE id = ?
                """,
                (password_hash, now, user_id),
            )

    def update_user_active_state(self, user_id: int, is_active: bool) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET is_active = ?, failed_login_attempts = 0, locked_until = NULL, updated_at = ?
                WHERE id = ?
                """,
                (1 if is_active else 0, now, user_id),
            )
            if not is_active:
                conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))

    def clear_login_failures(self, user_id: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET failed_login_attempts = 0, locked_until = NULL, updated_at = ?
                WHERE id = ?
                """,
                (_utc_now(), user_id),
            )

    def record_login_failure(self, user_id: int) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT failed_login_attempts, locked_until FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            attempts = int(row["failed_login_attempts"]) + 1 if row else 1
            locked_until = None
            if attempts >= self.LOGIN_MAX_ATTEMPTS:
                locked_until = (now + timedelta(minutes=self.LOGIN_LOCK_MINUTES)).replace(microsecond=0).isoformat()
            conn.execute(
                """
                UPDATE users
                SET failed_login_attempts = ?, locked_until = ?, updated_at = ?
                WHERE id = ?
                """,
                (attempts, locked_until, now.replace(microsecond=0).isoformat(), user_id),
            )
        return {
            "failed_login_attempts": attempts,
            "remaining_attempts": max(0, self.LOGIN_MAX_ATTEMPTS - attempts),
            "locked_until": locked_until,
        }

    def create_session(self, user_id: int, days: int = 7) -> str:
        session_id = secrets.token_urlsafe(32)
        now = datetime.now(timezone.utc)
        expires_at = (now + timedelta(days=days)).replace(microsecond=0).isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO sessions (id, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
                (session_id, user_id, expires_at, now.replace(microsecond=0).isoformat()),
            )
        return session_id

    def get_session_user(self, session_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT u.id, u.username, u.role, u.is_active, s.expires_at
                FROM sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.id = ?
                """,
                (session_id,),
            ).fetchone()
        if not row:
            return None
        if datetime.fromisoformat(row["expires_at"]) < datetime.now(timezone.utc):
            self.delete_session(session_id)
            return None
        return dict(row)

    def delete_session(self, session_id: str) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sessions WHERE id = ?", (session_id,))

    def list_global_symbols(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol, symbol_name, enabled, created_at FROM global_symbols ORDER BY symbol ASC"
            ).fetchall()
        return [dict(row) for row in rows]

    def add_global_symbol(self, symbol: str, symbol_name: str = "", enabled: bool = True) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO global_symbols (symbol, symbol_name, enabled, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET symbol_name = excluded.symbol_name, enabled = excluded.enabled
                """,
                (symbol, symbol_name, 1 if enabled else 0, _utc_now()),
            )

    def list_user_symbols(self, user_id: int) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT symbol, symbol_name, enabled, created_at FROM user_symbols WHERE user_id = ? ORDER BY symbol ASC",
                (user_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def count_user_symbols(self, user_id: int) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM user_symbols WHERE user_id = ?", (user_id,)).fetchone()
        return int(row["c"]) if row else 0

    def add_user_symbol(self, user_id: int, symbol: str, symbol_name: str = "", enabled: bool = True) -> None:
        symbol = symbol.strip()
        if not symbol:
            raise ValueError("Symbol is required")
        with self._connect() as conn:
            user_row = conn.execute("SELECT role FROM users WHERE id = ?", (user_id,)).fetchone()
            role = str(user_row["role"]) if user_row else "user"
            existing_rows = conn.execute(
                "SELECT symbol FROM user_symbols WHERE user_id = ?",
                (user_id,),
            ).fetchall()
            existing_symbols = {str(row["symbol"]) for row in existing_rows}
            if role != "admin" and symbol not in existing_symbols and existing_symbols:
                raise ValueError("Personal watchlist is limited to one symbol")
            conn.execute(
                """
                INSERT INTO user_symbols (user_id, symbol, symbol_name, enabled, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, symbol) DO UPDATE SET symbol_name = excluded.symbol_name, enabled = excluded.enabled
                """,
                (user_id, symbol, symbol_name, 1 if enabled else 0, _utc_now()),
            )

    def get_default_rule_config(self) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT config_json FROM rule_profiles WHERE scope = 'global' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return json.loads(row["config_json"]) if row else {}

    def get_forward_metrics_config(self, app_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        defaults = {
            "1m_horizon_minutes": int((app_cfg or {}).get("forward_metrics", {}).get("1m_horizon_minutes", 20)),
            "5m_horizon_minutes": int((app_cfg or {}).get("forward_metrics", {}).get("5m_horizon_minutes", 60)),
        }
        with self._connect() as conn:
            row = conn.execute("SELECT value_json FROM app_settings WHERE key = 'forward_metrics'").fetchone()
        if not row:
            return defaults
        try:
            stored = json.loads(row["value_json"] or "{}")
        except json.JSONDecodeError:
            return defaults
        return {
            "1m_horizon_minutes": max(1, int(stored.get("1m_horizon_minutes", defaults["1m_horizon_minutes"]))),
            "5m_horizon_minutes": max(5, int(stored.get("5m_horizon_minutes", defaults["5m_horizon_minutes"]))),
        }

    def save_forward_metrics_config(self, data: Dict[str, Any], app_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        current = self.get_forward_metrics_config(app_cfg)
        cleaned = {
            "1m_horizon_minutes": max(1, int(data.get("1m_horizon_minutes", current["1m_horizon_minutes"]))),
            "5m_horizon_minutes": max(5, int(data.get("5m_horizon_minutes", current["5m_horizon_minutes"]))),
        }
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO app_settings (key, value_json, updated_at)
                VALUES ('forward_metrics', ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (json.dumps(cleaned, ensure_ascii=False), _utc_now()),
            )
        return cleaned

    def get_user_rule_overrides(self, user_id: int) -> Dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT config_json FROM rule_profiles WHERE scope = 'user' AND owner_user_id = ?",
                (user_id,),
            ).fetchone()
        return json.loads(row["config_json"]) if row else {}

    def save_user_rule_overrides(self, user_id: int, overrides: Dict[str, Any]) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO rule_profiles (owner_user_id, scope, profile_name, config_json, created_at, updated_at)
                VALUES (?, 'user', 'personal', ?, ?, ?)
                ON CONFLICT(owner_user_id, scope) DO UPDATE SET config_json = excluded.config_json, updated_at = excluded.updated_at
                """,
                (user_id, json.dumps(overrides, ensure_ascii=False), now, now),
            )

    def get_notification_settings(self, user_id: int) -> NotificationSetting:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT owner_user_id, mode, telegram_token, telegram_chat_id, bell_on_alert
                FROM notification_settings
                WHERE owner_user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if not row:
            return NotificationSetting(owner_user_id=user_id, mode="local")
        return NotificationSetting(
            owner_user_id=int(row["owner_user_id"]),
            mode=row["mode"],
            telegram_token=row["telegram_token"],
            telegram_chat_id=row["telegram_chat_id"],
            bell_on_alert=bool(row["bell_on_alert"]),
        )

    def get_notification_settings_dict(self, user_id: int) -> Dict[str, Any]:
        setting = self.get_notification_settings(user_id)
        return {
            "owner_user_id": setting.owner_user_id,
            "mode": setting.mode,
            "telegram_token": setting.telegram_token,
            "telegram_chat_id": setting.telegram_chat_id,
            "bell_on_alert": setting.bell_on_alert,
        }

    def save_notification_settings(self, user_id: int, data: Dict[str, Any]) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notification_settings
                (owner_user_id, mode, telegram_token, telegram_chat_id, bell_on_alert, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(owner_user_id) DO UPDATE SET
                    mode = excluded.mode,
                    telegram_token = excluded.telegram_token,
                    telegram_chat_id = excluded.telegram_chat_id,
                    bell_on_alert = excluded.bell_on_alert,
                    updated_at = excluded.updated_at
                """,
                (
                    user_id,
                    data.get("mode", "local"),
                    data.get("telegram_token", ""),
                    data.get("telegram_chat_id", ""),
                    1 if data.get("bell_on_alert") else 0,
                    now,
                ),
            )

    def save_signal(self, signal: Signal, triggers: Iterable[Trigger]) -> str:
        signal_id = signal.signal_id or uuid.uuid4().hex
        now = _utc_now()
        context_snapshot = signal.context_snapshot if signal.context_snapshot is not None else (signal.context or {})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO signals
                (id, owner_user_id, scope, symbol, symbol_name, timeframe, ts, source_rule, rule_name,
                 message, strength, direction, dedupe_key, context_snapshot, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal_id,
                    signal.owner_user_id,
                    signal.scope,
                    signal.symbol,
                    signal.symbol_name,
                    signal.timeframe,
                    signal.ts.isoformat(),
                    signal.source_rule or signal.rule,
                    signal.rule,
                    signal.message,
                    signal.strength,
                    signal.direction,
                    signal.dedupe_key or signal_id,
                    json.dumps(context_snapshot, ensure_ascii=False),
                    now,
                ),
            )
            conn.execute("DELETE FROM signal_triggers WHERE signal_id = ?", (signal_id,))
            for trigger in triggers:
                conn.execute(
                    "INSERT INTO signal_triggers (signal_id, name, direction, message) VALUES (?, ?, ?, ?)",
                    (signal_id, trigger.name, trigger.direction, trigger.message),
                )
        return signal_id

    def update_signal_context_snapshot(self, signal_id: str, context_snapshot: Dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE signals SET context_snapshot = ? WHERE id = ?",
                (json.dumps(context_snapshot or {}, ensure_ascii=False), signal_id),
            )

    def list_signals_raw(
        self,
        owner_user_id: Optional[int],
        include_global: bool = True,
        limit: int = 400,
        offset: int = 0,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        text: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        clauses = []
        params: List[Any] = []
        if owner_user_id is None:
            clauses.append("owner_user_id IS NULL")
        elif include_global:
            clauses.append("(owner_user_id = ? OR owner_user_id IS NULL)")
            params.append(owner_user_id)
        else:
            clauses.append("owner_user_id = ?")
            params.append(owner_user_id)
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if timeframe:
            clauses.append("LOWER(timeframe) = ?")
            params.append(timeframe.lower())
        if text:
            like = f"%{text}%"
            clauses.append("(message LIKE ? OR symbol LIKE ? OR COALESCE(symbol_name, '') LIKE ?)")
            params.extend([like, like, like])
        where = " AND ".join(clauses) if clauses else "1 = 1"
        query = f"""
            SELECT *
            FROM signals
            WHERE {where}
            ORDER BY ts DESC, id DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["triggers"] = self.get_signal_triggers(item["id"])
            item["context_snapshot"] = json.loads(item["context_snapshot"] or "{}")
            items.append(item)
        return items

    def get_signal_triggers(self, signal_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT name, direction, message FROM signal_triggers WHERE signal_id = ? ORDER BY id ASC",
                (signal_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_signal(self, signal_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
        if not row:
            return None
        item = dict(row)
        item["triggers"] = self.get_signal_triggers(signal_id)
        item["context_snapshot"] = json.loads(item["context_snapshot"] or "{}")
        return item

    def purge_signals_before_trade_date(self, market_code: str, trade_date: str) -> int:
        market = str(market_code or "").upper()
        where = self._signal_market_where(market)
        if not where:
            return 0
        with self._connect() as conn:
            cursor = conn.execute(
                f"""
                DELETE FROM signals
                WHERE substr(ts, 1, 10) < ?
                  AND ({where})
                """,
                (trade_date,),
            )
            deleted = cursor.rowcount if cursor.rowcount is not None else 0
        return max(0, deleted)

    @staticmethod
    def _signal_market_where(market_code: str) -> str:
        if market_code == "US":
            return "symbol LIKE 'US.%'"
        if market_code == "CN":
            return "symbol LIKE 'SH.%' OR symbol LIKE 'SZ.%' OR symbol LIKE 'BJ.%'"
        if market_code == "HK":
            return "symbol LIKE 'HK.%'"
        return ""

    def add_runtime_event(self, level: str, event_type: str, message: str, payload: Dict[str, Any] | None = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runtime_events (level, event_type, message, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (level, event_type, message, json.dumps(payload or {}, ensure_ascii=False), _utc_now()),
            )

    def list_runtime_events(self, limit: int = 20, event_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        query = "SELECT id, level, event_type, message, payload_json, created_at FROM runtime_events"
        params: List[Any] = []
        if event_types:
            placeholders = ", ".join("?" for _ in event_types)
            query += f" WHERE event_type IN ({placeholders})"
            params.extend(event_types)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
            items.append(item)
        return items

    def create_notification_event(self, signal_id: str, owner_user_id: Optional[int], channel: str, status: str, error_text: str = "") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notification_events (signal_id, owner_user_id, channel, status, error_text, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (signal_id, owner_user_id, channel, status, error_text, _utc_now()),
            )

    def create_rule_template(self, name: str, description: str, config: Dict[str, Any], created_by_user_id: Optional[int], is_system: bool = False) -> int:
        now = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO rule_templates (name, description, created_by_user_id, is_system, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, description, created_by_user_id, 1 if is_system else 0, now),
            )
            template_id = int(cursor.lastrowid)
            conn.execute(
                """
                INSERT INTO rule_template_versions (template_id, version_num, config_json, created_by_user_id, created_at)
                VALUES (?, 1, ?, ?, ?)
                """,
                (template_id, json.dumps(config, ensure_ascii=False), created_by_user_id, now),
            )
        return template_id

    def list_rule_templates(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT t.id, t.name, t.description, t.is_system, t.created_at, v.id AS version_id, v.version_num, v.config_json
                FROM rule_templates t
                JOIN rule_template_versions v ON v.template_id = t.id
                WHERE v.id = (
                    SELECT id FROM rule_template_versions v2
                    WHERE v2.template_id = t.id
                    ORDER BY version_num DESC LIMIT 1
                )
                ORDER BY t.id ASC
                """
            ).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["config"] = json.loads(item.pop("config_json") or "{}")
            items.append(item)
        return items

    def get_rule_template_version_config(self, version_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT config_json FROM rule_template_versions WHERE id = ?",
                (version_id,),
            ).fetchone()
        return json.loads(row["config_json"]) if row else None

    def create_backtest_job(
        self,
        owner_user_id: int,
        symbol: str,
        trade_date: str,
        profile_scope: str,
        profile_owner_user_id: Optional[int],
        template_version_id: Optional[int],
        params: Dict[str, Any],
    ) -> str:
        job_id = uuid.uuid4().hex
        with self._connect() as conn:
            existing_rows = conn.execute(
                "SELECT id FROM backtest_jobs WHERE owner_user_id = ?",
                (owner_user_id,),
            ).fetchall()
            existing_ids = [row["id"] for row in existing_rows]
            if existing_ids:
                conn.executemany(
                    "DELETE FROM backtest_results WHERE job_id = ?",
                    [(existing_id,) for existing_id in existing_ids],
                )
                conn.execute(
                    "DELETE FROM backtest_jobs WHERE owner_user_id = ?",
                    (owner_user_id,),
                )
            conn.execute(
                """
                INSERT INTO backtest_jobs
                (id, owner_user_id, symbol, trade_date, profile_scope, profile_owner_user_id, template_version_id,
                 status, params_json, summary_json, error_text, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?, '{}', '', ?)
                """,
                (
                    job_id,
                    owner_user_id,
                    symbol,
                    trade_date,
                    profile_scope,
                    profile_owner_user_id,
                    template_version_id,
                    json.dumps(params, ensure_ascii=False),
                    _utc_now(),
                ),
            )
        return job_id

    def set_backtest_job_status(self, job_id: str, status: str, summary: Dict[str, Any] | None = None, error_text: str = "") -> None:
        finished_at = _utc_now() if status in {"done", "failed"} else None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE backtest_jobs
                SET status = ?, summary_json = ?, error_text = ?, finished_at = COALESCE(?, finished_at)
                WHERE id = ?
                """,
                (status, json.dumps(summary or {}, ensure_ascii=False), error_text, finished_at, job_id),
            )

    def replace_backtest_results(self, job_id: str, results: List[Dict[str, Any]]) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM backtest_results WHERE job_id = ?", (job_id,))
            for item in results:
                conn.execute(
                    """
                    INSERT INTO backtest_results (job_id, signal_ts, timeframe, signal_text, signal_id, result_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        job_id,
                        item["signal_ts"],
                        item["timeframe"],
                        item["signal_text"],
                        item.get("signal_id"),
                        json.dumps(item["result"], ensure_ascii=False),
                    ),
                )

    def get_backtest_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM backtest_jobs WHERE id = ?", (job_id,)).fetchone()
            if not row:
                return None
            result_rows = conn.execute(
                "SELECT signal_ts, timeframe, signal_text, signal_id, result_json FROM backtest_results WHERE job_id = ? ORDER BY id ASC",
                (job_id,),
            ).fetchall()
        item = dict(row)
        item["params"] = json.loads(item.pop("params_json") or "{}")
        item["summary"] = json.loads(item.pop("summary_json") or "{}")
        item["results"] = [
            {
                "signal_ts": result["signal_ts"],
                "timeframe": result["timeframe"],
                "signal_text": result["signal_text"],
                "signal_id": result["signal_id"],
                "result": json.loads(result["result_json"] or "{}"),
            }
            for result in result_rows
        ]
        return item

    def list_backtest_jobs(self, owner_user_id: Optional[int] = None, limit: int = 20) -> List[Dict[str, Any]]:
        query = "SELECT * FROM backtest_jobs"
        params: List[Any] = []
        if owner_user_id is not None:
            query += " WHERE owner_user_id = ?"
            params.append(owner_user_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        items = []
        for row in rows:
            item = dict(row)
            item["params"] = json.loads(item.pop("params_json") or "{}")
            item["summary"] = json.loads(item.pop("summary_json") or "{}")
            items.append(item)
        return items

    def analytics_summary(self, owner_user_id: Optional[int]) -> Dict[str, Any]:
        with self._connect() as conn:
            args: List[Any] = []
            where = "1 = 1"
            if owner_user_id is not None:
                where = "(owner_user_id = ? OR owner_user_id IS NULL)"
                args.append(owner_user_id)
            count_row = conn.execute(f"SELECT COUNT(*) AS c FROM signals WHERE {where}", args).fetchone()
            rule_rows = conn.execute(
                f"""
                SELECT source_rule, COUNT(*) AS c
                FROM signals
                WHERE {where}
                GROUP BY source_rule
                ORDER BY c DESC
                LIMIT 10
                """,
                args,
            ).fetchall()
            if owner_user_id is not None:
                job_rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS c
                    FROM backtest_jobs
                    WHERE owner_user_id = ?
                    GROUP BY status
                    ORDER BY status ASC
                    """,
                    (owner_user_id,),
                ).fetchall()
            else:
                job_rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS c
                    FROM backtest_jobs
                    GROUP BY status
                    ORDER BY status ASC
                    """
                ).fetchall()
        return {
            "signals_total": int(count_row["c"]),
            "top_rules": [dict(row) for row in rule_rows],
            "backtest_status": [dict(row) for row in job_rows],
        }
