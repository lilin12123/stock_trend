from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..infrastructure import PasswordHasher, SqliteStore


class AuthService:
    def __init__(self, store: SqliteStore, hasher: PasswordHasher) -> None:
        self.store = store
        self.hasher = hasher

    def login(self, username: str, password: str) -> Dict[str, Any]:
        user = self.store.get_user_by_username(username)
        if not user or not user.get("is_active"):
            return {"ok": False, "error_code": "invalid_credentials"}
        locked_until = user.get("locked_until")
        if locked_until:
            locked_at = datetime.fromisoformat(locked_until)
            if locked_at > datetime.now(timezone.utc):
                return {"ok": False, "error_code": "account_locked", "locked_until": locked_until}
            self.store.clear_login_failures(int(user["id"]))
        if not self.hasher.verify_password(password, user["password_hash"]):
            result = self.store.record_login_failure(int(user["id"]))
            if result.get("locked_until"):
                return {
                    "ok": False,
                    "error_code": "account_locked",
                    "locked_until": result["locked_until"],
                    "remaining_attempts": 0,
                }
            return {
                "ok": False,
                "error_code": "invalid_credentials",
                "remaining_attempts": result["remaining_attempts"],
            }
        self.store.clear_login_failures(int(user["id"]))
        return {"ok": True, "session_id": self.store.create_session(int(user["id"]))}

    def logout(self, session_id: str) -> None:
        self.store.delete_session(session_id)

    def authenticate(self, session_id: str) -> Optional[Dict[str, Any]]:
        return self.store.get_session_user(session_id)
