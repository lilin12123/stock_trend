from __future__ import annotations

from typing import Any, Dict

from ..domain import NotificationSetting


ADMIN_NOTIFICATION_MODES = {"none", "local", "telegram", "both"}
USER_NOTIFICATION_MODES = {"none", "local"}


def sanitize_notification_data(role: str, data: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(data.get("mode", "local") or "local")
    bell_on_alert = bool(data.get("bell_on_alert"))
    telegram_token = str(data.get("telegram_token", "") or "")
    telegram_chat_id = str(data.get("telegram_chat_id", "") or "")

    if role == "admin":
        if mode not in ADMIN_NOTIFICATION_MODES:
            mode = "local"
        return {
            "mode": mode,
            "telegram_token": telegram_token,
            "telegram_chat_id": telegram_chat_id,
            "bell_on_alert": bell_on_alert,
        }

    if mode not in USER_NOTIFICATION_MODES:
        mode = "local"
    return {
        "mode": mode,
        "telegram_token": "",
        "telegram_chat_id": "",
        "bell_on_alert": bell_on_alert,
    }


def sanitize_notification_setting(role: str, setting: NotificationSetting) -> NotificationSetting:
    sanitized = sanitize_notification_data(
        role,
        {
            "mode": setting.mode,
            "telegram_token": setting.telegram_token,
            "telegram_chat_id": setting.telegram_chat_id,
            "bell_on_alert": setting.bell_on_alert,
        },
    )
    return NotificationSetting(
        owner_user_id=setting.owner_user_id,
        mode=sanitized["mode"],
        telegram_token=sanitized["telegram_token"],
        telegram_chat_id=sanitized["telegram_chat_id"],
        bell_on_alert=sanitized["bell_on_alert"],
    )
