from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from ..domain import NotificationSetting
from ..notify.local import send_local
from ..notify.telegram import send_telegram


class NotificationDispatcher:
    def __init__(
        self,
        event_logger: Callable[[str, str, dict | None], None],
        delivery_logger: Callable[[str, Optional[int], str, str, str], None],
        max_workers: int = 4,
    ) -> None:
        self._event_logger = event_logger
        self._delivery_logger = delivery_logger
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def dispatch(self, signal_id: str, settings: NotificationSetting, message: str) -> None:
        if settings.mode in ("telegram", "both"):
            self._executor.submit(self._send_telegram, signal_id, settings, message)
        if settings.mode in ("local", "both"):
            self._executor.submit(self._send_local, signal_id, settings, message)

    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def _send_telegram(self, signal_id: str, settings: NotificationSetting, message: str) -> None:
        try:
            send_telegram(message, settings.telegram_token, settings.telegram_chat_id)
            self._delivery_logger(signal_id, settings.owner_user_id, "telegram", "sent", "")
            self._event_logger("notification", "telegram_sent", {"signal_id": signal_id, "owner_user_id": settings.owner_user_id})
        except Exception as exc:  # pragma: no cover - network and external API errors
            self._delivery_logger(signal_id, settings.owner_user_id, "telegram", "failed", str(exc))
            self._event_logger(
                "error",
                "telegram_failed",
                {"signal_id": signal_id, "owner_user_id": settings.owner_user_id, "error": str(exc)},
            )

    def _send_local(self, signal_id: str, settings: NotificationSetting, message: str) -> None:
        try:
            send_local(message)
            if settings.bell_on_alert:
                sys.stdout.write("\a")
                sys.stdout.flush()
            self._delivery_logger(signal_id, settings.owner_user_id, "local", "sent", "")
            self._event_logger("notification", "local_sent", {"signal_id": signal_id, "owner_user_id": settings.owner_user_id})
        except Exception as exc:  # pragma: no cover - platform-specific behavior
            self._delivery_logger(signal_id, settings.owner_user_id, "local", "failed", str(exc))
            self._event_logger(
                "error",
                "local_failed",
                {"signal_id": signal_id, "owner_user_id": settings.owner_user_id, "error": str(exc)},
            )
