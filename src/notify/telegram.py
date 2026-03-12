from __future__ import annotations

import json
import os
import urllib.request


def send_telegram(message: str, token: str, chat_id: str) -> None:
    token = token.strip()
    chat_id = chat_id.strip()
    if token.startswith("${"):
        env_key = token.strip("${}")
        token = os.getenv(env_key, "")
    if chat_id.startswith("${"):
        env_key = chat_id.strip("${}")
        chat_id = os.getenv(env_key, "")

    if not token or not chat_id:
        raise RuntimeError("Telegram token/chat_id is missing.")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message}
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()
