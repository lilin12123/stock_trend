from __future__ import annotations

import os
import platform
import subprocess


def send_local(message: str) -> None:
    system = platform.system().lower()
    if system == "darwin":
        script = f'display notification "{message}" with title "Intraday Alert"'
        try:
            subprocess.run(["osascript", "-e", script], check=False)
            return
        except FileNotFoundError:
            pass
    # Fallback: print and optional terminal bell
    print(f"[ALERT] {message}")
    if os.getenv("BELL_ON_ALERT") == "1":
        print("\a", end="")
