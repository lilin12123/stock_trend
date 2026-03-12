from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _day_key(ts: str) -> str:
    return ts.split("T")[0]


def write_signal(storage_dir: Path, data: Dict) -> None:
    storage_dir.mkdir(parents=True, exist_ok=True)
    day = _day_key(data["ts"])
    path = storage_dir / f"signals_{day}.jsonl"
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")


def load_signals(storage_dir: Path, max_days: int = 7) -> List[Dict]:
    if not storage_dir.exists():
        return []
    files = sorted(storage_dir.glob("signals_*.jsonl"))
    if max_days > 0:
        files = files[-max_days:]
    items: List[Dict] = []
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        items.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            continue
    return items
