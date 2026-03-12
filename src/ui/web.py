from __future__ import annotations

import json
import threading
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Deque, Dict, List
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from ..models import Signal
from ..storage.record import load_signals, write_signal


class SignalStore:
    def __init__(self, maxlen: int = 200, storage_dir: str | None = None, load_days: int = 7) -> None:
        self._signals: Deque[Dict] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._storage_dir = Path(storage_dir) if storage_dir else None
        self._day_bars: Dict[str, List[Dict]] = {}
        self._vwap_state: Dict[str, Dict[str, float | str]] = {}
        if self._storage_dir:
            for item in load_signals(self._storage_dir, load_days):
                self._signals.appendleft(item)

    def add(self, signal: Signal) -> None:
        payload = {
            "symbol": signal.symbol,
            "symbol_name": signal.symbol_name,
            "timeframe": signal.timeframe,
            "ts": signal.ts.isoformat(),
            "rule": signal.rule,
            "message": signal.message,
            "strength": signal.strength,
            "direction": signal.direction,
            "triggers": [{"name": t.name, "direction": t.direction} for t in (signal.triggers or [])],
            "recent_bars": (signal.context or {}).get("recent_bars", []),
        }
        if self._storage_dir:
            write_signal(self._storage_dir, payload)
        with self._lock:
            self._signals.appendleft(payload)

    def list(self, limit: int | None = None, offset: int = 0) -> List[Dict]:
        with self._lock:
            items = list(self._signals)
        if offset < 0:
            offset = 0
        if limit is None or limit <= 0:
            return items[offset:]
        return items[offset:offset + limit]

    @staticmethod
    def _normalize_tf(value: str | None) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _market_tz(symbol: str | None) -> str:
        if symbol and str(symbol).startswith("US."):
            return "America/New_York"
        return "Asia/Shanghai"

    @staticmethod
    def _day_key(ts: str, tz_name: str) -> str:
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return ts.split("T")[0] if "T" in ts else ts.split(" ")[0]
        if dt.tzinfo is None:
            return dt.date().isoformat()
        return dt.astimezone(ZoneInfo(tz_name)).date().isoformat()

    @staticmethod
    def _minute_key(ts: str, tz_name: str) -> str:
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return ts[:16]
        if dt.tzinfo is None:
            return dt.strftime("%Y-%m-%d %H:%M")
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def _net_strength(triggers) -> int:
        up = 0
        down = 0
        for t in triggers or []:
            if isinstance(t, dict):
                direction = t.get("direction")
            else:
                direction = None
            if direction == "up":
                up += 1
            elif direction == "down":
                down += 1
        return abs(up - down)

    @staticmethod
    def _count_to_level(n: int) -> str:
        if n <= 0:
            return "Lv0"
        if n == 1:
            return "Lv1"
        if n == 2:
            return "Lv2"
        if n == 3:
            return "Lv3"
        if n == 4:
            return "Lv4"
        return "Lv5"

    @staticmethod
    def _level_rank(level: str | None) -> int:
        order = {"Lv0": 0, "Lv1": 1, "Lv2": 2, "Lv3": 3, "Lv4": 4, "Lv5": 5}
        return order.get(level or "", 0)

    @staticmethod
    def _resolve_exclusive(triggers: List[Dict]) -> List[Dict]:
        groups = [
            ["rsi_overbought", "rsi_oversold"],
        ]
        by_name = {t.get("name"): t for t in triggers if t.get("name")}
        for group in groups:
            present = [by_name.get(n) for n in group if by_name.get(n)]
            if len(present) <= 1:
                continue
            present.sort(key=lambda x: x.get("ts", ""))
            keep = present[-1]
            for name in group:
                if name != keep.get("name"):
                    by_name.pop(name, None)
        return list(by_name.values())

    def _normalize_triggers(self, item: Dict) -> List[Dict]:
        base_ts = item.get("ts", "")
        out: Dict[str, Dict] = {}
        for t in item.get("triggers") or []:
            if not isinstance(t, dict):
                name = str(t)
                out[name] = {"name": name, "direction": "neutral", "ts": base_ts}
                continue
            name = t.get("name")
            if not name:
                continue
            entry = {
                "name": name,
                "direction": t.get("direction", "neutral"),
                "ts": t.get("ts", base_ts),
            }
            if name not in out or entry["ts"] >= out[name].get("ts", ""):
                out[name] = entry
        rule = item.get("rule")
        if rule and rule not in out:
            out[rule] = {"name": rule, "direction": item.get("direction") or "neutral", "ts": base_ts}
        return self._resolve_exclusive(list(out.values()))

    def list_filtered(
        self,
        limit: int | None = None,
        offset: int = 0,
        symbol: str | None = None,
        timeframe: str | None = None,
        level: str | None = None,
        level_1m: str | None = None,
        level_5m: str | None = None,
        text: str | None = None,
    ) -> List[Dict]:
        items = self.list(limit=None, offset=0)
        # 按市场时区过滤当日信号
        today_keys = {
            "HK": datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat(),
            "US": datetime.now(ZoneInfo("America/New_York")).date().isoformat(),
        }
        filtered_items: List[Dict] = []
        for item in items:
            tz_name = self._market_tz(item.get("symbol"))
            market = "US" if tz_name == "America/New_York" else "HK"
            day = self._day_key(str(item.get("ts", "")), tz_name)
            if day != today_keys.get(market):
                continue
            filtered_items.append(item)
        items = filtered_items
        if symbol:
            items = [i for i in items if i.get("symbol") == symbol]
        if timeframe:
            tf = self._normalize_tf(timeframe)
            items = [i for i in items if self._normalize_tf(i.get("timeframe")) == tf]
        if text:
            items = [i for i in items if text in str(i.get("message", "")) or text in str(i.get("symbol", ""))]

        # 计算触发与等级、合并同一分钟信号
        merged: Dict[str, Dict] = {}
        for item in items:
            triggers = self._normalize_triggers(item)
            level_val = self._count_to_level(self._net_strength(triggers))
            item = {
                **item,
                "triggers": triggers,
                "level": level_val,
            }
            tz_name = self._market_tz(item.get("symbol"))
            minute_key = self._minute_key(str(item.get("ts", "")), tz_name)
            key = f"{item.get('symbol')}|{self._normalize_tf(item.get('timeframe'))}|{minute_key}"
            existing = merged.get(key)
            if not existing:
                merged[key] = item
                continue
            # 合并触发
            all_triggers = (existing.get("triggers") or []) + (item.get("triggers") or [])
            all_triggers = self._resolve_exclusive(all_triggers)
            uniq: Dict[str, Dict] = {}
            for t in all_triggers:
                name = t.get("name") if isinstance(t, dict) else None
                if not name:
                    continue
                if name not in uniq or t.get("ts", "") >= uniq[name].get("ts", ""):
                    uniq[name] = t
            all_triggers = list(uniq.values())
            existing["triggers"] = all_triggers
            existing["level"] = self._count_to_level(self._net_strength(all_triggers))
            # 选择更强的为主信号
            if self._net_strength(item.get("triggers")) >= self._net_strength(existing.get("triggers")):
                existing["message"] = item.get("message")
                existing["rule"] = item.get("rule")
                existing["direction"] = item.get("direction")

        items = list(merged.values())
        # 等级筛选（合并后，按“>=”语义）
        if level:
            target = self._level_rank(level)
            items = [i for i in items if self._level_rank(i.get("level")) >= target]
        if level_1m:
            target_1m = self._level_rank(level_1m)
            items = [
                i for i in items
                if self._normalize_tf(i.get("timeframe")) != "1m"
                or self._level_rank(i.get("level")) >= target_1m
            ]
        if level_5m:
            target_5m = self._level_rank(level_5m)
            items = [
                i for i in items
                if self._normalize_tf(i.get("timeframe")) != "5m"
                or self._level_rank(i.get("level")) >= target_5m
            ]
        # 时间降序
        items.sort(key=lambda x: str(x.get("ts", "")), reverse=True)
        if offset < 0:
            offset = 0
        if limit is None or limit <= 0:
            return items[offset:]
        return items[offset:offset + limit]

    def update_bar(self, symbol: str, timeframe: str, bar: Dict) -> None:
        key = f"{symbol}:{timeframe}"
        with self._lock:
            bars = self._day_bars.setdefault(key, [])
            ts = str(bar.get("ts", ""))
            day = ts.split("T")[0] if "T" in ts else ts.split(" ")[0]
            state = self._vwap_state.get(key)
            if not state or state.get("day") != day:
                state = {"day": day, "cum_pv": 0.0, "cum_vol": 0.0}
                self._vwap_state[key] = state
                # 新交易日，清空当日K线缓存
                bars.clear()
            vol = float(bar.get("volume", 0.0))
            close = float(bar.get("close", 0.0))
            state["cum_pv"] = float(state["cum_pv"]) + close * vol
            state["cum_vol"] = float(state["cum_vol"]) + vol
            vwap = (state["cum_pv"] / state["cum_vol"]) if state["cum_vol"] else None
            bar["vwap"] = vwap

            if bars and bars[-1].get("ts") == bar.get("ts"):
                bars[-1] = bar
            else:
                bars.append(bar)

    def get_day_bars(self, symbol: str, timeframe: str) -> List[Dict]:
        key = f"{symbol}:{timeframe}"
        with self._lock:
            return list(self._day_bars.get(key, []))

    def set_warmup(self, info: Dict) -> None:
        with self._lock:
            self._warmup_info = info

    def get_warmup(self) -> Dict:
        with self._lock:
            return getattr(self, "_warmup_info", {})

    def set_meta(self, info: Dict) -> None:
        with self._lock:
            self._meta = info

    def get_meta(self) -> Dict:
        with self._lock:
            return getattr(self, "_meta", {})


class _Handler(BaseHTTPRequestHandler):
    store: SignalStore
    static_dir: Path
    static_cache: Dict[str, Dict[str, object]]

    def do_GET(self):
        if self.path.startswith("/api/signals"):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query, keep_blank_values=True)
            def _get(name: str) -> str | None:
                values = params.get(name)
                if not values:
                    return None
                return values[0]
            limit = int(_get("limit") or "200")
            offset = int(_get("offset") or "0")
            symbol = _get("symbol")
            timeframe = _get("tf")
            level = _get("level")
            level_1m = _get("level_1m")
            level_5m = _get("level_5m")
            text = _get("text")
            self._write_json(
                self.store.list_filtered(
                    limit=limit,
                    offset=offset,
                    symbol=symbol,
                    timeframe=timeframe,
                    level=level,
                    level_1m=level_1m,
                    level_5m=level_5m,
                    text=text,
                )
            )
            return
        if self.path.startswith("/api/warmup"):
            self._write_json(self.store.get_warmup())
            return
        if self.path.startswith("/api/meta"):
            self._write_json(self.store.get_meta())
            return
        if self.path.startswith("/api/day_bars"):
            query = self.path.split("?", 1)[-1] if "?" in self.path else ""
            params = dict(q.split("=", 1) for q in query.split("&") if "=" in q)
            symbol = params.get("symbol", "")
            timeframe = params.get("tf", "")
            since = params.get("since")
            bars = self.store.get_day_bars(symbol, timeframe)
            if since:
                bars = [b for b in bars if str(b.get("ts", "")) > since]
            self._write_json(bars)
            return
        if self.path == "/" or self.path.startswith("/index.html"):
            self._write_static("index.html", "text/html; charset=utf-8")
            return
        if self.path == "/app.js":
            self._write_static("app.js", "application/javascript; charset=utf-8")
            return
        if self.path == "/styles.css":
            self._write_static("styles.css", "text/css; charset=utf-8")
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return

    def _write_json(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_static(self, filename: str, content_type: str):
        path = self.static_dir / filename
        try:
            mtime = path.stat().st_mtime
            cached = self.static_cache.get(filename)
            if cached and cached.get("mtime") == mtime:
                body = cached.get("body", b"")
            else:
                body = path.read_bytes()
                self.static_cache[filename] = {"mtime": mtime, "body": body}
        except OSError:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class WebUI:
    def __init__(self, host: str, port: int, store: SignalStore) -> None:
        self.host = host
        self.port = port
        self.store = store
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        def _run():
            handler = _Handler
            handler.store = self.store
            handler.static_dir = Path(__file__).parent / "static"
            handler.static_cache = {}
            server = HTTPServer((self.host, self.port), handler)
            server.serve_forever()

        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
