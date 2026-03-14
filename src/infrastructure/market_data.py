from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List

from ..application.rule_runtime import parse_time_key
from ..domain import Bar


@dataclass
class _RuntimeState:
    symbols: List[str]
    timeframes: List[str]


class OpenDGateway:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._callback: Callable[[Bar], None] | None = None
        self._ctx = None
        self._handler = None
        self._state = _RuntimeState(symbols=[], timeframes=[])
        self._lock = threading.RLock()

    def start(self, symbols: List[str], timeframes: List[str], on_bar: Callable[[Bar], None]) -> None:
        with self._lock:
            self._callback = on_bar
            self._reconnect(symbols, timeframes)

    def apply_subscriptions(self, symbols: List[str], timeframes: List[str]) -> None:
        with self._lock:
            if self._callback is None:
                return
            self._reconnect(symbols, timeframes)

    def close(self) -> None:
        with self._lock:
            if self._ctx is not None:
                self._ctx.close()
                self._ctx = None
                self._handler = None

    def request_history(self, symbol: str, start: str, end: str, timeframe: str) -> List[Dict]:
        futu = self._load_futu()
        tf_map = {"1m": futu["SubType"].K_1M, "5m": futu["SubType"].K_5M}
        subtype = tf_map[timeframe]
        with futu["OpenQuoteContext"](host=self.host, port=self.port) as ctx:
            resp = ctx.request_history_kline(symbol, start=start, end=end, ktype=subtype, autype="qfq")
        return self._normalize_history_response(resp)

    def request_prev_day(self, symbol: str, day: str) -> Dict | None:
        futu = self._load_futu()
        with futu["OpenQuoteContext"](host=self.host, port=self.port) as ctx:
            resp = ctx.request_history_kline(symbol, start=day, end=day, ktype=futu["SubType"].K_DAY, autype="qfq")
        rows = self._normalize_history_response(resp)
        return rows[-1] if rows else None

    def _reconnect(self, symbols: List[str], timeframes: List[str]) -> None:
        self.close()
        self._state = _RuntimeState(symbols=list(symbols), timeframes=list(timeframes))
        if not symbols or self._callback is None:
            return

        futu = self._load_futu()

        class KlineHandler(futu["CurKlineHandlerBase"]):
            def __init__(self, outer: "OpenDGateway") -> None:
                super().__init__()
                self._outer = outer

            def on_recv_rsp(self, rsp_pb):
                ret, data = super().on_recv_rsp(rsp_pb)
                if ret != futu["RET_OK"]:
                    return ret, data
                for _, row in data.iterrows():
                    bar = Bar(
                        symbol=row["code"],
                        timeframe=self._outer._infer_timeframe(row),
                        ts=parse_time_key(row["time_key"]),
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                    if self._outer._callback is not None and bar.timeframe:
                        self._outer._callback(bar)
                return ret, data

        tf_map = {"1m": futu["SubType"].K_1M, "5m": futu["SubType"].K_5M}
        subtypes = [tf_map[tf] for tf in timeframes if tf in tf_map]
        self._ctx = futu["OpenQuoteContext"](host=self.host, port=self.port)
        self._handler = KlineHandler(self)
        self._ctx.set_handler(self._handler)
        ret, data = self._ctx.subscribe(symbols, subtypes)
        if ret != futu["RET_OK"]:
            raise RuntimeError(f"Futu subscribe failed: {data}")

    @staticmethod
    def _infer_timeframe(row) -> str:
        for key in ("ktype", "k_type", "kType"):
            if key in row and row[key] is not None:
                text = str(row[key]).upper()
                if "1M" in text:
                    return "1m"
                if "5M" in text:
                    return "5m"
        return "1m"

    @staticmethod
    def _normalize_history_response(resp) -> List[Dict]:
        if len(resp) == 2:
            ret, data = resp
        else:
            ret, data, _ = resp
        if ret != 0:
            raise RuntimeError(f"request_history_kline failed: {data}")
        rows: List[Dict] = []
        for _, row in data.iterrows():
            rows.append(
                {
                    "code": row["code"],
                    "time_key": row["time_key"],
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                }
            )
        return rows

    @staticmethod
    def _load_futu():
        from futu import CurKlineHandlerBase, OpenQuoteContext, RET_OK, SubType

        return {
            "CurKlineHandlerBase": CurKlineHandlerBase,
            "OpenQuoteContext": OpenQuoteContext,
            "RET_OK": RET_OK,
            "SubType": SubType,
        }
