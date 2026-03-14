from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, List

from futu import OpenQuoteContext, SubType, RET_OK, CurKlineHandlerBase

from ..models import Bar


_TIMEFRAME_MAP: Dict[str, SubType] = {
    "1m": SubType.K_1M,
    "5m": SubType.K_5M,
}


class KlineHandler(CurKlineHandlerBase):
    def __init__(self, on_bar: Callable[[Bar], None], default_timeframe: str | None = None) -> None:
        super().__init__()
        self.on_bar = on_bar
        self.default_timeframe = default_timeframe

    @staticmethod
    def _infer_timeframe(row) -> str | None:
        for key in ("ktype", "k_type", "kType"):
            if key in row:
                ktype = row[key]
                if ktype is None:
                    continue
                text = str(ktype).upper()
                if "1M" in text:
                    return "1m"
                if "5M" in text:
                    return "5m"
        return None

    def on_recv_rsp(self, rsp_pb):
        ret, data = super().on_recv_rsp(rsp_pb)
        if ret != RET_OK:
            return ret, data

        # 将Futu的K线回调转换为统一Bar结构
        for _, row in data.iterrows():
            ts = _parse_time_key(row["time_key"])
            timeframe = self._infer_timeframe(row) or self.default_timeframe
            if not timeframe:
                continue
            bar = Bar(
                symbol=row["code"],
                timeframe=timeframe,
                ts=ts,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
            self.on_bar(bar)
        return ret, data


class FutuClient:
    def __init__(self, host: str, port: int) -> None:
        self.quote_ctx = OpenQuoteContext(host=host, port=port)
        self.handlers: List[KlineHandler] = []

    def subscribe(self, symbols: List[str], timeframes: List[str], on_bar: Callable[[Bar], None]) -> None:
        # 订阅指定股票的1m/5m等周期K线
        subtypes = []
        for tf in timeframes:
            subtype = _TIMEFRAME_MAP.get(tf)
            if subtype is None:
                raise ValueError(f"Unsupported timeframe: {tf}")
            subtypes.append(subtype)
        handler = KlineHandler(on_bar=on_bar)
        self.quote_ctx.set_handler(handler)
        self.handlers.append(handler)
        ret, data = self.quote_ctx.subscribe(symbols, subtypes)
        if ret != RET_OK:
            raise RuntimeError(f"Futu subscribe failed: {data}")

    def close(self) -> None:
        self.quote_ctx.close()


def _parse_time_key(value) -> datetime:
    # 兼容不同时间格式
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value))
    text = str(value)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        # Futu sometimes returns "YYYY-MM-DD HH:MM:SS"
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
