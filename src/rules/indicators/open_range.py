from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from ...application.market_profile import market_in_primary_session, market_primary_session_open
from ...models import Bar, Trigger


@dataclass
class OpenRangeState:
    # 记录某个交易日开盘区间的高低点，以及该区间是否已经构建完成。
    session_date: Optional[str] = None
    range_end: Optional[datetime] = None
    high: Optional[float] = None
    low: Optional[float] = None
    ready: bool = False


def _session_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def open_range_breakout(
    bar: Bar,
    state: OpenRangeState,
    range_minutes: int,
    volume: float,
    vol_avg: Optional[float],
    volume_mult: float,
) -> Optional[Trigger]:
    # 开盘区间突破：先统计开盘前 N 分钟的高低点，再等待后续放量突破。
    if not market_in_primary_session(bar.symbol, bar.ts):
        return None

    session_key = _session_key(bar.ts)
    if state.session_date != session_key:
        # 进入新交易日时，重置开盘区间状态。
        state.session_date = session_key
        state.range_end = market_primary_session_open(bar.symbol, bar.ts) + timedelta(minutes=range_minutes)
        state.high = None
        state.low = None
        state.ready = False

    if state.range_end is None:
        return None

    if bar.ts <= state.range_end:
        # 开盘区间构建阶段：持续更新区间上沿和下沿。
        state.high = bar.high if state.high is None else max(state.high, bar.high)
        state.low = bar.low if state.low is None else min(state.low, bar.low)
        if bar.ts >= state.range_end:
            state.ready = True
        return None

    if not state.ready or state.high is None or state.low is None:
        return None

    if vol_avg is None or vol_avg <= 0:
        return None

    # 只把“价格突破 + 成交量确认”的情况视为有效突破。
    if volume < vol_avg * volume_mult:
        return None

    if bar.close > state.high:
        return Trigger(
            name="open_range_breakout",
            direction="up",
            message=(
                "▲▲ 迅速向上突破（开盘区间上破）\n"
                f"现价 {bar.close:.2f}，上破位 {state.high:.2f}，量能放大。"
            ),
        )

    if bar.close < state.low:
        return Trigger(
            name="open_range_breakout",
            direction="down",
            message=(
                "▼▼ 迅速向下突破（开盘区间下破）\n"
                f"现价 {bar.close:.2f}，下破位 {state.low:.2f}，量能放大。"
            ),
        )

    return None
