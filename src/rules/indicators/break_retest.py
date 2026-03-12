from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ...models import Bar, Trigger
from .open_range import OpenRangeState


@dataclass
class RetestState:
    # 记录突破后的观察状态，用于等待“回踩确认”出现。
    direction: Optional[str] = None  # "up" | "down"
    level: Optional[float] = None
    bars_since: int = 0
    retest_seen: bool = False


def break_retest(
    bar: Bar,
    or_state: OpenRangeState,
    rt_state: RetestState,
    retest_max_bars: int,
    retest_tolerance_pct: float,
) -> Optional[Trigger]:
    # 突破 + 回踩确认：先出现突破，再在有限根数内看到“不破关键位的回踩”。
    if not or_state.ready or or_state.high is None or or_state.low is None:
        return None

    tol = retest_tolerance_pct / 100.0

    # 尚未进入跟踪状态时，先等待价格真正突破开盘区间。
    if rt_state.direction is None:
        if bar.close > or_state.high:
            rt_state.direction = "up"
            rt_state.level = or_state.high
            rt_state.bars_since = 0
            rt_state.retest_seen = False
        elif bar.close < or_state.low:
            rt_state.direction = "down"
            rt_state.level = or_state.low
            rt_state.bars_since = 0
            rt_state.retest_seen = False
        return None

    rt_state.bars_since += 1
    # 超过观察窗口仍未完成回踩确认，则放弃这次突破。
    if rt_state.bars_since > retest_max_bars:
        rt_state.direction = None
        rt_state.level = None
        rt_state.bars_since = 0
        rt_state.retest_seen = False
        return None

    if rt_state.level is None:
        return None

    if rt_state.direction == "up":
        # 上破后的回踩：只要低点靠近突破位，就认为出现了测试动作。
        if bar.low <= rt_state.level * (1 + tol):
            rt_state.retest_seen = True
        # 回踩后重新收回关键位上方，视为确认成功。
        if rt_state.retest_seen and bar.close > rt_state.level * (1 + tol):
            rt_state.direction = None
            return Trigger(
                name="break_retest",
                direction="up",
                message=(
                    "▲▲ 突破+回踩确认\n"
                    f"现价 {bar.close:.2f}，回踩不破 {rt_state.level:.2f} 后再上行。"
                ),
            )

    if rt_state.direction == "down":
        # 下破后的回抽：只要高点靠近突破位，就认为出现了测试动作。
        if bar.high >= rt_state.level * (1 - tol):
            rt_state.retest_seen = True
        # 回抽后重新收回关键位下方，视为确认成功。
        if rt_state.retest_seen and bar.close < rt_state.level * (1 - tol):
            rt_state.direction = None
            return Trigger(
                name="break_retest",
                direction="down",
                message=(
                    "▼▼ 突破+回踩确认\n"
                    f"现价 {bar.close:.2f}，回踩不破 {rt_state.level:.2f} 后再下行。"
                ),
            )

    return None
