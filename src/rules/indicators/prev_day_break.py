from __future__ import annotations

from typing import Optional

from ...models import Bar, Trigger


def prev_day_break(
    bar: Bar,
    prev_high: Optional[float],
    prev_low: Optional[float],
    volume: float,
    vol_avg: Optional[float],
    volume_mult: float,
) -> Optional[Trigger]:
    # 昨日高低点突破：把前一交易日的关键位置当作天然支撑/阻力位。
    if prev_high is None or prev_low is None:
        return None
    if vol_avg is None or vol_avg <= 0:
        return None
    # 只有放量突破才更值得关注，否则容易是关键位附近的来回穿越。
    if volume < vol_avg * volume_mult:
        return None

    if bar.close > prev_high:
        return Trigger(
            name="prev_day_break",
            direction="up",
            message=(
                "▲▲ 昨日高点突破\n"
                f"现价 {bar.close:.2f}，上破昨日高 {prev_high:.2f}。"
            ),
        )

    if bar.close < prev_low:
        return Trigger(
            name="prev_day_break",
            direction="down",
            message=(
                "▼▼ 昨日低点突破\n"
                f"现价 {bar.close:.2f}，下破昨日低 {prev_low:.2f}。"
            ),
        )

    return None
