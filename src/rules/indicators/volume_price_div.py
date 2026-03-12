from __future__ import annotations

from typing import Optional, List

from ...models import Bar, Trigger


def volume_price_divergence(
    bar: Bar,
    recent_bars: List[dict],
    vol_avg: Optional[float],
    lookback: int,
    volume_mult: float,
) -> Optional[Trigger]:
    # 量价背离：价格创新高/新低，但成交量没有同步放大，提示趋势可能衰竭。
    if vol_avg is None or vol_avg <= 0:
        return None
    if len(recent_bars) < max(2, lookback):
        return None

    window = recent_bars[-lookback:]
    # 只和近期窗口里“前面那些 bar”比较，避免把当前 bar 自己算进去。
    prev_high = max(b["high"] for b in window[:-1])
    prev_low = min(b["low"] for b in window[:-1])

    low_volume = bar.volume <= vol_avg * volume_mult

    if bar.high >= prev_high and low_volume:
        return Trigger(
            name="volume_price_divergence",
            direction="down",
            message=(
                "▼▼ 量价背离（新高量弱）\n"
                f"现价 {bar.close:.2f}，创近期新高但量能不足。"
            ),
        )

    if bar.low <= prev_low and low_volume:
        return Trigger(
            name="volume_price_divergence",
            direction="up",
            message=(
                "▲▲ 量价背离（新低量弱）\n"
                f"现价 {bar.close:.2f}，创近期新低但量能不足。"
            ),
        )

    return None
