from __future__ import annotations

from typing import Optional

from ...models import Bar, Trigger


def vwap_deviation(
    bar: Bar,
    atr: Optional[float],
    vwap: Optional[float],
    volume: float,
    vol_avg: Optional[float],
    dev_atr_mult: float,
    volume_mult: float,
) -> Optional[Trigger]:
    # VWAP 偏离：价格显著远离当日平均成交成本，并且伴随放量。
    if atr is None or vwap is None or vol_avg is None or vol_avg <= 0:
        return None

    deviation = abs(bar.close - vwap)
    # 用 ATR 统一不同股票/不同时段的波动尺度，避免固定价差失真。
    if deviation < atr * dev_atr_mult:
        return None

    # 只有放量偏离才更像是资金推动，而不是随机噪声。
    if volume < vol_avg * volume_mult:
        return None

    direction = "up" if bar.close > vwap else "down"
    return Trigger(
        name="vwap_deviation",
        direction=direction,
        message=(
            f"{'▲▲' if direction=='up' else '▼▼'} 迅速向{'上' if direction=='up' else '下'}突破\n"
            f"现价 {bar.close:.2f}，{'高于' if direction=='up' else '低于'} VWAP {deviation:.2f} "
            f"(> {dev_atr_mult}x ATR)，量能放大。"
        ),
    )
