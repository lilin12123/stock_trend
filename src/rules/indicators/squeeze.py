from __future__ import annotations

from typing import Optional

from ...models import Bar, Trigger


def squeeze_breakout(
    bar: Bar,
    atr: Optional[float],
    atr_quantile: Optional[float],
    vwap: Optional[float],
    volume: float,
    vol_avg: Optional[float],
    low_quantile: float,
    volume_mult: float,
) -> Optional[Trigger]:
    # 波动率扩张：ATR 先处于低分位，随后放量抬升，视为压缩后出方向。
    if atr is None or atr_quantile is None:
        return None
    if atr_quantile <= 0:
        return None
    # ATR 还没高于低分位阈值时，说明仍在“低波动”而不是“扩张”。
    if atr < atr_quantile:
        return None
    if vol_avg is None or vol_avg <= 0:
        return None
    if volume < vol_avg * volume_mult:
        return None

    direction = direction_from_context(bar, vwap)
    return Trigger(
        name="squeeze_breakout",
        direction=direction,
        message=(
            f"{'▲▲' if direction=='up' else '▼▼'} 迅速向{'上' if direction=='up' else '下'}突破（波动率快速放大）\n"
            f"现价 {bar.close:.2f}，ATR 从低{int(low_quantile*100)}%分位上破，量能放大。"
        ),
    )


def direction_from_context(bar: Bar, vwap: Optional[float]) -> str:
    # 方向优先参考 VWAP 相对位置；若缺少 VWAP，则退化为当前 K 线方向。
    if vwap is not None:
        return "up" if bar.close >= vwap else "down"
    return "up" if bar.close >= bar.open else "down"
