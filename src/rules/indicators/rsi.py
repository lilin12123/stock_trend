from __future__ import annotations

from typing import Optional

from ...models import Bar, Trigger


def rsi_signal(
    bar: Bar,
    rsi: Optional[float],
    overbought: float,
    oversold: float,
) -> Optional[Trigger]:
    # RSI 极值：把短线过热/过冷状态转成提示信号，偏提醒而非单独确认趋势。
    if rsi is None:
        return None
    if rsi >= overbought:
        return Trigger(
            name="rsi_overbought",
            direction="down",
            message=(
                "▼▼ RSI 超买\n"
                f"现价 {bar.close:.2f}，RSI {rsi:.1f} ≥ {overbought:.1f}"
            ),
        )
    if rsi <= oversold:
        return Trigger(
            name="rsi_oversold",
            direction="up",
            message=(
                "▲▲ RSI 超卖\n"
                f"现价 {bar.close:.2f}，RSI {rsi:.1f} ≤ {oversold:.1f}"
            ),
        )
    return None
