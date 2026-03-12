from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional

from ..models import Bar, Signal


@dataclass
class OpenRangeState:
    # 旧版独立策略实现中使用的开盘区间状态。
    session_date: Optional[str] = None
    range_end: Optional[datetime] = None
    high: Optional[float] = None
    low: Optional[float] = None
    ready: bool = False


def _session_key(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d")


def _morning_session_open(ts: datetime) -> datetime:
    return ts.replace(hour=9, minute=30, second=0, microsecond=0)


def _is_morning_session(ts: datetime) -> bool:
    return time(9, 30) <= ts.time() < time(12, 0)


def open_range_breakout(
    bar: Bar,
    state: OpenRangeState,
    range_minutes: int,
    volume: float,
    vol_avg: Optional[float],
    volume_mult: float,
) -> Optional[Signal]:
    # 开盘区间突破：常用于捕捉日内趋势启动
    if not _is_morning_session(bar.ts):
        return None

    session_key = _session_key(bar.ts)
    if state.session_date != session_key:
        state.session_date = session_key
        state.range_end = _morning_session_open(bar.ts) + timedelta(minutes=range_minutes)
        state.high = None
        state.low = None
        state.ready = False

    if state.range_end is None:
        return None

    if bar.ts <= state.range_end:
        # 先构建开盘区间，再等待后续突破。
        state.high = bar.high if state.high is None else max(state.high, bar.high)
        state.low = bar.low if state.low is None else min(state.low, bar.low)
        if bar.ts >= state.range_end:
            state.ready = True
        return None

    if not state.ready or state.high is None or state.low is None:
        return None

    if vol_avg is None or vol_avg <= 0:
        return None

    # 旧版实现同样要求放量确认，避免把噪声突破当成信号。
    vol_ok = volume >= vol_avg * volume_mult
    if not vol_ok:
        return None

    if bar.close > state.high:
        strength = (bar.close - state.high) / (state.high if state.high else 1.0)
        return Signal(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar.ts,
            rule="open_range_breakout",
            message=(
                "▲▲ 迅速向上突破（开盘区间上破）\n"
                f"现价 {bar.close:.2f}，上破位 {state.high:.2f}，量能放大。"
            ),
            strength=strength,
        )

    if bar.close < state.low:
        strength = (state.low - bar.close) / (state.low if state.low else 1.0)
        return Signal(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar.ts,
            rule="open_range_breakout",
            message=(
                "▼▼ 迅速向下突破（开盘区间下破）\n"
                f"现价 {bar.close:.2f}，下破位 {state.low:.2f}，量能放大。"
            ),
            strength=strength,
        )

    return None


def vwap_deviation(
    bar: Bar,
    atr: Optional[float],
    vwap: Optional[float],
    volume: float,
    vol_avg: Optional[float],
    dev_atr_mult: float,
    volume_mult: float,
) -> Optional[Signal]:
    # VWAP偏离：偏离过大且有量能确认时提示
    if atr is None or vwap is None or vol_avg is None or vol_avg <= 0:
        return None

    deviation = abs(bar.close - vwap)
    # 用 ATR 统一偏离尺度，避免不同价格区间难以横向比较。
    if deviation < atr * dev_atr_mult:
        return None

    if volume < vol_avg * volume_mult:
        return None

    direction = "above" if bar.close > vwap else "below"
    strength = deviation / atr
    return Signal(
        symbol=bar.symbol,
        timeframe=bar.timeframe,
        ts=bar.ts,
        rule="vwap_deviation",
        message=(
            f"{direction_banner(direction)}\n"
            f"现价 {bar.close:.2f}，{direction_text(direction)} VWAP {deviation:.2f} "
            f"(> {dev_atr_mult}x ATR)，量能放大。"
        ),
        strength=strength,
    )


def squeeze_breakout(
    bar: Bar,
    atr: Optional[float],
    atr_quantile: Optional[float],
    vwap: Optional[float],
    volume: float,
    vol_avg: Optional[float],
    low_quantile: float,
    volume_mult: float,
) -> Optional[Signal]:
    # 波动率收缩后放大：从低ATR分位向上突破
    if atr is None or atr_quantile is None:
        return None

    if atr_quantile <= 0:
        return None

    # 只有 ATR 脱离低分位区间，才算进入波动扩张阶段。
    if atr < atr_quantile:
        return None

    if vol_avg is None or vol_avg <= 0:
        return None

    if volume < vol_avg * volume_mult:
        return None

    strength = atr / atr_quantile
    direction = direction_from_context(bar, vwap)
    return Signal(
        symbol=bar.symbol,
        timeframe=bar.timeframe,
        ts=bar.ts,
        rule="squeeze_breakout",
        message=(
            f"{direction_banner(direction)}（波动率快速放大）\n"
            f"现价 {bar.close:.2f}，ATR 从低{int(low_quantile*100)}%分位上破，量能放大。"
        ),
        strength=strength,
    )


def direction_text(direction: str) -> str:
    return "高于" if direction == "above" else "低于"


def direction_banner(direction: str) -> str:
    return "▲▲ 迅速向上突破" if direction == "above" else "▼▼ 迅速向下突破"


def direction_from_context(bar: Bar, vwap: Optional[float]) -> str:
    # 优先用VWAP判断方向；无VWAP时用当前K线方向
    if vwap is not None:
        return "above" if bar.close >= vwap else "below"
    return "above" if bar.close >= bar.open else "below"


def rsi_signal(
    bar: Bar,
    rsi: Optional[float],
    overbought: float,
    oversold: float,
) -> Optional[Signal]:
    # RSI 极值更多是风险提示，不直接代表趋势会立刻反转。
    if rsi is None:
        return None
    if rsi >= overbought:
        return Signal(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar.ts,
            rule="rsi_extreme",
            message=(
                "▼▼ RSI 超买\n"
                f"现价 {bar.close:.2f}，RSI {rsi:.1f} ≥ {overbought:.1f}"
            ),
            strength=(rsi - overbought) / max(1.0, 100.0 - overbought),
        )
    if rsi <= oversold:
        return Signal(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar.ts,
            rule="rsi_extreme",
            message=(
                "▲▲ RSI 超卖\n"
                f"现价 {bar.close:.2f}，RSI {rsi:.1f} ≤ {oversold:.1f}"
            ),
            strength=(oversold - rsi) / max(1.0, oversold),
        )
    return None
