from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from ..models import Bar, Signal, Trigger
from .indicators.open_range import OpenRangeState, open_range_breakout
from .indicators.vwap import vwap_deviation
from .indicators.squeeze import squeeze_breakout
from .indicators.rsi import rsi_signal
from .indicators.break_retest import RetestState, break_retest
from .indicators.volume_price_div import volume_price_divergence
from .indicators.prev_day_break import prev_day_break
from ..storage.state import StateStore


@dataclass
class RuleConfig:
    # 所有策略参数都通过配置注入，RuleEngine 只负责编排和执行。
    cooldown_seconds: int
    open_range_breakout: Dict[str, float]
    vwap_deviation: Dict[str, float]
    squeeze_breakout: Dict[str, float]
    rsi_extreme: Dict[str, float]
    break_retest: Dict[str, float]
    volume_price_divergence: Dict[str, float]
    prev_day_break: Dict[str, float]


@dataclass
class RuleEngine:
    config: RuleConfig
    store: StateStore
    # 开盘区间和突破回踩都依赖跨 bar 的状态，因此单独维护。
    or_states: Dict[str, OpenRangeState] = field(default_factory=dict)
    rt_states: Dict[str, RetestState] = field(default_factory=dict)

    def _cooldown_ok(self, key: str, rule: str, ts: datetime) -> bool:
        state = self.store.get(key)
        last_ts = state.last_signal_ts.get(rule)
        if last_ts is None:
            return True
        return (ts - last_ts) >= timedelta(seconds=self.config.cooldown_seconds)

    def _mark_signal(self, key: str, rule: str, ts: datetime) -> None:
        state = self.store.get(key)
        state.last_signal_ts[rule] = ts

    def on_bar(self, bar: Bar) -> List[Signal]:
        # 一根 K 线进入后，先更新基础指标，再按顺序尝试所有启用中的规则。
        key = f"{bar.symbol}:{bar.timeframe}"
        state = self.store.get(key)

        # 计算指标
        atr = state.atr.update(bar.high, bar.low, bar.close)
        vol_avg = state.vol_mean.update(bar.volume)
        rsi = state.rsi.update(bar.close)
        session_date = bar.ts.strftime("%Y-%m-%d")
        vwap = state.vwap.update(bar.close, bar.volume, session_date)

        if atr is not None:
            state.atr_window.update(atr)

        # 保存最近20根K线及指标，用于展示
        state.bars.append(
            {
                "ts": bar.ts.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "atr": atr,
                "vwap": vwap,
                "vol_avg": vol_avg,
                "rsi": rsi,
            }
        )

        triggers: List[Trigger] = []

        # 开盘区间突破：偏早盘趋势启动型信号。
        orb_cfg = self._rule_cfg(self.config.open_range_breakout, bar.timeframe)
        if orb_cfg.get("enabled"):
            or_state = self.or_states.setdefault(key, OpenRangeState())
            trigger = open_range_breakout(
                bar=bar,
                state=or_state,
                range_minutes=int(orb_cfg.get("range_minutes", 15)),
                volume=bar.volume,
                vol_avg=vol_avg,
                volume_mult=float(orb_cfg.get("volume_mult", 1.5)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # VWAP 偏离：偏强趋势/失衡型信号。
        vwap_cfg = self._rule_cfg(self.config.vwap_deviation, bar.timeframe)
        if vwap_cfg.get("enabled"):
            trigger = vwap_deviation(
                bar=bar,
                atr=atr,
                vwap=vwap,
                volume=bar.volume,
                vol_avg=vol_avg,
                dev_atr_mult=float(vwap_cfg.get("dev_atr_mult", 1.5)),
                volume_mult=float(vwap_cfg.get("volume_mult", 2.0)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # 波动率扩张：偏波动启动型信号。
        sq_cfg = self._rule_cfg(self.config.squeeze_breakout, bar.timeframe)
        if sq_cfg.get("enabled"):
            atr_quantile = state.atr_window.quantile(float(sq_cfg.get("low_quantile", 0.2)))
            trigger = squeeze_breakout(
                bar=bar,
                atr=atr,
                atr_quantile=atr_quantile,
                vwap=vwap,
                volume=bar.volume,
                vol_avg=vol_avg,
                low_quantile=float(sq_cfg.get("low_quantile", 0.2)),
                volume_mult=float(sq_cfg.get("volume_mult", 1.5)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # RSI 极值：偏过热/过冷提醒型信号。
        rsi_cfg = self._rule_cfg(self.config.rsi_extreme, bar.timeframe)
        if rsi_cfg.get("enabled"):
            trigger = rsi_signal(
                bar=bar,
                rsi=rsi,
                overbought=float(rsi_cfg.get("overbought", 70)),
                oversold=float(rsi_cfg.get("oversold", 30)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # 突破回踩确认：偏过滤假突破、做二次确认。
        br_cfg = self._rule_cfg(self.config.break_retest, bar.timeframe)
        if br_cfg.get("enabled"):
            or_state = self.or_states.setdefault(key, OpenRangeState())
            rt_state = self.rt_states.setdefault(key, RetestState())
            trigger = break_retest(
                bar=bar,
                or_state=or_state,
                rt_state=rt_state,
                retest_max_bars=int(br_cfg.get("retest_max_bars", 20)),
                retest_tolerance_pct=float(br_cfg.get("retest_tolerance_pct", 0.1)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # 量价背离：偏衰竭/反转提醒型信号。
        vpd_cfg = self._rule_cfg(self.config.volume_price_divergence, bar.timeframe)
        if vpd_cfg.get("enabled"):
            trigger = volume_price_divergence(
                bar=bar,
                recent_bars=list(state.bars),
                vol_avg=vol_avg,
                lookback=int(vpd_cfg.get("lookback", 20)),
                volume_mult=float(vpd_cfg.get("volume_mult", 0.8)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        # 昨日高低突破：偏关键位突破型信号。
        pdb_cfg = self._rule_cfg(self.config.prev_day_break, bar.timeframe)
        if pdb_cfg.get("enabled"):
            prev_high, prev_low = self.store.get_prev_day(bar.symbol)
            trigger = prev_day_break(
                bar=bar,
                prev_high=prev_high,
                prev_low=prev_low,
                volume=bar.volume,
                vol_avg=vol_avg,
                volume_mult=float(pdb_cfg.get("volume_mult", 1.5)),
            )
            if trigger and self._cooldown_ok(key, trigger.name, bar.ts):
                self._mark_signal(key, trigger.name, bar.ts)
                triggers.append(trigger)

        if not triggers:
            return []

        signal = self._build_signal(bar, triggers)
        return [self._attach_context(signal, state)]

    def _build_signal(self, bar: Bar, triggers: List[Trigger]) -> Signal:
        # 同一根 bar 可能命中多条规则，这里把它们聚合成一条最终信号。
        directions = {t.direction for t in triggers}
        direction = directions.pop() if len(directions) == 1 else "neutral"
        primary = triggers[0]
        names = [t.name for t in triggers]
        message = primary.message
        if len(names) > 1:
            message = message + "；同时触发: " + ",".join(names)
        return Signal(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar.ts,
            rule=primary.name,
            message=message,
            direction=direction,
            triggers=triggers,
        )

    def _attach_context(self, signal: Signal, state) -> Signal:
        # 在信号中附带最近K线与指标快照
        return Signal(
            symbol=signal.symbol,
            timeframe=signal.timeframe,
            ts=signal.ts,
            rule=signal.rule,
            message=signal.message,
            strength=signal.strength,
            symbol_name=signal.symbol_name,
            direction=signal.direction,
            triggers=signal.triggers,
            context={"recent_bars": list(state.bars)},
        )

    @staticmethod
    def _rule_cfg(base: Dict[str, float], timeframe: str) -> Dict[str, float]:
        # 先取规则默认参数，再叠加该 timeframe 的覆盖项。
        cfg = dict(base)
        overrides = base.get("tf_overrides", {}) if isinstance(base, dict) else {}
        if isinstance(overrides, dict):
            tf_cfg = overrides.get(timeframe)
            if isinstance(tf_cfg, dict):
                cfg.update(tf_cfg)
        return cfg
