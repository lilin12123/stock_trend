from __future__ import annotations

import argparse
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
from zoneinfo import ZoneInfo

import yaml
from futu import OpenQuoteContext, SubType, RET_OK

from .models import Bar
from .rules.rule_engine import RuleConfig, RuleEngine
from .storage.state import StateStore


_TIMEFRAME_MAP = {
    "1m": SubType.K_1M,
    "5m": SubType.K_5M,
}


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def hk_trading_session(ts) -> bool:
    t = ts.time()
    return (time(9, 30) <= t < time(12, 0)) or (time(13, 0) <= t < time(16, 0))


def build_rule_engine(cfg: Dict) -> RuleEngine:
    rules = cfg.get("rules", {})
    atr_period = max(
        int(rules.get("vwap_deviation", {}).get("atr_period", 14)),
        int(rules.get("squeeze_breakout", {}).get("atr_period", 14)),
    )
    vol_avg_period = int(rules.get("vwap_deviation", {}).get("vol_avg_period", 20))
    atr_lookback = int(rules.get("squeeze_breakout", {}).get("lookback", 60))

    store = StateStore(
        atr_period=atr_period,
        vol_avg_period=vol_avg_period,
        atr_lookback=atr_lookback,
    )

    config = RuleConfig(
        cooldown_seconds=int(rules.get("cooldown_seconds", 60)),
        open_range_breakout=rules.get("open_range_breakout", {}),
        vwap_deviation=rules.get("vwap_deviation", {}),
        squeeze_breakout=rules.get("squeeze_breakout", {}),
        rsi_extreme=rules.get("rsi_extreme", {}),
        break_retest=rules.get("break_retest", {}),
        volume_price_divergence=rules.get("volume_price_divergence", {}),
        prev_day_break=rules.get("prev_day_break", {}),
    )

    return RuleEngine(config=config, store=store)


def _parse_time_key(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value))
    text = str(value)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


def format_signal(signal) -> str:
    return f"{signal.rule}: {signal.message}"


def params_for_timeframe(cfg: Dict, timeframe: str) -> Tuple[int, float]:
    # 默认规则：1m=15分钟/0.5%，5m=60分钟/1.5%
    if timeframe == "1m":
        horizon = int(cfg.get("tf_1m_horizon_minutes", 15))
        threshold = float(cfg.get("tf_1m_min_move_pct", 0.5))
    else:
        horizon = int(cfg.get("tf_5m_horizon_minutes", 60))
        threshold = float(cfg.get("tf_5m_min_move_pct", 1.5))
    return horizon, threshold


def evaluate_signal(
    bars: List[Bar],
    idx: int,
    bar: Bar,
    timeframe: str,
    horizon_minutes: int,
    min_move_pct: float,
) -> Dict[str, float | str]:
    tf_minutes = 1 if timeframe == "1m" else 5
    window = max(1, int(horizon_minutes / tf_minutes))
    start = idx + 1
    end = min(len(bars), start + window)

    base = bar.close if bar.close else 1.0
    if start >= end:
        max_up_pct = 0.0
        max_down_pct = 0.0
    else:
        future = bars[start:end]
        max_high = max(b.high for b in future)
        min_low = min(b.low for b in future)
        max_up_pct = (max_high - base) / base * 100.0
        max_down_pct = (base - min_low) / base * 100.0

    passed = max_up_pct >= min_move_pct or max_down_pct >= min_move_pct
    label = "✅ 通过" if passed else "❌ 未通过"
    return {
        "max_up": max_up_pct,
        "max_down": max_down_pct,
        "passed": passed,
        "label": label,
    }


def init_stats() -> Dict[str, float]:
    return {
        "evaluated": 0,
        "effective": 0,
        "max_ups": [],
        "max_downs": [],
    }


def update_stats(stats: Dict[str, float], result: Dict[str, float | str]) -> None:
    stats["evaluated"] += 1
    stats["max_ups"].append(result["max_up"])
    stats["max_downs"].append(result["max_down"])
    if result["passed"]:
        stats["effective"] += 1


def finalize_stats(stats: Dict[str, float]) -> Dict[str, float]:
    max_ups: List[float] = stats["max_ups"]
    max_downs: List[float] = stats["max_downs"]
    evaluated = stats["evaluated"]
    effective = stats["effective"]
    return {
        "evaluated": evaluated,
        "effective": effective,
        "hit_rate": (effective / evaluated * 100.0) if evaluated else 0.0,
        "avg_max_up": sum(max_ups) / len(max_ups) if max_ups else 0.0,
        "avg_max_down": sum(max_downs) / len(max_downs) if max_downs else 0.0,
        "p50_max_up": percentile(max_ups, 0.5),
        "p50_max_down": percentile(max_downs, 0.5),
    }


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    idx = int(q * (len(values) - 1))
    return values[idx]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--symbol", default="HK.00981")
    parser.add_argument("--date", default="2026-03-10")
    args = parser.parse_args()

    cfg = load_config(str(Path(args.config).expanduser()))
    app_cfg = cfg.get("app", {})
    tz = ZoneInfo(app_cfg.get("tz", "Asia/Hong_Kong"))

    host = app_cfg.get("open_d", {}).get("host", "127.0.0.1")
    port = int(app_cfg.get("open_d", {}).get("port", 11111))

    engine = build_rule_engine(cfg)

    # 使用OpenD历史K线回放日内信号
    backtest_cfg = cfg.get("backtest", {})

    with OpenQuoteContext(host=host, port=port) as ctx:
        # 读取昨日高低
        date_obj = datetime.strptime(args.date, "%Y-%m-%d")
        yesterday = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        resp_day = ctx.request_history_kline(
            args.symbol,
            start=yesterday,
            end=yesterday,
            ktype=SubType.K_DAY,
            autype="qfq",
        )
        if len(resp_day) == 2:
            ret_day, data_day = resp_day
        else:
            ret_day, data_day, _ = resp_day
        if ret_day == RET_OK and len(data_day) > 0:
            row = data_day.iloc[-1]
            engine.store.set_prev_day(args.symbol, float(row["high"]), float(row["low"]))

        for tf in ("1m", "5m"):
            subtype = _TIMEFRAME_MAP[tf]
            resp = ctx.request_history_kline(
                args.symbol,
                start=args.date,
                end=args.date,
                ktype=subtype,
                autype="qfq",
            )
            # 兼容不同版本返回值（2或3项）
            if len(resp) == 2:
                ret, data = resp
            else:
                ret, data, _ = resp
            if ret != RET_OK:
                raise RuntimeError(f"request_history_kline failed: {data}")

            print(f"\n=== {args.symbol} {tf} {args.date} ===")
            signals: List[Tuple[int, Bar, str]] = []
            bars: List[Bar] = []
            for _, row in data.iterrows():
                ts = _parse_time_key(row["time_key"]).replace(tzinfo=tz)
                bar = Bar(
                    symbol=row["code"],
                    timeframe=tf,
                    ts=ts,
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
                if not hk_trading_session(bar.ts):
                    continue
                bars.append(bar)
                for s in engine.on_bar(bar):
                    signals.append((len(bars) - 1, bar, format_signal(s)))

            horizon_minutes, min_move_pct = params_for_timeframe(backtest_cfg, tf)
            stats = init_stats()
            for idx, bar, text in signals:
                result = evaluate_signal(bars, idx, bar, tf, horizon_minutes, min_move_pct)
                update_stats(stats, result)
                print(
                    f"{bar.ts} {text}\n"
                    f"------验证: {result['label']} "
                    f"(最大涨幅 {result['max_up']:.2f}%，最大跌幅 {result['max_down']:.2f}%)\n"
                )

            total_signals = len(signals)
            summary = finalize_stats(stats)
            print(f"总信号数: {total_signals}")
            print(
                f"有效信号数: {summary['effective']} / {summary['evaluated']} "
                f"(命中率 {summary['hit_rate']:.1f}%)"
            )
            print(
                f"平均最大涨幅: {summary['avg_max_up']:.2f}%  平均最大跌幅: {summary['avg_max_down']:.2f}%"
            )
            print(
                f"中位最大涨幅: {summary['p50_max_up']:.2f}%  中位最大跌幅: {summary['p50_max_down']:.2f}%"
            )


if __name__ == "__main__":
    main()
