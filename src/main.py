from __future__ import annotations

import argparse
import time as time_mod
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List
from zoneinfo import ZoneInfo

import yaml

# 主程序：订阅 OpenD、计算指标、触发规则并通知
from futu import OpenQuoteContext, SubType, RET_OK

from .data.futu_client import FutuClient
from .models import Bar, Signal
from .notify.local import send_local
from .notify.telegram import send_telegram
from .rules.rule_engine import RuleConfig, RuleEngine
from .storage.state import StateStore
from .ui.web import SignalStore, WebUI


def load_config(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def hk_trading_session(ts) -> bool:
    # 仅在港股盘中处理信号
    t = ts.time()
    return (time(9, 30) <= t < time(12, 0)) or (time(13, 0) <= t < time(16, 0))


def _parse_time_key(value) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value))
    text = str(value)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


def build_rule_engine(cfg: Dict) -> RuleEngine:
    rules = cfg.get("rules", {})
    atr_period = max(
        int(rules.get("vwap_deviation", {}).get("atr_period", 14)),
        int(rules.get("squeeze_breakout", {}).get("atr_period", 14)),
    )
    rsi_period = int(rules.get("rsi_extreme", {}).get("period", 14))
    vol_avg_period = int(rules.get("vwap_deviation", {}).get("vol_avg_period", 20))
    atr_lookback = int(rules.get("squeeze_breakout", {}).get("lookback", 60))

    store = StateStore(
        atr_period=atr_period,
        vol_avg_period=vol_avg_period,
        atr_lookback=atr_lookback,
    )
    store.update_periods(atr_period, vol_avg_period, atr_lookback, rsi_period)

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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    config_path = Path(args.config).expanduser()
    cfg = load_config(str(config_path))

    app_cfg = cfg.get("app", {})
    tz = ZoneInfo(app_cfg.get("tz", "Asia/Hong_Kong"))

    symbols: List[str] = app_cfg.get("symbols", [])
    symbol_names: Dict[str, str] = app_cfg.get("symbol_names", {})
    timeframes: List[str] = app_cfg.get("timeframes", ["1m", "5m"])

    notify_cfg = app_cfg.get("notify", {})
    notify_mode = notify_cfg.get("mode", "local")

    engine = build_rule_engine(cfg)
    # Web UI 用于本地查看最近信号
    web_cfg = app_cfg.get("web", {})
    web_enabled = bool(web_cfg.get("enabled", True))
    web_host = web_cfg.get("host", "127.0.0.1")
    web_port = int(web_cfg.get("port", 8088))
    storage_cfg = app_cfg.get("storage", {})
    storage_dir = storage_cfg.get("dir", "data/signals")
    load_days = int(storage_cfg.get("load_days", 7))
    signal_store = SignalStore(maxlen=int(web_cfg.get("max_signals", 200)), storage_dir=storage_dir, load_days=load_days)
    if web_enabled:
        signal_store.set_meta({"symbols": symbols, "symbol_names": symbol_names, "timeframes": timeframes})
        WebUI(web_host, web_port, signal_store).start()

    # 处理每根K线
    def handle_bar(bar: Bar) -> None:
        if bar.ts.tzinfo is None:
            bar_ts = bar.ts.replace(tzinfo=tz)
        else:
            bar_ts = bar.ts.astimezone(tz)
        bar = Bar(
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            ts=bar_ts,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )

        if not hk_trading_session(bar.ts):
            return
        # 仅在K线收盘时处理（避免区间内提前更新）
        if bar.timeframe in ("1m", "5m"):
            now = datetime.now(tz)
            if now < bar.ts:
                return

        if web_enabled:
            signal_store.update_bar(
                bar.symbol,
                bar.timeframe,
                {
                    "ts": bar.ts.isoformat(),
                    "open": bar.open,
                    "high": bar.high,
                    "low": bar.low,
                    "close": bar.close,
                    "volume": bar.volume,
                },
            )

        signals = engine.on_bar(bar)
        for signal in signals:
            name = symbol_names.get(signal.symbol)
            if name:
                signal = Signal(
                    symbol=signal.symbol,
                    timeframe=signal.timeframe,
                    ts=signal.ts,
                    rule=signal.rule,
                    message=signal.message,
                    strength=signal.strength,
                    symbol_name=name,
                    context=signal.context,
                    direction=signal.direction,
                    triggers=signal.triggers,
                )
            if signal.symbol_name:
                head = f"{signal.symbol_name}({signal.symbol})"
            else:
                head = signal.symbol
            text = f"{head} {signal.timeframe} {signal.rule}: {signal.message}"
            if web_enabled:
                signal_store.add(signal)
            if notify_mode in ("telegram", "both"):
                telegram_cfg = notify_cfg.get("telegram", {})
                send_telegram(text, telegram_cfg.get("token", ""), telegram_cfg.get("chat_id", ""))
            if notify_mode in ("local", "both"):
                send_local(text)

    client = FutuClient(host=app_cfg.get("open_d", {}).get("host", "127.0.0.1"),
                        port=int(app_cfg.get("open_d", {}).get("port", 11111)))

    try:
        # 启动前预热：读取当天历史K线，填充指标窗口
        warmup_cfg = app_cfg.get("warmup", {})
        if warmup_cfg.get("enabled", True):
            today = datetime.now(tz).strftime("%Y-%m-%d")
            yesterday = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")
            total_warmup = 0
            with OpenQuoteContext(host=app_cfg.get("open_d", {}).get("host", "127.0.0.1"),
                                  port=int(app_cfg.get("open_d", {}).get("port", 11111))) as ctx:
                tf_map = {"1m": SubType.K_1M, "5m": SubType.K_5M}
                # 读取昨日高低
                for symbol in symbols:
                    resp_day = ctx.request_history_kline(
                        symbol,
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
                        store = engine.store
                        store.set_prev_day(symbol, float(row["high"]), float(row["low"]))

                for symbol in symbols:
                    for tf in timeframes:
                        subtype = tf_map.get(tf)
                        if subtype is None:
                            continue
                        resp = ctx.request_history_kline(
                            symbol,
                            start=today,
                            end=today,
                            ktype=subtype,
                            autype="qfq",
                        )
                        if len(resp) == 2:
                            ret, data = resp
                        else:
                            ret, data, _ = resp
                        if ret != RET_OK:
                            continue
                        for _, row in data.iterrows():
                            ts = _parse_time_key(row["time_key"])
                            bar = Bar(
                                symbol=row["code"],
                                timeframe=tf,
                                ts=ts.replace(tzinfo=tz) if ts.tzinfo is None else ts.astimezone(tz),
                                open=float(row["open"]),
                                high=float(row["high"]),
                                low=float(row["low"]),
                                close=float(row["close"]),
                                volume=float(row["volume"]),
                            )
                            if not hk_trading_session(bar.ts):
                                continue
                            if web_enabled:
                                signal_store.update_bar(
                                    bar.symbol,
                                    bar.timeframe,
                                    {
                                        "ts": bar.ts.isoformat(),
                                        "open": bar.open,
                                        "high": bar.high,
                                        "low": bar.low,
                                        "close": bar.close,
                                        "volume": bar.volume,
                                    },
                                )
                            # 预热不发送提示
                            engine.on_bar(bar)
                            total_warmup += 1

            if web_enabled:
                signal_store.set_warmup({"total": total_warmup, "date": today})

        client.subscribe(symbols, timeframes, handle_bar)
        print("Subscribed. Waiting for bars...")
        while True:
            time_mod.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.close()


if __name__ == "__main__":
    main()
