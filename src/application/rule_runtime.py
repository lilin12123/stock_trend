from __future__ import annotations

import os
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict

import yaml

from .market_profile import market_trading_session
from ..rules.rule_engine import RuleConfig, RuleEngine
from ..storage.state import StateStore


def expand_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.getenv(value[2:-1], "")
    if isinstance(value, dict):
        return {k: expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(item) for item in value]
    return value


def load_config(path: str) -> Dict[str, Any]:
    config_path = Path(path).expanduser()
    with config_path.open("r", encoding="utf-8") as f:
        return expand_env(yaml.safe_load(f) or {})


def deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = deep_merge_dict(current, value)
        else:
            merged[key] = value
    return merged

def parse_time_key(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value))
    text = str(value)
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


def build_rule_engine(rules: Dict[str, Any]) -> RuleEngine:
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
