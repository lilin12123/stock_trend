from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..application.market_profile import market_trading_session, normalize_market_datetime
from ..application.rule_runtime import build_rule_engine, parse_time_key
from ..domain import Bar
from ..infrastructure import OpenDGateway, SqliteStore


def params_for_timeframe(cfg: Dict[str, Any], timeframe: str) -> Tuple[int, float]:
    if timeframe == "1m":
        return int(cfg.get("tf_1m_horizon_minutes", 15)), float(cfg.get("tf_1m_min_move_pct", 0.5))
    return int(cfg.get("tf_5m_horizon_minutes", 60)), float(cfg.get("tf_5m_min_move_pct", 1.5))


def normalize_timeframes(value: Any) -> List[str]:
    valid = {"1m", "5m"}
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return ["1m", "5m"]
    picked = [str(item).strip().lower() for item in value if str(item).strip().lower() in valid]
    deduped = list(dict.fromkeys(picked))
    return deduped or ["1m", "5m"]


def criteria_for_timeframe(default_cfg: Dict[str, Any], overrides: Dict[str, Any], timeframe: str) -> Tuple[int, float]:
    base_horizon, base_move = params_for_timeframe(default_cfg, timeframe)
    tf_cfg = overrides.get(timeframe, {}) if isinstance(overrides, dict) else {}
    horizon = int(tf_cfg.get("horizon_minutes", base_horizon))
    min_move = float(tf_cfg.get("min_move_pct", base_move))
    return horizon, min_move


def evaluate_signal(
    bars: List[Bar],
    idx: int,
    bar: Bar,
    timeframe: str,
    horizon_minutes: int,
    min_move_pct: float,
) -> Dict[str, Any]:
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
        max_high = max(item.high for item in future)
        min_low = min(item.low for item in future)
        max_up_pct = (max_high - base) / base * 100.0
        max_down_pct = (base - min_low) / base * 100.0
    passed = max_up_pct >= min_move_pct or max_down_pct >= min_move_pct
    return {
        "max_up": max_up_pct,
        "max_down": max_down_pct,
        "passed": passed,
        "label": "✅ 通过" if passed else "❌ 未通过",
    }


@dataclass
class BacktestRunner:
    store: SqliteStore
    gateway: OpenDGateway
    default_rules: Dict[str, Any]
    backtest_cfg: Dict[str, Any]
    tz_name: str

    def submit(
        self,
        owner_user_id: int,
        symbol: str,
        trade_date: str,
        rules: Dict[str, Any],
        timeframes: List[str] | None = None,
        criteria: Dict[str, Any] | None = None,
    ) -> str:
        symbol = symbol.strip()
        trade_date = trade_date.strip()
        if not symbol:
            raise ValueError("Symbol is required")
        datetime.strptime(trade_date, "%Y-%m-%d")
        selected_timeframes = normalize_timeframes(timeframes or ["1m", "5m"])
        evaluation_criteria = criteria or {}
        job_id = self.store.create_backtest_job(
            owner_user_id=owner_user_id,
            symbol=symbol,
            trade_date=trade_date,
            profile_scope="user",
            profile_owner_user_id=owner_user_id,
            template_version_id=None,
            params={"rules": rules, "timeframes": selected_timeframes, "criteria": evaluation_criteria},
        )
        thread = threading.Thread(
            target=self._run_job,
            args=(job_id, symbol, trade_date, rules, selected_timeframes, evaluation_criteria),
            daemon=True,
        )
        thread.start()
        return job_id

    def run_once(
        self,
        symbol: str,
        trade_date: str,
        rules: Dict[str, Any],
        timeframes: List[str] | None = None,
        criteria: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return self._run(symbol, trade_date, rules, normalize_timeframes(timeframes or ["1m", "5m"]), criteria or {})

    def _run_job(
        self,
        job_id: str,
        symbol: str,
        trade_date: str,
        rules: Dict[str, Any],
        timeframes: List[str],
        criteria: Dict[str, Any],
    ) -> None:
        self.store.set_backtest_job_status(job_id, "running")
        try:
            payload = self._run(symbol, trade_date, rules, timeframes, criteria)
            self.store.replace_backtest_results(job_id, payload["results"])
            self.store.set_backtest_job_status(job_id, "done", payload["summary"])
        except Exception as exc:  # pragma: no cover - depends on OpenD runtime
            self.store.set_backtest_job_status(job_id, "failed", error_text=str(exc))

    def _run(
        self,
        symbol: str,
        trade_date: str,
        rules: Dict[str, Any],
        timeframes: List[str],
        criteria: Dict[str, Any],
    ) -> Dict[str, Any]:
        engine = build_rule_engine(rules)
        date_obj = datetime.strptime(trade_date, "%Y-%m-%d")
        yesterday = (date_obj - timedelta(days=1)).strftime("%Y-%m-%d")
        prev_day = self.gateway.request_prev_day(symbol, yesterday)
        if prev_day:
            engine.store.set_prev_day(symbol, float(prev_day["high"]), float(prev_day["low"]))

        all_results: List[Dict[str, Any]] = []
        summary_by_timeframe: Dict[str, Any] = {}
        total_evaluated = 0
        total_effective = 0
        for timeframe in timeframes:
            history = self.gateway.request_history(symbol, trade_date, trade_date, timeframe)
            bars: List[Bar] = []
            signals: List[Tuple[int, Bar, Any]] = []
            for row in history:
                ts = parse_time_key(row["time_key"])
                bar = Bar(
                    symbol=row["code"],
                    timeframe=timeframe,
                    ts=normalize_market_datetime(row["code"], ts, self.tz_name),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
                if not market_trading_session(bar.symbol, bar.ts, self.tz_name):
                    continue
                bars.append(bar)
                for signal in engine.on_bar(bar):
                    signals.append((len(bars) - 1, bar, signal))

            horizon_minutes, min_move_pct = criteria_for_timeframe(self.backtest_cfg, criteria, timeframe)
            evaluated = 0
            effective = 0
            max_ups: List[float] = []
            max_downs: List[float] = []
            for idx, bar, signal in signals:
                result = evaluate_signal(bars, idx, bar, timeframe, horizon_minutes, min_move_pct)
                evaluated += 1
                effective += 1 if result["passed"] else 0
                max_ups.append(result["max_up"])
                max_downs.append(result["max_down"])
                all_results.append(
                    {
                        "signal_ts": bar.ts.isoformat(),
                        "timeframe": timeframe,
                        "signal_text": signal.message,
                        "result": {
                            **result,
                            "rule_key": signal.rule,
                            "signal_message": signal.message,
                            "direction": signal.direction,
                            "horizon_minutes": horizon_minutes,
                            "min_move_pct": min_move_pct,
                        },
                    }
                )
            total_evaluated += evaluated
            total_effective += effective
            summary_by_timeframe[timeframe] = {
                "evaluated": evaluated,
                "effective": effective,
                "hit_rate": (effective / evaluated * 100.0) if evaluated else 0.0,
                "avg_max_up": sum(max_ups) / len(max_ups) if max_ups else 0.0,
                "avg_max_down": sum(max_downs) / len(max_downs) if max_downs else 0.0,
                "criteria": {"horizon_minutes": horizon_minutes, "min_move_pct": min_move_pct},
            }
        summary = {
            "overall": {
                "timeframes": timeframes,
                "evaluated": total_evaluated,
                "effective": total_effective,
                "hit_rate": (total_effective / total_evaluated * 100.0) if total_evaluated else 0.0,
            },
            "by_timeframe": summary_by_timeframe,
        }
        return {"results": all_results, "summary": summary}
