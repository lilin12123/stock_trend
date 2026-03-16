from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from .market_profile import market_timezone
from ..infrastructure import SqliteStore


class SignalQueryService:
    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    def list_signals(
        self,
        owner_user_id: Optional[int],
        limit: int = 200,
        offset: int = 0,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        level_1m: Optional[str] = None,
        level_5m: Optional[str] = None,
        text: Optional[str] = None,
    ) -> List[Dict]:
        raw = self.store.list_signals_raw(
            owner_user_id=owner_user_id,
            include_global=True,
            limit=max(limit * 4, 400),
            offset=0,
            symbol=symbol,
            timeframe=timeframe,
            text=text,
        )
        merged: Dict[str, Dict] = {}
        for item in raw:
            tz_name = market_timezone(item.get("symbol"), "Asia/Hong_Kong")
            normalized = self._normalize_triggers(item)
            if not normalized:
                normalized = self._fallback_trigger(item)
            key = self._aggregate_key(item, tz_name)
            aggregated = merged.get(key)
            if not aggregated:
                aggregated = {
                    **item,
                    "id": key,
                    "scope": "merged",
                    "owner_user_id": owner_user_id,
                    "triggers": [],
                    "dominant_items": {},
                }
                merged[key] = aggregated
            aggregated["context_snapshot"] = self._merge_context_snapshot(
                aggregated.get("context_snapshot") or {},
                item.get("context_snapshot") or {},
            )
            aggregated["triggers"] = self._merge_triggers(aggregated.get("triggers") or [], normalized)
            item_strengths = self._direction_counts(normalized)
            for direction in ("up", "down"):
                if item_strengths[direction] <= 0:
                    continue
                current = aggregated["dominant_items"].get(direction)
                if not current or item_strengths[direction] >= current["strength"]:
                    aggregated["dominant_items"][direction] = {
                        "strength": item_strengths[direction],
                        "message": item.get("message"),
                        "rule_name": item.get("rule_name") or item.get("source_rule"),
                        "source_rule": item.get("source_rule") or item.get("rule_name"),
                        "created_at": item.get("created_at"),
                    }

        items = []
        for aggregated in merged.values():
            counts = self._direction_counts(aggregated.get("triggers") or [])
            if counts["up"] == counts["down"]:
                continue
            direction = "up" if counts["up"] > counts["down"] else "down"
            trigger_count = abs(counts["up"] - counts["down"])
            dominant = aggregated.get("dominant_items", {}).get(direction, {})
            filtered_triggers = [trigger for trigger in aggregated.get("triggers") or [] if trigger.get("direction") == direction]
            base = {key: value for key, value in aggregated.items() if key != "dominant_items"}
            items.append(
                {
                    **base,
                    "direction": direction,
                    "triggers": filtered_triggers,
                    "evaluation": (base.get("context_snapshot") or {}).get("forward_metrics"),
                    "level": self._count_to_level(trigger_count),
                    "trigger_count": trigger_count,
                    "message": dominant.get("message") or aggregated.get("message"),
                    "rule_name": dominant.get("rule_name") or aggregated.get("rule_name") or aggregated.get("source_rule"),
                    "source_rule": dominant.get("source_rule") or aggregated.get("source_rule"),
                    "created_at": dominant.get("created_at") or aggregated.get("created_at"),
                }
            )
        if level_1m:
            target = self._level_rank(level_1m)
            items = [item for item in items if self._normalize_tf(item.get("timeframe")) != "1m" or self._level_rank(item.get("level")) >= target]
        if level_5m:
            target = self._level_rank(level_5m)
            items = [item for item in items if self._normalize_tf(item.get("timeframe")) != "5m" or self._level_rank(item.get("level")) >= target]
        items.sort(key=lambda item: str(item.get("ts", "")), reverse=True)
        return items[offset:offset + limit]

    @staticmethod
    def _normalize_tf(value: str | None) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _bar_key(ts: str, tz_name: str) -> str:
        try:
            dt = datetime.fromisoformat(ts)
        except ValueError:
            return ts[:19]
        if dt.tzinfo is None:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _unique_trigger_names(triggers: List[Dict]) -> List[str]:
        names = {
            trigger.get("name")
            for trigger in triggers or []
            if isinstance(trigger, dict) and trigger.get("name")
        }
        return sorted(names)

    def _direction_counts(self, triggers: List[Dict]) -> Dict[str, int]:
        counts = {"up": 0, "down": 0}
        for trigger in self._merge_triggers([], triggers or []):
            direction = trigger.get("direction")
            if direction in counts:
                counts[direction] += 1
        return counts

    @staticmethod
    def _count_to_level(n: int) -> str:
        return f"Lv{max(0, min(5, n))}" if n < 5 else "Lv5"

    @staticmethod
    def _level_rank(level: str | None) -> int:
        levels = {"Lv0": 0, "Lv1": 1, "Lv2": 2, "Lv3": 3, "Lv4": 4, "Lv5": 5}
        return levels.get(level or "", 0)

    @staticmethod
    def _resolve_exclusive(triggers: List[Dict]) -> List[Dict]:
        uniq: Dict[tuple, Dict] = {}
        for trigger in triggers:
            name = trigger.get("name")
            direction = trigger.get("direction", "neutral")
            if not name:
                continue
            key = (name, direction)
            current = uniq.get(key)
            if not current or str(trigger.get("ts", "")) >= str(current.get("ts", "")):
                uniq[key] = trigger
        return list(uniq.values())

    def _merge_triggers(self, left: List[Dict], right: List[Dict]) -> List[Dict]:
        merged = self._resolve_exclusive((left or []) + (right or []))
        uniq: Dict[tuple, Dict] = {}
        for trigger in merged:
            name = trigger.get("name")
            direction = trigger.get("direction", "neutral")
            if not name:
                continue
            key = (name, direction)
            current = uniq.get(key)
            if not current or str(trigger.get("ts", "")) >= str(current.get("ts", "")):
                uniq[key] = trigger
        return list(uniq.values())

    def _fallback_trigger(self, item: Dict) -> List[Dict]:
        direction = item.get("direction")
        rule_name = item.get("rule_name") or item.get("source_rule")
        if direction not in {"up", "down"} or not rule_name:
            return []
        return [{"name": rule_name, "direction": direction, "ts": item.get("ts", "")}]

    def _aggregate_key(self, item: Dict, tz_name: str) -> str:
        return "|".join(
            [
                str(item.get("symbol") or ""),
                self._normalize_tf(item.get("timeframe")),
                self._bar_key(str(item.get("ts", "")), tz_name),
            ]
        )

    def _normalize_triggers(self, item: Dict) -> List[Dict]:
        base_ts = item.get("ts", "")
        out: Dict[str, Dict] = {}
        for trigger in item.get("triggers") or []:
            name = trigger.get("name")
            if not name:
                continue
            entry = {"name": name, "direction": trigger.get("direction", "neutral"), "ts": base_ts}
            out[name] = entry
        rule_name = item.get("rule_name")
        if rule_name and rule_name not in out:
            out[rule_name] = {"name": rule_name, "direction": item.get("direction") or "neutral", "ts": base_ts}
        return self._resolve_exclusive(list(out.values()))

    @staticmethod
    def _merge_context_snapshot(left: Dict, right: Dict) -> Dict:
        merged = {**(left or {})}
        right = right or {}
        left_metrics = (left or {}).get("forward_metrics")
        right_metrics = right.get("forward_metrics")
        if right_metrics and (
            not left_metrics
            or (right_metrics.get("completed") and not left_metrics.get("completed"))
            or (right_metrics.get("observed_bars", 0) >= left_metrics.get("observed_bars", 0))
        ):
            merged["forward_metrics"] = right_metrics
        return merged
