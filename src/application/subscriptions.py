from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from ..application.rule_runtime import build_rule_engine, deep_merge_dict
from ..domain import NotificationSetting


@dataclass
class ProfileRuntime:
    key: str
    scope: str
    owner_user_id: Optional[int]
    username: str
    symbols: Set[str]
    symbol_names: Dict[str, str]
    rules: Dict
    notification: Optional[NotificationSetting]
    engine: object


class SubscriptionPlanner:
    def build_profiles(
        self,
        default_rules: Dict,
        users: List[Dict],
        global_symbols: List[Dict],
        user_symbols_by_user: Dict[int, List[Dict]],
        user_rule_overrides: Dict[int, Dict],
        notifications: Dict[int, NotificationSetting],
    ) -> List[ProfileRuntime]:
        global_enabled = [item for item in global_symbols if item.get("enabled")]
        global_names = {item["symbol"]: item.get("symbol_name") or item["symbol"] for item in global_enabled}
        profiles: List[ProfileRuntime] = [
            ProfileRuntime(
                key="global",
                scope="global",
                owner_user_id=None,
                username="global",
                symbols={item["symbol"] for item in global_enabled},
                symbol_names=global_names,
                rules=default_rules,
                notification=None,
                engine=build_rule_engine(default_rules),
            )
        ]

        for user in users:
            if not user.get("is_active"):
                continue
            personal_symbols = [item for item in user_symbols_by_user.get(int(user["id"]), []) if item.get("enabled")]
            personal_names = {item["symbol"]: item.get("symbol_name") or item["symbol"] for item in personal_symbols}
            effective_symbols = {item["symbol"] for item in personal_symbols}
            effective_rules = deep_merge_dict(default_rules, user_rule_overrides.get(int(user["id"]), {}))
            profiles.append(
                ProfileRuntime(
                    key=f"user:{user['id']}",
                    scope="user",
                    owner_user_id=int(user["id"]),
                    username=str(user["username"]),
                    symbols=effective_symbols,
                    symbol_names=personal_names,
                    rules=effective_rules,
                    notification=notifications.get(int(user["id"])),
                    engine=build_rule_engine(effective_rules),
                )
            )
        return profiles

    @staticmethod
    def active_symbols(profiles: List[ProfileRuntime]) -> List[str]:
        merged: Set[str] = set()
        for profile in profiles:
            merged.update(profile.symbols)
        return sorted(merged)
