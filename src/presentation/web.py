from __future__ import annotations

import html
import json
import re
from datetime import datetime
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from ..application.auth_service import AuthService
from ..application.backtesting import BacktestRunner
from ..application.monitoring import MonitoringService
from ..application.notification_policy import sanitize_notification_data
from ..application.query_service import SignalQueryService
from ..infrastructure import SqliteStore


class LocalWebApp:
    def __init__(
        self,
        host: str,
        port: int,
        store: SqliteStore,
        auth_service: AuthService,
        query_service: SignalQueryService,
        monitoring_service: MonitoringService,
        backtest_runner: BacktestRunner,
        app_cfg: Dict[str, Any],
    ) -> None:
        self.host = host
        self.port = port
        self.store = store
        self.auth_service = auth_service
        self.query_service = query_service
        self.monitoring_service = monitoring_service
        self.backtest_runner = backtest_runner
        self.app_cfg = app_cfg
        self.auth_cfg = app_cfg.get("auth", {})
        self.static_dir = Path(__file__).parent / "static"
        self.manual_path = Path(__file__).resolve().parents[2] / "项目功能使用说明书.md"

    def serve_forever(self) -> None:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer._dispatch(self)

            def do_POST(self) -> None:  # noqa: N802
                outer._dispatch(self)

            def do_PUT(self) -> None:  # noqa: N802
                outer._dispatch(self)

            def log_message(self, format, *args):  # noqa: A003
                return

        server = ThreadingHTTPServer((self.host, self.port), Handler)
        server.serve_forever()

    def _dispatch(self, handler: BaseHTTPRequestHandler) -> None:
        try:
            parsed = urlparse(handler.path)
            path = parsed.path
            method = handler.command.upper()
            user = self._current_user(handler)

            if path == "/api/auth/login" and method == "POST":
                payload = self._read_json(handler)
                result = self.auth_service.login(payload.get("username", ""), payload.get("password", ""))
                if not result.get("ok"):
                    body = {"error": "Login failed", "error_code": result.get("error_code")}
                    if result.get("locked_until"):
                        body["locked_until"] = result["locked_until"]
                    if result.get("remaining_attempts") is not None:
                        body["remaining_attempts"] = result["remaining_attempts"]
                    return self._write_json(handler, body, status=401)
                return self._set_session_cookie(handler, str(result["session_id"]))
            if path == "/api/auth/registration" and method == "GET":
                return self._write_json(handler, self._registration_state())
            if path == "/api/auth/register" and method == "POST":
                payload = self._read_json(handler)
                username = payload.get("username", "").strip()
                password = payload.get("password", "")
                registration = self._registration_state()
                if not registration["enabled"]:
                    return self._write_json(
                        handler,
                        {"error": "Registration is disabled", "error_code": "registration_disabled"},
                        status=403,
                    )
                if registration["limit_reached"]:
                    return self._write_json(
                        handler,
                        {
                            "error": "Registration limit reached",
                            "error_code": "registration_limit_reached",
                            "max_users": registration["max_users"],
                        },
                        status=403,
                    )
                if not username or not password:
                    return self._write_json(
                        handler,
                        {"error": "Username and password are required", "error_code": "invalid_registration_data"},
                        status=400,
                    )
                password_hash = self.auth_service.hasher.hash_password(password)
                try:
                    user_id = self.store.create_user(username, password_hash, "user")
                except ValueError as exc:
                    error_code = "username_taken" if "already exists" in str(exc).lower() else "invalid_registration_data"
                    return self._write_json(handler, {"error": str(exc), "error_code": error_code}, status=400)
                return self._write_json(handler, {"ok": True, "user_id": user_id})
            if path == "/api/auth/logout" and method == "POST":
                session_id = self._session_id(handler)
                if session_id:
                    self.auth_service.logout(session_id)
                return self._clear_session_cookie(handler)
            if path == "/api/me" and method == "GET":
                if not user:
                    return self._write_json(handler, {"authenticated": False}, status=401)
                notification = self._sanitize_notification_payload(user, self.store.get_notification_settings_dict(int(user["id"])))
                return self._write_json(
                    handler,
                    {
                        "authenticated": True,
                        "user": {
                            "id": int(user["id"]),
                            "username": user["username"],
                            "role": user["role"],
                            "is_active": bool(user["is_active"]),
                        },
                        "notification": notification,
                    },
                )

            if not user:
                if path in ("/", "/index.html", "/app.js", "/styles.css", "/brand-icon.svg", "/manual"):
                    if path == "/manual":
                        return self._write_manual(handler)
                    return self._write_static(handler, "index.html" if path in ("/", "/index.html") else path.lstrip("/"))
                return self._write_json(handler, {"error": "Unauthorized"}, status=401)

            if path == "/api/runtime/status" and method == "GET":
                return self._write_json(handler, self.monitoring_service.get_status().__dict__)
            if path == "/api/runtime/apply-config" and method == "POST":
                return self._write_json(handler, self.monitoring_service.apply_config())
            if path == "/api/signals" and method == "GET":
                params = parse_qs(parsed.query)
                page = self.query_service.list_signals_page(
                    owner_user_id=int(user["id"]),
                    limit=int(params.get("limit", ["100"])[0]),
                    offset=int(params.get("offset", ["0"])[0]),
                    symbol=params.get("symbol", [None])[0],
                    timeframe=params.get("tf", [None])[0],
                    level_1m=params.get("level_1m", [None])[0],
                    level_5m=params.get("level_5m", [None])[0],
                    text=params.get("text", [None])[0],
                )
                return self._write_json(handler, page)
            if path == "/api/market-snapshots" and method == "GET":
                return self._write_json(handler, self.monitoring_service.get_market_snapshots())
            if path.startswith("/api/signals/") and method == "GET":
                signal_id = path.rsplit("/", 1)[-1]
                item = self.store.get_signal(signal_id)
                if not item:
                    return self._write_json(handler, {"error": "Not found"}, status=404)
                if item.get("owner_user_id") not in (None, int(user["id"])) and user["role"] != "admin":
                    return self._write_json(handler, {"error": "Forbidden"}, status=403)
                return self._write_json(handler, item)
            if path == "/api/chart/day-bars" and method == "GET":
                params = parse_qs(parsed.query)
                bars = self.monitoring_service.get_day_bars(
                    params.get("symbol", [""])[0],
                    params.get("tf", [""])[0],
                    params.get("since", [None])[0],
                )
                return self._write_json(handler, bars)
            if path == "/api/stocks/global":
                if method == "GET":
                    return self._write_json(handler, self.store.list_global_symbols())
                self._require_admin(user)
                payload = self._read_json(handler)
                self.store.add_global_symbol(payload.get("symbol", "").strip(), payload.get("symbol_name", "").strip(), bool(payload.get("enabled", True)))
                return self._write_json(handler, {"ok": True})
            if path == "/api/stocks/mine":
                if method == "GET":
                    return self._write_json(handler, self.store.list_user_symbols(int(user["id"])))
                payload = self._read_json(handler)
                try:
                    self.store.add_user_symbol(
                        int(user["id"]),
                        payload.get("symbol", "").strip(),
                        payload.get("symbol_name", "").strip(),
                        bool(payload.get("enabled", True)),
                    )
                except ValueError as exc:
                    error_code = "watchlist_limit_reached" if "limited to one symbol" in str(exc).lower() else "invalid_symbol"
                    return self._write_json(handler, {"error": str(exc), "error_code": error_code}, status=400)
                return self._write_json(handler, {"ok": True})
            if path == "/api/rules/default" and method == "GET":
                return self._write_json(handler, self.store.get_default_rule_config())
            if path == "/api/forward-metrics":
                if method == "GET":
                    return self._write_json(handler, self.store.get_forward_metrics_config(self.app_cfg))
                self._require_admin(user)
                payload = self._read_json(handler)
                saved = self.store.save_forward_metrics_config(payload, self.app_cfg)
                self.monitoring_service.apply_config()
                return self._write_json(handler, saved)
            if path == "/api/rules/mine":
                if method == "GET":
                    return self._write_json(handler, self.store.get_user_rule_overrides(int(user["id"])))
                payload = self._read_json(handler)
                self.store.save_user_rule_overrides(int(user["id"]), payload)
                return self._write_json(handler, {"ok": True})
            if path == "/api/notifications/mine":
                if method == "GET":
                    settings = self.store.get_notification_settings_dict(int(user["id"]))
                    return self._write_json(handler, self._sanitize_notification_payload(user, settings))
                payload = self._read_json(handler)
                self.store.save_notification_settings(int(user["id"]), self._sanitize_notification_payload(user, payload))
                return self._write_json(handler, {"ok": True})
            if path == "/api/users":
                self._require_admin(user)
                if method == "GET":
                    users = [item.__dict__ for item in self.store.list_users()]
                    return self._write_json(handler, users)
                payload = self._read_json(handler)
                role = payload.get("role", "user")
                if role == "user":
                    registration = self._registration_state()
                    if registration["limit_reached"]:
                        return self._write_json(
                            handler,
                            {
                                "error": "Registration limit reached",
                                "error_code": "registration_limit_reached",
                                "max_users": registration["max_users"],
                            },
                            status=403,
                        )
                password_hash = self.auth_service.hasher.hash_password(payload.get("password", "changeme"))
                try:
                    user_id = self.store.create_user(payload.get("username", "").strip(), password_hash, role)
                except ValueError as exc:
                    error_code = "username_taken" if "already exists" in str(exc).lower() else "invalid_user_data"
                    return self._write_json(handler, {"error": str(exc), "error_code": error_code}, status=400)
                return self._write_json(handler, {"ok": True, "user_id": user_id})
            if path.startswith("/api/users/") and path.endswith("/password") and method == "PUT":
                self._require_admin(user)
                parts = path.strip("/").split("/")
                user_id = int(parts[2])
                payload = self._read_json(handler)
                password_hash = self.auth_service.hasher.hash_password(payload.get("password", "changeme"))
                self.store.update_user_password(user_id, password_hash)
                return self._write_json(handler, {"ok": True})
            if path.startswith("/api/users/") and method == "PUT":
                self._require_admin(user)
                parts = path.strip("/").split("/")
                if len(parts) != 3:
                    return self._write_json(handler, {"error": "Not found"}, status=404)
                user_id = int(parts[2])
                target_user = self.store.get_user_by_id(user_id)
                if not target_user:
                    return self._write_json(handler, {"error": "Not found"}, status=404)
                payload = self._read_json(handler)
                if "is_active" in payload:
                    is_active = bool(payload["is_active"])
                    if int(user["id"]) == user_id and not is_active:
                        return self._write_json(handler, {"error": "Cannot disable current admin session"}, status=400)
                    self.store.update_user_active_state(user_id, is_active)
                return self._write_json(handler, {"ok": True})
            if path == "/api/rule-templates":
                if method == "GET":
                    return self._write_json(handler, self.store.list_rule_templates())
                self._require_admin(user)
                payload = self._read_json(handler)
                template_id = self.store.create_rule_template(
                    name=payload.get("name", "").strip(),
                    description=payload.get("description", "").strip(),
                    config=payload.get("config", {}),
                    created_by_user_id=int(user["id"]),
                )
                return self._write_json(handler, {"ok": True, "template_id": template_id})
            if path == "/api/backtests":
                if method == "GET":
                    return self._write_json(handler, self.store.list_backtest_jobs(int(user["id"])))
                payload = self._read_json(handler)
                symbol = payload.get("symbol", "").strip()
                trade_date = payload.get("date", "").strip()
                timeframes = payload.get("timeframes") or []
                if not symbol:
                    return self._write_json(handler, {"error": "Symbol is required"}, status=400)
                try:
                    datetime.strptime(trade_date, "%Y-%m-%d")
                except ValueError:
                    return self._write_json(handler, {"error": "Trade date must be in YYYY-MM-DD format"}, status=400)
                if not isinstance(timeframes, list) or not any(str(item).strip().lower() in {"1m", "5m"} for item in timeframes):
                    return self._write_json(handler, {"error": "At least one timeframe is required"}, status=400)
                rules = payload.get("rules")
                if not isinstance(rules, dict):
                    rules = self.store.get_default_rule_config()
                criteria = payload.get("criteria")
                if not isinstance(criteria, dict):
                    criteria = {}
                job_id = self.backtest_runner.submit(int(user["id"]), symbol, trade_date, rules, timeframes, criteria)
                return self._write_json(handler, {"ok": True, "job_id": job_id})
            if path == "/api/backtests/config" and method == "GET":
                cfg = self.backtest_runner.backtest_cfg
                return self._write_json(
                    handler,
                    {
                        "timeframes": ["1m", "5m"],
                        "criteria": {
                            "1m": {
                                "horizon_minutes": int(cfg.get("tf_1m_horizon_minutes", 15)),
                                "min_move_pct": float(cfg.get("tf_1m_min_move_pct", 0.5)),
                            },
                            "5m": {
                                "horizon_minutes": int(cfg.get("tf_5m_horizon_minutes", 60)),
                                "min_move_pct": float(cfg.get("tf_5m_min_move_pct", 1.5)),
                            },
                        },
                    },
                )
            if path.startswith("/api/backtests/") and method == "GET":
                job_id = path.rsplit("/", 1)[-1]
                item = self.store.get_backtest_job(job_id)
                if not item:
                    return self._write_json(handler, {"error": "Not found"}, status=404)
                if int(item["owner_user_id"]) != int(user["id"]) and user["role"] != "admin":
                    return self._write_json(handler, {"error": "Forbidden"}, status=403)
                return self._write_json(handler, item)
            if path == "/api/analytics/summary" and method == "GET":
                return self._write_json(handler, self.store.analytics_summary(int(user["id"])))

            if path in ("/", "/index.html", "/app.js", "/styles.css", "/brand-icon.svg", "/manual"):
                if path == "/manual":
                    return self._write_manual(handler)
                return self._write_static(handler, "index.html" if path in ("/", "/index.html") else path.lstrip("/"))

            return self._write_json(handler, {"error": "Not found"}, status=404)
        except PermissionError:
            return self._write_json(handler, {"error": "Forbidden"}, status=403)
        except Exception as exc:
            return self._write_json(handler, {"error": str(exc)}, status=500)

    def _require_admin(self, user: Dict[str, Any]) -> None:
        if user.get("role") != "admin":
            raise PermissionError

    def _registration_state(self) -> Dict[str, Any]:
        registration_cfg = self.auth_cfg.get("public_registration", {})
        enabled = bool(registration_cfg.get("enabled", True))
        max_users = int(registration_cfg.get("max_users", 200))
        max_users = max(0, max_users)
        registered_users = self.store.count_users(role="user")
        remaining_slots = max(0, max_users - registered_users)
        return {
            "enabled": enabled,
            "max_users": max_users,
            "registered_users": registered_users,
            "remaining_slots": remaining_slots,
            "limit_reached": remaining_slots <= 0,
        }

    def _read_json(self, handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
        length = int(handler.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = handler.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _current_user(self, handler: BaseHTTPRequestHandler) -> Optional[Dict[str, Any]]:
        session_id = self._session_id(handler)
        return self.auth_service.authenticate(session_id) if session_id else None

    def _sanitize_notification_payload(self, user: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        return sanitize_notification_data(str(user.get("role", "user")), payload)

    @staticmethod
    def _session_id(handler: BaseHTTPRequestHandler) -> Optional[str]:
        header = handler.headers.get("Cookie", "")
        if not header:
            return None
        jar = cookies.SimpleCookie()
        jar.load(header)
        morsel = jar.get("session_id")
        return morsel.value if morsel else None

    def _write_static(self, handler: BaseHTTPRequestHandler, filename: str) -> None:
        path = self.static_dir / filename
        if not path.exists():
            self._write_json(handler, {"error": "Not found"}, status=404)
            return
        content_type = "text/html; charset=utf-8"
        if filename.endswith(".js"):
            content_type = "application/javascript; charset=utf-8"
        elif filename.endswith(".css"):
            content_type = "text/css; charset=utf-8"
        elif filename.endswith(".svg"):
            content_type = "image/svg+xml"
        body = path.read_bytes()
        handler.send_response(200)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _write_manual(self, handler: BaseHTTPRequestHandler) -> None:
        if not self.manual_path.exists():
            return self._write_json(handler, {"error": "Manual not found"}, status=404)
        markdown_text = self.manual_path.read_text(encoding="utf-8")
        toc_html, body_html = self._markdown_to_html(markdown_text)
        page = f"""<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>日内趋势雷达 - 使用说明</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f4ec;
      --panel: #fffdf8;
      --ink: #16202a;
      --muted: #617080;
      --line: rgba(22, 32, 42, 0.12);
      --accent: #cc5a2b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Noto Sans SC", sans-serif;
      color: var(--ink);
      scroll-behavior: smooth;
      background:
        radial-gradient(circle at top left, rgba(204, 90, 43, 0.14), transparent 28%),
        linear-gradient(135deg, #f7f4ec 0%, #efe5d6 100%);
    }}
    .manual-shell {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 48px;
      display: grid;
      grid-template-columns: 280px minmax(0, 1fr);
      gap: 20px;
    }}
    .manual-nav,
    .manual-card {{
      background: rgba(255, 251, 245, 0.9);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: 0 22px 55px rgba(48, 36, 27, 0.12);
    }}
    .manual-nav {{
      position: sticky;
      top: 20px;
      align-self: start;
      padding: 22px 18px;
      max-height: calc(100vh - 40px);
      overflow: hidden;
    }}
    .manual-card {{
      padding: 28px;
    }}
    .manual-nav h2 {{
      font-size: 20px;
      margin: 0 0 14px;
      padding: 0;
      border: 0;
    }}
    .manual-toc {{
      display: grid;
      gap: 8px;
      max-height: calc(100vh - 110px);
      overflow-y: auto;
      padding-right: 6px;
    }}
    .manual-toc::-webkit-scrollbar {{
      width: 8px;
    }}
    .manual-toc::-webkit-scrollbar-thumb {{
      background: rgba(22, 32, 42, 0.18);
      border-radius: 999px;
    }}
    .manual-toc::-webkit-scrollbar-track {{
      background: rgba(22, 32, 42, 0.04);
      border-radius: 999px;
    }}
    .manual-toc a {{
      color: var(--ink);
      text-decoration: none;
      line-height: 1.5;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(22, 32, 42, 0.03);
      border: 1px solid transparent;
      display: block;
    }}
    .manual-toc a:hover {{
      border-color: rgba(204, 90, 43, 0.18);
      background: rgba(204, 90, 43, 0.08);
    }}
    .manual-toc .toc-level-3 {{
      margin-left: 14px;
      font-size: 14px;
      color: var(--muted);
    }}
    .manual-toc .toc-level-4 {{
      margin-left: 28px;
      font-size: 13px;
      color: var(--muted);
      padding-top: 6px;
      padding-bottom: 6px;
    }}
    h1, h2, h3, h4 {{
      font-family: "Iowan Old Style", "Georgia", serif;
      margin-top: 0;
      scroll-margin-top: 20px;
    }}
    h1 {{ font-size: 34px; margin-bottom: 20px; }}
    h2 {{
      margin-top: 28px;
      margin-bottom: 14px;
      padding-bottom: 8px;
      border-bottom: 1px solid var(--line);
    }}
    h3 {{ margin-top: 24px; margin-bottom: 10px; color: var(--accent); }}
    h4 {{
      margin-top: 20px;
      margin-bottom: 8px;
      color: #7b341e;
      font-size: 18px;
    }}
    p {{ line-height: 1.75; margin: 10px 0; }}
    ul {{ margin: 10px 0 10px 20px; padding: 0; }}
    li {{ margin: 8px 0; line-height: 1.7; }}
    code {{
      font-family: "SFMono-Regular", "Menlo", monospace;
      background: #fff7ef;
      padding: 2px 6px;
      border-radius: 8px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin: 14px 0 18px;
      overflow: hidden;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: #fffdf8;
    }}
    th, td {{
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      line-height: 1.7;
    }}
    th {{
      background: rgba(204, 90, 43, 0.08);
      color: #7b341e;
      font-weight: 700;
    }}
    tr:last-child td {{
      border-bottom: 0;
    }}
    .manual-meta {{
      color: var(--muted);
      margin-bottom: 18px;
    }}
    @media (max-width: 960px) {{
      .manual-shell {{
        grid-template-columns: 1fr;
      }}
      .manual-nav {{
        position: static;
      }}
    }}
  </style>
</head>
<body>
  <main class="manual-shell">
    <aside class="manual-nav">
      <h2>目录</h2>
      <nav class="manual-toc">
        {toc_html}
      </nav>
    </aside>
    <article class="manual-card">
      <div class="manual-meta">日内趋势雷达文档</div>
      {body_html}
    </article>
  </main>
</body>
</html>""".encode("utf-8")
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Content-Length", str(len(page)))
        handler.end_headers()
        handler.wfile.write(page)

    @staticmethod
    def _markdown_to_html(markdown_text: str) -> tuple[str, str]:
        parts = []
        toc_parts = []
        in_list = False
        slug_counts: Dict[str, int] = {}
        lines = markdown_text.splitlines()
        index = 0
        while index < len(lines):
            line = lines[index].rstrip()
            stripped = line.strip()
            if not stripped:
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                index += 1
                continue
            if stripped.startswith("# "):
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                heading = stripped[2:]
                anchor = LocalWebApp._anchor_id(heading, slug_counts)
                parts.append(f'<h1 id="{anchor}">{html.escape(heading)}</h1>')
                index += 1
                continue
            if stripped.startswith("## "):
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                heading = stripped[3:]
                anchor = LocalWebApp._anchor_id(heading, slug_counts)
                toc_parts.append(f'<a class="toc-level-2" href="#{anchor}">{html.escape(heading)}</a>')
                parts.append(f'<h2 id="{anchor}">{html.escape(heading)}</h2>')
                index += 1
                continue
            if stripped.startswith("### "):
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                heading = stripped[4:]
                anchor = LocalWebApp._anchor_id(heading, slug_counts)
                toc_parts.append(f'<a class="toc-level-3" href="#{anchor}">{html.escape(heading)}</a>')
                parts.append(f'<h3 id="{anchor}">{html.escape(heading)}</h3>')
                index += 1
                continue
            if stripped.startswith("#### "):
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                heading = stripped[5:]
                anchor = LocalWebApp._anchor_id(heading, slug_counts)
                toc_parts.append(f'<a class="toc-level-4" href="#{anchor}">{html.escape(heading)}</a>')
                parts.append(f'<h4 id="{anchor}">{LocalWebApp._inline_markdown(heading)}</h4>')
                index += 1
                continue
            if (
                stripped.startswith("|")
                and index + 1 < len(lines)
                and LocalWebApp._is_table_separator(lines[index + 1].strip())
            ):
                if in_list:
                    parts.append("</ul>")
                    in_list = False
                headers = LocalWebApp._split_table_row(stripped)
                rows = []
                index += 2
                while index < len(lines):
                    row_line = lines[index].strip()
                    if not row_line.startswith("|") or LocalWebApp._is_table_separator(row_line):
                        break
                    rows.append(LocalWebApp._split_table_row(row_line))
                    index += 1
                parts.append(LocalWebApp._render_table(headers, rows))
                continue
            if stripped.startswith("- "):
                if not in_list:
                    parts.append("<ul>")
                    in_list = True
                parts.append(f"<li>{LocalWebApp._inline_markdown(stripped[2:])}</li>")
                index += 1
                continue
            if in_list:
                parts.append("</ul>")
                in_list = False
            parts.append(f"<p>{LocalWebApp._inline_markdown(stripped)}</p>")
            index += 1
        if in_list:
            parts.append("</ul>")
        return "\n".join(toc_parts), "\n".join(parts)

    @staticmethod
    def _inline_markdown(text: str) -> str:
        escaped = html.escape(text)
        escaped = re.sub(r"`([^`]+)`", r"<code>\1</code>", escaped)
        escaped = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", escaped)
        return escaped

    @staticmethod
    def _anchor_id(text: str, slug_counts: Dict[str, int]) -> str:
        slug = re.sub(r"[^\w\u4e00-\u9fff]+", "-", text.strip().lower()).strip("-")
        slug = slug or "section"
        count = slug_counts.get(slug, 0)
        slug_counts[slug] = count + 1
        return slug if count == 0 else f"{slug}-{count + 1}"

    @staticmethod
    def _is_table_separator(text: str) -> bool:
        if not text.startswith("|"):
            return False
        cells = [cell.strip() for cell in text.strip().strip("|").split("|")]
        if not cells:
            return False
        return all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells)

    @staticmethod
    def _split_table_row(text: str) -> list[str]:
        return [cell.strip() for cell in text.strip().strip("|").split("|")]

    @staticmethod
    def _render_table(headers: list[str], rows: list[list[str]]) -> str:
        thead = "".join(f"<th>{LocalWebApp._inline_markdown(cell)}</th>" for cell in headers)
        body_rows = []
        for row in rows:
            padded = row + [""] * max(0, len(headers) - len(row))
            body_rows.append(
                "<tr>" + "".join(f"<td>{LocalWebApp._inline_markdown(cell)}</td>" for cell in padded[: len(headers)]) + "</tr>"
            )
        tbody = "".join(body_rows)
        return f"<table><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"

    def _write_json(self, handler: BaseHTTPRequestHandler, data: Any, status: int = 200) -> None:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _set_session_cookie(self, handler: BaseHTTPRequestHandler, session_id: str) -> None:
        handler.send_response(200)
        handler.send_header("Set-Cookie", f"session_id={session_id}; HttpOnly; Path=/; SameSite=Lax")
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        body = json.dumps({"ok": True}).encode("utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    def _clear_session_cookie(self, handler: BaseHTTPRequestHandler) -> None:
        handler.send_response(200)
        handler.send_header("Set-Cookie", "session_id=; HttpOnly; Path=/; Max-Age=0; SameSite=Lax")
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        body = json.dumps({"ok": True}).encode("utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
