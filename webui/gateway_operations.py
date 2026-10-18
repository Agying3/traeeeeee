"""Agent2API gateway operations facade."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

try:
    from fastapi import APIRouter, HTTPException
except ImportError:
    APIRouter = None
    HTTPException = RuntimeError


class GatewayError(RuntimeError):
    def __init__(self, status: int, message: str):
        self.status = status
        super().__init__(message)


def _redact(value: Any) -> Any:
    secret_names = ("token", "password", "secret", "private", "credential", "authorization", "cookie", "apikey", "api_key", "accesskey", "access_key")
    if isinstance(value, Mapping):
        return {str(k): ("[redacted]" if any(x in str(k).lower() for x in secret_names) else _redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


class GatewayClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: float = 15):
        self.base_url = base_url.rstrip("/")
        self.username, self.password, self.timeout = username, password, timeout
        self._token = ""
        self._lock = threading.Lock()

    def request(self, method: str, path: str, body: Any = None, *, auth: bool = True, timeout: float | None = None) -> tuple[int, dict]:
        headers = {"content-type": "application/json", "accept": "application/json"}
        if auth:
            if not self._token:
                self.login()
            headers["authorization"] = "Bearer " + self._token
        req = urllib.request.Request(self.base_url + path, method=method.upper(), headers=headers,
                                     data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(req, timeout=timeout or self.timeout) as response:
                raw = response.read().decode("utf-8", "replace")
                return response.status, json.loads(raw or "{}")
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8", "replace")
            try:
                detail = json.loads(raw or "{}")
            except ValueError:
                detail = {"error": raw[:500]}
            if error.code == 401 and auth:
                self._token = ""
            raise GatewayError(error.code, json.dumps(_redact(detail), ensure_ascii=False)[:800]) from error
        except (OSError, ValueError) as error:
            raise GatewayError(503, str(error)) from error

    def login(self) -> str:
        if not self.username or not self.password:
            raise GatewayError(503, "请配置 A2A_ADMIN_USER 和 A2A_ADMIN_PASS 后连接外部网关")
        status, result = self.request("POST", "/api/panel/login", {"username": self.username, "password": self.password}, auth=False)
        token = str((result.get("data") or {}).get("accessToken") or "")
        if status not in (200, 201) or not token:
            raise GatewayError(status, "gateway login failed")
        self._token = token
        return token

    def health(self) -> dict:
        _, result = self.request("GET", "/health", auth=False, timeout=5)
        return {"ok": True, "data": _redact(result)}

    @staticmethod
    def _data(result: Mapping[str, Any]) -> Mapping[str, Any]:
        return result.get("data") if isinstance(result.get("data"), Mapping) else result

    def accounts(self) -> list[dict]:
        _, result = self.request("GET", "/api/accounts")
        data = self._data(result)
        return [_redact(item) for item in data.get("accounts", [])] if isinstance(data.get("accounts", []), list) else []

    def create_account(self, credential: Mapping[str, Any]) -> dict:
        _, result = self.request("POST", "/api/accounts", dict(credential))
        return _redact(dict(self._data(result).get("account") or self._data(result)))

    def account(self, account_id: str) -> dict:
        for item in self.accounts():
            if str(item.get("id") or item.get("uid")) == str(account_id):
                return _redact(item)
        raise GatewayError(404, "account not found")

    def update_account(self, account_id: str, body: Mapping[str, Any]) -> dict:
        payload = dict(body)
        _, result = self.request("PATCH", "/api/accounts/" + urllib.parse.quote(str(account_id), safe=""), payload)
        return _redact(dict(self._data(result).get("account") or self._data(result)))

    def delete_account(self, account_id: str) -> dict:
        _, result = self.request("DELETE", "/api/accounts/" + urllib.parse.quote(str(account_id), safe=""))
        return _redact(result)

    def usage(self, account_id: str | None = None) -> dict:
        path = "/api/accounts/usage" + (("?id=" + urllib.parse.quote(account_id, safe="")) if account_id else "")
        _, result = self.request("GET", path)
        return _redact(result)

    def refresh_expiring(self) -> dict:
        _, result = self.request("POST", "/api/accounts/refresh-expiring")
        return _redact(result)

    def checkin(self) -> dict:
        _, result = self.request("POST", "/api/accounts/checkin", {})
        return _redact(result)

    def proxies(self) -> dict:
        _, result = self.request("GET", "/api/proxies/pool")
        data = self._data(result)
        return {"proxies": _redact(data.get("items", []))}

    def proxy_pool(self) -> dict:
        _, result = self.request("GET", "/api/proxies/pool")
        return _redact(result)

    def create_proxy(self, url: str, name: str = "") -> dict:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in ("http", "https", "socks5", "socks5h") or not parsed.hostname or not parsed.port:
            raise GatewayError(400, "请输入含端口的 HTTP 或 SOCKS5 代理 URL")
        payload = {"name": name, "protocol": "socks5" if parsed.scheme == "socks5h" else parsed.scheme,
                   "host": parsed.hostname, "port": parsed.port,
                   "username": urllib.parse.unquote(parsed.username or ""),
                   "password": urllib.parse.unquote(parsed.password or "")}
        _, result = self.request("POST", "/api/proxies/pool", payload)
        return _redact(result)

    def test_proxy(self, proxy_id: str) -> dict:
        _, result = self.request("POST", "/api/proxies/pool/test", {"id": str(proxy_id)})
        return _redact(result)

    def delete_proxy(self, proxy_id: str) -> dict:
        _, result = self.request("POST", "/api/proxies/pool/remove", {"id": str(proxy_id)})
        return _redact(result)

    def explicit_json(self, method: str, path: str, body: Any = None) -> dict:
        _, result = self.request(method, path, body)
        return _redact(result)


class GatewayOperations:
    def __init__(self, data_dir: str | Path, credentials_supplier: Callable[[], Iterable[Any]] | None = None,
                 base_url: str | None = None, admin_user: str | None = None, admin_password: str | None = None,
                 config: Mapping[str, Any] | None = None, client: GatewayClient | None = None):
        self.data_dir = Path(data_dir); self.data_dir.mkdir(parents=True, exist_ok=True)
        self.credentials_supplier = credentials_supplier or (lambda: ())
        persisted = self._read_config()
        cfg = {**persisted, **dict(config or {})}
        self.config = {"enable_auto_import": bool(cfg.get("enable_auto_import", False)), "maintenance_enabled": bool(cfg.get("maintenance_enabled", False)),
                       "maintenance_interval": max(1, int(cfg.get("maintenance_interval", 3600))), "max_retries": max(1, int(cfg.get("max_retries", 5))), **cfg}
        url = base_url or self.config.get("gateway_base_url") or os.environ.get("A2A_BASE_URL", "http://127.0.0.1:3065")
        user = admin_user or self.config.get("admin_user") or os.environ.get("A2A_ADMIN_USER", "admin")
        password = admin_password or self.config.get("admin_password") or os.environ.get("A2A_ADMIN_PASS", "")
        self.client = client or GatewayClient(url, user, password)
        self.outbox_path = self.data_dir / "gateway-import-outbox.json"
        self.config_path = self.data_dir / "gateway-operations-config.json"
        self._task = self._maintenance_task = None; self._stop = asyncio.Event(); self._lock = asyncio.Lock()

    def _read_config(self) -> dict:
        try:
            value = json.loads((self.data_dir / "gateway-operations-config.json").read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError): return {}

    def save_config(self, config: Mapping[str, Any]) -> dict:
        cfg = dict(config)
        cfg["maintenance_interval"] = max(1, int(cfg.get("maintenance_interval", self.config.get("maintenance_interval", 3600))))
        cfg["max_retries"] = max(1, int(cfg.get("max_retries", self.config.get("max_retries", 5))))
        cfg["enable_auto_import"] = bool(cfg.get("enable_auto_import", self.config.get("enable_auto_import", True)))
        cfg["maintenance_enabled"] = bool(cfg.get("maintenance_enabled", self.config.get("maintenance_enabled", False)))
        self.config = {**self.config, **cfg}; self.config_path.write_text(json.dumps(self.config, ensure_ascii=False, indent=2), encoding="utf-8"); return self.config

    def _load(self) -> dict:
        try: return json.loads(self.outbox_path.read_text(encoding="utf-8"))
        except (OSError, ValueError): return {"items": {}}

    def _save(self, data: dict) -> None:
        tmp = self.outbox_path.with_suffix(".tmp"); tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"); tmp.replace(self.outbox_path)

    @staticmethod
    def _credential(value: Any) -> tuple[dict, str]:
        if isinstance(value, (str, Path)):
            raw = Path(value).read_bytes(); return json.loads(raw.decode()), hashlib.sha256(raw).hexdigest()
        item = dict(value); raw = json.dumps(item, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode(); return item, hashlib.sha256(raw).hexdigest()

    @staticmethod
    def _identity(credential: Mapping[str, Any]) -> str:
        account = credential.get("account") or {}; return str(account.get("uid") or credential.get("uid") or credential.get("phone") or hashlib.sha256(json.dumps(credential, sort_keys=True).encode()).hexdigest())

    async def import_once(self) -> dict:
        async with self._lock:
            state = self._load(); items = state.setdefault("items", {}); now = time.time(); scanned = imported = failed = 0
            existing = {str(a.get("id")): a for a in await asyncio.to_thread(self.client.accounts)}
            for source in self.credentials_supplier() or ():
                try: credential, digest = self._credential(source); identity = self._identity(credential); scanned += 1
                except Exception: failed += 1; continue
                identity_key = hashlib.sha256(identity.encode()).hexdigest()
                entry = items.setdefault(identity_key, items.pop(identity, {"attempts": 0, "status": "pending"}))
                if entry.get("digest") != digest:
                    entry.update({"attempts": 0, "status": "pending"}); entry.pop("next_at", None); entry.pop("error", None)
                if entry.get("status") == "success": continue
                if entry.get("status") == "failed" and entry.get("attempts", 0) >= self.config["max_retries"]: continue
                if entry.get("next_at", 0) > now: continue
                try:
                    match = next((a for a in existing.values() if str(a.get("uid")) == identity), None)
                    account = match or await asyncio.to_thread(self.client.create_account, credential)
                    entry.update({"status": "success", "digest": digest, "attempts": 0, "account_id": str(account.get("id", "")), "updated_at": now}); imported += 1
                except Exception as error:
                    attempts = int(entry.get("attempts", 0)) + 1; entry.update({"status": "failed", "digest": digest, "attempts": attempts, "next_at": now + min(3600, 2 ** min(attempts, 10)), "error": type(error).__name__}); failed += 1
            self._save(state); return {"scanned": scanned, "imported": imported, "failed": failed, "pending": sum(v.get("status") != "success" for v in items.values())}

    def outbox_summary(self) -> dict:
        items = self._load().get("items", {}); return {"total": len(items), "success": sum(v.get("status") == "success" for v in items.values()), "failed": sum(v.get("status") == "failed" for v in items.values()), "pending": sum(v.get("status") == "pending" for v in items.values())}

    async def maintenance_once(self) -> dict:
        results = {}
        for name, method in (("refresh", "refresh_expiring"), ("usage", "usage"), ("checkin", "checkin")):
            try:
                call = getattr(self.client, method)
                results[name] = await asyncio.to_thread(call)
            except Exception as error: results[name] = {"ok": False, "error": type(error).__name__}
        return results

    def proxy_request(self, method: str, path: str, body: Any = None) -> dict:
        return self.client.explicit_json(method, path, body)

    def request_logs(self, path: str) -> dict:
        return self.proxy_request("GET", path)

    def request_stats(self, path: str) -> dict:
        return self.proxy_request("GET", path)

    async def _loop(self) -> None:
        while not self._stop.is_set():
            if self.config["enable_auto_import"]:
                try: await self.import_once()
                except Exception: pass
            try: await asyncio.wait_for(self._stop.wait(), timeout=10)
            except asyncio.TimeoutError: pass

    async def _maintenance_loop(self) -> None:
        while not self._stop.is_set():
            if self.config["maintenance_enabled"]:
                try: await self.maintenance_once()
                except Exception: pass
            try: await asyncio.wait_for(self._stop.wait(), timeout=self.config["maintenance_interval"])
            except asyncio.TimeoutError: pass

    def start(self) -> asyncio.Task:
        if self._task is None or self._task.done(): self._stop.clear(); self._task = asyncio.create_task(self._loop()); self._maintenance_task = asyncio.create_task(self._maintenance_loop())
        return self._task

    async def stop(self) -> None:
        self._stop.set()
        for task in (self._task, self._maintenance_task):
            if task: await task
        self._task = self._maintenance_task = None

    def router(self, prefix: str = "/api/gateway-operations"):
        if APIRouter is None: raise RuntimeError("FastAPI is required for router")
        router = APIRouter(prefix=prefix)

        async def offline_safe(call, fallback: dict) -> dict:
            # {意图：网关离线时仪表盘仍要能渲染，读接口降级为 200 + offline 标记，而不是让每 4 秒的轮询刷 5xx。}
            try:
                return await asyncio.to_thread(call)
            except GatewayError as error:
                payload = dict(fallback)
                payload.update(offline=True, gateway_status=error.status)
                return payload

        @router.get("/state")
        async def state():
            try: health = await asyncio.to_thread(self.client.health)
            except GatewayError as error: health = {"ok": False, "error": "网关离线", "gateway_status": error.status}
            return {"health": health, "config": _redact(self.config), "maintenance": {"enabled": self.config["maintenance_enabled"], "interval": self.config["maintenance_interval"]}, "outbox": self.outbox_summary()}
        @router.get("/accounts")
        async def accounts():
            try:
                return {"accounts": await asyncio.to_thread(self.client.accounts)}
            except GatewayError as error:
                return {"accounts": [], "offline": True, "gateway_status": error.status}
        @router.get("/accounts/{account_id}")
        async def account(account_id: str): return await offline_safe(lambda: {"account": self.client.account(account_id)}, {"account": None})
        @router.put("/accounts/{account_id}")
        async def update_account(account_id: str, body: dict): return await asyncio.to_thread(self.client.update_account, account_id, body)
        @router.delete("/accounts/{account_id}")
        async def delete_account(account_id: str): return await asyncio.to_thread(self.client.delete_account, account_id)
        @router.post("/accounts/{account_id}/usage")
        async def usage(account_id: str): return await asyncio.to_thread(self.client.usage, account_id)
        @router.get("/proxies")
        async def proxies(): return await offline_safe(lambda: {"proxies": self.client.proxies().get("proxies", [])}, {"proxies": []})
        @router.post("/proxies")
        async def create_proxy(body: dict): return await asyncio.to_thread(self.client.create_proxy, str(body.get("url", "")), str(body.get("name", "")))
        @router.post("/proxies/{proxy_id}/test")
        async def test_proxy(proxy_id: str): return await asyncio.to_thread(self.client.test_proxy, proxy_id)
        @router.delete("/proxies/{proxy_id}")
        async def delete_proxy(proxy_id: str): return await asyncio.to_thread(self.client.delete_proxy, proxy_id)
        @router.get("/config")
        async def get_config(): return _redact(self.config)
        @router.put("/config")
        async def put_config(body: dict): return self.save_config(body)
        @router.post("/maintenance")
        async def maintenance(): return await self.maintenance_once()
        @router.post("/import")
        async def import_credentials(): return await self.import_once()
        @router.post("/retry/{identity}")
        async def retry(identity: str):
            state = self._load(); entry = state.get("items", {}).get(identity)
            if entry: entry.update({"status": "pending", "attempts": 0}); entry.pop("next_at", None); self._save(state)
            return {"ok": bool(entry)}
        @router.get("/outbox")
        async def outbox(): return self.outbox_summary()
        return router

    def account_status(self) -> dict:
        return {"accounts": len(self.client.accounts()), "outbox": self.outbox_summary()}
