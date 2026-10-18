"""Reusable OpenAI-compatible upstream router with persistent keys and request metadata."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import secrets
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse


class UpstreamService:
    """Gateway relay; integration owns app mounting and configuration callbacks."""

    def __init__(self, gateway_base: str, config_getter: Callable[[], dict] | None = None,
                 master_key_getter: Callable[[], str] | None = None,
                 stats_callback: Callable[..., Any] | None = None,
                 db_path: str | Path = "upstream.sqlite3", client: httpx.AsyncClient | None = None):
        self.gateway_base = gateway_base.rstrip("/")
        self.config_getter = config_getter or (lambda: {})
        self.master_key_getter = master_key_getter or (lambda: "")
        self.stats_callback = stats_callback
        self.db_path = str(db_path)
        self._owns_client = client is None
        # The local gateway must bypass inherited and Windows system proxies.
        self._client = client or httpx.AsyncClient(timeout=self._timeout(False), trust_env=False)
        self._db_lock = asyncio.Lock()
        # Empty intersections are intentionally deny-all: a missing policy must never widen access.
        self._init_db()
        self.router = APIRouter()
        self._routes()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    def get_router(self) -> APIRouter:
        return self.router

    @__import__('contextlib').contextmanager
    def _connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self):
        with self._connect() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS upstream_keys (
              id TEXT PRIMARY KEY, key_hash TEXT UNIQUE NOT NULL, label TEXT NOT NULL DEFAULT '',
              group_id TEXT, models_json TEXT NOT NULL DEFAULT '[]', expires_at REAL,
              disabled INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS upstream_groups (
              id TEXT PRIMARY KEY, name TEXT UNIQUE NOT NULL, models_json TEXT NOT NULL DEFAULT '[]',
              metadata_json TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS upstream_requests (
              id TEXT PRIMARY KEY, endpoint TEXT NOT NULL DEFAULT '', model TEXT, key_id TEXT,
              status INTEGER, stream INTEGER, usage_json TEXT, ttfb_ms REAL, latency_ms REAL,
              error TEXT, created_at REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_upstream_requests_created ON upstream_requests(created_at);
            """)
            columns = {r[1] for r in c.execute("PRAGMA table_info(upstream_requests)")}
            if "endpoint" not in columns:
                c.execute("ALTER TABLE upstream_requests ADD COLUMN endpoint TEXT NOT NULL DEFAULT ''")

    def _config(self) -> dict:
        value = self.config_getter() or {}
        return value if isinstance(value, dict) else {}

    def _master(self, request: Request) -> bool:
        supplied = request.headers.get("x-master-key", "")
        if not supplied:
            auth = request.headers.get("authorization", "")
            supplied = auth[7:] if auth.lower().startswith("bearer ") else ""
        # The injected/environment master callback has precedence; config cannot bypass it.
        expected = str(self.master_key_getter() or "")
        return bool(expected and hmac.compare_digest(supplied, expected))

    def _key(self, request: Request):
        auth = request.headers.get("authorization", "")
        raw = auth[7:] if auth.lower().startswith("bearer ") else request.headers.get("x-api-key", "")
        if not raw:
            raise HTTPException(401, "missing API key")
        digest = hashlib.sha256(raw.encode()).hexdigest()
        with self._connect() as c:
            row = c.execute("SELECT * FROM upstream_keys WHERE key_hash=?", (digest,)).fetchone()
        if row:
            if row["disabled"] or (row["expires_at"] is not None and row["expires_at"] <= time.time()):
                raise HTTPException(401, "API key expired or disabled")
            return row
        expected = str(self.master_key_getter() or "")
        env_key = str(self._config().get("upstream_api_key", "") or "")
        if expected and hmac.compare_digest(raw, expected):
            return {"id": "master", "group_id": None, "models_json": "[]"}
        if not expected and env_key and hmac.compare_digest(raw, env_key):
            return {"id": "master", "group_id": None, "models_json": "[]"}
        raise HTTPException(401, "invalid API key")

    def _allowed(self, row, model: str):
        if row["id"] == "master":
            return bool(model)
        try:
            own = set(json.loads(row["models_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            return False
        allowed = own if own else None
        gid = row["group_id"]
        if gid:
            with self._connect() as c:
                g = c.execute("SELECT models_json FROM upstream_groups WHERE id=?", (gid,)).fetchone()
            if not g:
                return False
            try:
                group_models = set(json.loads(g[0] or "[]"))
                if group_models:
                    allowed = group_models if allowed is None else allowed & group_models
            except (TypeError, ValueError, json.JSONDecodeError):
                return False
        return allowed is None or model in allowed

    def _valid_models(self, value):
        if not isinstance(value, list) or any(not isinstance(x, str) or not x for x in value):
            raise HTTPException(422, "models must be an array of non-empty strings")
        return value

    def _valid_expiry(self, value):
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise HTTPException(422, "expires_at must be finite")
        return float(value)

    def _check_group(self, group_id):
        if group_id is None:
            return
        with self._connect() as c:
            if not c.execute("SELECT 1 FROM upstream_groups WHERE id=?", (group_id,)).fetchone():
                raise HTTPException(422, "group does not exist")

    def _sanitize_error(self, value) -> str:
        text = str(value or "")
        return "upstream error" if text else ""

    async def _record(self, values):
        async with self._db_lock:
            with self._connect() as c:
                c.execute("INSERT OR REPLACE INTO upstream_requests (id, endpoint, model, key_id, status, stream, usage_json, ttfb_ms, latency_ms, error, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)", values)
                cfg = self._config()
                days = max(1, int(cfg.get("upstream_log_retention_days", 30)))
                seconds = max(1, float(cfg.get("upstream_log_retention_seconds", days * 86400)))
                c.execute("DELETE FROM upstream_requests WHERE created_at < ?", (time.time() - seconds,))
                cap = max(1, int(cfg.get("upstream_log_retention_rows", 10000)))
                c.execute("DELETE FROM upstream_requests WHERE id IN (SELECT id FROM upstream_requests ORDER BY created_at DESC LIMIT -1 OFFSET ?)", (cap,))
        if self.stats_callback:
            try:
                result = self.stats_callback(values)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass

    def _routes(self):
        @self.router.get("/v1/models")
        async def models(request: Request):
            row = self._key(request)
            response = await self._json("GET", "/v1/models", None, request, row["id"], False)
            if isinstance(response, JSONResponse):
                try:
                    data = json.loads(response.body)
                    data["data"] = [x for x in data.get("data", []) if self._allowed(row, str(x.get("id", "")))]
                    return JSONResponse(data, status_code=response.status_code)
                except (TypeError, ValueError, json.JSONDecodeError):
                    return response
            return response

        @self.router.post("/v1/chat/completions")
        async def chat(request: Request):
            return await self._completion(request, "/v1/chat/completions")

        @self.router.post("/v1/responses")
        async def responses(request: Request):
            return await self._completion(request, "/v1/responses")

        @self.router.post("/api/upstream/keys")
        async def create_key(request: Request):
            self._require_master(request); body = await request.json()
            models = self._valid_models(body.get("models", [])); self._check_group(body.get("group_id"))
            expiry = self._valid_expiry(body.get("expires_at"))
            raw, kid = "sk-" + secrets.token_urlsafe(32), uuid.uuid4().hex
            with self._connect() as c:
                c.execute("INSERT INTO upstream_keys VALUES (?,?,?,?,?,?,?,?)", (kid, hashlib.sha256(raw.encode()).hexdigest(), str(body.get("label", "")), body.get("group_id"), json.dumps(models), expiry, 0, time.time()))
            return {"id": kid, "api_key": raw, "shown_once": True}

        @self.router.get("/api/upstream/keys")
        async def list_keys(request: Request):
            self._require_master(request)
            with self._connect() as c: rows = c.execute("SELECT id,label,group_id,models_json,expires_at,disabled,created_at FROM upstream_keys").fetchall()
            return {"data": [dict(r, models=json.loads(dict(r).pop("models_json"))) for r in rows]}

        @self.router.patch("/api/upstream/keys/{key_id}")
        async def update_key(key_id: str, request: Request):
            self._require_master(request); body = await request.json()
            with self._connect() as c:
                if not c.execute("SELECT 1 FROM upstream_keys WHERE id=?", (key_id,)).fetchone(): raise HTTPException(404, "key not found")
            fields = {k: body[k] for k in ("group_id", "disabled") if k in body}
            if "models" in body: fields["models_json"] = json.dumps(self._valid_models(body["models"]))
            if "expires_at" in body: fields["expires_at"] = self._valid_expiry(body["expires_at"])
            self._check_group(fields.get("group_id"))
            if fields:
                with self._connect() as c: c.execute("UPDATE upstream_keys SET " + ",".join(f"{k}=?" for k in fields) + " WHERE id=?", (*fields.values(), key_id))
            return {"ok": True}

        @self.router.get("/api/upstream/groups")
        async def groups(request: Request):
            self._require_master(request)
            with self._connect() as c: rows = c.execute("SELECT * FROM upstream_groups").fetchall()
            return {"data": [dict(r, models=json.loads(dict(r).pop("models_json")), metadata=json.loads(dict(r).pop("metadata_json"))) for r in rows]}

        @self.router.post("/api/upstream/groups")
        async def create_group(request: Request):
            self._require_master(request); b = await request.json(); models = self._valid_models(b.get("models", [])); gid = b.get("id") or uuid.uuid4().hex
            with self._connect() as c: c.execute("INSERT INTO upstream_groups VALUES (?,?,?,?,?)", (gid, b.get("name", gid), json.dumps(models), json.dumps(b.get("metadata", {})), time.time()))
            return {"id": gid}

        @self.router.get("/api/upstream/requests")
        async def request_logs(request: Request):
            self._require_master(request); limit = min(int(request.query_params.get("limit", 100)), 1000)
            with self._connect() as c: rows = c.execute("SELECT * FROM upstream_requests ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return {"data": [dict(r) for r in rows]}

    def _require_master(self, request):
        if not self._master(request): raise HTTPException(401, "master key required")

    async def _completion(self, request, path):
        row = self._key(request); body = await request.json()
        if not isinstance(body, dict): raise HTTPException(400, "JSON object required")
        model = body.get("model")
        if not isinstance(model, str) or not model: raise HTTPException(422, "model required")
        if not self._allowed(row, model): raise HTTPException(403, "model is not allowed")
        return await self._json("POST", path, body, request, row["id"], bool(body.get("stream")))

    async def _json(self, method, path, body, request, key_id, stream):
        started = time.perf_counter(); rid = uuid.uuid4().hex; model = body.get("model", "") if body else ""
        try:
            async with self._client_ctx() as client:
                if stream:
                    req = client.build_request(method, self.gateway_base + path, json=body, timeout=self._timeout(True))
                    response = await client.send(req, stream=True)
                    if response.status_code >= 400:
                        raw = await response.aread(); await response.aclose()
                        try: detail = json.loads(raw)
                        except Exception: detail = {"error": {"message": "upstream request failed"}}
                        await self._record((rid, path, model, key_id, response.status_code, 1, None, None, (time.perf_counter()-started)*1000, "upstream error", time.time()))
                        return JSONResponse(detail, status_code=response.status_code)
                    return StreamingResponse(self._stream(response, rid, path, body, key_id, started), media_type="text/event-stream", headers={"cache-control": "no-cache"})
                response = await client.request(method, self.gateway_base + path, json=body, timeout=self._timeout(False))
                if response.status_code >= 400:
                    await self._record((rid, path, model, key_id, response.status_code, 0, None, None, (time.perf_counter()-started)*1000, "upstream error", time.time()))
                    return JSONResponse({"error": {"message": "upstream request failed"}}, status_code=response.status_code)
                data = response.json(); usage = data.get("usage")
                await self._record((rid, path, model, key_id, response.status_code, 0, json.dumps(usage) if usage else None, (time.perf_counter()-started)*1000, (time.perf_counter()-started)*1000, "", time.time()))
                return JSONResponse(data, status_code=response.status_code)
        except asyncio.CancelledError:
            await self._record((rid, path, model, key_id, 499, int(stream), None, None, (time.perf_counter()-started)*1000, "cancelled", time.time()))
            raise
        except httpx.HTTPError:
            await self._record((rid, path, model, key_id, 503, int(stream), None, None, (time.perf_counter()-started)*1000, "upstream transport error", time.time()))
            raise HTTPException(503, "upstream unavailable")

    def _timeout(self, stream):
        cfg = self._config(); total = float(cfg.get("upstream_total_timeout", 120)); idle = float(cfg.get("upstream_idle_timeout", 30))
        return httpx.Timeout(total, read=idle if stream else total)

    @asynccontextmanager
    async def _client_ctx(self):
        yield self._client

    async def _stream(self, response, rid, endpoint, body, key_id, started) -> AsyncIterator[bytes]:
        first = None; usage = None; status = response.status_code; buf = b""; failed = False
        total = float(self._config().get("upstream_stream_total_timeout", 0))
        def error_frame(message):
            if endpoint == "/v1/responses":
                return ("event: response.failed\ndata: " + json.dumps({"type":"response.failed", "response":{"status":"failed", "error":{"code":"upstream_error", "message":message}}}) + "\n\n").encode()
            return ("data: " + json.dumps({"error":{"message":message}}) + "\n\ndata: [DONE]\n\n").encode()
        try:
            async with asyncio.timeout(total if total > 0 else None):
                chunks = response.aiter_bytes() if response.is_stream_consumed else response.aiter_raw()
                async for chunk in chunks:
                    if first is None: first = (time.perf_counter()-started)*1000
                    buf = (buf + chunk)[-65536:]
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        if not line.startswith(b"data:"): continue
                        payload = line[5:].strip()
                        if payload == b"[DONE]": continue
                        try:
                            item = json.loads(payload)
                            usage = item.get("usage") or (item.get("response", {}) or {}).get("usage") or usage
                            if item.get("type") == "response.failed" or item.get("error"):
                                failed = True
                        except Exception: pass
                    if failed:
                        status = 502
                        yield error_frame("upstream stream failed")
                        break
                    else: yield chunk
        except asyncio.CancelledError:
            status = 499
            raise
        except (asyncio.TimeoutError, httpx.HTTPError):
            status = 504
            failed = True
            yield error_frame("upstream stream interrupted")
        finally:
            await response.aclose()
            await self._record((rid, endpoint, body.get("model", ""), key_id, status, 1, json.dumps(usage) if usage else None, first, (time.perf_counter()-started)*1000, "cancelled" if status == 499 else ("" if not failed else "stream failed"), time.time()))
