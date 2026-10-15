# -*- coding: utf-8 -*-
"""Durable local ledger for Haozhu registration attempts.

The ledger is deliberately local: it records operational evidence and resource
ownership, while never asserting that a provider-side charge or release has
been settled.  Raw response evidence is kept separately with restrictive file
permissions; the SQLite event view contains only redacted summaries.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
import tempfile
import time
from pathlib import Path
from contextlib import contextmanager
from typing import Any


_SECRET_KEY = re.compile(
    r"(?:pass(?:word)?|token|secret|authorization|cookie|set-cookie|"
    r"verification[_ -]?code|sms|raw(?:[_ -]?response)?|credential|access|refresh)",
    re.I,
)
_CODE_KEY = re.compile(r"(?:^|[_ -])(code|otp|yzm)(?:$|[_ -])", re.I)
_METADATA = {"code_source", "credential_saved", "sms_sent", "upstream_code", "status_code", "error_code",
             "receipt_hash", "evidence_available", "code_received", "num_raw"}


def _private_key(key):
    return key not in _METADATA and bool(_SECRET_KEY.search(key) or _CODE_KEY.search(key))


def _now() -> float:
    return time.time()


def account_fingerprint(account: str) -> str:
    """Stable non-reversible account identity used to scope recovery."""
    return hashlib.sha256(str(account or "").encode("utf-8")).hexdigest()[:24]


def receipt_hash(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def identity_fingerprint(attempt_id: str, run_id: str, sid: str, uid: str, account: str) -> str:
    value = "\0".join(str(part or "") for part in (attempt_id, run_id, sid, uid, account))
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return repr(value)


def _redact(value: Any, key: str = "") -> Any:
    if _private_key(key):
        return "[redacted]"
    if isinstance(value, dict):
        return {str(k): _redact(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(v, key) for v in value]
    if isinstance(value, str):
        # Do not preserve URL query credentials in network exception text.
        value = re.sub(r"(?i)([?&](?:token|password|pass|authorization|cookie|code)=[^&\s]+)",
                       lambda m: m.group(1).split("=", 1)[0] + "=[redacted]", value)
        value = re.sub(r"(?i)(https?://)[^\s/@:]+:[^\s/@]*@", r"\1[redacted]@", value)
        value = re.sub(r"(?i)((?:password|pass|token|cookie|authorization|verification_code)\s*[:=]\s*)[^&\s,;]+",
                       r"\1[redacted]", value)
        return value[:1000]
    return value


def _row(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


class Ledger:
    """SQLite-backed attempt registry and append-only evidence stream."""

    def __init__(self, path: str | os.PathLike | None = None):
        if path is None:
            path = Path(os.environ.get("TRAE_DATA_DIR", str(Path(__file__).resolve().parents[1] / "data"))) / "state" / "haozhu-ledger.sqlite3"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_dir = self.path.parent / "haozhu-evidence"
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
            os.chmod(self.evidence_dir, 0o700)
        except OSError:
            pass
        self._init()

    @contextmanager
    def _connect(self):
        conn = sqlite3.connect(str(self.path), timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        try:
            yield conn
        finally:
            if conn.in_transaction:
                conn.rollback()
            conn.close()

    def _init(self) -> None:
        with self._connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL DEFAULT '',
                    phone TEXT NOT NULL DEFAULT '',
                    sid TEXT NOT NULL DEFAULT '',
                    uid TEXT NOT NULL DEFAULT '',
                    account TEXT NOT NULL DEFAULT '',
                    account_fingerprint TEXT NOT NULL DEFAULT '',
                    identity_fingerprint TEXT NOT NULL DEFAULT '',
                    proxy_json TEXT NOT NULL DEFAULT '',
                    state TEXT NOT NULL DEFAULT 'created',
                    held INTEGER NOT NULL DEFAULT 0,
                    code_source TEXT NOT NULL DEFAULT '',
                    credential_saved INTEGER NOT NULL DEFAULT 0,
                    sms_sent INTEGER NOT NULL DEFAULT 0,
                    error_category TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT '',
                    owner_pid INTEGER NOT NULL DEFAULT 0,
                    original_attempt_id TEXT NOT NULL DEFAULT '',
                    created REAL NOT NULL,
                    updated REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS attempts_held_idx ON attempts(held, updated);
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempt_id TEXT NOT NULL DEFAULT '',
                    event TEXT NOT NULL,
                    ts REAL NOT NULL,
                    summary_json TEXT NOT NULL,
                    evidence_path TEXT NOT NULL DEFAULT '',
                    receipt_hash TEXT NOT NULL DEFAULT ''
                );
                CREATE UNIQUE INDEX IF NOT EXISTS events_receipt_idx ON events(attempt_id,event,receipt_hash)
                    WHERE receipt_hash<>'';
                CREATE INDEX IF NOT EXISTS events_attempt_idx ON events(attempt_id, id DESC);
                CREATE TABLE IF NOT EXISTS token_leases (
                    name TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    expires REAL NOT NULL
                );
                """
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(attempts)")}
            if "identity_fingerprint" not in columns:
                try:
                    db.execute("ALTER TABLE attempts ADD COLUMN identity_fingerprint TEXT NOT NULL DEFAULT ''")
                except sqlite3.OperationalError:
                    if "identity_fingerprint" not in {row[1] for row in db.execute("PRAGMA table_info(attempts)")}:
                        raise

    def _write_evidence(self, attempt_id: str, event: str, raw: Any) -> str:
        stamp = f"{int(_now() * 1000)}-{os.getpid()}-{secrets.token_hex(5)}"
        path = self.evidence_dir / f"{stamp}-{re.sub(r'[^A-Za-z0-9_.-]', '_', event)[:30]}.json"
        payload = {"attempt_id": attempt_id, "event": event, "ts": _now(), "raw": _json_safe(raw)}
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as out:
                json.dump(payload, out, ensure_ascii=False, separators=(",", ":"))
                out.flush()
                os.fsync(out.fileno())
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            path.unlink(missing_ok=True)
            raise
        return str(path)

    def begin(self, attempt_id: str, run_id: str, sid: str, uid: str = "", account: str = "",
              phone: str = "", proxy: Any = None, **fields) -> dict:
        if not attempt_id:
            raise ValueError("attempt_id is required")
        now = _now()
        account = str(account or "")
        values = {
            "run_id": str(run_id or ""), "phone": str(phone or ""), "sid": str(sid or ""),
            "uid": str(uid or ""), "account": account,
            "account_fingerprint": account_fingerprint(account),
            "proxy_json": json.dumps(_redact(proxy), ensure_ascii=False) if proxy is not None else "",
            "state": str(fields.get("state") or ("held" if phone else "created")),
            "held": int(fields.get("held", bool(phone))),
            "owner_pid": int(fields.get("owner_pid", os.getpid())),
            "original_attempt_id": str(fields.get("original_attempt_id") or ""),
            "created": now, "updated": now,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if existing:
                for key in ("run_id", "account", "sid", "uid", "phone"):
                    if values[key] and existing[key] and values[key] != existing[key]:
                        raise ValueError(f"attempt {key} identity mismatch")
                # {意图：重复初始化保留原占用；所有权变更只走显式的原子转移。}

            else:
                db.execute(
                    """INSERT INTO attempts(attempt_id,run_id,phone,sid,uid,account,account_fingerprint,
                    identity_fingerprint,proxy_json,state,held,owner_pid,original_attempt_id,created,updated)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (attempt_id, values["run_id"], values["phone"], values["sid"], values["uid"],
                     values["account"], values["account_fingerprint"],
                     identity_fingerprint(attempt_id, values["run_id"], values["sid"], values["uid"], values["account"]),
                     values["proxy_json"], values["state"], values["held"], values["owner_pid"],
                     values["original_attempt_id"], now, now),
                )
            db.execute("COMMIT")
        return self.get_attempt(attempt_id) or {}

    @staticmethod
    def resource_lease(record):
        identity = "\0".join(str(record.get(k) or "") for k in
                              ("account_fingerprint", "sid", "phone"))
        return "finish-" + account_fingerprint(identity)

    def transfer(self, previous: dict, attempt_id: str, run_id: str, owner_pid: int) -> dict:
        now = _now()
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT * FROM attempts WHERE attempt_id=?",
                             (previous["attempt_id"],)).fetchone()
            new = db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if not old or not old["held"] or any(old[k] != previous[k] for k in
                    ("run_id", "phone", "sid", "uid", "account_fingerprint", "owner_pid", "updated")):
                raise ValueError("phone ownership changed; reload the original attempt")
            if not new or new["held"] or new["phone"] or new["run_id"] != run_id or new["account"] != old["account"]:
                raise ValueError("destination attempt identity mismatch")
            identity = identity_fingerprint(attempt_id, run_id, old["sid"], old["uid"], old["account"])
            db.execute("""UPDATE attempts SET phone=?,sid=?,uid=?,identity_fingerprint=?,state='held',held=1,
                       owner_pid=?,original_attempt_id=?,code_source=?,sms_sent=?,updated=? WHERE attempt_id=?""",
                       (old["phone"], old["sid"], old["uid"], identity, owner_pid, old["attempt_id"],
                        old["code_source"], old["sms_sent"], now, attempt_id))
            db.execute("UPDATE attempts SET held=0,state='adopted',updated=? WHERE attempt_id=?",
                       (now, old["attempt_id"]))
            for aid, event, summary in (
                (old["attempt_id"], "ownership_transferred", {"successor_attempt_id": attempt_id}),
                (attempt_id, "phone_adopted", {"original_attempt_id": old["attempt_id"],
                                              "phone": old["phone"], "sid": old["sid"], "uid": old["uid"]})):
                summary.update(event=event, ts=now)
                db.execute("INSERT INTO events(attempt_id,event,ts,summary_json) VALUES(?,?,?,?)",
                           (aid, event, now, json.dumps(summary, ensure_ascii=False)))
            db.execute("COMMIT")
        return self.get_attempt(attempt_id)

    def record(self, event: str, attempt_id: str = "", **fields) -> dict:
        event = str(event or "event")
        raw = {}
        summary = {}
        for key, value in fields.items():
            if _private_key(key):
                raw[key] = value
                summary[key] = "[redacted]"
            else:
                summary[key] = _redact(_json_safe(value), key)
        # Explicit raw payloads are evidence only, never API-visible event data.
        for key in list(summary):
            if key.startswith("raw_"):
                raw[key] = fields.get(key)
                summary[key] = "[evidence]"
        receipt = str(fields.get("receipt_hash") or "")
        if not receipt and any(k in fields for k in ("sms", "raw_sms", "receipt")):
            receipt = receipt_hash(fields.get("sms") or fields.get("raw_sms") or fields.get("receipt"))
        if receipt and event == "code_received":
            receipt = receipt_hash(str(fields.get("code_source") or "unknown") + "\0" + receipt)
        if receipt:
            with self._connect() as db:
                old = db.execute("SELECT * FROM events WHERE attempt_id=? AND event=? AND receipt_hash=?",
                                 (attempt_id, event, receipt)).fetchone()
            if old:
                return _row(old) or {}
        evidence_path = self._write_evidence(attempt_id, event, raw) if raw else ""
        summary["event"] = event
        summary["ts"] = _now()
        if evidence_path:
            summary["evidence_available"] = True
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            if receipt:
                old = db.execute("SELECT * FROM events WHERE attempt_id=? AND event=? AND receipt_hash=?",
                                 (attempt_id, event, receipt)).fetchone()
                if old:
                    db.execute("COMMIT")
                    return _row(old) or {}
            db.execute("INSERT INTO events(attempt_id,event,ts,summary_json,evidence_path,receipt_hash) VALUES(?,?,?,?,?,?)",
                       (attempt_id, event, summary["ts"], json.dumps(summary, ensure_ascii=False), evidence_path, receipt))
            attempt = db.execute("SELECT 1 FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            if attempt:
                updates = {k: _redact(fields[k], k) for k in (
                    "phone", "sid", "uid", "account", "state", "code_source", "error_category",
                    "last_error", "original_attempt_id") if k in fields}
                if any(k in updates for k in ("sid", "uid", "account")):
                    current = dict(db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone())
                    current.update(updates)
                    updates["account_fingerprint"] = account_fingerprint(current["account"])
                    updates["identity_fingerprint"] = identity_fingerprint(
                        attempt_id, current["run_id"], current["sid"], current["uid"], current["account"])
                if "held" in fields:
                    updates["held"] = int(bool(fields["held"]))
                if "credential_saved" in fields:
                    updates["credential_saved"] = int(bool(fields["credential_saved"]))
                if "sms_sent" in fields:
                    updates["sms_sent"] = int(bool(fields["sms_sent"]))
                if updates:
                    updates["updated"] = _now()
                    sql = "UPDATE attempts SET " + ",".join(f"{k}=?" for k in updates) + " WHERE attempt_id=?"
                    db.execute(sql, (*updates.values(), attempt_id))
            db.execute("COMMIT")
        return summary

    def _public_attempt(self, row):
        record = _row(row) or {}
        if record:
            record.pop("proxy_json", None)
        return record or None

    def get_attempt(self, attempt_id: str) -> dict | None:
        with self._connect() as db:
            return self._public_attempt(db.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone())

    def held(self) -> list[dict]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM attempts WHERE held=1 ORDER BY updated").fetchall()
            return [self._public_attempt(row) for row in rows]

    def receipt_evidence(self, attempt_id: str) -> dict:
        with self._connect() as db:
            rows = db.execute("SELECT event,summary_json FROM events WHERE attempt_id=? "
                              "AND event IN ('code_received','budget_reconciled')", (attempt_id,)).fetchall()
        flags = {"platform": False, "manual": False, "operator": False}
        for event, summary in rows:
            data = json.loads(summary)
            if event == "code_received" and data.get("code_source") in ("platform", "manual"):
                flags[data["code_source"]] = True
            flags["operator"] |= event == "budget_reconciled" and data.get("received") is True
        return flags

    def events(self, limit: int = 100, attempt_id: str = "") -> list[dict]:
        limit = max(1, min(int(limit), 1000))
        query = "SELECT id,attempt_id,event,ts,summary_json,evidence_path,receipt_hash FROM events"
        params: list[Any] = []
        if attempt_id:
            query += " WHERE attempt_id=?"
            params.append(attempt_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        out = []
        with self._connect() as db:
            for row in db.execute(query, params):
                item = dict(row)
                try:
                    item["summary"] = json.loads(item.pop("summary_json"))
                except (TypeError, ValueError):
                    item["summary"] = {}
                # Evidence path is metadata only; callers never receive its contents.
                item["evidence"] = bool(item.pop("evidence_path", ""))
                out.append(item)
        return out

    def overview(self) -> dict:
        with self._connect() as db:
            counts = {str(r[0]): int(r[1]) for r in db.execute("SELECT event,COUNT(*) FROM events GROUP BY event")}
            receipts = {str(r[0] or "unknown"): int(r[1]) for r in db.execute(
                "SELECT json_extract(summary_json,'$.code_source'),COUNT(*) FROM events "
                "WHERE event='code_received' GROUP BY json_extract(summary_json,'$.code_source')")}
            held_count = int(db.execute("SELECT COUNT(*) FROM attempts WHERE held=1").fetchone()[0])
            snaps = []
            for r in db.execute("SELECT summary_json FROM events WHERE event='summary' ORDER BY id DESC LIMIT 20"):
                try:
                    data = json.loads(r[0])
                    snaps.append({k: data[k] for k in ("money", "num", "num_raw", "num_verified") if k in data})
                except (TypeError, ValueError):
                    pass
            acquired = int(db.execute("SELECT COUNT(DISTINCT attempt_id) FROM events WHERE event='phone_acquired'").fetchone()[0])
        return {
            "evidence_counts": counts,
            "receipt_counts": receipts,
            "local_held": held_count,
            "balance_snapshots": snaps,
            "acquisition_count": acquired,
            "billing_proven": False,
        }

    def acquire_lease(self, name: str, owner: str, ttl: float = 30) -> bool:
        now, expires = _now(), _now() + float(ttl)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT owner,expires FROM token_leases WHERE name=?", (name,)).fetchone()
            if row and float(row["expires"]) > now and row["owner"] != owner:
                db.execute("COMMIT")
                return False
            db.execute("INSERT INTO token_leases(name,owner,expires) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,expires=excluded.expires",
                       (name, owner, expires))
            db.execute("COMMIT")
        return True

    def release_lease(self, name: str, owner: str) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM token_leases WHERE name=? AND owner=?", (name, owner))

    def save_token(self, name: str, token: str) -> None:
        path = self.path.parent / f"{name}.json"
        fd, temp = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
        try:
            os.chmod(temp, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as out:
                json.dump({"token": str(token), "updated": _now()}, out)
                out.flush(); os.fsync(out.fileno())
            os.replace(temp, path)
        finally:
            Path(temp).unlink(missing_ok=True)

    def load_token(self, name: str) -> str:
        path = self.path.parent / f"{name}.json"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return str(data.get("token") or "")
        except (OSError, ValueError, TypeError):
            return ""


__all__ = ["Ledger", "account_fingerprint", "identity_fingerprint", "receipt_hash"]
