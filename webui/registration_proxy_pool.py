"""Persistent registration proxy selection, cooldown, and health state."""
from __future__ import annotations

import concurrent.futures
import hashlib
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class RegistrationProxyPool:
    def __init__(self, proxy_file: Path, state_file: Path, cooldown_seconds: int = 1800):
        self.proxy_file = Path(proxy_file)
        self.state_file = Path(state_file)
        self.cooldown_seconds = max(1, int(cooldown_seconds))
        self._lock = threading.RLock()
        self._state = self._load_state()

    @staticmethod
    def read_lines(path: Path) -> list[str]:
        try:
            return [line.strip() for line in path.read_text(encoding="utf-8-sig").splitlines()
                    if line.strip() and not line.lstrip().startswith("#")]
        except OSError:
            return []

    @staticmethod
    def _key(line: str) -> str:
        return hashlib.sha256(line.encode("utf-8")).hexdigest()

    def _load_state(self) -> dict:
        try:
            value = json.loads(self.state_file.read_text(encoding="utf-8"))
            if isinstance(value, dict) and isinstance(value.get("entries", {}), dict):
                value.setdefault("cursor", 0)
                value.setdefault("entries", {})
                return value
        except (OSError, ValueError):
            pass
        return {"cursor": 0, "entries": {}}

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_file.with_suffix(self.state_file.suffix + ".tmp")
        temporary.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(self.state_file)

    def lines(self) -> list[str]:
        return self.read_lines(self.proxy_file)

    def select(self, busy=()) -> int | None:
        """Select the next non-busy proxy whose cooldown has elapsed."""
        with self._lock:
            lines = self.lines()
            if not lines:
                return None
            busy_ids = {int(i) for i in busy if str(i).isdigit()}
            now = time.time()
            start = int(self._state.get("cursor", 0)) % len(lines)
            for step in range(len(lines)):
                idx = (start + step) % len(lines)
                if idx in busy_ids:
                    continue
                entry = self._state["entries"].setdefault(self._key(lines[idx]), {})
                used = float(entry.get("last_used", 0) or 0)
                if now - used < self.cooldown_seconds:
                    continue
                self._state["cursor"] = (idx + 1) % len(lines)
                self._save()
                return idx
            return None

    def mark_used(self, index: int) -> None:
        with self._lock:
            lines = self.lines()
            if not 0 <= int(index) < len(lines):
                return
            entry = self._state["entries"].setdefault(self._key(lines[int(index)]), {})
            entry["last_used"] = time.time()
            entry["uses"] = int(entry.get("uses", 0)) + 1
            self._save()

    def record_result(self, index: int, success: bool) -> None:
        with self._lock:
            lines = self.lines()
            if not 0 <= int(index) < len(lines):
                return
            entry = self._state["entries"].setdefault(self._key(lines[int(index)]), {})
            field = "successes" if success else "failures"
            entry[field] = int(entry.get(field, 0)) + 1
            entry["last_result"] = "ok" if success else "fail"
            entry["last_result_at"] = time.time()
            self._save()

    @staticmethod
    def _proxy_url(line: str) -> str:
        if "://" in line:
            return line
        parts = line.split(":", 3)
        if len(parts) != 4:
            raise ValueError("格式应为 host:port:user:pass 或带协议的代理 URL")
        host, port, user, password = parts
        return "http://{}:{}@{}:{}".format(
            urllib.parse.quote(user, safe=""), urllib.parse.quote(password, safe=""), host, port)

    @classmethod
    def _probe(cls, index: int, line: str, timeout: float) -> dict:
        try:
            proxy_url = cls._proxy_url(line)
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({
                "http": proxy_url, "https": proxy_url}))
            request = urllib.request.Request("https://api.ipify.org?format=json",
                                             headers={"User-Agent": "TraeProxyHealth/1.0"})
            started = time.monotonic()
            with opener.open(request, timeout=timeout) as response:
                body = response.read(512).decode("utf-8", "replace")
                if response.status != 200 or not body:
                    raise OSError("探测端点响应异常")
            return {"idx": index, "ok": True, "latency_ms": round((time.monotonic() - started) * 1000)}
        except Exception as error:
            return {"idx": index, "ok": False, "error": str(error)[:180]}

    def check_all(self, timeout: float = 8.0) -> list[dict]:
        lines = self.lines()
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            results = list(executor.map(lambda pair: self._probe(*pair, timeout), enumerate(lines)))
        with self._lock:
            now = time.time()
            for result, line in zip(results, lines):
                entry = self._state["entries"].setdefault(self._key(line), {})
                entry.update(healthy=result["ok"], last_check=now)
                if result["ok"]:
                    entry["latency_ms"] = result["latency_ms"]
                    entry.pop("last_error", None)
                else:
                    entry["last_error"] = result["error"]
                    entry["last_used"] = max(float(entry.get("last_used", 0) or 0), now)
            self._save()
        return results

    def snapshot(self, busy=()) -> dict:
        with self._lock:
            lines = self.lines()
            busy_ids = {int(i) for i in busy if str(i).isdigit()}
            now = time.time()
            items = []
            for idx, line in enumerate(lines):
                entry = self._state["entries"].get(self._key(line), {})
                used = float(entry.get("last_used", 0) or 0)
                remaining = max(0, int(self.cooldown_seconds - (now - used))) if used else 0
                parts = line.split(":", 3) if "://" not in line else []
                host = (parts[0] + ":" + parts[1]) if len(parts) == 4 else urllib.parse.urlsplit(line).netloc
                items.append({"idx": idx, "host": host, "healthy": entry.get("healthy"),
                              "last_check": entry.get("last_check"), "last_error": entry.get("last_error", ""),
                              "latency_ms": entry.get("latency_ms"), "uses": entry.get("uses", 0),
                              "successes": entry.get("successes", 0), "failures": entry.get("failures", 0),
                              "cooling": remaining > 0, "ready_in_sec": remaining,
                              "busy": idx in busy_ids})
            return {"items": items, "total": len(items),
                    "available": sum(1 for x in items if not x["cooling"] and x["healthy"] is not False and not x["busy"]),
                    "cooling": sum(1 for x in items if x["cooling"]),
                    "busy": sum(1 for x in items if x["busy"]),
                    "cooldown_seconds": self.cooldown_seconds}
