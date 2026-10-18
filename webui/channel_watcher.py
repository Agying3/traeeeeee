"""Async-compatible Haozhu channel event watcher.

The watcher is deliberately callback based: integrations provide catalog fetch,
dispatch, task lookup, persistence and engine state.  No provider or engine import
is required, which keeps tests offline and avoids changing global registration
configuration.
"""
from __future__ import annotations

import asyncio
import copy
import inspect
import math
import re
import threading
import time
import uuid
from collections import defaultdict


class ChannelConfigError(ValueError):
    pass


class ChannelWatcher:
    """Watch one or more projects and dispatch newly actionable channels."""

    VERSION = 1

    def __init__(self, fetch, dispatch, task_getter=None, *, state=None,
                 persist=None, registration_enabled=None, engine_halted=None,
                 now=None):
        self.fetch = fetch
        self.dispatch = dispatch
        self.task_getter = task_getter or (lambda _task_id: None)
        self.persist = persist or (lambda _state: None)
        self.registration_enabled = registration_enabled or (lambda: True)
        self.engine_halted = engine_halted or (lambda: False)
        self.now = now or time.time
        self._lock = threading.RLock()
        self._tick_lock = asyncio.Lock()
        self.state = self._normalise_state(state or {})

    @staticmethod
    def _normalise_state(state):
        result = copy.deepcopy(state)
        result.setdefault("version", ChannelWatcher.VERSION)
        result.setdefault("projects", {})
        result.setdefault("snapshots", {})
        result.setdefault("pending", [])
        result.setdefault("tasks", {})
        result.setdefault("events", [])
        result.setdefault("stats", {"ticks": 0, "fetch_errors": 0, "dispatch_errors": 0,
                                     "events": 0, "dispatched": 0, "completed": 0,
                                     "success": 0, "cost": 0.0})
        result.setdefault("cooldown", {})
        result.setdefault("known_uids", {})
        return result

    @staticmethod
    def validate_config(config):
        if not isinstance(config, dict):
            raise ChannelConfigError("config must be an object")
        projects = config.get("projects", [])
        if not isinstance(projects, list):
            raise ChannelConfigError("projects must be a list")
        seen = set()
        clean = []
        for raw in projects:
            if not isinstance(raw, dict):
                raise ChannelConfigError("project must be an object")
            sid = str(raw.get("sid", "")).strip()
            hex_sid = str(raw.get("hex_sid", "")).strip()
            name = str(raw.get("name", "")).strip()
            if not re.fullmatch(r"[0-9]+", sid):
                raise ChannelConfigError("sid must be numeric")
            if not hex_sid or len(hex_sid) > 128 or not re.fullmatch(r"[A-Za-z0-9_-]+", hex_sid):
                raise ChannelConfigError("hex_sid is invalid")
            if not name or len(name) > 200:
                raise ChannelConfigError("name is required")
            try:
                max_price = float(raw.get("max_price"))
                min_stock = int(raw.get("min_stock"))
            except (TypeError, ValueError):
                raise ChannelConfigError("max_price and min_stock must be numeric")
            if not math.isfinite(max_price) or max_price <= 0 or min_stock < 1:
                raise ChannelConfigError("max_price must be positive and min_stock must be at least one")
            key = sid
            if key in seen:
                raise ChannelConfigError("duplicate sid")
            seen.add(key)
            clean.append({"sid": sid, "hex_sid": hex_sid, "name": name,
                          "max_price": max_price, "min_stock": min_stock,
                          "enabled": bool(raw.get("enabled", True))})
        interval = float(config.get("interval", 30))
        per_trigger = int(config.get("per_trigger", 1))
        workers = int(config.get("workers", 1))
        if (not math.isfinite(interval) or interval < 1 or interval > 3600
                or per_trigger < 1 or per_trigger > 10 or workers < 1 or workers > 5):
            raise ChannelConfigError("interval must be 1..3600, per_trigger 1..10 and workers 1..5")
        cooldown = float(config.get("cooldown", config.get("cooldown_seconds", 60)))
        if not math.isfinite(cooldown) or cooldown < 0:
            raise ChannelConfigError("cooldown must be non-negative")
        return {"projects": clean, "interval": interval, "per_trigger": per_trigger,
                "workers": workers, "cooldown": cooldown}

    def configure(self, config):
        clean = self.validate_config(config)
        with self._lock:
            self.state["projects"] = {p["sid"]: copy.deepcopy(p) for p in clean["projects"]}
            self.state["settings"] = {k: clean[k] for k in ("interval", "per_trigger", "workers", "cooldown")}
            self._save()
        return copy.deepcopy(clean)

    def _save(self):
        self.persist(copy.deepcopy(self.state))

    async def _call(self, fn, *args):
        value = fn(*args)
        return await value if inspect.isawaitable(value) else value

    def _paused(self, project):
        return (not project.get("enabled", True) or not bool(self.registration_enabled())
                or bool(self.engine_halted()))

    @staticmethod
    def _price(row):
        value = row.get("price", row.get("sms_unit_price"))
        try:
            value = float(value)
            return value if math.isfinite(value) else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _stock(row):
        try:
            return int(row.get("stock", -1))
        except (TypeError, ValueError):
            return -1

    def _event(self, project, row, previous, kind):
        event_id = uuid.uuid4().hex
        candidate = copy.deepcopy(row)
        candidate.update(sid=project["sid"], project_name=project["name"],
                        hex_sid=project["hex_sid"], event=kind, watch_event_id=event_id)
        event = {"id": event_id, "type": kind, "sid": project["sid"],
                 "uid": str(row.get("uid", "")), "at": self.now(),
                 "price": self._price(row), "stock": self._stock(row),
                 "previous": copy.deepcopy(previous) if previous else None,
                 "status": "pending"}
        return event, candidate

    def _detect(self, project, rows):
        sid = project["sid"]
        old = self.state["snapshots"].get(sid, {})
        known = self.state["known_uids"].setdefault(sid, [])
        active_event_keys = {(e.get("sid"), e.get("uid"), e.get("type"))
                             for e in self.state.get("events", []) if e.get("status") in ("pending", "dispatched", "dispatch_error")}
        active_event_keys.update((e.get("event", {}).get("sid"), e.get("event", {}).get("uid"), e.get("event", {}).get("type"))
                                 for e in self.state.get("pending", []))
        result = []
        current = {}
        for row in rows or []:
            if not isinstance(row, dict) or not row.get("uid"):
                continue
            uid = str(row["uid"])
            current[uid] = copy.deepcopy(row)
            before = old.get(uid)
            was_known = uid in known
            if not was_known:
                known.append(uid)
            price, stock = self._price(row), self._stock(row)
            kinds = []
            if before is None:
                kinds.append("relisted" if was_known else "new_uid")
            else:
                if (stock > self._stock(before) and stock >= project["min_stock"]
                        and price is not None and price <= project["max_price"]):
                    kinds.append("restock")
                if ((self._price(before) is None or self._price(before) > project["max_price"])
                        and price is not None and price <= project["max_price"]):
                    kinds.append("price_threshold")
            for kind in dict.fromkeys(kinds):
                if (sid, uid, kind) in active_event_keys:
                    continue
                if stock < project["min_stock"] and kind in ("new_uid", "relisted"):
                    continue
                if price is None or price > project["max_price"]:
                    continue
                result.append(self._event(project, row, before, kind))
        self.state["snapshots"][sid] = current
        return result

    def _event_ref(self, event_id):
        for event in self.state.get("events", []):
            if event.get("id") == event_id:
                return event
        return None

    def _update_event(self, event_id, **changes):
        event = self._event_ref(event_id)
        if event is not None:
            event.update(changes)
        for entry in self.state.get("pending", []):
            if entry.get("event", {}).get("id") == event_id:
                entry["event"] = copy.deepcopy(event or entry["event"])

    def _valid_pending(self, entry):
        event = entry.get("event", {})
        project = self.state.get("projects", {}).get(str(event.get("sid")))
        candidate = entry.get("candidate", {})
        price = self._price(candidate)
        return (project and project.get("enabled", True) and price is not None
                and price <= float(project.get("max_price", 0))
                and self._stock(candidate) >= int(project.get("min_stock", 1)))

    @staticmethod
    def _cost_value(status):
        value = status.get("received_cost", status.get("cost", status.get("budget_spent", 0)))
        try:
            return max(0.0, float(value or 0))
        except (TypeError, ValueError):
            return 0.0

    async def _reconcile_tasks(self):
        for task_id, info in list(self.state["tasks"].items()):
            try:
                status = await self._call(self.task_getter, task_id)
            except Exception:
                continue
            if not isinstance(status, dict):
                continue
            mapped = {"done": "success", "success": "success", "failed": "failed"}.get(status.get("status"))
            current_cost = self._cost_value(status)
            previous_cost = float(info.get("accounted_cost", 0) or 0)
            if current_cost > previous_cost:
                self.state["stats"]["cost"] += current_cost - previous_cost
                info["accounted_cost"] = current_cost
            uncertain = status.get("uncertain_cost", status.get("uncertain", info.get("uncertain_cost", 0)))
            try:
                info["uncertain_cost"] = float(uncertain or 0)
            except (TypeError, ValueError):
                info["uncertain_cost"] = 0.0
            event = self._event_ref(info.get("event_id"))
            if mapped and info.get("status") not in ("success", "failed"):
                info.update(status=mapped, completed_at=self.now())
                self.state["stats"]["completed"] += 1
                if mapped == "success":
                    self.state["stats"]["success"] += 1
            elif mapped:
                info["status"] = mapped
            if event is not None:
                task_ids = [str(t) for t, v in self.state["tasks"].items()
                            if v.get("event_id") == info.get("event_id")]
                done = [self.state["tasks"][t] for t in task_ids]
                event.update(task_count=len(task_ids), cost=sum(float(x.get("accounted_cost", 0) or 0) for x in done),
                             uncertain_cost=sum(float(x.get("uncertain_cost", 0) or 0) for x in done))
                if done and all(x.get("status") in ("success", "failed") for x in done):
                    event["status"] = "success" if all(x.get("status") == "success" for x in done) else "failed"
                    event["success"] = sum(x.get("status") == "success" for x in done)
                    event["duration"] = max(0, self.now() - float(event.get("at", self.now())))
                    if event["success"] == 0:
                        key = "%s:%s" % (event.get("sid"), event.get("uid"))
                        self.state["cooldown"][key] = self.now() + float(self.state.get("settings", {}).get("cooldown", 60))

    async def tick(self, config=None):
        async with self._tick_lock:
            if config is not None:
                self.configure(config)
            with self._lock:
                projects = copy.deepcopy(self.state.get("projects", {}))
                settings = copy.deepcopy(self.state.get("settings", {"per_trigger": 1, "workers": 1}))
            active_sids = set(projects)
            for bucket in ("snapshots", "known_uids", "cooldown"):
                self.state[bucket] = {k: v for k, v in self.state.get(bucket, {}).items()
                                      if str(k).split(":", 1)[0] in active_sids}
            self.state["pending"] = [e for e in self.state.get("pending", [])
                                     if str(e.get("event", {}).get("sid")) in active_sids]
            await self._reconcile_tasks()
            self.state["stats"]["ticks"] += 1
            generated = []
            for project in projects.values():
                if self._paused(project):
                    continue
                blocked_until = 0
                if blocked_until > self.now():
                    continue
                try:
                    rows = await self._call(self.fetch, copy.deepcopy(project))
                    generated.extend(self._detect(project, rows))
                except Exception as error:
                    self.state["stats"]["fetch_errors"] += 1
                    self.state["events"].append({"type": "fetch_error", "sid": project["sid"],
                                                  "error": type(error).__name__, "at": self.now()})
            self.state["events"].extend(event for event, _ in generated)
            self.state["stats"]["events"] += len(generated)
            self.state["pending"].extend({"event": event, "candidate": candidate} for event, candidate in generated)
            self.state["pending"] = [entry for entry in self.state["pending"] if self._valid_pending(entry)]
            await self._dispatch_pending(settings)
            self.state["events"] = self.state["events"][-1000:]
            self._save()
            return copy.deepcopy(self.state)

    async def _dispatch_pending(self, settings):
        limit = min(3, max(1, int(settings.get("per_trigger", 1))))
        chosen = self.state["pending"][:limit]
        sem = asyncio.Semaphore(max(1, int(settings.get("workers", 1))))
        async def one(entry):
            event, candidate = entry["event"], entry["candidate"]
            project = self.state["projects"].get(event["sid"], {})
            if self._paused(project):
                return entry, None
            if self.state["cooldown"].get("%s:%s" % (event["sid"], event["uid"]), 0) > self.now():
                return entry, None
            options = {"sid": event["sid"], "uid": event["uid"],
                       "sms_unit_price": self._price(candidate),
                       "source": "channel_watcher", "watch_event_id": event["id"]}
            dispatch_config = copy.deepcopy(project)
            dispatch_config["run_options"] = options
            candidate_for_dispatch = copy.deepcopy(candidate)
            # {意图：快照字段同时落在候选任务和配置，兼容直接入队与引擎适配器。}
            candidate_for_dispatch["run_options"] = copy.deepcopy(options)
            try:
                async with sem:
                    ids = await self._call(self.dispatch, candidate_for_dispatch, dispatch_config)
                ids = [ids] if isinstance(ids, str) else list(ids or [])
                for task_id in ids:
                    self.state["tasks"][str(task_id)] = {"event_id": event["id"], "status": "dispatched",
                                                          "sid": event["sid"], "uid": event["uid"], "at": self.now()}
                event = self._event_ref(event["id"]) or event
                event.update(status="dispatched", task_count=len(ids), cost=0.0, uncertain_cost=0.0,
                             success=0, duration=0.0)
                self.state["stats"]["dispatched"] += len(ids)
                return entry, ids
            except Exception as error:
                self.state["stats"]["dispatch_errors"] += 1
                event = self._event_ref(event["id"]) or event
                event.update(status="dispatch_error", error=type(error).__name__)
                return entry, None
        results = await asyncio.gather(*(one(entry) for entry in chosen))
        dispatched = {id(entry) for entry, ids in results if ids is not None}
        self.state["pending"] = [entry for entry in self.state["pending"] if id(entry) not in dispatched]

    async def run(self, config=None, stop=None):
        settings = self.validate_config(config) if config is not None else self.state.get("settings", {})
        while not (stop and stop()):
            await self.tick(config if config is not None else None)
            await asyncio.sleep(float(settings.get("interval", 30)))

    def snapshot(self):
        with self._lock:
            return copy.deepcopy(self.state)
