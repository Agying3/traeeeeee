"""Passive page diagnostics with bounded waits and durable failure evidence."""

import asyncio
import inspect
import json
import time
import traceback
from datetime import datetime
from pathlib import Path


PAGE_STATE_JS = """() => {
    const rect = el => {
        const r = el.getBoundingClientRect();
        return {x: r.x, y: r.y, width: r.width, height: r.height};
    };
    return {
        url: location.href,
        readyState: document.readyState,
        title: document.title,
        images: [...document.images].map(img => ({
            src: img.currentSrc || img.src,
            complete: img.complete,
            naturalWidth: img.naturalWidth,
            naturalHeight: img.naturalHeight,
            rect: rect(img)
        })),
        iframes: [...document.querySelectorAll('iframe')].map(el => ({
            src: el.src, rect: rect(el)
        }))
    };
}"""


class PageDiagnostics:
    def __init__(self, context, page, output_dir, *, keep_open=True, report=print,
                 watch_tokens=()):
        self.context = context
        self.page = page
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.events_path = self.output_dir / "events.jsonl"
        self.summary_path = self.output_dir / "summary.json"
        self.keep_open = keep_open
        self.report = report
        self.watch_tokens = tuple(watch_tokens)
        self.requests = []
        self.responses = []
        self.failures = []
        self.pending = set()
        self.sequence = 0
        self.finalized = False
        self.state = {"status": "started", "waits": {}, "snapshots": [],
                      "request_counts": {}}
        page.on("request", self._on_request)
        page.on("requestfailed", self._on_request_failed)
        page.on("response", self._on_response)
        page.on("pageerror", lambda error: self.event("page_error", error=str(error)))
        page.on("console", self._on_console)
        page.on("framenavigated", lambda frame: self.event("frame_navigated", url=frame.url))
        page.on("close", lambda: self.event("page_closed"))
        context.on("close", lambda: self.event("context_closed"))
        self.event("started")

    def _matches(self, url):
        return any(token in url for token in self.watch_tokens)

    def _storage_error(self, step, error):
        self.state.setdefault("storage_errors", []).append({"step": step, "error": repr(error)})
        self.report(f"[LOG ERROR] {step}: {error}")

    def _write_summary(self):
        try:
            temporary = self.summary_path.with_suffix(".json.tmp")
            temporary.write_text(json.dumps(self.state, ensure_ascii=False, indent=2),
                                 encoding="utf-8")
            temporary.replace(self.summary_path)
        except OSError as error:
            self._storage_error("summary", error)

    def event(self, kind, **details):
        self.sequence += 1
        row = {"sequence": self.sequence, "time": datetime.now().astimezone().isoformat(),
               "kind": kind, **details}
        try:
            with self.events_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError as error:
            self._storage_error("events", error)
        self.state["last_event"] = row
        self._write_summary()
        return row

    def _on_request(self, request):
        if request.method == "OPTIONS":
            return
        if self._matches(request.url):
            row = self.event("request_started", url=request.url, method=request.method)
            self.requests.append(row)
            for token in self.watch_tokens:
                if token in request.url:
                    counts = self.state["request_counts"]
                    counts[token] = counts.get(token, 0) + 1
            self._write_summary()

    def _on_request_failed(self, request):
        if request.resource_type == "script":
            self.failures.append(request.url)
        self.event("request_failed", url=request.url, method=request.method,
                   resource_type=request.resource_type, error=request.failure)

    def _on_response(self, response):
        if response.request.method == "OPTIONS":
            return
        if self._matches(response.url):
            self.responses.append(self.event("response_received", url=response.url,
                                             status=response.status))
        if response.status >= 400:
            self.event("http_error", url=response.url, status=response.status)

    def _on_console(self, message):
        if message.type == "error":
            self.event("console_error", text=message.text)

    def spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.pending.add(task)

        def finished(done):
            self.pending.discard(done)
            if done.cancelled():
                self.event("background_capture_cancelled")
            else:
                error = done.exception()
                if error:
                    self.event("background_capture_error", error=repr(error))

        task.add_done_callback(finished)
        return task

    async def drain(self, timeout_s=5):
        if not self.pending:
            return
        _, unfinished = await asyncio.wait(tuple(self.pending), timeout=timeout_s)
        if unfinished:
            self.event("background_capture_timeout", count=len(unfinished))
            for task in unfinished:
                task.cancel()
            await asyncio.gather(*unfinished, return_exceptions=True)

    def latest_request(self, token, since=0):
        return next((row for row in reversed(self.requests[since:]) if token in row["url"]), None)

    def latest_response(self, token, since=0):
        return next((row for row in reversed(self.responses[since:]) if token in row["url"]), None)

    async def interactive_ready(self):
        """页面真正可交互: readyState=complete 且主文档脚本无损坏。"""
        if self.page.is_closed():
            return False
        state = await self.page.evaluate(
            "() => ({ready: document.readyState, frames: [...document.querySelectorAll('iframe')].length})")
        if state.get("ready") != "complete":
            return False
        # readyState=complete 后仍可能有迟到的脚本损坏记录; 只认主导航前后加载的脚本
        return not self.failures

    async def stable_interactive(self, stable_s=3.0):
        """连续 stable_s 秒保持 interactive_ready, 避免刚 complete 又出损坏。"""
        started = time.monotonic()
        ok_since = None
        while time.monotonic() - started < 60:
            if await self.interactive_ready():
                ok_since = ok_since or time.monotonic()
                if time.monotonic() - ok_since >= stable_s:
                    return True
            else:
                ok_since = None
            await asyncio.sleep(0.5)
        return False

    async def wait_for(self, label, probe, *, timeout_s=120, interval_s=0.25):
        started = time.monotonic()
        deadline = started + timeout_s
        last_error = None
        self.state["status"] = "waiting:" + label
        self.event("wait_started", label=label, timeout_s=timeout_s)
        next_report = started + 15
        while not self.page.is_closed():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                result = probe()
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(result, timeout=min(10, remaining))
                if result:
                    elapsed = round(time.monotonic() - started, 3)
                    self.state["waits"][label] = {"status": "ready", "elapsed_s": elapsed}
                    self.event("wait_ready", label=label, elapsed_s=elapsed)
                    return result
            except Exception as error:
                text = repr(error)
                if text != last_error:
                    self.event("probe_error", label=label, error=text)
                    last_error = text
            now = time.monotonic()
            if now >= next_report:
                self.report(f"[WAIT] {label}: {now - started:.0f}/{timeout_s:g}s")
                next_report = now + 15
            await asyncio.sleep(min(interval_s, max(0, deadline - now)))
        elapsed = round(time.monotonic() - started, 3)
        self.state["waits"][label] = {"status": "timeout", "elapsed_s": elapsed,
                                      "last_error": last_error}
        self.event("wait_timeout", label=label, elapsed_s=elapsed, error=last_error)
        return None

    async def snapshot(self, label):
        ordinal = len(self.state["snapshots"]) + 1
        prefix = f"{ordinal:03d}-{label}"
        evidence = {"label": label, "url": self.page.url, "errors": [], "frames": []}
        screenshot_path = self.output_dir / (prefix + ".png")
        try:
            await self.page.screenshot(path=str(screenshot_path), timeout=10000)
            evidence["screenshot"] = screenshot_path.name
        except Exception as error:
            evidence["errors"].append({"step": "screenshot", "error": repr(error)})
        for index, frame in enumerate(list(self.page.frames)):
            entry = {"url": frame.url}
            try:
                entry["state"] = await asyncio.wait_for(frame.evaluate(PAGE_STATE_JS), timeout=5)
            except Exception as error:
                entry["state_error"] = repr(error)
            try:
                html = await asyncio.wait_for(frame.content(), timeout=5)
                html_path = self.output_dir / f"{prefix}-frame-{index}.html"
                html_path.write_text(html, encoding="utf-8")
                entry["html"] = html_path.name
            except Exception as error:
                entry["html_error"] = repr(error)
            evidence["frames"].append(entry)
        state_path = self.output_dir / (prefix + ".json")
        try:
            state_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
            self.state["snapshots"].append(state_path.name)
            self.event("snapshot_saved", label=label, path=state_path.name,
                       errors=evidence["errors"])
        except OSError as error:
            self._storage_error("snapshot", error)
            self.event("snapshot_write_error", label=label, error=repr(error))
        return evidence

    async def preserve_window(self):
        if not self.keep_open or self.page.is_closed():
            return
        closed = asyncio.Event()
        self.page.on("close", lambda: closed.set())
        self.context.on("close", lambda: closed.set())
        self.state["window_policy"] = "manual_close"
        self.event("waiting_for_manual_close")
        self.report("[HOLD] Evidence saved; keep the window open until manually closed.")
        if not self.page.is_closed():
            await closed.wait()
        self.event("manual_close_observed")

    async def finish(self, status, detail="", *, error=None, hold=False):
        try:
            if not self.finalized:
                self.finalized = True
                self.state.update({"status": status, "detail": detail})
                if error is not None:
                    self.state["exception"] = {"type": type(error).__name__, "message": str(error),
                                                "traceback": "".join(traceback.format_exception(error))}
                self.event("finished", status=status, detail=detail,
                           exception=self.state.get("exception"))
                await self.drain()
                await self.snapshot(status)
        except Exception as capture_error:
            self.event("final_capture_error", error=repr(capture_error))
            self.report(f"[LOG ERROR] final capture: {capture_error}")
        finally:
            # Evidence failures must also leave the page available for inspection.
            if hold:
                await self.preserve_window()
