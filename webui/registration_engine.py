"""Registration task lifecycle and file IPC; no upstream requests here."""
import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path


class RegistrationEngine:
    def __init__(self, state, config, save, log, metrics, command, terminate, runs, cwd):
        self.state, self.config, self.save, self.log = state, config, save, log
        self.metrics, self.command, self.terminate = metrics, command, terminate
        self.runs, self.cwd = Path(runs), Path(cwd)
        self.active = {}

    async def start(self):
        if self.state["running"]:
            return {"ok": True}
        self.state["running"] = True
        self.state["worker"] = asyncio.create_task(self.supervise())
        return {"ok": True}

    async def supervise(self):
        tasks = set()
        try:
            while self.state["running"]:
                for done in list(tasks):
                    if done.done():
                        await done
                        tasks.remove(done)
                limit = max(1, min(5, int(self.config().get("workers", 1))))
                while len(tasks) < limit:
                    task = next((t for t in self.state["queue"] if t["status"] == "pending"), None)
                    if task is None:
                        break
                    task["status"] = "running"
                    tasks.add(asyncio.create_task(self.run(task)))
                if not tasks:
                    break
                await asyncio.sleep(0.05)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.state["running"] = False
            self.save()
            self.log("引擎空闲停止")

    async def run(self, task):
        task.setdefault("id", uuid.uuid4().hex)
        task.update(run_id=uuid.uuid4().hex, started=time.time(), stage="注册启动", cred=False)
        task.pop("wait_code_since", None)
        task.pop("error", None)
        self.runs.mkdir(parents=True, exist_ok=True)
        base = self.runs / task["run_id"]
        progress, code = base.with_suffix(".progress.json"), base.with_suffix(".code")
        logfile = base.with_suffix(".log")
        cfg = self.config()
        env = {**os.environ, "TRAE_PROGRESS_FILE": str(progress), "TRAE_CODE_FILE": str(code),
               "TRAE_RUN_OPTIONS": json.dumps(cfg), "TRAE_PROFILE_TAG": task["run_id"],
               "PYTHONUNBUFFERED": "1"}
        proc = None
        timed_out = False
        cancelled = False
        self.save()
        try:
            with logfile.open("ab", buffering=0) as output:
                proc = await asyncio.create_subprocess_exec(*self.command(task), cwd=self.cwd,
                        env=env, stdout=output, stderr=asyncio.subprocess.STDOUT)
                self.state["process"] = proc
                self.active[task["id"]] = {"task": task, "proc": proc, "progress": progress,
                                           "code": code, "timeout": False}
                await asyncio.wait_for(proc.wait(), timeout=float(cfg.get("task_timeout_seconds", 900)))
                timed_out = self.active[task["id"]]["timeout"] or proc.returncode == 2
                if proc.returncode == 0 and not timed_out:
                    task.update(status="ok", stage="凭证落袋", cred=True)
                    self.state["done"] += 1
                else:
                    task.update(status="fail", stage="等码超时" if timed_out else "注册失败")
        except asyncio.CancelledError:
            cancelled = True
            task.update(status="pending", stage="已停止，等待重试")
            raise
        except Exception as error:
            task.update(status="fail", stage="任务超时" if isinstance(error, asyncio.TimeoutError) else "运行异常",
                        error=str(error))
            timed_out = isinstance(error, asyncio.TimeoutError)
        finally:
            # {意图：必须先结束旧进程再重排任务，避免同一任务并发取号。}
            if proc and proc.returncode is None:
                await self.terminate(proc)
            self.active.pop(task["id"], None)
            self.state["process"] = next((a["proc"] for a in self.active.values()), None)
            code.unlink(missing_ok=True)
            task.pop("wait_code_since", None)
            if not cancelled:
                success = task["status"] == "ok"
                self.metrics({"event": "ok" if success else "fail", "phone": task.get("phone", ""),
                              "proxy": task.get("proxy"), "stage": task["stage"],
                              "took_s": round(time.time() - task["started"], 2)})
                if not success:
                    current = self.config()
                    if current.get("timeout_requeue", True) and task.get("retries", 0) < current.get("max_retries", 2):
                        task["retries"] = task.get("retries", 0) + 1
                        task.update(status="pending", stage=f"等待重试 {task['retries']}")
                    else:
                        self.state["fail"] += 1
            text = logfile.read_text(encoding="utf-8", errors="replace") if logfile.exists() else ""
            for line in text.splitlines()[-30:]:
                self.log("af: " + line)
            self.save()
            result = base.with_suffix(".json")
            temporary = result.with_suffix(".tmp")
            temporary.write_text(json.dumps(task, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(result)

    async def watchdog(self):
        while True:
            for item in list(self.active.values()):
                task = item["task"]
                try:
                    event = json.loads(item["progress"].read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                previous = (task.get("stage"), task.get("status"))
                for key in ("phone", "stage", "wait_code_since"):
                    if key in event:
                        task[key] = event[key]
                status = event.get("status")
                if status in ("running", "waiting_code"):
                    task["status"] = status
                since = task.get("wait_code_since")
                if since and time.time() - since >= float(self.config().get("code_timeout_minutes", 5)) * 60:
                    item["timeout"] = True
                    await self.terminate(item["proc"])
                if previous != (task.get("stage"), task.get("status")):
                    self.save()
            await asyncio.sleep(0.1)

    def submit_code(self, task, value):
        if not re.fullmatch(r"[0-9]{4,8}", value):
            raise ValueError("验证码应为4至8位数字")
        item = self.active.get(task.get("id"))
        if item is None:
            raise ValueError("任务进程已结束")
        path = item["code"]
        temporary = path.with_suffix(".tmp-code")
        temporary.write_text(value, encoding="utf-8")
        temporary.replace(path)
