"""Registration task lifecycle and file IPC; no upstream requests here."""
import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path


class RegistrationEngine:
    def __init__(self, state, config, save, log, metrics, command, terminate, runs, cwd,
                 protection=None, proxy_indices=None, proxy_selector=None, proxy_used=None,
                 proxy_result=None):
        self.state, self.config, self.save, self.log = state, config, save, log
        self.metrics, self.command, self.terminate = metrics, command, terminate
        self.runs, self.cwd = Path(runs), Path(cwd)
        self.protection, self.proxy_indices = protection, proxy_indices or (lambda: [])
        self.proxy_selector, self.proxy_used, self.proxy_result = proxy_selector, proxy_used, proxy_result
        self.active = {}
        self.stopping = False
        self._transition_lock = asyncio.Lock()

    async def start(self):
        async with self._transition_lock:
            return await self._start()

    async def _start(self):
        if self.stopping:
            return {"ok": False, "reason": "任务正在收尾，请稍后启动"}
        if self.state["running"]:
            return {"ok": True}
        if self.active:
            return {"ok": False, "reason": "仍有残留进程，请再次停止并核对"}
        if self.protection:
            reason = await self.protection.preflight()
            if reason:
                self.pause(reason)
                return {"ok": False, "reason": reason}
        self.state.update(running=True, stop_reason="", halted=False,
                          batch_id=uuid.uuid4().hex, batch_ok=0, batch_attempts=0,
                          failure_streaks={})
        self.state["worker"] = asyncio.create_task(self.supervise())
        self.save()
        return {"ok": True, "batch_id": self.state["batch_id"]}

    def pause(self, reason):
        self.state.update(stop_reason=reason, halted=True)
        self.log(reason)
        self.save()

    async def stop(self):
        async with self._transition_lock:
            await self._stop()

    async def _stop(self):
        self.stopping = True
        self.state.update(running=False, halted=True, stop_reason="用户停止")
        worker = self.state.get("worker")
        try:
            if worker and not worker.done():
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            for key, item in list(self.active.items()):
                proc = item.get("proc")
                try:
                    if proc and proc.returncode is None:
                        await self.terminate(proc)
                    if proc and proc.returncode is None:
                        continue
                    if self.protection and proc:
                        item["task"].update(await self.protection.after_exit(item["task"], interrupted=True))
                    self.active.pop(key, None)
                except Exception as error:
                    self.log("残留进程待处理: " + type(error).__name__)
            self.state["worker"] = None
            self.save()
        finally:
            self.stopping = False

    def _proxy_mode(self, task):
        return task.get("proxy_mode", "fixed" if task.get("proxy") is not None else "auto")

    def _assign_proxy(self, task):
        if self._proxy_mode(task) != "auto":
            return True
        if self.proxy_selector is None:
            return bool(task.get("proxy") is not None)
        busy = [x["task"].get("proxy") for x in self.active.values()]
        selected = self.proxy_selector(busy)
        if selected is None:
            task["stage"] = "等待代理冷却或释放"
            return False
        task["proxy"] = selected
        task.pop("proxy_error", None)
        return True

    def _candidate(self):
        busy = {str(x["task"].get("proxy")) for x in self.active.values()}
        for task in self.state["queue"]:
            if task["status"] != "pending" or task.get("retry_at", 0) > time.time():
                continue
            if self._proxy_mode(task) == "auto":
                if self._assign_proxy(task):
                    return task
                continue
            if str(task.get("proxy")) not in busy:
                return task
        return None

    async def supervise(self):
        tasks = set()
        try:
            while self.state["running"]:
                for done in list(tasks):
                    if done.done():
                        await done
                        tasks.remove(done)
                if self.state.get("halted"):
                    if not tasks:
                        break
                    await asyncio.sleep(0.05)
                    continue
                cfg = self.config()
                target = int(cfg.get("target_successes", 0))
                limit = max(1, min(5, int(cfg.get("workers", 1))))
                while len(tasks) < limit:
                    # {目标：名额在创建任务前预留；检查与预留之间没有 await，避免并发超发。}
                    if target and self.state.get("batch_ok", 0) + len(tasks) >= target:
                        break
                    cap = int(cfg.get("max_batch_attempts", 2000))
                    if cap and self.state.get("batch_attempts", 0) >= cap:
                        self.pause("达到本批尝试上限，已停止派发，等待在途任务收尾")
                        break
                    task = self._candidate()
                    if task is None:
                        break
                    task["status"] = "running"
                    task["batch_id"] = self.state.get("batch_id", "")
                    self.state["batch_attempts"] = self.state.get("batch_attempts", 0) + 1
                    tasks.add(asyncio.create_task(self.run(task)))
                    # Let run() register its proxy reservation before selecting another task.
                    await asyncio.sleep(0)
                    if self.state.get("halted"):
                        break
                if not tasks:
                    if target and self.state.get("batch_ok", 0) >= target:
                        self.state["stop_reason"] = "已达到成功目标"
                        break
                    pending = [t for t in self.state["queue"] if t["status"] == "pending"]
                    if not pending:
                        break
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.pause("调度异常: " + type(error).__name__)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self.protection:
                try:
                    await self.protection.batch_end(self.state.get("batch_id", ""))
                except Exception as error:
                    self.log("批次对账待重试: " + type(error).__name__)
            self.state["running"] = False
            self.save()
            self.log("引擎停止；在途任务已收尾" + (": " + self.state["stop_reason"]
                     if self.state.get("stop_reason") else ""))

    def _progress(self, task, path):
        try:
            event = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        for key in ("phone", "stage", "wait_code_since", "error_category", "code_source"):
            if key in event:
                task[key] = event[key]
        if event.get("status") in ("running", "waiting_code"):
            task["status"] = event["status"]

    def _failure(self, task):
        category = task.get("error_category") or "registration"
        streaks = self.state.setdefault("failure_streaks", {})
        if self.state.get("last_failure_category") != category:
            streaks.clear()
        streaks[category] = streaks.get(category, 0) + 1
        self.state["last_failure_category"] = category
        cfg = self.config()
        fatal = category in ("fatal", "auth", "balance", "audit", "budget")
        threshold = int(cfg.get("consecutive_failures", 15))
        if fatal or threshold and streaks[category] >= threshold:
            self.pause(f"暂停派发: {category} 连续失败 {streaks[category]} 次")
        return category

    def _rotate_proxy(self, task):
        if not self.config().get("retry_rotate_proxy", True) or task.get("reuse_phone"):
            return
        task["proxy_mode"] = "auto"
        task["proxy"] = None
        task.pop("proxy_error", None)

    async def run(self, task):
        task.setdefault("id", uuid.uuid4().hex)
        previous_attempt = task.get("attempt_id", "")
        task.update(run_id=uuid.uuid4().hex, started=time.time(), stage="注册启动", cred=False)
        task["attempt_id"] = task["run_id"]
        task.pop("budget_state", None)
        task.pop("held", None)
        task.pop("credential_saved", None)
        for key in ("wait_code_since", "error", "error_category", "code_source", "retry_at"):
            task.pop(key, None)
        if not task.get("reuse_phone"):
            task["phone"] = ""
        self.runs.mkdir(parents=True, exist_ok=True)
        base = self.runs / task["run_id"]
        progress, code = base.with_suffix(".progress.json"), base.with_suffix(".code")
        logfile = base.with_suffix(".log")
        cfg = {**self.config(), **task.get("run_options", {})}
        # {意图：项目、渠道和单价随任务固定，排队期间修改全局配置不改变已触发任务。}
        task["sms_unit_price"] = cfg.get("sms_unit_price", 1.1)
        env = {**os.environ, "TRAE_PROGRESS_FILE": str(progress), "TRAE_CODE_FILE": str(code),
               "TRAE_RUN_OPTIONS": json.dumps(cfg), "TRAE_PROFILE_TAG": task["run_id"],
               "TRAE_RUN_ID": task["run_id"], "TRAE_ATTEMPT_ID": task["attempt_id"],
               "TRAE_BATCH_ID": task.get("batch_id", ""), "TRAE_PROXY_ID": str(task.get("proxy", "")),
               "PYTHONUNBUFFERED": "1"}
        if task.get("reuse_phone") and previous_attempt:
            env["TRAE_REUSE_ATTEMPT_ID"] = previous_attempt
        if self.protection:
            env["TRAE_DATA_DIR"] = str(self.protection.directory.parent)
        proc, timed_out, cancelled, spawned, admitted = None, False, False, False, False
        self.active[task["id"]] = {"task": task, "proc": None, "progress": progress,
                                   "code": code, "timeout": False}
        try:
            self.save()
            if self.protection:
                reason = await self.protection.before_launch(task)
                if reason:
                    task.update(status="pending", stage=reason)
                    self.pause(reason)
                    return
                admitted = True
            if self.proxy_used is not None and task.get("proxy") is not None:
                self.proxy_used(task["proxy"])
            with logfile.open("ab", buffering=0) as output:
                launch = asyncio.create_task(asyncio.create_subprocess_exec(
                    *self.command(task), cwd=self.cwd, env=env,
                    stdout=output, stderr=asyncio.subprocess.STDOUT))
                try:
                    proc = await asyncio.shield(launch)
                except asyncio.CancelledError:
                    proc = await launch
                    spawned = True
                    self.active[task["id"]]["proc"] = proc
                    raise
                spawned = True
                self.state["process"] = proc
                self.active[task["id"]]["proc"] = proc
                await asyncio.wait_for(proc.wait(), timeout=float(cfg.get("task_timeout_seconds", 900)))
                timed_out = self.active[task["id"]]["timeout"] or proc.returncode == 2
                self._progress(task, progress)
                task.update(status="ok" if proc.returncode == 0 and not timed_out else "fail",
                            stage="凭证落袋" if proc.returncode == 0 and not timed_out else
                                  "等码超时" if timed_out else "注册失败",
                            cred=proc.returncode == 0 and not timed_out)
        except asyncio.CancelledError:
            cancelled = True
            task.update(status="reconciliation_unknown", stage="已停止，核对号码状态")
        except Exception as error:
            timed_out = isinstance(error, asyncio.TimeoutError)
            task.update(status="fail", stage="任务超时" if timed_out else "运行异常",
                        error=type(error).__name__)
        finally:
            # {意图：必须先结束旧进程再重排任务，避免同一任务并发取号。}
            self.active[task["id"]]["finishing"] = True
            if proc and proc.returncode is None:
                try:
                    await self.terminate(proc)
                except Exception as error:
                    task.update(status="reconciliation_unknown", stage="进程结束待确认",
                                error_category="audit", error=type(error).__name__)
                    self.pause("子进程未确认结束，暂停派发")
            still_running = bool(proc and proc.returncode is None)
            if still_running:
                task.update(status="reconciliation_unknown", stage="进程结束待确认", error_category="audit")
                self.pause("子进程未确认结束，暂停派发")
            if admitted and self.protection and not still_running:
                try:
                    details = await self.protection.after_exit(task, interrupted=cancelled or timed_out,
                                                               spawned=spawned)
                    task.update(details)
                    task.setdefault("budget_attempts", {})[task["attempt_id"]] = {
                        "estimated": details.get("estimated_cost", 0),
                        "uncertain": details.get("uncertain_cost", 0)}
                    if details.get("held"):
                        self.pause("遗留号码尚未完成清理，暂停派发")
                except Exception as error:
                    task.update(status="reconciliation_unknown", error_category="audit",
                                stage="清理或对账待恢复", error=type(error).__name__)
                    self.pause("审计状态待恢复，暂停派发")
            if not still_running:
                self.active.pop(task["id"], None)
            self.state["process"] = next((a["proc"] for a in self.active.values() if a["proc"]), None)
            code.unlink(missing_ok=True)
            task.pop("wait_code_since", None)
            if spawned or task["status"] not in ("pending",):
                success = task["status"] == "ok"
                if success:
                    self.state["done"] += 1
                    self.state["batch_ok"] = self.state.get("batch_ok", 0) + 1
                    self.state["failure_streaks"] = {}
                elif not cancelled:
                    task.setdefault("error_category", "sms_timeout" if timed_out else "registration")
                    self._failure(task)
                if spawned and self.proxy_result is not None and task.get("proxy") is not None:
                    self.proxy_result(task["proxy"], success)
                self.metrics({"event": "ok" if success else "fail", "phone": task.get("phone", ""),
                              "run_id": task["run_id"], "attempt_id": task["attempt_id"],
                              "proxy": task.get("proxy"), "stage": task["stage"],
                              "error_category": task.get("error_category"),
                              "code_source": task.get("code_source"),
                              "took_s": round(time.time() - task["started"], 2)})
                if not success and not cancelled:
                    current = self.config()
                    retryable = task["status"] == "fail" and not self.state.get("halted")
                    if retryable and current.get("timeout_requeue", True) and task.get("retries", 0) < current.get("max_retries", 2):
                        task["retries"] = task.get("retries", 0) + 1
                        self._rotate_proxy(task)
                        task.update(status="pending", stage=f"等待重试 {task['retries']}",
                                    retry_at=time.time() + float(current.get("retry_delay_seconds", 5)))
                    else:
                        self.state["fail"] += 1
            text = logfile.read_text(encoding="utf-8", errors="replace") if logfile.exists() else ""
            for line in text.splitlines()[-30:]:
                # Only short stage diagnostics go to the UI; raw SMS evidence stays local.
                if "sms=" not in line and "code=" not in line and "token" not in line.lower():
                    self.log("af: " + line[:240])
            self.save()
            result = base.with_suffix(".json")
            temporary = result.with_suffix(".tmp")
            temporary.write_text(json.dumps(task, ensure_ascii=False, indent=2), encoding="utf-8")
            temporary.replace(result)

    async def watchdog(self):
        while True:
            for item in list(self.active.values()):
                task = item["task"]
                proc = item.get("proc")
                if proc is None or proc.returncode is not None or item.get("finishing"):
                    continue
                previous = (task.get("stage"), task.get("status"))
                self._progress(task, item["progress"])
                since = task.get("wait_code_since")
                if since and item.get("proc") and time.time() - since >= float(self.config().get("code_timeout_minutes", 5)) * 60 + 2:
                    item["timeout"] = True
                    await self.terminate(item["proc"])
                if previous != (task.get("stage"), task.get("status")):
                    self.save()
            await asyncio.sleep(0.1)

    def submit_code(self, task, value):
        if not re.fullmatch(r"[0-9]{4,8}", value):
            raise ValueError("验证码应为4至8位数字")
        item = self.active.get(task.get("id"))
        if item is None or item.get("proc") is None:
            raise ValueError("任务进程已结束")
        path = item["code"]
        temporary = path.with_suffix(".tmp-code")
        temporary.write_text(value, encoding="utf-8")
        temporary.replace(path)
