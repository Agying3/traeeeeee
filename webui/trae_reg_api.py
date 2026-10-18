# trae_reg_api.py — 注册机 WebUI 后端(FastAPI, 8898)。
# 路由域: /api/engine /api/queue /api/haozhu /api/credentials /api/proxies /api/gateway /api/settings /api/logs
# 无数据库：凭证、队列状态和配置使用 runtime 提供的数据目录。
import asyncio
import json
import os
import re
import uuid
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import runtime
from registration_proxy_pool import RegistrationProxyPool

PROGRAM_DIR = Path(__file__).resolve().parents[1]
MANAGED = bool(os.environ.get("TRAE_DATA_DIR"))
TOOLS = PROGRAM_DIR / "tools"
CRED_DIR = runtime.CRED_DIR
CONFIG = runtime.CONFIG_DIR / "webui.json" if MANAGED else Path(__file__).parent / "config.json"
GW = os.environ.get("A2A_BASE_URL", "http://127.0.0.1:3065").rstrip("/")
UPSTREAM_API_KEY = os.environ.get("TRAE_UPSTREAM_API_KEY", "").strip()
SERVICE_STARTED = time.time()
SERVICE_LOGGER = runtime.configure_logging() if MANAGED else None
BACKGROUND_TASKS = []

app = FastAPI(title="Trae注册机")

# ── 内存态 ──────────────────────────────────────────────
state = {"queue": [], "running": False, "done": 0, "fail": 0, "worker": None,
         "stop_reason": "", "halted": False, "batch_id": "", "batch_ok": 0,
         "batch_attempts": 0, "failure_streaks": {}}
log_lines: list[str] = ["[WebUI] 注册机后端启动"]


def log(msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    log_lines.append(line)
    del log_lines[:-500]
    if SERVICE_LOGGER:
        SERVICE_LOGGER.info(msg)


def load_config() -> dict:
    if CONFIG.exists():
        return json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    return {"workers": 1, "code_timeout_minutes": 5, "slider_retry": 2,
            "timeout_requeue": True, "max_retries": 2,
            "continuous": {"enabled": False, "min_queue": 3, "batch": 2,
                           "daily_max": 20, "min_balance": 5.0, "tick_s": 20}}


# ── 队列/引擎(持久化: JSON原子写, 重启恢复 — 号是真金白银, 不能丢) ──
QUEUE_FILE = runtime.STATE_DIR / "queue.json" if MANAGED else Path(__file__).parent / "queue.json"
METRICS_FILE = runtime.STATE_DIR / "metrics.json" if MANAGED else Path(__file__).parent / "metrics.json"
METRICS_MAX = 5000
UPSTREAM_STATS_FILE = runtime.STATE_DIR / "upstream-stats.json"
UPSTREAM_STATS_LOCK = threading.Lock()


def upstream_stats() -> dict:
    try:
        return json.loads(UPSTREAM_STATS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"requests": 0, "success": 0, "failed": 0, "last_at": None, "last_model": ""}


def record_upstream(ok: bool, model: str = ""):
    with UPSTREAM_STATS_LOCK:
        stats = upstream_stats()
        stats["requests"] += 1
        stats["success" if ok else "failed"] += 1
        stats["last_at"] = time.time()
        if model:
            stats["last_model"] = str(model)[:120]
        runtime.atomic_json(UPSTREAM_STATS_FILE, stats)


def _atomic_write(path: Path, data):
    runtime.atomic_json(path, data)

def queue_save():
    try:
        _atomic_write(QUEUE_FILE, {"queue": state["queue"],
                                  "done": state["done"], "fail": state["fail"],
                                  "guard": {k: state.get(k) for k in ("stop_reason", "halted", "batch_id",
                                              "batch_ok", "batch_attempts", "failure_streaks")}})
    except Exception as error:
        state.update(halted=True, stop_reason="队列保存失败，暂停派发")
        log("队列保存失败: " + type(error).__name__)
        raise


def queue_load():
    if QUEUE_FILE.exists():
        try:
            d = json.loads(QUEUE_FILE.read_text(encoding="utf-8-sig"))
            state["queue"] = d.get("queue", [])
            state["done"] = d.get("done", 0)
            state["fail"] = d.get("fail", 0)
            state.update({k: v for k, v in d.get("guard", {}).items() if k in state})
            # 重启后 running 任务标回 pending(可重跑)
            for t in state["queue"]:
                if t.get("status") in ("running", "waiting_code"):
                    t["status"] = "reconciliation_unknown"; t["stage"] = "重启恢复，等待核对"
        except Exception as error:
            log(f"队列读取失败，保留原文件: {error}")
            raise


def metrics_save(event: dict):
    try:
        rows = []
        if METRICS_FILE.exists():
            rows = json.loads(METRICS_FILE.read_text(encoding="utf-8-sig"))
        rows.append({"ts": time.time(), **event})
        del rows[:-METRICS_MAX]
        _atomic_write(METRICS_FILE, rows)
    except Exception:
        pass


queue_load()


class RegReq(BaseModel):
    count: int = Field(1, ge=1, le=10)
    proxy: int | None = Field(None, ge=0)


class BatchReq(BaseModel):
    count: int = Field(1, ge=1, le=500)
    proxy_start: int | None = Field(None, ge=0)
    proxy_end: int | None = Field(None, ge=0)


@app.get("/api/engine/state")
async def engine_state():
    return {"running": state["running"], "queue": state["queue"],
            "done": state["done"], "fail": state["fail"],
            "creds": len(list(CRED_DIR.glob("*.json"))),
            "stop_reason": state.get("stop_reason", ""), "halted": state.get("halted", False),
            "batch_id": state.get("batch_id", ""), "batch_ok": state.get("batch_ok", 0),
            "batch_attempts": state.get("batch_attempts", 0),
            "failure_streaks": state.get("failure_streaks", {})}


@app.post("/api/queue")
async def queue_add(req: RegReq):
    proxy_label = f"#{req.proxy}" if req.proxy is not None else "自动选择"
    for _ in range(max(1, min(req.count, 10))):
        task = {"phone": "", "proxy": req.proxy,
                "proxy_mode": "fixed" if req.proxy is not None else "auto",
                "stage": "pending", "status": "pending", "cred": False, "added": time.time()}
        state["queue"].append(task)
    log(f"队列 +{req.count}（代理{proxy_label}）")
    queue_save()
    return {"queued": req.count, "total": len(state["queue"])}


@app.post("/api/queue/batch")
async def queue_batch(req: BatchReq):
    """批量入队；未指定代理范围时由引擎派发阶段自动选号。"""
    if req.proxy_start is not None or req.proxy_end is not None:
        if req.proxy_start is None or req.proxy_end is None or req.proxy_end < req.proxy_start:
            raise HTTPException(400, "代理范围无效")
        count = req.proxy_end - req.proxy_start + 1
        if count > 500:
            raise HTTPException(400, "单次最多批量 500 条")
        for proxy in range(req.proxy_start, req.proxy_end + 1):
            state["queue"].append({"phone": "", "proxy": proxy, "proxy_mode": "fixed",
                                   "stage": "pending", "status": "pending", "cred": False,
                                   "added": time.time()})
        label = f"代理#{req.proxy_start}~#{req.proxy_end}"
    else:
        count = req.count
        for _ in range(count):
            state["queue"].append({"phone": "", "proxy": None, "proxy_mode": "auto",
                                   "stage": "pending", "status": "pending", "cred": False,
                                   "added": time.time()})
        label = "自动选择代理"
    log(f"批量入队 +{count}（{label}）")
    queue_save()
    return {"queued": count, "total": len(state["queue"])}


@app.post("/api/queue/retry-failed")
async def queue_retry_failed():
    """一键重试: 所有失败任务重置为待处理(等价于逐个点重试按钮)。"""
    retryable = []
    skipped = []
    for i, t in enumerate(state["queue"]):
        if t.get("status") == "fail":
            try:
                _retry_guard(t)
            except HTTPException:
                skipped.append(i)
                continue
            retryable.append(i)
            t["retries"] = 0
            if t.get("proxy_mode", "fixed") == "auto" or load_config().get("retry_rotate_proxy", True):
                t["proxy_mode"] = "auto"
                t["proxy"] = None
            t["status"] = "pending"; t["stage"] = "手动重试"; t["cred"] = False
    log(f"一键重试失败任务: {len(retryable)} 条" if retryable else "无失败任务可重试")
    queue_save()
    return {"retried": len(retryable), "skipped": skipped, "total": len(state["queue"])}


class CodeReq(BaseModel):
    idx: int
    code: str


@app.post("/api/queue/code")
async def queue_code(req: CodeReq):  # 手动输码: 豪猪没抓到时人工兜底
    if req.idx < 0 or req.idx >= len(state["queue"]):
        raise HTTPException(404, "任务不存在")
    t = state["queue"][req.idx]
    if t.get("status") != "waiting_code":
        raise HTTPException(400, f"该任务不在等码状态({t.get('status')})")
    try:
        ENGINE.submit_code(t, req.code.strip())
    except ValueError as error:
        raise HTTPException(400, str(error))
    log(f"任务#{req.idx} 手动验证码已交给注册进程")
    queue_save()
    return {"ok": True}


class TaskIdx(BaseModel):
    idx: int
    reuse_phone: bool = False


def _retry_guard(task, reuse=False):
    if task.get("id") in ENGINE.active:
        raise HTTPException(409, "任务仍在收尾，请先停止引擎")
    if ENGINE.protection is None:
        return
    row = PROTECTION.ledger().get_attempt(task.get("attempt_id", "")) or {}
    if reuse:
        from haozhu_ledger import account_fingerprint
        from haozhu_sms import _pid_alive
        if not row.get("held") or row.get("phone") != task.get("phone"):
            raise HTTPException(409, "复用需有仍在占用的原号码记录")
        if row.get("account_fingerprint") != account_fingerprint(PROTECTION.credentials().get("user", "")):
            raise HTTPException(409, "原号码与当前豪猪账号不匹配")
        if _pid_alive(row.get("owner_pid")):
            raise HTTPException(409, "原号码进程仍在运行")
    elif (row.get("held") or task.get("budget_state") == "uncertain"
          or PROTECTION.reservation_state(task.get("attempt_id", "")) in ("reserved", "uncertain")):
        raise HTTPException(409, "先在豪猪页清理号码并核对待定预算")


@app.post("/api/queue/retry")
async def queue_retry(req: TaskIdx):
    if req.idx < 0 or req.idx >= len(state["queue"]):
        raise HTTPException(404, "任务不存在")
    t = state["queue"][req.idx]
    if t.get("status") in ("running", "waiting_code", "ok"):
        raise HTTPException(409, "当前任务请先停止；成功任务无需重试")
    if req.reuse_phone and not __import__("re").fullmatch(r"1[0-9]{10}", t.get("phone", "")):
        raise HTTPException(400, "任务尚未保存号码")
    _retry_guard(t, req.reuse_phone)
    t["reuse_phone"] = req.reuse_phone
    t["retries"] = 0
    if not req.reuse_phone:
        t["proxy_mode"] = "auto"
        t["proxy"] = None
    t["status"] = "pending"; t["stage"] = "手动重试"; t["cred"] = False
    log(f"重试: 任务#{req.idx}")
    queue_save()
    return {"ok": True}


@app.post("/api/queue/remove")
async def queue_remove(req: TaskIdx):
    if 0 <= req.idx < len(state["queue"]):
        t = state["queue"][req.idx]
        if t.get("status") in ("running", "waiting_code") or t.get("id") in ENGINE.active:
            raise HTTPException(409, "请先停止运行中的任务")
        if ENGINE.protection is not None:
            _retry_guard(t)
        removed = state["queue"].pop(req.idx)
        log(f"移除任务: {removed.get('phone') or '未取号'}")
        queue_save()
    return {"ok": True}


def _af_cmd(task) -> list:
    """注册链命令(测试替换点: 换成假引擎脚本即可不烧真钱全链演练)。"""
    command = [sys.executable, "-X", "utf8", str(TOOLS / "af_http.py"), str(task["proxy"])]
    if task.get("reuse_phone") and task.get("phone"):
        command += ["--phone", task["phone"]]
    return command


from registration_engine import RegistrationEngine
from registration_protection import RegistrationProtection


_proxy_pool = None


def get_proxy_pool():
    global _proxy_pool
    state_file = runtime.STATE_DIR / "proxy-pool-state.json"
    if _proxy_pool is None or _proxy_pool.proxy_file != PROXY_FILE or _proxy_pool.state_file != state_file:
        _proxy_pool = RegistrationProxyPool(PROXY_FILE, state_file)
    return _proxy_pool


def _proxy_indices():
    return list(range(len(RegistrationProxyPool.read_lines(PROXY_FILE))))


def _proxy_snapshot():
    busy = [item["task"].get("proxy") for item in ENGINE.active.values()]
    return get_proxy_pool().snapshot(busy)


def _proxy_result(index, success):
    get_proxy_pool().record_result(index, success)


PROTECTION = RegistrationProtection(runtime.STATE_DIR, load_config, lambda: _hz_summary_safe(), log,
                                    lambda: _hz_client().load_credentials(), _proxy_indices)

ENGINE = RegistrationEngine(state, load_config, queue_save, log, metrics_save,
                            lambda task: _af_cmd(task),
                            lambda proc: _terminate_registration(proc), runtime.RUN_DIR, PROGRAM_DIR,
                             protection=PROTECTION, proxy_indices=_proxy_indices,
                              proxy_selector=lambda busy: get_proxy_pool().select(busy),
                              proxy_used=lambda index: get_proxy_pool().mark_used(index),
                              proxy_result=_proxy_result)


@app.post("/api/engine/start")
async def engine_start():
    if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
        raise HTTPException(409, "当前服务处于离线维护模式，注册引擎保持停止")
    return await ENGINE.start()

async def _terminate_registration(proc):
    if proc.returncode is not None:
        return
    if os.name == "nt":
        killer = await asyncio.create_subprocess_exec(
            "taskkill", "/PID", str(proc.pid), "/T", "/F",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await killer.wait()
    else:
        proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=8)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


# ── 看门狗: 等码超时自动标记/重入队(sense同款) ──────────
async def watchdog():
    await ENGINE.watchdog()


@app.on_event("startup")
async def _start_watchdog():
    cont_load()
    if os.environ.get("TRAE_REGISTRATION_DISABLED") != "1":
        try:
            await PROTECTION.recover()
            PROTECTION.reconcile_reservations()
        except Exception as error:
            ENGINE.pause("启动恢复待重试: " + type(error).__name__)
    BACKGROUND_TASKS.extend([asyncio.create_task(watchdog()),
                             asyncio.create_task(continuous_loop()),
                             asyncio.create_task(haozhu_keepalive()),
                             asyncio.create_task(channel_watch_loop())])
    GATEWAY_OPS.start()
    log("注册机服务就绪")


@app.on_event("shutdown")
async def _shutdown_program():
    for task in BACKGROUND_TASKS:
        task.cancel()
    await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)
    await GATEWAY_OPS.stop()
    await UPSTREAM.close()
    await engine_stop()
    queue_save()
    cont_save()
    log("注册机服务已停止，队列已保留")


# ── 连续加号(自动补号值班员; sense WatcherService 的简化移植) ──
# 值班员不做注册链路的事——入队/跑引擎全复用现有队列机制:
#   观察  队列 pending+running < min_queue → 缺号
#   防线  每日上限 daily_max / 豪猪余额 min_balance / 代理池空 → 暂停留因, 恢复条件消失自动复工
#   派活  入队 want 个任务(代理轮转, 一号一IP) → 引擎没跑就拉起
#   记账  当日加号数跨重启保留(continuous.json, 日期翻篇自动清零)
CONT_STATE_FILE = runtime.STATE_DIR / "continuous.json" if MANAGED else Path(__file__).parent / "continuous.json"

cont: dict = {"added": 0, "date": "", "paused": "", "balance": None,
              "proxy_cursor": 0, "logs": []}


def cont_save():
    try:
        _atomic_write(CONT_STATE_FILE, {"added": cont["added"], "date": cont["date"],
                                        "proxy_cursor": cont["proxy_cursor"],
                                        "logs": cont["logs"][-50:]})
    except Exception:
        pass


def cont_load():
    try:
        d = json.loads(CONT_STATE_FILE.read_text(encoding="utf-8-sig"))
        cont["date"] = d.get("date") or ""
        # 只有同一天才恢复计数(跨天由值班轮自己清零)
        cont["added"] = int(d.get("added", 0)) if cont["date"] == time.strftime("%Y-%m-%d") else 0
        cont["proxy_cursor"] = int(d.get("proxy_cursor", 0))
        cont["logs"] = list(d.get("logs", []))[-50:]
    except Exception:
        pass


def cont_note(msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    cont["logs"].append(line)
    del cont["logs"][:-50]
    log("连续加号: " + msg)
    cont_save()


def _proxy_total() -> int:
    return len(get_proxy_pool().lines())


def _proxy_available() -> int:
    busy = [item["task"].get("proxy") for item in ENGINE.active.values()]
    snapshot = get_proxy_pool().snapshot(busy)
    return sum(1 for item in snapshot["items"]
               if not item["busy"] and not item["cooling"] and item["healthy"] is not False)


async def continuous_loop():
    last_tick = 0.0
    while True:
        await asyncio.sleep(2)  # 心跳粒度(tick_s 配置热生效, 不用等服务重启)
        c = load_config().get("continuous", {})
        if not c.get("enabled"):
            continue
        tick = max(2, int(c.get("tick_s", 20)))
        if time.time() - last_tick < tick:
            continue
        last_tick = time.time()
        try:
            await _continuous_tick(c)
        except Exception as e:
            cont_note(f"值班轮异常(跳过本轮): {e}")


async def _continuous_tick(c: dict):
    if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1" or state.get("halted"):
        cont["paused"] = "engine_halted"
        return
    # 日期翻篇 → 当日计数清零
    today = time.strftime("%Y-%m-%d")
    if cont["date"] != today:
        cont["date"], cont["added"] = today, 0
        cont_save()
    daily_max = max(1, int(c.get("daily_max", 20)))
    min_queue = max(1, int(c.get("min_queue", 3)))
    batch = max(1, int(c.get("batch", 2)))
    min_balance = float(c.get("min_balance", 5))

    # 防线1: 今日上限
    if cont["added"] >= daily_max:
        if cont["paused"] != "daily":
            cont["paused"] = "daily"
            cont_note(f"今日加号已达上限 {cont['added']}/{daily_max}，停止补号")
        return
    active = sum(1 for t in state["queue"] if t["status"] in ("pending", "running", "waiting_code"))
    if active >= min_queue:
        if cont["paused"] not in ("", "daily"):
            cont_note("队列回充到位，值班恢复待命")
        cont["paused"] = ""
        return
    # 防线2: 代理池空(af_http 一个任务必须一个代理索引)
    if _proxy_total() <= 0:
        if cont["paused"] != "proxy":
            cont["paused"] = "proxy"
            cont_note("代理池为空，暂停补号（每号需一IP，先去代理池页导入）")
        return
    if _proxy_available() <= 0:
        if cont["paused"] != "proxy":
            cont["paused"] = "proxy"
            cont_note("代理池全部冷却或占用中，暂停补号（冷却结束自动恢复）")
        return
    # 防线3: 豪猪余额(查询失败不挡——豪猪偶发抖动不该停摆值班)
    s = await asyncio.to_thread(_hz_summary_safe)
    if s is None:
        cont["paused"] = "balance_unknown"
        return
    if s is not None:
        cont["balance"] = s.get("balance")
        if float(s.get("balance", 0) or 0) < min_balance:
            if cont["paused"] != "balance":
                cont["paused"] = "balance"
                cont_note(f"豪猪余额 ¥{float(s.get('balance', 0) or 0):.2f} 低于下限 "
                          f"¥{min_balance:.2f}，暂停补号（充值后自动恢复）")
            return
    # 派活: 入队(代理轮转) → 引擎没跑就拉起(worker 空闲自停, 下轮缺号再拉)
    want = min(batch, daily_max - cont["added"], min_queue - active)
    for _ in range(want):
        state["queue"].append({"phone": "", "proxy": None, "proxy_mode": "auto",
                               "stage": "pending", "status": "pending", "cred": False,
                               "added": time.time()})
    cont["added"] += want
    cont["paused"] = ""
    queue_save()
    cont_note(f"自动补号 +{want}（今日 {cont['added']}/{daily_max}，队列 {active}→{active + want}）")
    if not state["running"]:
        await engine_start()


@app.get("/api/continuous")
async def continuous_get():
    c = load_config().get("continuous", {})
    active = sum(1 for t in state["queue"] if t["status"] in ("pending", "running", "waiting_code"))
    today = time.strftime("%Y-%m-%d")
    todays = [t for t in state["queue"] if time.strftime("%Y-%m-%d", time.localtime(t.get("added", 0))) == today]
    return {"config": c, "added_today": cont["added"], "dispatched_today": cont["added"],
            "registered_today": sum(t.get("status") == "ok" for t in todays),
            "imported_today": sum(t.get("gateway_imported", False) for t in todays), "date": cont["date"],
            "paused": cont["paused"], "balance": cont["balance"],
            "queue_active": active, "proxy_total": _proxy_total(),
            "proxy_available": _proxy_available(),
            "logs": cont["logs"][-15:]}


class ContConfig(BaseModel):
    enabled: bool = False
    min_queue: int = 3
    batch: int = 2
    daily_max: int = 20
    min_balance: float = 5.0
    tick_s: int = 20


@app.post("/api/continuous")
async def continuous_set(c: ContConfig):
    cfg = load_config()
    cfg["continuous"] = {"enabled": c.enabled, "min_queue": max(1, c.min_queue),
                         "batch": max(1, c.batch), "daily_max": max(1, c.daily_max),
                         "min_balance": max(0.0, c.min_balance), "tick_s": max(2, c.tick_s)}
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    cont_note("配置已更新，值班" + ("启动" if c.enabled else "停用"))
    return {"ok": True}


# ── 指标聚合(事件流水→时间窗现算) ────────────────────────
@app.get("/api/metrics")
async def metrics(window_h: int = 24):
    rows = []
    if METRICS_FILE.exists():
        try:
            rows = json.loads(METRICS_FILE.read_text(encoding="utf-8-sig"))
        except Exception:
            rows = []
    since = time.time() - window_h * 3600
    rows = [r for r in rows if r.get("ts", 0) >= since]
    ok = [r for r in rows if r.get("event") == "ok"]
    fail = [r for r in rows if r.get("event") == "fail"]
    took = [r["took_s"] for r in ok if r.get("took_s")]
    proxy_ok, proxy_fail = {}, {}
    for r in fail:
        p = f"#{r.get('proxy')}"
        proxy_fail[p] = proxy_fail.get(p, 0) + 1
    for r in ok:
        p = f"#{r.get('proxy')}"
        proxy_ok[p] = proxy_ok.get(p, 0) + 1
    return {"window_h": window_h, "total": len(rows),
            "ok": len(ok), "fail": len(fail),
            "rate": round(100 * len(ok) / len(rows)) if rows else None,
            "avg_s": round(sum(took) / len(took), 1) if took else None,
            "stage_fail": proxy_fail, "proxy_ok": proxy_ok}


@app.get("/api/dashboard")
async def dashboard(window_h: int = 0):
    """仪表盘聚合：只用本机真实数据（注册队列 + 队列内时间戳）分桶，不依赖外部服务。

    window_h=0 表示不限时间窗（默认），因为队列里的历史任务可能早于 24 小时，
    窗口太小会让仪表盘全空。
    """
    now = time.time()
    since = 0 if window_h <= 0 else now - window_h * 3600
    tasks = [t for t in state["queue"] if float(t.get("added", 0) or 0) >= since]

    def bucket_of(task):
        status = task.get("status")
        if status == "ok":
            return "ok"
        if status in ("fail", "reconciliation_unknown"):
            return "fail"
        return "active"

    proxies = {}
    stages = {}
    for task in tasks:
        kind = bucket_of(task)
        key = "auto" if task.get("proxy") is None else str(task.get("proxy"))
        proxy = proxies.setdefault(key, {"ok": 0, "fail": 0, "active": 0})
        proxy[kind] += 1
        if kind == "fail":
            name = str(task.get("stage") or "未知阶段")
            stages[name] = stages.get(name, 0) + 1

    trend = []
    stamps = [float(t.get("added", 0) or 0) for t in tasks if float(t.get("added", 0) or 0) > 0]
    span = (max(stamps) - min(stamps)) if stamps else 0
    if span <= 2 * 3600:
        step, label = 600, "10 分钟"
    elif span <= 2 * 86400:
        step, label = 3600, "小时"
    elif span <= 30 * 86400:
        step, label = 86400, "天"
    else:
        step, label = 7 * 86400, "周"
    if stamps:
        first, last = int(min(stamps) // step) * step, int(max(stamps) // step) * step
        slots_all = {}
        for task in tasks:
            at = float(task.get("added", 0) or 0)
            if at <= 0:
                continue
            key = int(at // step) * step
            slot = slots_all.setdefault(key, {"ok": 0, "fail": 0, "active": 0})
            slot[bucket_of(task)] += 1
        # 空洞补零，折线才连续；上限 120 个点避免超长窗口撑爆响应。
        cursor, filled = first, []
        while cursor <= last and len(filled) < 120:
            filled.append(cursor)
            cursor += step
        for at in filled:
            slot = slots_all.get(at, {"ok": 0, "fail": 0, "active": 0})
            settled = slot["ok"] + slot["fail"]
            trend.append({"at": at, "ok": slot["ok"], "fail": slot["fail"], "active": slot["active"],
                          "total": slot["ok"] + slot["fail"] + slot["active"],
                          "rate": round(100 * slot["ok"] / settled) if settled else None})

    ok = sum(1 for t in tasks if bucket_of(t) == "ok")
    fail = sum(1 for t in tasks if bucket_of(t) == "fail")
    active = sum(1 for t in tasks if bucket_of(t) == "active")
    settled = ok + fail
    proxy_rows = []
    for key, value in proxies.items():
        decided = value["ok"] + value["fail"]
        proxy_rows.append({"proxy": key, "ok": value["ok"], "fail": value["fail"],
                           "active": value["active"],
                           "rate": round(100 * value["ok"] / decided) if decided else None})
    proxy_rows.sort(key=lambda row: (-(row["ok"] + row["fail"]), row["proxy"]))
    return {"window_h": window_h, "generated_at": now, "queued_total": len(state["queue"]),
            "bucket_seconds": step, "bucket_label": label,
            "cards": {"tasks": len(tasks), "ok": ok, "fail": fail, "active": active,
                      "rate": round(100 * ok / settled) if settled else None,
                      "creds": len(list(CRED_DIR.glob("*.json"))),
                      "waiting_code": sum(1 for t in tasks if t.get("status") == "waiting_code"),
                      "retried": sum(1 for t in tasks if int(t.get("retries", 0) or 0) > 0),
                      # 累计口径与豪猪页一致，避免两处数字对不上被当成 bug。
                      "lifetime_ok": state["done"], "lifetime_fail": state["fail"],
                      "failed_batch": state.get("failure_streaks", {})},
            "trend": trend,
            "proxies": proxy_rows[:18],
            "proxy_total": len(proxy_rows),
            "stages": dict(sorted(stages.items(), key=lambda item: -item[1])[:8])}


@app.post("/api/engine/stop")
async def engine_stop():
    await ENGINE.stop()
    queue_save()
    log("引擎已停止，任务保留；待核对任务保留预算预留")
    return {"ok": True}


# ── 豪猪(真连接: haozhu_sms 客户端直连 api.haozhuma.com) ──
def _hz_client():
    sys.path.insert(0, str(TOOLS))
    import haozhu_sms as hz
    return hz


def _hz_summary_safe():
    """豪猪余额/占用(尽力而为; 缺凭据或网络抖动返回 None, 不挡任何页面)。
    测试替换点: 指向假余额即可演练余额防线。"""
    try:
        hz = _hz_client()
        creds = hz.load_credentials()
        if not (creds.get("user") and creds.get("password") and creds.get("sid")):
            return None
        return hz.build_client(creds).summary()
    except Exception:
        return None


def _hz_catalog_module():
    _hz_client()
    import haozhu_catalog
    return haozhu_catalog


async def haozhu_keepalive():
    while True:
        await asyncio.sleep(60)
        catalog = _hz_catalog_module()
        if os.environ.get("TRAE_HAOZHU_CATALOG_DISABLED") == "1":
            continue
        try:
            await asyncio.to_thread(lambda: catalog.get_client().keepalive_if_due())
        except catalog.CatalogError:
            log("豪猪 H5 保活失败，请在豪猪配置中查看会话状态")


@app.get("/api/haozhu/config")
async def haozhu_get():
    hz = _hz_client()
    creds = hz.load_credentials()
    catalog = _hz_catalog_module()
    pool = catalog.uid_list(creds.get("uids", ""))
    legacy = str(creds.get("uid", "") or "").strip()
    if legacy and legacy not in pool:
        pool.insert(0, legacy)
    return {"user": creds.get("user", ""), "sid": creds.get("sid", ""), "uid": legacy,
            "uids": "\n".join(pool), "uid_pool_count": len(pool),
            "balance": PROTECTION.cached_balance if PROTECTION.cached_balance is not None else "",
            "configured": bool(creds.get("user") and creds.get("password") and creds.get("sid")),
            "h5_configured": bool(creds.get("h5_session")),
            "h5": await asyncio.to_thread(lambda: catalog.get_client().info()),
            "token_configured": bool(creds.get("token")),
            "catalog_paused": os.environ.get("TRAE_HAOZHU_CATALOG_DISABLED") == "1",
            "provider_paused": os.environ.get("TRAE_REGISTRATION_DISABLED") == "1"}


class HzConfig(BaseModel):
    user: str | None = None
    pass_: str = Field("", alias="pass", validation_alias=__import__("pydantic").AliasChoices("pass", "pass_"))
    token: str = ""
    h5_session: str | None = Field(None, max_length=256)
    sid: str | None = None
    uid: str | None = None
    uids: str | None = None


@app.post("/api/haozhu/config")
async def haozhu_set(c: HzConfig):
    changes = c.model_dump(exclude={"pass_", "token"}, exclude_none=True)
    if c.pass_:
        changes["password"] = c.pass_
    if c.token:
        changes["token"] = c.token
    catalog = _hz_catalog_module()
    try:
        result = await asyncio.to_thread(catalog.save_config, changes)
    except catalog.CatalogError as error:
        raise HTTPException(error.status, str(error))
    log("豪猪配置已保存")
    return result


class HzSession(BaseModel):
    session: str = Field(max_length=256)


@app.post("/api/haozhu/session")
async def haozhu_session(c: HzSession):
    catalog = _hz_catalog_module()
    try:
        await asyncio.to_thread(catalog.save_config, {"h5_session": c.session})
    except catalog.CatalogError as error:
        raise HTTPException(error.status, str(error))
    return {"ok": True, "cleared": not c.session.strip(),
            "h5": await asyncio.to_thread(lambda: catalog.get_client().info())}


async def _catalog_call(method, *args, **kwargs):
    catalog = _hz_catalog_module()
    try:
        return await asyncio.to_thread(lambda: getattr(catalog.get_client(), method)(*args, **kwargs))
    except catalog.CatalogError as error:
        raise HTTPException(error.status, str(error))


@app.get("/api/haozhu/search")
async def haozhu_search(kw: str = ""):
    return {"items": await _catalog_call("projects", kw)}


@app.get("/api/haozhu/uids")
async def haozhu_uids(sid: str = "", sort: str = "default", normal_only: bool = False):
    return {"items": await _catalog_call("channels", sid, sort=sort, normal_only=normal_only)}


@app.get("/api/haozhu/my-uids")
async def haozhu_my_uids(kw: str = "", sort: str = "default", normal_only: bool = False):
    return {"items": await _catalog_call("channels", keyword=kw, mine=True, sort=sort, normal_only=normal_only)}


class HzUID(BaseModel):
    uid: str = Field(min_length=1, max_length=128)


@app.post("/api/haozhu/add-uid")
async def haozhu_add_uid(c: HzUID):
    return await _catalog_call("add_uid", c.uid)


class HzSelection(BaseModel):
    sid: str
    uids: list[str] = Field(default_factory=list, max_length=100)
    append: bool = False


@app.post("/api/haozhu/select")
async def haozhu_select(c: HzSelection):
    catalog = _hz_catalog_module()
    try:
        return catalog.select_channels(c.sid, c.uids, c.append)
    except catalog.CatalogError as error:
        raise HTTPException(error.status, str(error))


@app.get("/api/haozhu/ledger")
async def haozhu_ledger(limit: int = 100, attempt_id: str = ""):
    data = await asyncio.to_thread(PROTECTION.overview)
    data["events"] = await asyncio.to_thread(PROTECTION.ledger().events,
                                             limit=max(1, min(limit, 500)), attempt_id=attempt_id)
    return data


@app.post("/api/haozhu/refresh")
async def haozhu_refresh():
    if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
        raise HTTPException(409, "离线维护模式，余额查询使用已存快照")
    balance = await PROTECTION.snapshot("manual_refresh")
    if balance is None:
        raise HTTPException(502, "豪猪余额查询失败")
    return {"balance": balance, "updated": PROTECTION.last_snapshot}


@app.post("/api/haozhu/recover")
async def haozhu_recover():
    if ENGINE.active or state["running"] or ENGINE.stopping:
        raise HTTPException(409, "请先停止引擎，等待在途任务收尾")
    if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
        raise HTTPException(409, "离线维护模式，号码清理保持待恢复")
    results = await PROTECTION.recover()
    PROTECTION.reconcile_reservations()
    return {"results": results, "state": PROTECTION.overview()}


class BudgetReconcile(BaseModel):
    attempt_id: str = Field(min_length=1, max_length=128)
    received: bool
    note: str = Field(min_length=1, max_length=200)


@app.post("/api/haozhu/budget/reconcile")
async def budget_reconcile(req: BudgetReconcile):
    if ENGINE.active or state["running"] or ENGINE.stopping:
        raise HTTPException(409, "请先停止引擎，等待在途任务收尾")
    try:
        result = PROTECTION.reconcile_budget(req.attempt_id, req.received, req.note)
    except ValueError as error:
        raise HTTPException(409, str(error))
    for task in state["queue"]:
        if task.get("attempt_id") == req.attempt_id:
            task["budget_state"] = result["budget_state"]
    queue_save()
    return result


class BudgetAdd(BaseModel):
    amount: float = Field(gt=0, le=1000000, allow_inf_nan=False)
    note: str = Field("手动追加预算", max_length=200)


@app.post("/api/haozhu/budget")
async def budget_add(req: BudgetAdd):
    return PROTECTION.add_budget(req.amount, req.note)

# ── 凭证库 ──────────────────────────────────────────────
def _creds() -> list[dict]:
    items = []
    for f in sorted(CRED_DIR.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
            exp = d.get("auth", {}).get("expiresAt") or 0
            left = max(0, (exp / 1000 - time.time()) / 86400) if exp else 0
            items.append({
                "phone": d.get("phone", "?"), "uid": d.get("account", {}).get("uid", "?"),
                "expires": f"{left:.0f}天", "status": "valid" if left > 0 else "expired",
                "file": f.name, "path": str(f), "in_gateway": False})
        except Exception:
            continue
    return items


@app.get("/api/credentials")
async def credentials():
    items = _creds()
    # 网关在库账号
    gw_uids = set()
    try:
        tok = _gw_login()
        if tok:
            req = urllib.request.Request(GW + "/api/accounts",
                                         headers={"authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=5) as r:
                for a in json.loads(r.read().decode()).get("data", {}).get("accounts", []):
                    gw_uids.add(str(a.get("uid")))
    except Exception:
        pass
    for it in items:
        it["in_gateway"] = str(it["uid"]) in gw_uids
    return {"items": items, "in_gateway": sum(i["in_gateway"] for i in items)}


@app.post("/api/credentials/import-gateway")
async def creds_import_gw():
    sys.path.insert(0, str(TOOLS))
    from a2a_gateway import cmd_import  # type: ignore
    n = 0
    for it in _creds():
        r = subprocess.run([sys.executable, "-X", "utf8",
                            str(TOOLS / "a2a_gateway.py"), "import", it["path"]],
                           cwd=str(ROOT), capture_output=True)
        if r.returncode == 0:
            n += 1
        log(f"导入 {it['phone']}: {'OK' if r.returncode == 0 else r.stdout.decode()[:80]}")
    return {"imported": n}


@app.get("/api/credentials/export")
async def creds_export(format: str = "agent2api"):
    from credential_export import build_agent2api_export, build_credentials_export
    if format not in ("agent2api", "nested"):
        raise HTTPException(422, "导出格式只能为 agent2api 或 nested")
    builder = build_agent2api_export if format == "agent2api" else build_credentials_export
    payload = builder(CRED_DIR)
    if not payload["count"]:
        raise HTTPException(404, "没有可导出的账号凭证；请检查凭证目录和文件格式")
    filename = f"trae-{format}-{payload['count']}-{time.strftime('%Y%m%d-%H%M%S')}.json"
    return PlainTextResponse(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"',
                 "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                 "X-Credential-Count": str(payload["count"])})


# ── 代理池 ──────────────────────────────────────────────
PROXY_FILE = Path(os.environ.get("TRAE_PROXY_FILE", str(TOOLS / "proxies.txt")))


@app.get("/api/proxies")
async def proxies():
    return _proxy_snapshot()


class BulkProxies(BaseModel):
    bulk: str = ""


@app.post("/api/proxies")
async def proxies_add(b: BulkProxies):
    lines = [l.strip() for l in b.bulk.splitlines() if l.strip()]
    if not lines:
        raise HTTPException(400, "空")
    PROXY_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROXY_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"代理池已导入 {len(lines)} 条")
    return {"added": len(lines)}


@app.post("/api/proxies/check")
async def proxies_check():
    """真实连通性检查: 经每条代理出网取出口IP, 结果写入代理池状态。"""
    results = await asyncio.to_thread(get_proxy_pool().check_all)
    ok = sum(1 for r in results if r.get("ok"))
    log(f"代理健康检查: {ok}/{len(results)} 可用")
    return {"ok": True, "checked": len(results), "healthy": ok}


@app.get("/api/proxies/export", response_class=PlainTextResponse)
async def proxies_export():  # sense同款: 文本导出
    body = PROXY_FILE.read_text(encoding="utf-8") if PROXY_FILE.exists() else ""
    return PlainTextResponse(body.strip(), media_type="text/plain")


class ProxyRemove(BaseModel):
    idx: int


@app.post("/api/proxies/remove")
async def proxies_remove(req: ProxyRemove):
    lines = get_proxy_pool().lines()
    if req.idx < 0 or req.idx >= len(lines):
        raise HTTPException(404, "序号越界")
    removed = lines.pop(req.idx)
    PROXY_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROXY_FILE.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return {"ok": True, "removed": removed}


# ── 网关 ────────────────────────────────────────────────
def _gw_login() -> str:
    return GATEWAY_OPS.client.login()


@app.get("/api/gateway/state")
async def gateway_state():
    alive, accounts, models, acc_list, model_list = False, 0, 0, [], []
    try:
        with urllib.request.urlopen(GW + "/health", timeout=3) as r:
            alive = r.status == 200
        if alive:
            tok = _gw_login()
            req = urllib.request.Request(GW + "/api/accounts",
                                         headers={"authorization": "Bearer " + tok})
            with urllib.request.urlopen(req, timeout=5) as r:
                acc_list = json.loads(r.read().decode()).get("data", {}).get("accounts", [])
            accounts = len(acc_list)
            with urllib.request.urlopen(GW + "/v1/models", timeout=5) as r:
                model_list = [m.get("id") for m in json.loads(r.read().decode()).get("data", [])]
            models = len(model_list)
    except Exception:
        alive = alive or False
    return {"alive": alive, "accounts": accounts, "models": models,
            "account_list": acc_list[:6], "model_list": model_list}


@app.post("/api/gateway/start")
async def gateway_start():
    r = subprocess.run([sys.executable, "-X", "utf8",
                        str(TOOLS / "a2a_gateway.py"), "start"],
                       cwd=str(ROOT), capture_output=True)
    out = r.stdout.decode("utf-8", "replace")
    log("网关: " + out.strip())
    return {"ok": r.returncode == 0, "out": out}


# {意图：原本地直连转发（openai_models/openai_chat/openai_responses 与 _gw_json/_gw_stream）
# 已由 UpstreamService 取代（密钥策略、模型白名单、请求日志、增量流式）。
# 这里不再保留第二套 /v1 实现：它会重复注册同名路由，且其 SSE 错误帧曾用字面 "\\n\\n"
# 而不是真换行，客户端会解析失败。}



@app.get("/api/upstream/config")
async def upstream_config():
    return {"configured": bool(UPSTREAM_API_KEY or load_config().get("upstream_api_key")),
            "managed_by_env": bool(UPSTREAM_API_KEY), "base_url": "http://127.0.0.1:8898/v1"}


@app.get("/api/upstream/stats")
async def get_upstream_stats():
    return {"base_url": "http://127.0.0.1:8898/v1", **upstream_stats()}


class UpstreamKey(BaseModel):
    api_key: str = Field(min_length=16, max_length=256)


@app.post("/api/upstream/key")
async def set_upstream_key(req: UpstreamKey):
    global UPSTREAM_API_KEY
    cfg = load_config()
    cfg["upstream_api_key"] = req.api_key
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(CONFIG, cfg)
    UPSTREAM_API_KEY = req.api_key
    return {"ok": True, "configured": True}


@app.get("/api/settings")
async def settings_get():
    return load_config()


class ProgramSettings(BaseModel):
    workers: int = Field(1, ge=1, le=5)
    code_timeout_minutes: float = Field(5, gt=0, le=60)
    slider_retry: int = Field(2, ge=0, le=10)
    timeout_requeue: bool = True
    max_retries: int = Field(2, ge=0, le=10)
    task_timeout_seconds: float = Field(900, gt=0, le=7200)
    target_successes: int = Field(0, ge=0, le=100000)
    max_batch_attempts: int = Field(2000, ge=0, le=100000)
    consecutive_failures: int = Field(15, ge=0, le=10000)
    min_balance: float = Field(1.1, ge=0, le=1000000, allow_inf_nan=False)
    sms_unit_price: float = Field(1.1, gt=0, le=1000, allow_inf_nan=False)
    budget_limit: float = Field(0, ge=0, le=1000000, allow_inf_nan=False)
    retry_delay_seconds: float = Field(5, ge=0, le=600, allow_inf_nan=False)
    retry_rotate_proxy: bool = True
    blacklist_on_sms_timeout: bool = False


@app.post("/api/settings")
async def settings_set(c: ProgramSettings):
    cfg = load_config(); cfg.update(c.model_dump(exclude_unset=True))
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(CONFIG, cfg)
    log("设置已保存")
    return {"ok": True}


@app.get("/api/logs")
async def logs():
    return {"lines": log_lines[-200:]}


@app.get("/health")
async def program_health():
    return {"status": "ok", "service": "trae-registration", "version": runtime.VERSION,
            "pid": os.getpid(), "root": str(PROGRAM_DIR), "python": sys.executable,
            "data": str(runtime.DATA_DIR), "uptime_s": round(time.time() - SERVICE_STARTED),
            "instance": os.environ.get("TRAE_SERVICE_INSTANCE", "")}


from fastapi import Request


@app.post("/api/program/stop")
async def program_stop(request: Request):
    instance = os.environ.get("TRAE_SERVICE_INSTANCE", "")
    if not instance or request.headers.get("X-Service-Instance") != instance:
        raise HTTPException(403, "服务实例不匹配")
    callback = getattr(app.state, "shutdown", None)
    if callback is None:
        raise HTTPException(409, "请使用固定入口启动服务")
    asyncio.get_running_loop().call_later(0.2, callback)
    return {"ok": True}


@app.get("/")
async def index():
    # {意图：仪表盘界面改版频繁，禁用缓存避免操作员看到旧版页面。}
    return FileResponse(Path(__file__).parent / "index.html", headers={"cache-control": "no-store"})


from channel_watcher import ChannelWatcher, ChannelConfigError
from gateway_operations import GatewayOperations, GatewayError
from upstream_service import UpstreamService

WATCH_FILE = runtime.STATE_DIR / "channel-watch.json"

def _watch_config():
    return {"enabled": False, "projects": [], "interval": 300, "per_trigger": 1,
            "workers": 1, "cooldown": 86400, **load_config().get("channel_watch", {})}

async def _watch_fetch(project):
    catalog = _hz_catalog_module().get_client()
    def fetch():
        with catalog.lock:
            catalog.cache = {key: value for key, value in catalog.cache.items() if key[0] != 8}
            return catalog.channels(project["hex_sid"])
    return await asyncio.to_thread(fetch)

async def _watch_dispatch(candidate, project):
    if not _watch_config()["enabled"] or os.environ.get("TRAE_REGISTRATION_DISABLED") == "1" or state.get("halted"):
        raise ValueError("监控派发已暂停")
    if not _proxy_available():
        raise ValueError("等待可用注册代理")
    await _catalog_call("add_uid", candidate["uid"])
    count = _watch_config()["per_trigger"]
    event_id = project["run_options"]["watch_event_id"]
    existing = [t["id"] for t in state["queue"] if t.get("watch_event_id") == event_id]
    if existing:
        return existing
    tasks = []
    for _ in range(count):
        task = {"id": uuid.uuid4().hex, "phone": "", "proxy": None, "proxy_mode": "auto",
                "stage": "pending", "status": "pending", "cred": False, "added": time.time(),
                "source": "channel_watcher", "watch_event_id": event_id,
                "run_options": {"sid": project["sid"], "uid": candidate["uid"],
                                "uids": [candidate["uid"]], "sms_unit_price": candidate["price"]}}
        tasks.append(task)
    # {意图：派发先持久化，监控重启用事件 ID 找回任务，避免同一触发重复取号。}
    state["queue"].extend(tasks)
    queue_save()
    if not state["running"]:
        await engine_start()
    return [t["id"] for t in tasks]

def _watch_task(task_id):
    task = next((t for t in state["queue"] if t.get("id") == task_id), None)
    if task is None:
        return None
    status = "success" if task["status"] == "ok" else "failed" if task["status"] == "fail" else task["status"]
    costs = task.get("budget_attempts", {})
    return {"status": status, "received_cost": sum(v.get("estimated", 0) for v in costs.values()),
            "uncertain_cost": sum(v.get("uncertain", 0) for v in costs.values())}

try:
    _watch_state = json.loads(WATCH_FILE.read_text(encoding="utf-8"))
except (OSError, ValueError):
    _watch_state = {}
WATCHER = ChannelWatcher(_watch_fetch, _watch_dispatch, _watch_task, state=_watch_state,
    persist=lambda value: runtime.atomic_json(WATCH_FILE, value),
    registration_enabled=lambda: _watch_config()["enabled"] and os.environ.get("TRAE_REGISTRATION_DISABLED") != "1",
    engine_halted=lambda: state.get("halted", False))
WATCHER.configure(_watch_config())

async def channel_watch_loop():
    last_tick = 0
    while True:
        try:
            if time.monotonic() - last_tick >= _watch_config()["interval"]:
                await WATCHER.tick()
                last_tick = time.monotonic()
        except Exception as error:
            log("渠道监控本轮失败: " + type(error).__name__)
            last_tick = time.monotonic()
        await asyncio.sleep(2)

@app.get("/api/watch")
async def watch_get():
    paused = "离线维护模式，自动派发暂停" if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1" else state.get("stop_reason", "") if state.get("halted") else ""
    return {"config": _watch_config(), "state": WATCHER.snapshot(), "paused": paused}

@app.put("/api/watch")
async def watch_put(request: Request):
    raw = await request.json()
    if not isinstance(raw, dict):
        raise HTTPException(400, "规则必须是 JSON 对象")
    raw["cooldown"] = raw.get("cooldown_seconds", raw.get("cooldown", 86400))
    try:
        clean = WATCHER.validate_config(raw)
        if not 60 <= clean["interval"] <= 3600 or not 1 <= clean["per_trigger"] <= 10:
            raise ValueError("轮询间隔为60至3600秒，每次任务数为1至10")
        clean.update(enabled=bool(raw.get("enabled", False)), cooldown=raw["cooldown"])
        cfg = load_config(); cfg["channel_watch"] = clean
        _atomic_write(CONFIG, cfg)
        WATCHER.configure(clean)
    except (ValueError, TypeError) as error:
        raise HTTPException(400, str(error))
    return await watch_get()

@app.post("/api/watch/tick")
async def watch_tick():
    await WATCHER.tick()
    return await watch_get()

GATEWAY_OPS = GatewayOperations(runtime.STATE_DIR, lambda: CRED_DIR.glob("*.json"),
                               base_url=GW, config=load_config().get("gateway_operations", {}))
app.include_router(GATEWAY_OPS.router())

@app.exception_handler(GatewayError)
async def gateway_operation_error(request: Request, error: GatewayError):
    return JSONResponse({"detail": "网关操作失败，请检查3065服务或输入参数", "gateway_status": error.status}, status_code=error.status)

UPSTREAM = UpstreamService(GW, config_getter=load_config,
    master_key_getter=lambda: UPSTREAM_API_KEY or str(load_config().get("upstream_api_key", "")),
    stats_callback=lambda values: record_upstream(200 <= values[4] < 400, values[2]),
    db_path=runtime.STATE_DIR / "upstream.sqlite3")
# {意图：仅保留一个兼容协议入口，避免旧路由抢先匹配而跳过模型策略。}
app.router.routes[:] = [route for route in app.router.routes if getattr(route, "path", "") not in
                       ("/v1/models", "/v1/chat/completions", "/v1/responses")]
app.include_router(UPSTREAM.router)

@app.get("/operations.js")
async def operations_script():
    return FileResponse(Path(__file__).parent / "operations.js", media_type="text/javascript", headers={"cache-control": "no-store"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8898)
