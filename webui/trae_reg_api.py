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
import time
import urllib.request
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import runtime

PROGRAM_DIR = Path(__file__).resolve().parents[1]
MANAGED = bool(os.environ.get("TRAE_DATA_DIR"))
TOOLS = PROGRAM_DIR / "tools"
CRED_DIR = runtime.CRED_DIR if MANAGED else ROOT / "tmp" / "register-credentials"
CONFIG = runtime.CONFIG_DIR / "webui.json" if MANAGED else Path(__file__).parent / "config.json"
GW = os.environ.get("A2A_BASE_URL", "http://127.0.0.1:3065").rstrip("/")
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
    count: int = 1
    proxy: int = 19


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
    for _ in range(max(1, min(req.count, 10))):
        state["queue"].append({"phone": "", "proxy": req.proxy, "stage": "pending",
                                "status": "pending", "cred": False, "added": time.time()})
    log(f"队列 +{req.count} (代理#{req.proxy})")
    queue_save()
    return {"queued": req.count, "total": len(state["queue"])}


class BatchReq(BaseModel):
    proxy_start: int = 19
    proxy_end: int = 19


@app.post("/api/queue/batch")
async def queue_batch(req: BatchReq):
    """一键批量入队: 代理 #start..#end 每号一条任务(等价于逐个点加入队列)。"""
    if req.proxy_start < 0 or req.proxy_end < req.proxy_start:
        raise HTTPException(400, "代理范围无效")
    count = req.proxy_end - req.proxy_start + 1
    if count > 500:
        raise HTTPException(400, "单次最多批量 500 条")
    for p in range(req.proxy_start, req.proxy_end + 1):
        state["queue"].append({"phone": "", "proxy": p, "stage": "pending",
                                "status": "pending", "cred": False, "added": time.time()})
    log(f"批量入队 +{count} (代理#{req.proxy_start}~#{req.proxy_end})")
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


def _proxy_indices():
    if not PROXY_FILE.exists():
        return []
    return list(range(sum(1 for line in PROXY_FILE.read_text(encoding="utf-8-sig").splitlines()
                          if line.strip() and not line.strip().startswith("#"))))


PROTECTION = RegistrationProtection(runtime.STATE_DIR, load_config, lambda: _hz_summary_safe(), log,
                                    lambda: _hz_client().load_credentials(), _proxy_indices)

ENGINE = RegistrationEngine(state, load_config, queue_save, log, metrics_save,
                            lambda task: _af_cmd(task),
                            lambda proc: _terminate_registration(proc), runtime.RUN_DIR, PROGRAM_DIR,
                             protection=PROTECTION, proxy_indices=_proxy_indices)


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
                             asyncio.create_task(haozhu_keepalive())])
    log("注册机服务就绪")


@app.on_event("shutdown")
async def _shutdown_program():
    for task in BACKGROUND_TASKS:
        task.cancel()
    await asyncio.gather(*BACKGROUND_TASKS, return_exceptions=True)
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
    if not PROXY_FILE.exists():
        return 0
    return sum(1 for ln in PROXY_FILE.read_text(encoding="utf-8").splitlines()
               if ln.strip() and not ln.strip().startswith("#"))


def _next_proxy() -> str:
    """代理轮转: 一号一IP轮着用(换指纹换IP约束, cursor 跨重启保留)。空池返回 ''。"""
    n = _proxy_total()
    if n <= 0:
        return ""
    idx = cont["proxy_cursor"] % n
    cont["proxy_cursor"] += 1
    return str(idx)


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
        state["queue"].append({"phone": "", "proxy": _next_proxy(), "stage": "pending",
                               "status": "pending", "cred": False, "added": time.time()})
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
    return {"config": c, "added_today": cont["added"], "date": cont["date"],
            "paused": cont["paused"], "balance": cont["balance"],
            "queue_active": active, "proxy_total": _proxy_total(),
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

    asyncio.get_event_loop().create_task(worker())
    return {"ok": True}


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
async def creds_export():
    items = _creds()
    body = json.dumps([json.loads(Path(i["path"]).read_text(encoding="utf-8"))
                       for i in items], ensure_ascii=False, indent=1)
    return PlainTextResponse(body, media_type="application/json")


# ── 代理池 ──────────────────────────────────────────────
PROXY_FILE = Path(os.environ.get("TRAE_PROXY_FILE", str(TOOLS / "proxies.txt")))


@app.get("/api/proxies")
async def proxies():
    items = []
    if PROXY_FILE.exists():
        for i, ln in enumerate(PROXY_FILE.read_text(encoding="utf-8").splitlines()):
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                items.append({"idx": i, "line": ln, "ok": None, "used": 0})
    return {"items": items}


class BulkProxies(BaseModel):
    bulk: str = ""


@app.post("/api/proxies")
async def proxies_add(b: BulkProxies):
    lines = [l.strip() for l in b.bulk.splitlines() if l.strip()]
    if not lines:
        raise HTTPException(400, "空")
    PROXY_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROXY_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"added": len(lines)}


@app.post("/api/proxies/check")
async def proxies_check():
    return {"ok": True, "note": "健康检查走 af_http 出网自动观测"}


@app.get("/api/proxies/export", response_class=PlainTextResponse)
async def proxies_export():  # sense同款: 文本导出
    body = PROXY_FILE.read_text(encoding="utf-8") if PROXY_FILE.exists() else ""
    return PlainTextResponse(body.strip(), media_type="text/plain")


class ProxyRemove(BaseModel):
    idx: int


@app.post("/api/proxies/remove")
async def proxies_remove(req: ProxyRemove):
    if not PROXY_FILE.exists():
        raise HTTPException(404, "无代理文件")
    lines = [l for l in PROXY_FILE.read_text(encoding="utf-8").splitlines()
             if l.strip() and not l.strip().startswith("#")]
    if req.idx < 0 or req.idx >= len(lines):
        raise HTTPException(404, "序号越界")
    removed = lines.pop(req.idx)
    PROXY_FILE.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return {"ok": True, "removed": removed}


# ── 网关 ────────────────────────────────────────────────
def _gw_login() -> str:
    body = json.dumps({"username": "admin", "password": ""}).encode()
    req = urllib.request.Request(GW + "/api/panel/login", data=body,
                                 headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        d = json.loads(r.read().decode())
    return (d.get("data") or {}).get("accessToken") or ""


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


# ── 设置/日志 ───────────────────────────────────────────
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
    return FileResponse(Path(__file__).parent / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8898)
