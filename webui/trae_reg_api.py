# trae_reg_api.py — 注册机 WebUI 后端(FastAPI, 8898)。
# 路由域: /api/engine /api/queue /api/haozhu /api/credentials /api/proxies /api/gateway /api/settings /api/logs
# 无数据库：凭证、队列状态和配置使用 runtime 提供的数据目录。
import asyncio
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "trae.cn" / "tools"
CRED_DIR = ROOT / "tmp" / "register-credentials"
CONFIG = ROOT / "trae.cn" / "webui" / "config.json"
GW = "http://127.0.0.1:3065"

app = FastAPI(title="Trae注册机")

# ── 内存态 ──────────────────────────────────────────────
state = {"queue": [], "running": False, "done": 0, "fail": 0, "worker": None}
log_lines: list[str] = ["[WebUI] 注册机后端启动"]


def log(msg: str):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    log_lines.append(line)
    del log_lines[:-500]


def load_config() -> dict:
    if CONFIG.exists():
        return json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    return {"workers": 1, "code_timeout_minutes": 5, "slider_retry": 2,
            "timeout_requeue": True, "max_retries": 2}


# ── 队列/引擎(持久化: JSON原子写, 重启恢复 — 号是真金白银, 不能丢) ──
QUEUE_FILE = ROOT / "trae.cn" / "webui" / "queue.json"
METRICS_FILE = ROOT / "trae.cn" / "webui" / "metrics.json"
METRICS_MAX = 5000


def _atomic_write(path: Path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def queue_save():
    try:
        _atomic_write(QUEUE_FILE, {"queue": state["queue"],
                                  "done": state["done"], "fail": state["fail"]})
    except Exception:
        pass


def queue_load():
    if QUEUE_FILE.exists():
        try:
            d = json.loads(QUEUE_FILE.read_text(encoding="utf-8-sig"))
            state["queue"] = d.get("queue", [])
            state["done"] = d.get("done", 0)
            state["fail"] = d.get("fail", 0)
            # 重启后 running 任务标回 pending(可重跑)
            for t in state["queue"]:
                if t.get("status") == "running":
                    t["status"] = "pending"; t["stage"] = "重启恢复"
        except Exception:
            pass


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
            "creds": len(list(CRED_DIR.glob("*.json")))}


@app.post("/api/queue")
async def queue_add(req: RegReq):
    for _ in range(max(1, min(req.count, 10))):
        state["queue"].append({"phone": "", "proxy": req.proxy, "stage": "pending",
                                "status": "pending", "cred": False, "added": time.time()})
    log(f"队列 +{req.count} (代理#{req.proxy})")
    queue_save()
    return {"queued": req.count, "total": len(state["queue"])}


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
    t["manual_code"] = req.code.strip()
    log(f"手动输码: 任务#{req.idx} = {req.code.strip()}")
    queue_save()
    return {"ok": True}


class TaskIdx(BaseModel):
    idx: int


@app.post("/api/queue/retry")
async def queue_retry(req: TaskIdx):
    if req.idx < 0 or req.idx >= len(state["queue"]):
        raise HTTPException(404, "任务不存在")
    t = state["queue"][req.idx]
    t["status"] = "pending"; t["stage"] = "手动重试"; t["cred"] = False
    log(f"重试: 任务#{req.idx}")
    queue_save()
    return {"ok": True}


@app.post("/api/queue/remove")
async def queue_remove(req: TaskIdx):
    if 0 <= req.idx < len(state["queue"]):
        removed = state["queue"].pop(req.idx)
        log(f"移除任务: {removed.get('phone') or '未取号'}")
        queue_save()
    return {"ok": True}


@app.post("/api/engine/start")
async def engine_start():
    if state["running"]:
        return {"ok": True}
    state["running"] = True

    async def run_one(task):
        task["status"] = "running"
        started = time.time()
        cmd = [sys.executable, "-X", "utf8", str(TOOLS / "af_http.py"), str(task["proxy"])]
        try:
            task["stage"] = "af_http 全链"
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=str(ROOT), stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=900)
            text = (out or b"").decode("utf-8", "replace")
            # 从输出抓手机号/阶段(队列行更真实)
            import re
            m = re.search(r"手机号[^\d]*(1\d{10})", text)
            if m:
                task["phone"] = m.group(1)
            stages = [s for s in ("滑块通过", "凭证落袋", "短信", "mobile_ticket") if s in text]
            if stages:
                task["stage"] = "→".join(stages[-2:])
            for ln in text.splitlines()[-30:]:
                log("af: " + ln)
            took = round(time.time() - started, 1)
            if proc.returncode == 0:
                task["status"] = "ok"; task["stage"] = "凭证落袋"; task["cred"] = True
                state["done"] += 1
                metrics_save({"event": "ok", "phone": task["phone"],
                              "proxy": task["proxy"], "took_s": took})
            else:
                task["status"] = "fail"; state["fail"] += 1
                metrics_save({"event": "fail", "phone": task["phone"],
                              "proxy": task["proxy"], "took_s": took,
                              "tail": text.splitlines()[-1][:100] if text else ""})
        except Exception as e:
            task["status"] = "fail"; state["fail"] += 1
            log(f"任务异常: {e}")
            metrics_save({"event": "fail", "phone": task.get("phone", ""),
                          "proxy": task.get("proxy"), "took_s": 0, "tail": str(e)[:100]})
        queue_save()

    async def worker():
        while state["running"]:
            task = next((t for t in state["queue"] if t["status"] == "pending"), None)
            if not task:
                break
            await run_one(task)
        state["running"] = False
        log("引擎空闲停止")

    asyncio.get_event_loop().create_task(worker())
    return {"ok": True}


# ── 看门狗: 等码超时自动标记/重入队(sense同款) ──────────
async def watchdog():
    while True:
        await asyncio.sleep(60)
        cfg = load_config()
        limit = (cfg.get("code_timeout_minutes", 5)) * 60
        for t in state["queue"]:
            if t.get("status") == "running" and t.get("wait_code_since") \
                    and time.time() - t["wait_code_since"] > limit:
                t["stage"] = "等码超时"
                if cfg.get("timeout_requeue") and t.get("retries", 0) < cfg.get("max_retries", 2):
                    t["status"] = "pending"; t["retries"] = t.get("retries", 0) + 1
                    log(f"看门狗: 超时重入队(第{t['retries']}次) {t.get('phone')}")
                else:
                    t["status"] = "fail"; state["fail"] += 1
                    log(f"看门狗: 超时终局 {t.get('phone')}")
                queue_save()


@app.on_event("startup")
async def _start_watchdog():
    asyncio.get_event_loop().create_task(watchdog())


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
    state["running"] = False
    state["queue"] = [t for t in state["queue"] if t["status"] == "running"]
    queue_save()
    log("引擎停止指令")
    return {"ok": True}


# ── 豪猪(真连接: 余额/占用/项目) ────────────────────────
def _hz_client():
    sys.path.insert(0, str(TOOLS))
    import haozhu_sms as hz
    return hz


@app.get("/api/haozhu/config")
async def haozhu_get():
    try:
        hz = _hz_client()
        cfg = hz.load_config()
        balance, holding = "", 0
        try:  # 余额真数据(失败不挡页面)
            bal = hz.get_balance() if hasattr(hz, "get_balance") else None
            balance = str(bal) if bal is not None else ""
        except Exception:
            pass
        return {"user": cfg.get("user", ""), "sid": cfg.get("sid", ""),
                "balance": balance, "holding": holding}
    except Exception as e:
        return {"user": "", "sid": "", "error": str(e)[:120]}


class HzConfig(BaseModel):
    user: str = ""
    pass_: str = ""
    sid: str = ""


@app.post("/api/haozhu/config")
async def haozhu_set(c: HzConfig):
    sys.path.insert(0, str(TOOLS))
    import haozhu_sms as hz
    cfg = hz.load_config()
    cfg["user"] = c.user or cfg.get("user", "")
    cfg["sid"] = c.sid or cfg.get("sid", "")
    if c.pass_:
        cfg["pass"] = c.pass_
    hz.save_config(cfg)
    return {"ok": True}


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
PROXY_FILE = ROOT / "trae.cn" / "tools" / "proxies.txt"


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


@app.post("/api/settings")
async def settings_set(c: dict):
    cfg = load_config(); cfg.update(c)
    CONFIG.parent.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    log("设置已保存")
    return {"ok": True}


@app.get("/api/logs")
async def logs():
    return {"lines": log_lines[-200:]}


@app.get("/")
async def index():
    return FileResponse(Path(__file__).parent / "index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8898)
