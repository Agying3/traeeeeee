# a2a_gateway.py — headless 网关管家(不碰桌面端): 启动/登录/导入凭证/健康检查, 全 HTTP API
# 用法:
#   python a2a_gateway.py start      # 启动 headless 网关(3065, 后台, 幂等)
#   python a2a_gateway.py login      # 面板登录 → 打印 accessToken
#   python a2a_gateway.py import <credential.json>   # 导入凭证
#   python a2a_gateway.py health     # 健康检查 + 账号列表
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXE = ROOT / "agent2api/desktop-tauri/src-tauri/target/debug/agent2api-server.exe"
UI_DIR = ROOT / "agent2api/desktop-tauri/ui"
DATA_DIR = ROOT / "tmp/a2a-data"
BASE = "http://127.0.0.1:3065"

ADMIN_USER = os.environ.get("A2A_ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("A2A_ADMIN_PASS", "")


def req(method, path, body=None, token=None, timeout=15):
    r = urllib.request.Request(BASE + path, method=method)
    r.add_header("content-type", "application/json")
    if token:
        r.add_header("authorization", "Bearer " + token)
    data = json.dumps(body).encode() if body is not None else None
    try:
        with urllib.request.urlopen(r, data, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def alive():
    try:
        req("GET", "/health", timeout=3)
        return True
    except Exception:
        return False


def cmd_start():
    if alive():
        print("[OK] 网关已在跑(3065)")
        return 0
    if not EXE.exists():
        print(f"[X] 没有二进制: {EXE}")
        return 1
    env = dict(os.environ)
    env.update({
        "AGENT2API_ALLOW_NO_KEY": "1",
        "AGENT2API_ADMIN_USER": ADMIN_USER,
        "AGENT2API_ADMIN_PASSWORD": ADMIN_PASS,
        "AGENT2API_HOST": "127.0.0.1",
        "AGENT2API_PROXY_HOME": str(DATA_DIR),
        "AGENT2API_UI_DIR": str(UI_DIR),
        "AGENT2API_CAPTCHA_ENABLED": "0",  # 本机 headless, 关人机验证
    })
    subprocess.Popen(
        [str(EXE)], env=env,
        stdout=open(ROOT / "tmp/a2a-server.log", "ab"),
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS)
    for _ in range(20):
        time.sleep(1)
        if alive():
            print("[OK] 网关已启动(3065) headless, 无GUI")
            return 0
    print("[X] 启动超时 — 看 tmp/a2a-server.log")
    return 1


def cmd_login():
    st, r = req("POST", "/api/panel/login",
                {"username": ADMIN_USER, "password": ADMIN_PASS})
    tok = (r.get("data") or {}).get("accessToken") or ""
    print(tok if tok else f"[X] 登录失败 HTTP={st}: {r}")
    return 0 if tok else 1


def cmd_import(path):
    cred = json.loads(Path(path).read_text(encoding="utf-8"))
    st, lr = req("POST", "/api/panel/login",
                 {"username": ADMIN_USER, "password": ADMIN_PASS})
    tok = (lr.get("data") or {}).get("accessToken") or ""
    if not tok:
        print(f"[X] 登录失败: {lr}")
        return 1
    st, r = req("POST", "/api/accounts", cred, token=tok)
    acc = (r.get("data") or {}).get("account") or {}
    if st in (200, 201) and acc:
        print(f"[OK] 导入成功 uid={acc.get('uid')} id={str(acc.get('id'))[:20]}...")
        return 0
    print(f"[X] 导入失败 HTTP={st}: {json.dumps(r, ensure_ascii=False)[:300]}")
    return 1


def cmd_health():
    st, h = req("GET", "/health")
    st2, lr = req("POST", "/api/panel/login",
                  {"username": ADMIN_USER, "password": ADMIN_PASS})
    tok = (lr.get("data") or {}).get("accessToken") or ""
    _, accs = req("GET", "/api/accounts", token=tok)
    lst = accs.get("data", {}).get("accounts", [])
    print(f"网关: {'活' if st == 200 else '死'}  账号数: {len(lst)}")
    for a in lst:
        print(f"  {a.get('provider')} uid={a.get('uid')} "
              f"enabled={a.get('enabled')} hasRefresh={a.get('hasRefreshToken')} "
              f"expires={a.get('expiresAt')}")
    _, m = req("GET", "/v1/models")
    print(f"v1/models: {len(m.get('data', []))} 个")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "health"
    sys.exit({"start": cmd_start, "login": cmd_login,
              "import": lambda: cmd_import(sys.argv[2]),
              "health": cmd_health}.get(cmd, cmd_health)())
