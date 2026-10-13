# af_http.py — IDE 登录流程与请求留痕。
# 链路: 养fp浏览器(零交互) → HTTP send_code → 浏览器拖滑块(SDK内嵌, 唯一浏览器交互)
#       → HTTP重发拿ticket → 豪猪收码 → HTTP sms_login(IsLogin翻True的观测点) → 授权页观测 → 换证
# 抓包: 全部请求(request+response+body)落 tmp/http-trace/<run>.jsonl
# 用法: python af_http.py <代理行号> --hz-uid <对接码>
import argparse
import asyncio
import json
import sys
import time
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(ROOT / "slider-captcha-lab-main"))

import trae_pipeline_lib as lib
from stage1_browser_captcha import PROXY_FILE, solve_slider, real_click, WAIT_SECONDS
from page_diagnostics import PageDiagnostics

RUN_ID = time.strftime("%Y%m%d-%H%M%S") + "-afhttp"
RUN_DIR = ROOT / "tmp" / "register-diagnostics" / RUN_ID
TRACE = ROOT / "tmp" / "http-trace" / (RUN_ID + ".jsonl")
TRACE.parent.mkdir(parents=True, exist_ok=True)
PROFILES = ROOT / "tmp" / "cloak-profiles"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/203.0.113.1 Safari/537.36")

_trace_f = open(TRACE, "w", encoding="utf-8")

def pr(*a):
    print(*a, flush=True)

def trace(kind, **kw):
    rec = {"t": int(time.time() * 1000), "kind": kind}
    rec.update(kw)
    _trace_f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    _trace_f.flush()

def enc_phone(p):
    return "".join(f"{ord(c) ^ 0x05:02x}" for c in p)

def load_proxy(idx):
    lines = lib.load_proxies(PROXY_FILE)
    return lines[idx].split(":", 3)

def curl(method, url, idx, body=None, headers=None, timeout=30):
    """全抓包的 HTTP 请求(走代理)"""
    ip, port, user, pwd = load_proxy(idx)
    cmd = ["curl", "-sS", "-x", f"http://{user}:{pwd}@{ip}:{port}",
           "-X", method, url, "--max-time", str(timeout),
           "-w", "\n__HTTP:%{http_code}"]
    for k, v in (headers or {}).items():
        cmd += ["-H", f"{k}: {v}"]
    if body is not None:
        cmd += ["--data", body]
    r = subprocess.run(cmd, capture_output=True, timeout=timeout + 15,
                       encoding="utf-8", errors="replace")
    out = r.stdout or ""
    status = None
    if "\n__HTTP:" in out:
        out, tail = out.rsplit("\n__HTTP:", 1)
        status = tail.strip()
    trace("http", method=method, url=url, status=status, req_body=body,
          resp_body=out[:4000], stderr=(r.stderr or "")[:200])
    return status, out

def send_code_url(extra=""):
    return ("https://www.trae.cn/passport/web/send_code/"
            "?passport_jssdk_version=3.0.11&passport_jssdk_type=normal&aid=711126&language=zh"
            "&account_sdk_source=web&mix_mode=1&fixed_mix_mode=1&mobile=" + extra)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("proxy_index", type=int)
    parser.add_argument("--hz-uid", default="58562-U682TDEZW4")
    parser.add_argument("--variant", default="solo")
    args = parser.parse_args()
    idx = args.proxy_index

    from haozhu_sms import build_client
    hz = build_client({})
    hz.ensure_login()
    phone = hz.get_phone(uid=args.hz_uid)
    pr(f"[i] 豪猪取号: {phone}")
    trace("phone", phone=phone)

    ip, port, user, pwd = load_proxy(idx)
    seed = lib.fingerprint_seed_for(idx)
    profile = PROFILES / f"cloak-http-{idx}"
    profile.mkdir(parents=True, exist_ok=True)
    pr(f"[i] 代理#{idx} {ip}:{port} seed={seed} (换指纹换IP: 新profile cloak-http-{idx})")

    from cloakbrowser import launch_persistent_context_async
    ctx = await launch_persistent_context_async(
        user_data_dir=str(profile), headless=False,
        proxy=f"http://{user}:{pwd}@{ip}:{port}",
        geoip=True, humanize=True,
        args=[f"--fingerprint={seed}", "--window-size=700,800", "--disable-http2", "--lang=zh-CN"],
        locale="zh-CN",
    )
    page = ctx.pages[0] if ctx.pages else await ctx.new_page()
    diagnostics = PageDiagnostics(ctx, page, RUN_DIR, keep_open=False, report=pr,
                                  watch_tokens=("send_code", "captcha/get", "captcha/verify",
                                                "GetRefreshToken", "CheckLogin", "sms_login"))
    cdp = await ctx.new_cdp_session(page)

    # ── 阶段0: 浏览器养fp(零交互, 全程抓包只观察) ──
    pr("[P0] 打开授权页养fp(零交互)...")
    context = lib.LoginContext(args.variant, 0)
    auth_url = lib.verification_uri("https://www.trae.cn", context)
    trace("auth_url", url=auth_url)
    try:
        await page.goto(auth_url, wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
    except Exception as e:
        pr(f"[P0] 导航异常(重试): {str(e)[:80]}")
        for _ in range(3):
            await asyncio.sleep(3)
            try:
                await page.goto("about:blank", wait_until="commit", timeout=8000)
            except Exception:
                pass
            try:
                await page.goto(auth_url, wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
                break
            except Exception as e2:
                pr(f"[P0] 重试失败: {str(e2)[:80]}")
                continue
    await asyncio.sleep(5)
    # 等登录表单(fp养熟的标志)
    for _ in range(30):
        try:
            if await page.locator('input[placeholder="输入手机号"]').is_visible():
                break
        except Exception:
            pass
        await asyncio.sleep(1)
    cookies = await ctx.cookies()
    fp = next((c["value"] for c in cookies if c["name"] == "s_v_web_id"), "")
    cookie_hdr = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    pr(f"[P0] ★ fp={fp[:30]}... cookies={len(cookies)}")
    trace("p0_done", fp=fp, cookies=[c["name"] for c in cookies])

    # ── 阶段1: HTTP send_code(带养熟fp+cookies) ──
    pr("[P1] HTTP send_code...")
    hdrs = {"User-Agent": UA, "Origin": "https://www.trae.cn",
            "Referer": auth_url[:200], "Cookie": cookie_hdr,
            "Content-Type": "application/x-www-form-urlencoded"}
    body = f"mix_mode=1&mobile={enc_phone(phone)}&type=3731&is6Digits=true&fixed_mix_mode=1"
    url1 = send_code_url(f"&fp={fp}&verifyFp={fp}") if fp else send_code_url()
    st, out = curl("POST", url1, idx, body, hdrs)
    pr(f"[P1] HTTP={st} {out[:120]}")
    conf = None
    try:
        j = json.loads(out)
        d = j.get("data") or {}
        if str(d.get("error_code")) == "1105" and d.get("verify_center_decision_conf"):
            conf = json.loads(d["verify_center_decision_conf"])
    except Exception:
        pass
    if not conf:
        pr("[X] P1 未拿到 1105 挑战 — 停")
        await ctx.close(); return 1
    pr(f"[P1] ★ 1105: fp={str(conf.get('fp'))[:26]}...")

    # ── 阶段2: 同浏览器, SDK内嵌滑块(浏览器唯一交互: 拖) ──
    # SDK滑块只在页面点"获取验证码"后出现 — 纯HTTP版用 page.evaluate 触发SDK自身的发送
    # (这不是填表: 手机号已由P1的HTTP发过, 这里只是让SDK把滑块渲染出来)
    pr("[P2] 触发SDK滑块渲染(通过页面SDK API)...")
    
    # 必须挂本页监听喂它, 否则 verify 200 到了判定器也看不见
    import stage1_browser_captcha as s1
    s1.netlog = s1net = []
    s1.verdicts = s1verdicts = []

    def s1_on_response(resp):
        url = resp.url
        if resp.request.method == "OPTIONS" or not any(
                w in url for w in ("send_code", "captcha/get", "captcha/verify", "sms_login")):
            return
        entry = {"t": int(time.time() * 1000), "url": url, "status": resp.status, "body": None}
        s1net.append(entry)

        async def cap():
            try:
                entry["body"] = await asyncio.wait_for(resp.text(), timeout=20)
            except Exception as e:
                entry["body_error"] = repr(e)
            if "captcha/verify" in url:
                s1verdicts.append(entry.copy())
            trace("net", url=url, status=resp.status, body=(entry["body"] or "")[:1500])
        asyncio.ensure_future(cap())
    page.on("response", s1_on_response)

    # 授权页登录表单里把号填上(与SDK共参), 点获取验证码 → SDK出滑块
    # 注: 这是"让人机验证出现"的必要触发, 非注册动作(注册走P4 HTTP)
    await page.fill('input[placeholder="输入手机号"]', phone)
    if not await real_click(page, "我已阅读并同意", starts=True):
        pr("[P2][!] 协议勾选未点中(继续)")
    if not await real_click(page, "获取验证码"):
        pr("[P2][!] 获取验证码按钮未点中(继续)")
    await asyncio.sleep(3)

    # 等滑块iframe
    async def frame_visible():
        try:
            return await page.locator('iframe[src*="verifycenter"]').first.is_visible()
        except Exception:
            return False
    if not await diagnostics.wait_for("verification_frame", frame_visible, timeout_s=30):
        # 可能已直接success(无滑块)
        pr("[P2] 滑块iframe未现 — 可能无需滑块")
    else:
        # solve_slider 用三防线+CDP(唯一交互: 拖)
        passed, v = await solve_slider(page, ctx, cdp, diagnostics, 3)
        if not passed:
            pr("[X] 滑块未过 — 停")
            await ctx.close(); return 1
        pr("[P2] ★ 滑块通过")

    # ── 阶段3: HTTP 重发(带fp) → mobile_ticket ──
    pr("[P3] HTTP重发 send_code...")
    cookies = await ctx.cookies()
    cookie_hdr = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    hdrs["Cookie"] = cookie_hdr
    st, out = curl("POST", send_code_url(f"&fp={fp}&verifyFp={fp}"), idx, body, hdrs)
    pr(f"[P3] HTTP={st} {out[:120]}")
    ticket = ""
    try:
        ticket = (json.loads(out).get("data") or {}).get("mobile_ticket") or ""
    except Exception:
        pass
    if not ticket:
        pr("[X] P3 未拿到 mobile_ticket — 停")
        await ctx.close(); return 1
    pr(f"[P3] ★ mobile_ticket={ticket[:24]}... 短信应已下发")

    # ── 阶段4: 豪猪收码 → HTTP sms_login(观测 IsLogin 翻 True) ──
    pr("[P4] 豪猪收码...")
    sms_code = hz.poll_code(phone, rounds=60, log=pr)
    if not sms_code:
        pr("[X] 收码失败")
        await ctx.close(); return 1
    # HTTP sms_login — 用 lib 的现成实现(纯HTTP, 走代理); 返回 (parsed, resp)
    pr("[P4] HTTP sms_login...")
    ip2, port2, user2, pwd2 = load_proxy(idx)
    try:
        parsed, resp = lib.sms_login(phone, sms_code,
                                     cookies={c["name"]: c["value"] for c in cookies},
                                     csrf_token=next((c["value"] for c in cookies
                                                      if c["name"] == "passport_csrf_token"), None),
                                     proxies={"http": f"http://{user2}:{pwd2}@{ip2}:{port2}",
                                              "https": f"http://{user2}:{pwd2}@{ip2}:{port2}"})
        trace("sms_login_raw", status=resp.status_code, body=resp.text[:2500],
              set_cookie=resp.headers.get("Set-Cookie", "")[:1500])
        login_cookies = lib.extract_login_cookies(resp)
        ok = str(parsed.get("message", "")) == "success"
        pr(f"[P4] sms_login HTTP={resp.status_code} message={parsed.get('message')} "
           f"Set-Cookie键={len(login_cookies)}")
        if not ok:
            pr(f"[P4][!] body: {resp.text[:200]}")
        trace("sms_login", ok=ok, message=str(parsed.get("message"))[:60],
              cookie_keys=list(login_cookies.keys())[:30])
        if not login_cookies:
            pr("[X] 无登录cookie — 停")
            await ctx.close(); return 1
    except Exception as e:
        pr(f"[X] sms_login 异常: {str(e)[:130]}")
        trace("sms_login_error", error=str(e)[:300])
        await ctx.close(); return 1

    # ── 阶段5: 授权页观测(IsLogin 真值) + 换证 ──
    pr("[P5] 授权页观测(IsLogin)...")
    cookies = await ctx.cookies()
    # 把 sms_login 的 cookie 注入浏览器(观测用)
    add = [{"name": k, "value": v, "domain": ".trae.cn", "path": "/"}
           for k, v in login_cookies.items()]
    try:
        await ctx.add_cookies(add)
    except Exception as e:
        pr(f"[P5] 注入cookie异常: {e}")
    
    islogin = None
    netcap = []
    async def on_resp(resp):
        url = resp.url
        if not any(w in url for w in ("CheckLogin", "GetRefreshToken", "oauth", "login")):
            return
        async def cap():
            try:
                b = await asyncio.wait_for(resp.text(), timeout=20)
            except Exception:
                return
            netcap.append({"url": url, "status": resp.status, "body": b[:3000]})
            trace("net", url=url, status=resp.status, body=b[:2000])
            if "CheckLogin" in url:
                try:
                    j2 = json.loads(b)
                    nonlocal islogin
                    islogin = j2.get("Result", {}).get("IsLogin")
                    pr(f"[P5] CheckLogin: OID={j2.get('ResponseMetadata', {}).get('OID')} IsLogin={islogin}")
                except Exception:
                    pass
        asyncio.ensure_future(cap())
    page.on("response", on_resp)
    try:
        await page.goto("about:blank")
        await page.goto(auth_url, wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
    except Exception as e:
        pr(f"[P5] 导航异常: {str(e)[:80]}")
    # 等到 CheckLogin 出结果(最多18s)
    for _ in range(18):
        if islogin is not None:
            break
        await asyncio.sleep(1)
    await asyncio.sleep(4)

    pr(f"[P5] IsLogin={islogin}")
    trace("p5_verdict", islogin=islogin)

    # 若 IsLogin=True → 授权页应出现"同意" → 页面自己走换证(抓包)
    if islogin:
        pr("[P5] ★ IsLogin=True — 找授权按钮/等GetRefreshToken...")
        for i in range(30):
            await asyncio.sleep(1)
            if any("GetRefreshToken" in n["url"] for n in netcap):
                break
    else:
        pr("[P5] IsLogin 非 True — 记录后收尾(纯HTTP观测完成)")

    # 快照收尾
    try:
        await diagnostics.snapshot("final")
    except Exception:
        pass
    await ctx.close()
    pr("[i] 完成. 抓包: " + str(TRACE))
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
