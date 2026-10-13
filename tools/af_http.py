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

    
    # 候选替代(未定案, 下一步实验对象):
    #   a) page.evaluate 直接调页面 SDK 的发送函数(SDK 自渲染滑块, 零表单操作)
    #   b) P1 的 1105 conf 已到手 — 把验证 iframe 的 URL 构造出来后 navigate,
    #      SDK 在授权页上下文内自行渲染(需验证 5014 上下文绑定)
    # await page.fill('input[placeholder="输入手机号"]', phone)
    # if not await real_click(page, "我已阅读并同意", starts=True):
    #     pr("[P2][!] 协议勾选未点中(继续)")
    # if not await real_click(page, "获取验证码"):
    #     pr("[P2][!] 获取验证码按钮未点中(继续)")
    # await asyncio.sleep(3)
    pr("[P2][!] 填表触发已按铁律禁用 — 当前无合法滑块触发方式, 流程到此为止")
    pr("[P2][i] 待实验: SDK evaluate 触发 或 授权页上下文 iframe URL 直航")
    await ctx.close(); return 1

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

    # ── 阶段4: 豪猪收码 → 页内 fetch sms_login（授权页上下文与浏览器 cookie） ──
    pr("[P4] 豪猪收码...")
    sms_code = hz.poll_code(phone, rounds=60, log=pr)
    if not sms_code:
        pr("[X] 收码失败")
        await ctx.close(); return 1
    
    pr("[P4] 页内 fetch sms_login(授权页上下文)...")
    try:
        r_inpage = await page.evaluate(
            """async ({mob, code, csrf}) => {
              return fetch('https://www.trae.cn/passport/web/sms_login/?passport_jssdk_version=3.0.11&passport_jssdk_type=normal&aid=711126&language=zh&account_sdk_source=web&mix_mode=1&fixed_mix_mode=1',
                {method:'POST', credentials:'include',
                 headers:{'content-type':'application/x-www-form-urlencoded',
                          'x-tt-passport-csrf-token':csrf},
                 body:'mix_mode=1&mobile='+mob+'&code='+code+'&fixed_mix_mode=1'})
               .then(r=>r.text()).catch(e=>'ERR:'+e);
            }""",
            {"mob": enc_phone(phone), "code": enc_phone(sms_code),
             "csrf": next((c["value"] for c in cookies if c["name"] == "passport_csrf_token"), "")})
        pr(f"[P4] 页内sms_login: {str(r_inpage)[:130]}")
        trace("sms_login_inpage", body=str(r_inpage)[:1500])
        ok = "success" in str(r_inpage)
        if not ok:
            pr("[X] 页内sms_login未success — 停")
            await ctx.close(); return 1
    except Exception as e:
        pr(f"[X] 页内sms_login异常: {str(e)[:130]}")
        trace("sms_login_error", error=str(e)[:300])
        await ctx.close(); return 1
    await asyncio.sleep(2)
    cookies = await ctx.cookies()
    login_cookies = {c["name"]: c["value"] for c in cookies}
    pr(f"[P4] ★ 登录cookie在浏览器jar里: {len(cookies)}个(含sessionid={('sessionid' in login_cookies)})")

    # ── 阶段5: 纯HTTP GetPCAuthCode → ExchangeToken → 导出(发现②: 参数全自知) ──
    
    # 它由授权页JS调 CheckLogin 后种下 — 注入登录cookie后静开一次授权页收集之
    pr("[P5a] 注入cookie → 静开授权页收集 X-Cloudide-Session...")
    add = [{"name": k, "value": v, "domain": ".trae.cn", "path": "/"}
           for k, v in login_cookies.items()]
    try:
        await ctx.add_cookies(add)
    except Exception as e:
        pr(f"[P5a] 注入cookie异常: {e}")
    for nav_try in range(3):
        try:
            await page.goto("about:blank", wait_until="commit", timeout=8000)
        except Exception:
            pass
        try:
            await page.goto(auth_url, wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
            break
        except Exception as e:
            pr(f"[P5a] 导航重试{nav_try + 1}: {str(e)[:70]}")
            await asyncio.sleep(3)
    await asyncio.sleep(8)
    cookies = await ctx.cookies()
    trace("p5a_cookies", names=[c["name"] for c in cookies])
    has_xcs = any(c["name"] == "X-Cloudide-Session" for c in cookies)
    pr(f"[P5a] X-Cloudide-Session={'★有' if has_xcs else '✗无'} cookies={len(cookies)}")
    # IsLogin 真值观测(顺手): 页内 fetch CheckLogin, 带全部当前cookie
    try:
        r_cl = await page.evaluate(
            "() => fetch('https://api.trae.cn/cloudide/api/v3/trae/CheckLogin',"
            "{method:'POST', credentials:'include',"
            "headers:{'content-type':'application/json'}, body:'{}'})"
            ".then(r=>r.text()).catch(e=>'ERR')")
        m_oid = __import__("re").search(r'"OID\\?":\\?"(\d+)', str(r_cl))
        m_il = __import__("re").search(r'"IsLogin\\?":\\?(\w+)', str(r_cl))
        pr(f"[P5a] 页内CheckLogin: OID={m_oid.group(1) if m_oid else '?'} "
           f"IsLogin={m_il.group(1) if m_il else '?'}")
        trace("p5a_checklogin", body=str(r_cl)[:600])
    except Exception as e:
        pr(f"[P5a] 页内CheckLogin失败: {str(e)[:80]}")

    pr("[P5b] 纯HTTP GetPCAuthCode...")
    all_cookies = {c["name"]: c["value"] for c in cookies}
    all_cookies.update(login_cookies)
    cookie_hdr2 = "; ".join(f"{k}={v}" for k, v in all_cookies.items())
    gpc_body = json.dumps({
        "ClientID": lib.client_id_for(args.variant),
        "CodeChallenge": context.code_challenge,
        "CodeChallengeMethod": "S256",
        "DeviceID": context.device_id,
        "PlatformCode": "SOLO_PC",
    })
    gpc_hdrs = {"User-Agent": UA, "Origin": "https://www.trae.cn",
                "Referer": auth_url[:200], "Cookie": cookie_hdr2,
                "Content-Type": "application/json"}
    st, out = curl("POST", "https://api.trae.cn/cloudide/api/v3/trae/oauth/GetPCAuthCode",
                   idx, gpc_body, gpc_hdrs)
    pr(f"[P5] GetPCAuthCode HTTP={st} {out[:150]}")
    auth_code = ""
    try:
        j3 = json.loads(out)
        res = j3.get("Result") or j3.get("data") or j3
        auth_code = res.get("AuthCode") or res.get("authCode") or ""
    except Exception:
        pass
    if not auth_code:
        pr("[X] 未拿到 AuthCode — 停(响应留痕在trace, 诊断头/cookie问题)")
        try:
            await diagnostics.snapshot("gpc_fail")
        except Exception:
            pass
        await ctx.close(); return 1
    pr(f"[P5] ★ AuthCode={auth_code[:24]}... (10分钟内换证)")

    # ── P6: ExchangeToken → GetUserInfo → agent2api 导出 ──
    pr("[P6] ExchangeToken...")
    import requests as rq
    sess = rq.Session()
    sess.cookies.update(all_cookies)
    sess.proxies = {"http": f"http://{user2}:{pwd2}@{ip2}:{port2}",
                    "https": f"http://{user2}:{pwd2}@{ip2}:{port2}"}
    try:
        exchange = lib.exchange_auth_code(context, auth_code, lib.DEFAULT_LOGIN_HOST,
                                          session=sess, trace=None)
        pr("[P6] ★ ExchangeToken success")
    except Exception as e:
        pr(f"[X] ExchangeToken 失败: {str(e)[:150]}")
        await ctx.close(); return 1
    identity = lib.get_user_info(exchange["access"], session=sess, trace=None)
    credential = lib.build_agent2api_credential(context, exchange, identity, args.variant)
    credential["phone"] = phone
    out_dir = ROOT / "tmp" / "register-credentials"
    out_dir.mkdir(parents=True, exist_ok=True)
    outp = out_dir / f"{RUN_ID}.json"
    outp.write_text(json.dumps(credential, ensure_ascii=False, indent=1), encoding="utf-8")
    pr(f"\n[完成] ★★ 注册号IDE凭证: {outp}")
    pr(f"[完成] uid={identity.get('uid')} phone={phone} expires={exchange.get('expires_ms')}")
    trace("credential_done", phone=phone, uid=identity.get("uid"), path=str(outp))
    try:
        await ctx.close()
    except Exception:
        pass
    return 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
