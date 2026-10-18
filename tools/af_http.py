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
import os

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, os.environ.get("TRAE_SLIDER_LIB", str(pathlib.Path(__file__).resolve().parents[1] / "vendor" / "slider")))

import trae_pipeline_lib as lib
from stage1_browser_captcha import PROXY_FILE, solve_slider, WAIT_SECONDS
from page_diagnostics import PageDiagnostics
from registration_control import emit, options, public_log, wait_sms

DATA_DIR = pathlib.Path(os.environ.get("TRAE_DATA_DIR", str(pathlib.Path(__file__).resolve().parents[1] / "data")))
RUN_ID = os.environ.get("TRAE_RUN_ID") or (time.strftime("%Y%m%d-%H%M%S") + "-afhttp-" + __import__("uuid").uuid4().hex[:8])
RUN_DIR = DATA_DIR / "register-diagnostics" / RUN_ID
TRACE = DATA_DIR / "http-trace" / (RUN_ID + ".jsonl")
PROFILES = DATA_DIR / "cloak-profiles"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36")

_trace_f = None

# The worker must settle every acquired SMS number, including abrupt failures.
_ACTIVE_HZ = None
_ACTIVE_PHONE = ""
_ACTIVE_ATTEMPT = os.environ.get("TRAE_ATTEMPT_ID", "")
_SMS_SENT = False
_RUN_SUCCESS = False


def _set_number_state(hz, phone):
    global _ACTIVE_HZ, _ACTIVE_PHONE, _SMS_SENT, _RUN_SUCCESS, _ACTIVE_ATTEMPT
    _ACTIVE_HZ = hz
    _ACTIVE_PHONE = phone
    _ACTIVE_ATTEMPT = hz.attempt_id or _ACTIVE_ATTEMPT or __import__("uuid").uuid4().hex
    _SMS_SENT = False
    _RUN_SUCCESS = False


def _mark_sms_sent():
    global _SMS_SENT
    _SMS_SENT = True
    if _ACTIVE_HZ and _ACTIVE_ATTEMPT:
        _ACTIVE_HZ._record("sms_dispatched", phone=_ACTIVE_PHONE, sms_sent=True, state="running")


def _mark_run_success():
    global _RUN_SUCCESS
    _RUN_SUCCESS = True
    if _ACTIVE_HZ and _ACTIVE_ATTEMPT:
        _ACTIVE_HZ._record("credential_saved", phone=_ACTIVE_PHONE, credential_saved=True, state="completed")


async def _settle_number():
    global _ACTIVE_HZ, _ACTIVE_PHONE, _SMS_SENT, _RUN_SUCCESS
    hz, phone, attempt = _ACTIVE_HZ, _ACTIVE_PHONE, _ACTIVE_ATTEMPT
    if not hz or not phone or not attempt:
        return
    try:
        from haozhu_sms import finish_attempt
        allow_blacklist = os.environ.get("TRAE_HAOZHU_BLACKLIST", "0").lower() in ("1", "true", "yes")
        result = finish_attempt(attempt, ledger=hz.ledger, client=hz, run_id=RUN_ID,
                                allow_blacklist=allow_blacklist, reason=("success" if _RUN_SUCCESS else "worker_exit"))
        if result.get("ok"):
            pr(f"[i] 豪猪号 {phone} 已处置: {result.get('state')}")
        else:
            pr(f"[!] 豪猪号 {phone} 处置待恢复: {result.get('error_category') or result.get('skipped')}")
    except Exception as exc:
        pr(f"[!] 豪猪号处置记录失败: {type(exc).__name__}")
    finally:
        _ACTIVE_HZ = None
        _ACTIVE_PHONE = ""
        _SMS_SENT = False
        _RUN_SUCCESS = False

def pr(*a):
    message = public_log(" ".join(map(str, a)))
    print(message, flush=True)
    if message.startswith(("[P", "[完成]")):
        emit(message[:140], status="running")

def trace(kind, **kw):
    global _trace_f
    from haozhu_ledger import _redact
    # {意图：抓包原文只入独立证据；常规轨迹和进度仅保留脱敏摘要。}
    if _ACTIVE_HZ:
        _ACTIVE_HZ._record("trace_" + str(kind), raw_response=kw)
    if _trace_f is None:
        TRACE.parent.mkdir(parents=True, exist_ok=True)
        _trace_f = open(TRACE, "a", encoding="utf-8")
    rec = {"t": int(time.time() * 1000), "kind": kind}
    rec.update(_redact(kw))
    for key, value in list(rec.items()):
        if key in ("body", "req_body", "resp_body", "response", "stderr"):
            rec[key] = "[evidence]"
        elif isinstance(value, str):
            rec[key] = public_log(value)
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
          resp_body=out, stderr=(r.stderr or "")[:200])
    return status, out

def send_code_url(extra=""):
    return ("https://www.trae.cn/passport/web/send_code/"
            "?passport_jssdk_version=3.0.11&passport_jssdk_type=normal&aid=711126&language=zh"
            "&account_sdk_source=web&mix_mode=1&fixed_mix_mode=1&mobile=" + extra)

async def _main_impl():
    parser = argparse.ArgumentParser()
    parser.add_argument("proxy_index", type=int)
    parser.add_argument("--hz-uid", default="")
    parser.add_argument("--variant", default="solo")
    parser.add_argument("--phone", default="", help="Reuse an already acquired SMS session")
    args = parser.parse_args()
    if args.phone and not __import__("re").fullmatch(r"1[0-9]{10}", args.phone):
        parser.error("Invalid phone")
    idx = args.proxy_index

    from haozhu_sms import build_client
    from haozhu_ledger import Ledger, receipt_hash
    options = json.loads(os.environ.get("TRAE_RUN_OPTIONS", "{}"))
    sms_options = {key: options[key] for key in ("sid", "uid", "uids") if key in options}
    hz = build_client(sms_options, ledger=Ledger(), attempt_id=_ACTIVE_ATTEMPT, run_id=RUN_ID)
    hz.begin_attempt(proxy={"index": idx})
    _set_number_state(hz, "")
    hz.ensure_login()
    phone = (hz.adopt_phone(args.phone, from_attempt_id=os.environ.get("TRAE_REUSE_ATTEMPT_ID", ""))["phone"]
             if args.phone else hz.get_phone_multi(uid=args.hz_uid))
    _set_number_state(hz, phone)
    pr(f"[i] {'复用已取号码' if args.phone else '豪猪取号'}: {phone}")
    trace("phone", phone=phone)
    emit("号码已取得", status="running", phone=phone, attempt_id=hz.attempt_id,
         run_id=hz.run_id, sid=hz.sid, uid=hz.uid, account=hz.user)

    ip, port, user, pwd = load_proxy(idx)
    seed = lib.fingerprint_seed_for(idx)
    profile_tag = os.environ.get("TRAE_PROFILE_TAG", "")
    profile = PROFILES / (f"cloak-http-{idx}-" + profile_tag if profile_tag else f"cloak-http-{idx}")
    profile.mkdir(parents=True, exist_ok=True)
    pr(f"[i] 代理#{idx} {ip}:{port} seed={seed} (换指纹换IP: 新profile cloak-http-{idx})")

    from cloakbrowser import launch_persistent_context_async
    ctx = await launch_persistent_context_async(
        user_data_dir=str(profile), headless=os.environ.get("TRAE_HEADLESS", "0") == "1",
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

    # ── 浏览器交互与请求流程的诊断步骤 ──
    pr("[P0] 打开授权页养fp(零交互)...")
    context = lib.LoginContext(args.variant, 0)
    # context 落盘（PKCE code_verifier 是私有的，P6 换证必需；异常退出后仍需保留上下文）。
    try:
        (DATA_DIR / "http-trace" / f"{RUN_ID}-context.json").write_text(
            json.dumps(context.to_json(), ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
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
    hz._record("send_requested", phone=phone, stage="first_send", state="running")
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
    # #1「重发请求缺少挑战指纹会再次得到 1105」{意图：使用 P1 返回的挑战 fp 贯穿滑块和重发；依据：请求链路验证。}
    fp = str(conf.get("fp") or fp)
    pr(f"[P1] ★ 1105: fp={fp[:26]}...")

    # ── 阶段2: 同浏览器, SDK内嵌滑块(浏览器唯一交互: 拖) ──
    # SDK滑块只在页面点"获取验证码"后出现 — 纯HTTP版用 page.evaluate 触发SDK自身的发送
    # (这不是填表: 手机号已由P1的HTTP发过, 这里只是让SDK把滑块渲染出来)
    pr("[P2] 触发SDK滑块渲染(通过页面SDK API)...")
    # 接线约束：solve_slider 的哨兵读取 stage1.netlog/verdicts，
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

    # 浏览器仅负责滑块交互；手机号、协议和验证码请求走 HTTP。
    # 页面填表路径已移除，由 P1 的 send_code 和滑块模板流程替代。

    # ── P2-b: 真实URL模板拼滑块页, SDK自渲染 ──
    pr("[P2b] 拼真实模板滑块URL(SDK自渲染)...")
    from urllib.parse import quote
    # 浏览器环境参数取自当前页面 evaluate 结果，异常时使用兼容默认值。
    env_js = await page.evaluate(
        "() => JSON.stringify({"
        "screen: {w: window.screen.width, h: window.screen.height},"
        "browser: {w: window.outerWidth, h: window.outerHeight},"
        "page: {w: window.innerWidth, h: window.innerHeight},"
        "document: {width: document.documentElement.clientWidth},"
        "product_host: 'www.trae.cn', vc_version: '1.0.0.428',"
        f"maskTime: Date.now(), h5_check_version: '4.0.4'}})")
    try:
        env_obj = json.loads(env_js)
    except Exception:
        env_obj = {"screen": {"w": 1920, "h": 1080}, "browser": {"w": 700, "h": 800},
                   "page": {"w": 684, "h": 619}, "document": {"width": 684},
                   "product_host": "www.trae.cn", "vc_version": "1.0.0.428",
                   "maskTime": int(time.time() * 1000), "h5_check_version": "4.0.4"}
    # verify_data = 1105 conf 原文(P1 HTTP 响应自带 fp/verify_ticket/detail/log_id)
    verify_data = json.dumps(conf, ensure_ascii=False, separators=(",", ":"))
    slider_url = ("https://rmc.bytedance.com/verifycenter/captcha/v2"
                  "?from=iframe"
                  f"&fp={quote(str(conf.get('fp') or fp), safe='')}"
                  f"&env={quote(json.dumps(env_obj, separators=(',', ':')), safe='')}"
                  "&aid=711126&scene_level=p2"
                  "&host=https%3A%2F%2Fverify.zijieapi.com"
                  "&lang=zh"
                  f"&verify_data={quote(verify_data, safe='')}"
                  "&extraConfig=%7B%7D")
    pr(f"[P2b] slider_url前200: {slider_url[:200]}")
    trace("slider_url", url=slider_url)

    # iframe 注入形态学复刻(20:51成功样例): 授权页保持活跃(产品页JS/埋点持续运行,
    # 风控词典5014条: 拖动期间需要产品页行为信号), 滑块 iframe 从授权页注入并自初始化
    # — 与 hf3 死例的区别: URL 是逐字节真模板(20:51 events[152]), SDK 前置序列由页面自拉
    slider_iframe_id = "hf-p2b-slider"
    await page.evaluate(
        """(url) => {
            document.querySelectorAll('#hf-p2b-slider').forEach(e => e.remove());
            const f = document.createElement('iframe');
            f.id = 'hf-p2b-slider';
            f.src = url;
            f.style.cssText = 'position:fixed;left:30px;top:80px;width:340px;height:420px;'
                              + 'z-index:99999;border:1px solid #ccc;background:#fff;';
            document.body.appendChild(f);
        }""", slider_url)
    pr("[P2b] iframe已注入授权页(父页保持活跃), 等SDK自渲染...")
    await asyncio.sleep(6)

    # 等滑块渲染(SDK iframe 内自渲染; 顶层特征词做兜底)
    async def slider_visible():
        for sel in ('#hf-p2b-slider', 'iframe[src*="verifycenter"]', 'text=按住左边按钮'):
            try:
                if await page.locator(sel).first.is_visible():
                    return True
            except Exception:
                continue
        return False
    if not await diagnostics.wait_for("slider_visible", slider_visible, timeout_s=30):
        pr("[X] P2b 滑块未渲染 — 注入失败(留痕已存)")
        await diagnostics.snapshot("p2b_noslider")
        await ctx.close(); return 1
    pr("[P2b] ★ 滑块已渲染(iframe注入, 授权页活跃, 零表单) — 拖")

    # 顶层渲染模式: 无 verifycenter iframe, solve_slider 的 iframe 定位需兼容
    # 真实文案"按住左边按钮拖动完成上方拼图"与 solve_slider 预期一致即可拖
    async def frame_visible():
        return await slider_visible()
    if not await diagnostics.wait_for("verification_frame", frame_visible, timeout_s=30):
        # 可能已直接success(无滑块)
        pr("[P2] 滑块iframe未现 — 可能无需滑块")
    else:
        # solve_slider 用三防线+CDP(唯一交互: 拖)
        passed, v = await solve_slider(page, ctx, cdp, diagnostics, 1 + int(options().get("slider_retry", 2)))
        if not passed:
            pr("[X] 滑块未过 — 停")
            await ctx.close(); return 1
        pr("[P2] ★ 滑块通过")

    # ── 阶段3: HTTP 重发(带fp) → mobile_ticket ──
    pr("[P3] HTTP重发 send_code...")
    cookies = await ctx.cookies()
    cookie_hdr = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    hdrs["Cookie"] = cookie_hdr
    hz._record("send_requested", phone=phone, stage="retry_send", state="running")
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
    _mark_sms_sent()
    hz._record("send_ticket", phone=phone, ticket=receipt_hash(ticket), sms_sent=True, state="running")
    pr(f"[P3] ★ mobile_ticket={ticket[:24]}... 短信应已下发")

    # ── 阶段4: 豪猪收码 → 页内 fetch sms_login（授权页上下文与浏览器 cookie） ──
    pr("[P4] 豪猪收码...")
    try:
        sms_code = await wait_sms(hz, phone)
    except TimeoutError:
        await ctx.close()
        return 2
    if not sms_code:
        pr("[X] 收码失败")
        await ctx.close(); return 1
    # 页内 fetch：复用浏览器 cookie 和 fp 链，保持页面上下文一致。
    pr("[P4] 页内 fetch sms_login(授权页上下文)...")
    hz._record("sms_login_requested", phone=phone, state="running")
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
    hz._record("post_login", phone=phone, state="logged_in")
    await asyncio.sleep(2)
    cookies = await ctx.cookies()
    login_cookies = {c["name"]: c["value"] for c in cookies}
    pr(f"[P4] ★ 登录cookie在浏览器jar里: {len(cookies)}个(含sessionid={('sessionid' in login_cookies)})")

    # ── 阶段4.5: cloudide Login — 设置 X-Cloudide-Session ──
    # 样例: POST api.trae.cn/cloudide/api/v3/trae/Login?type=sms&msToken=<页面msToken>&a_bogus=...
    # body: {UtmSource:"",...,LoginChannel:"native_ide",LoginPlatform:"solo"}
    # msToken 从页面 window 取(byted_acrawler/webmssdk 惯例); a_bogus 由页内 SDK 生成
    # → 页内 fetch 执行, 让页面的签名 SDK 自动补齐(零表单, 上下文请求)
    pr("[P4.5] 页内 fetch cloudide Login(种 X-Cloudide-Session)...")
    try:
        r_login = await page.evaluate(
            """async () => {
              // msToken: 页面全局变量(字节系 webmssdk 惯例), 找不到就从 cookie/URL 里捞
              let ms = window.msToken || '';
              if (!ms) { try { ms = window.byted_acrawler && window.byted_acrawler.getConfig
                        && window.byted_acrawler.getConfig().msToken || ''; } catch(e) {} }
              if (!ms) { const m = document.cookie.match(/msToken=([^;]+)/); ms = m ? m[1] : ''; }
              const body = {UtmSource:"",UtmMedium:"",UtmCampaign:"",UtmTerm:"",UtmContent:"",
                            BDVID:"",ActivityName:"",LandingPageUrl:"",extYoudao:"",
                            LoginChannel:"native_ide",LoginPlatform:"solo"};
              let url = 'https://api.trae.cn/cloudide/api/v3/trae/Login?type=sms';
              if (ms) url += '&msToken=' + encodeURIComponent(ms);
              const r = await fetch(url, {method:'POST', credentials:'include',
                headers:{'content-type':'application/json'},
                body: JSON.stringify(body)});
              return {msToken: ms ? ms.slice(0,20) : '(未找到)', status: r.status,
                      resp: await r.text()};
            }""")
        pr(f"[P4.5] cloudide Login: msToken={r_login.get('msToken')} "
           f"HTTP={r_login.get('status')} resp={str(r_login.get('resp'))[:110]}")
        trace("cloudide_login", body=str(r_login)[:1500])
    except Exception as e:
        pr(f"[P4.5] cloudide Login 异常: {str(e)[:130]}")
    # 等 Set-Cookie 生效 + 授权页刷新（Login 后重新加载授权页以接续登录状态）
    await asyncio.sleep(2)
    try:
        await page.goto(auth_url, wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
    except Exception as e:
        pr(f"[P4.5] 授权页刷新: {str(e)[:70]}")
    await asyncio.sleep(6)
    cookies = await ctx.cookies()
    has_xcs = any(c["name"] == "X-Cloudide-Session" for c in cookies)
    pr(f"[P4.5] X-Cloudide-Session={'★有' if has_xcs else '✗无'} cookies={len(cookies)}")
    trace("p45_cookies", names=[c["name"] for c in cookies])
    login_cookies = {c["name"]: c["value"] for c in cookies}

    # ── 阶段5: 页内 fetch GetPCAuthCode（复用当前页面上下文） → ExchangeToken → 导出 ──
    # 页面上下文会携带 X-Cloudide-Session；裸 HTTP 请求可能缺少页面凭据。
    # 样例的请求头只有 referer+content-type(无特殊头), 差异在"请求来自页面"(同源凭证链)
    # → 改页内 fetch: credentials:include 自动带全部 cookie + 页面上下文
    pr("[P5] 页内 fetch GetPCAuthCode...")
    gpc_js_body = json.dumps({
        "ClientID": lib.client_id_for(args.variant),
        "CodeChallenge": context.code_challenge,
        "CodeChallengeMethod": "S256",
        "DeviceID": context.device_id,
        "PlatformCode": "SOLO_PC",
    })
    auth_code = ""
    try:
        r_gpc = await page.evaluate(
            """async (body) => {
              const r = await fetch('https://api.trae.cn/cloudide/api/v3/trae/oauth/GetPCAuthCode',
                {method:'POST', credentials:'include',
                 headers:{'content-type':'application/json'},
                 body: body});
              return {status: r.status, resp: await r.text()};
            }""", gpc_js_body)
        pr(f"[P5] 页内GetPCAuthCode: HTTP={r_gpc.get('status')} {str(r_gpc.get('resp'))[:130]}")
        trace("gpc_inpage", body=str(r_gpc)[:1500])
        try:
            j3 = json.loads(r_gpc.get("resp") or "{}")
            res = j3.get("Result") or j3.get("data") or j3
            auth_code = res.get("AuthCode") or res.get("authCode") or ""
        except Exception:
            pass
    except Exception as e:
        pr(f"[P5] 页内GetPCAuthCode异常: {str(e)[:130]}")
    # 前置：api.trae.cn 需要 X-Cloudide-Session cookie，
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

    pr("[P5b] 纯HTTP GetPCAuthCode(页内版失败时的对照)...")
    all_cookies = {c["name"]: c["value"] for c in cookies}
    all_cookies.update(login_cookies)
    if not auth_code:
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
        pr(f"[P5b] GetPCAuthCode HTTP={st} {out[:150]}")
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
    _ip, _port, _user, _pwd = load_proxy(idx)
    sess.proxies = {"http": f"http://{_user}:{_pwd}@{_ip}:{_port}",
                    "https": f"http://{_user}:{_pwd}@{_ip}:{_port}"}
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
    out_dir = DATA_DIR / ("credentials" if os.environ.get("TRAE_DATA_DIR") else "register-credentials")
    out_dir.mkdir(parents=True, exist_ok=True)
    outp = out_dir / f"{RUN_ID}.json"
    hz._record("credential_write_requested", phone=phone, state="exporting")
    temporary = outp.with_name(outp.name + "." + __import__("uuid").uuid4().hex + ".tmp")
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as destination:
            json.dump(credential, destination, ensure_ascii=False, indent=1)
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, outp)
    finally:
        temporary.unlink(missing_ok=True)
    _mark_run_success()
    pr(f"\n[完成] ★★ 注册号IDE凭证: {outp}")
    pr(f"[完成] uid={identity.get('uid')} phone={phone} expires={exchange.get('expires_ms')}")
    trace("credential_done", phone=phone, uid=identity.get("uid"), path=str(outp))
    try:
        await ctx.close()
    except Exception:
        pass
    return 0


async def main():
    try:
        return await _main_impl()
    finally:
        await _settle_number()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
