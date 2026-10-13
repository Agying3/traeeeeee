# -*- coding: utf-8 -*-
"""Stage 1(浏览器段): 指纹浏览器过滑块 → 短信发出 → 导出会话(cookies/fp/csrf) → 关浏览器

浏览器只承担"让短信发出来"这一件事; 之后注册/登录/凭证全走 HTTP。

- 浏览器: CloakBrowser(内核级指纹, seed 由代理号派生; 一号一指纹一IP, 持久 profile)
- 滑块: 使用宽度、模板和内容三重识别(宽度校验/模板匹配/内容扫描) + human_track 轨迹 + CDP 注入
- 通过后: SDK 自动重试 send_code → message=success → 导出 cookies+localStorage+UA
- 留痕: data/register-diagnostics/<run-id>/ 诊断文件与运行记录

用法(单独调试):
  python stage1_browser_captcha.py <代理行号0基> <手机号> [--headless] [--attempts N]
程序集成入口为 run_stage1()。
"""

import asyncio
import json
import pathlib
import random
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[2]
LAB = ROOT / "slider-captcha-lab-main"
sys.path.insert(0, str(LAB))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from playwright.async_api import TimeoutError as BrowserTimeout  # noqa: E402
from page_diagnostics import PageDiagnostics  # noqa: E402
import trae_pipeline_lib as lib  # noqa: E402

PROXY_FILE = ROOT / "Webshare 250 proxies.txt"
LEDGER = ROOT / "tmp" / "proxy-ledger.json"
PROFILES = ROOT / "tmp" / "cloak-profiles"
RUNS_LOG = ROOT / "tmp" / "register-runs.jsonl"
RUN_ID = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
RUN_DIR = ROOT / "tmp" / "register-diagnostics" / RUN_ID
SESSIONS = ROOT / "tmp" / "register-sessions"
WAIT_SECONDS = 120.0
KEEP_OPEN = True  # 出错时保留窗口人工检查(留痕纪律)

netlog = []
verdicts = []


def pr(*a):
    print(*a, flush=True)


def log_run(stage, detail=""):
    try:
        RUNS_LOG.parent.mkdir(exist_ok=True)
        with RUNS_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"time": time.strftime("%Y-%m-%dT%H:%M:%S"), "run_id": RUN_ID,
                                "stage": stage, "detail": detail}, ensure_ascii=False) + "\n")
    except Exception as e:
        pr(f"[!] 运行日志落盘失败: {e}")


def dump_netlog():
    try:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        (RUN_DIR / "network.json").write_text(
            json.dumps(netlog, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as e:
        pr(f"[!] 网络留痕失败: {e}")


async def real_click(page, text, starts=False):
    """真实鼠标点击含指定文字的可见叶子元素(可信事件)。"""
    r = await page.evaluate("""({text, starts}) => {
        const match = e => starts ? e.textContent.trim().startsWith(text) : e.textContent.trim() === text;
        const els = [...document.querySelectorAll('button,div,span')].filter(e => {
            const r = e.getBoundingClientRect();
            return e.offsetParent !== null && r.width > 0 && r.height > 0 && match(e);
        });
        const leaves = els.filter(e => ![...e.children].some(match));
        const el = leaves[0] || els[0];
        if (!el) return null;
        const r = el.getBoundingClientRect();
        return {x: r.x + r.width/2, y: r.y + r.height/2, w: r.width};
    }""", {"text": text, "starts": starts})
    if r:
        await page.mouse.click(r["x"], r["y"])
        return True
    return False


async def solve_slider(page, ctx, cdp, diagnostics, max_attempts):
    """三防线识别 + 拟人轨迹 + CDP 注入, 直到 captcha/verify 判定通过。

    返回 (passed, 最后一次判定响应)。识别策略与 lab_solve_trae.py 完全一致(6/6 成绩)。
    """
    import cv2
    import numpy as np
    from human_track import generate_drag
    from captcha_recognizer.slider import Slider
    yolo = Slider()

    prev_bg_url = None
    for attempt in range(1, max_attempts + 1):
        pr(f"[i] ===== 滑块第 {attempt}/{max_attempts} 次尝试 =====")
        info = iframe_frame = None

        async def images_ready():
            nonlocal info, iframe_frame
            for fr in page.frames:
                if "verifycenter" not in (fr.url or ""):
                    continue
                info = await fr.evaluate("""() => {
                    const bg = document.querySelector('img.captcha_verify_image, img[id*="verify_image"]');
                    const piece = document.querySelector('img.captcha-verify-image-slide, img[id*="slide"]');
                    const slider = document.querySelector('.captcha-slider-dragged-area');
                    const r = el => el ? el.getBoundingClientRect() : null;
                    return { bgUrl: bg && bg.src, bgNat: bg && bg.naturalWidth, bgRect: r(bg),
                             bgComplete: bg && bg.complete,
                             pieceUrl: piece && piece.src, pieceNat: piece && piece.naturalWidth, pieceRect: r(piece),
                             pieceComplete: piece && piece.complete, sliderRect: r(slider) };
                }""")
                iframe_frame = fr
                loaded = (info.get("bgUrl") and info.get("pieceUrl")
                          and info.get("bgComplete") and info.get("pieceComplete")
                          and (info.get("bgNat") or 0) > 0 and (info.get("pieceNat") or 0) > 0
                          and (info.get("bgRect") or {}).get("width", 0) > 0
                          and (info.get("pieceRect") or {}).get("width", 0) > 0)
                return loaded and (prev_bg_url is None or info["bgUrl"] != prev_bg_url)
            return False

        if not await diagnostics.wait_for(f"images_{attempt}", images_ready, timeout_s=WAIT_SECONDS, interval_s=0.5):
            return False, None
        prev_bg_url = info["bgUrl"]

        async def grab(u):
            r = await ctx.request.get(u, timeout=WAIT_SECONDS * 1000)
            if not r.ok:
                raise RuntimeError(f"图片请求返回 HTTP {r.status}")
            return await r.body()
        try:
            bg = cv2.imdecode(np.frombuffer(await grab(info["bgUrl"]), np.uint8), cv2.IMREAD_COLOR)
            piece = cv2.imdecode(np.frombuffer(await grab(info["pieceUrl"]), np.uint8), cv2.IMREAD_UNCHANGED)
        except Exception as e:
            pr(f"[!] 图片下载失败: {e}")
            return False, None
        if bg is None or piece is None:
            pr("[!] 图片解码失败")
            return False, None

        # 拼图块不透明区
        alpha = (piece[:, :, 3] > 128).astype(np.uint8) if piece.ndim == 3 and piece.shape[2] == 4 \
            else ((cv2.cvtColor(piece, cv2.COLOR_BGR2GRAY) > 30).astype(np.uint8))
        ys, xs = np.where(alpha > 0)
        ex0 = int(xs.min()) if len(xs) else 0
        piece_w = int(xs.max() - xs.min() + 1) if len(xs) else 0
        y0, y1, x0, x1 = int(ys.min()), int(ys.max()), int(xs.min()), int(xs.max())

        # 防线0: YOLO
        box, conf = yolo.identify(bg[:, :, ::-1])
        gx1, gy1, gx2, gy2 = [float(v) for v in box]
        gap_x, gap_w = gx1, gx2 - gx1
        pr(f"[i] YOLO: conf={float(conf):.3f} 缺口@x={gap_x:.0f} w={gap_w:.0f} 拼图宽={piece_w}")

        # 防线1: 宽度校验(真缺口宽≈拼图块不透明区宽), 不符走防线2模板匹配
        method = "yolo"
        if abs(gap_w - piece_w) > 18:
            tpl = piece[y0:y1 + 1, x0:x1 + 1, :3]
            g_bg = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY).astype(np.float32)
            t_bg = cv2.cvtColor(tpl, cv2.COLOR_BGR2GRAY).astype(np.float32)
            res = np.nan_to_num(cv2.matchTemplate(g_bg, t_bg, cv2.TM_CCOEFF_NORMED, mask=alpha[y0:y1 + 1, x0:x1 + 1]),
                                nan=-1, posinf=-1, neginf=-1)
            _, maxv, _, maxloc = cv2.minMaxLoc(res)
            gap_x, method = float(maxloc[0] + x0), f"tmpl({maxv:.2f})"
            pr(f"[i] 宽度不匹配 → 模板匹配兜底: x={gap_x:.0f} score={maxv:.3f}")

        # 防线3: 内容相关扫描交叉验证(真缺口内容被压暗, 匹配得分高; 与YOLO不一致信扫描)
        scale = info["bgRect"]["width"] / info["bgNat"]
        tpl_c = piece[y0:y1 + 1, ex0:ex0 + piece_w, :3]
        m_c = alpha[y0:y1 + 1, ex0:ex0 + piece_w]
        g_bg = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY).astype(np.float32)
        t_c = cv2.cvtColor(tpl_c, cv2.COLOR_BGR2GRAY).astype(np.float32)
        res = np.nan_to_num(cv2.matchTemplate(g_bg, t_c, cv2.TM_CCOEFF_NORMED, mask=m_c),
                            nan=-1, posinf=-1, neginf=-1)
        _, scan_v, _, scan_loc = cv2.minMaxLoc(res)
        scan_x = float(scan_loc[0])
        if scan_v >= 0.35:
            target_natural = scan_x
            method = "scan" + ("+yolo一致" if abs(scan_x - gap_x) < 30 else "(信扫描)")
        else:
            target_natural = gap_x
            method += "-only(扫描分低)"
        target_left = (target_natural - ex0) * scale
        pr(f"[i] 内容扫描: x={scan_x:.0f} score={scan_v:.3f} → target_left={target_left:.1f}px 来源={method}")

        # 轨迹与拖动(字节 1:1; 保留'差一点'正偏)
        D = target_left + random.uniform(0, 2.0)
        track = generate_drag(D, bias_px=0)
        pr(f"[i] 轨迹: {len(track)}事件 {sum(t[2] for t in track):.0f}ms 位移{D:.1f}px")

        sbb = info["sliderRect"] or {}
        ibb = await page.locator('iframe[src*="verifycenter"]').first.bounding_box()
        if not ibb or sbb.get("width", 0) <= 0:
            pr("[!] 布局未就绪")
            return False, None
        sx = ibb["x"] + sbb["x"] + sbb["width"] / 2
        sy = ibb["y"] + sbb["y"] + sbb["height"] / 2

        await page.mouse.move(sx - random.uniform(60, 120), sy + random.uniform(-25, 25),
                              steps=random.randint(8, 14))
        await page.wait_for_timeout(random.randint(200, 400))
        await page.mouse.move(sx, sy, steps=5)
        await page.wait_for_timeout(random.randint(250, 450))

        async def cdp_event(typ, x, y, buttons):
            params = {"type": typ, "x": x, "y": y, "button": "left", "buttons": buttons,
                      "pointerType": "mouse"}
            if typ in ("mousePressed", "mouseReleased"):
                params["clickCount"] = 1
            await cdp.send("Input.dispatchMouseEvent", params)

        n_start = len(verdicts)
        await cdp_event("mousePressed", sx, sy, 1)
        await page.wait_for_timeout(random.randint(50, 110))
        t0 = time.perf_counter()
        sched = cum = cum_dy = 0.0
        for dx, dy, dt_ms in track:
            sched += dt_ms
            cum += dx
            cum_dy += dy
            await cdp_event("mouseMoved", sx + cum, sy + cum_dy, 1)
            elapsed = (time.perf_counter() - t0) * 1000
            if sched > elapsed:
                await asyncio.sleep((sched - elapsed) / 1000)
        await page.wait_for_timeout(random.randint(10, 40))
        await cdp_event("mouseReleased", sx + cum, sy + cum_dy, 0)

        v = await diagnostics.wait_for(f"verdict_{attempt}",
                                       lambda: verdicts[-1] if len(verdicts) > n_start else None,
                                       timeout_s=WAIT_SECONDS)
        body = (v or {}).get("body") or ""
        passed = '"code":200' in body
        pr(f"[i] 判定: {body[:120] or '(无响应)'} → {'★通过' if passed else '未通过'}")
        await diagnostics.snapshot(f"slider-attempt-{attempt}")
        if not v:
            return False, None
        if passed:
            return True, v
        await page.wait_for_timeout(2500)  # 服务器自动换题, 进入下一次尝试
    return False, None


async def run_stage1(proxy_index: int, phone: str, headless: bool = False, attempts: int = 3,
                     keep_open_on_error: bool = True):
    """完整浏览器段: 返回 session 文件路径(短信已发出), 失败抛异常。"""
    from cloakbrowser import launch_persistent_context_async

    global KEEP_OPEN
    KEEP_OPEN = keep_open_on_error
    lines = lib.load_proxies(PROXY_FILE)
    proxy_line = lines[proxy_index]
    ip, port, user, pwd = proxy_line.split(":", 3)
    lib.record_proxy_use(LEDGER, proxy_line, "stage1_captcha", RUN_ID)
    seed = lib.fingerprint_seed_for(proxy_index)
    profile_dir = PROFILES / f"cloak-{proxy_index}"
    profile_dir.mkdir(parents=True, exist_ok=True)
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    SESSIONS.mkdir(parents=True, exist_ok=True)
    pr(f"[i] 代理#{proxy_index} {ip}:{port} 指纹seed={seed} profile={profile_dir.name}")

    ctx = await launch_persistent_context_async(
        user_data_dir=str(profile_dir),
        headless=headless,
        proxy=f"http://{user}:{pwd}@{ip}:{port}",
        geoip=True,                      # 时区/经纬度跟代理走
        humanize=True,                   # 勾协议/点按钮用拟人鼠标
        args=[f"--fingerprint={seed}", "--window-size=700,800", "--disable-http2", "--lang=zh-CN"],
        locale="zh-CN",
    )
    diagnostics = None
    sms_sent = False
    try:
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        diagnostics = PageDiagnostics(ctx, page, RUN_DIR, keep_open=KEEP_OPEN, report=pr,
                                      watch_tokens=("send_code", "captcha/get", "captcha/verify", "sms_login"))
        diagnostics.state.update({"run_id": RUN_ID, "profile": str(profile_dir), "proxy": f"{ip}:{port}"})
        cdp = await ctx.new_cdp_session(page)

        def on_response(resp):
            url = resp.url
            if resp.request.method == "OPTIONS" or not any(
                    w in url for w in ("send_code", "captcha/get", "captcha/verify", "sms_login")):
                return
            entry = {"t": int(time.time() * 1000), "url": url, "status": resp.status, "body": None}
            netlog.append(entry)

            async def capture():
                try:
                    entry["body"] = await asyncio.wait_for(resp.text(), timeout=WAIT_SECONDS)
                except Exception as error:
                    entry["body_error"] = repr(error)
                if "captcha/verify" in url:
                    verdicts.append(entry.copy())
                pr(f"    [NET] {resp.status} {url.split('?')[0][-55:]} {(entry['body'] or '')[:90]}")
                dump_netlog()
            diagnostics.spawn(capture())
        page.on("response", on_response)

        # ── 打开登录页(就绪检测+自动刷新, 治代理链路 CDN JS 损坏) ──
        page_loaded = False
        for load_try in range(3):
            try:
                await page.goto("https://www.trae.cn/login", wait_until="domcontentloaded",
                                timeout=WAIT_SECONDS * 1000)
            except BrowserTimeout:
                pass
            if await diagnostics.stable_interactive():
                page_loaded = True
                break
            if load_try < 2:
                pr(f"[i] 页面未就绪(脚本损坏{len(diagnostics.failures)}个), 刷新重载")
                diagnostics.failures.clear()
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=WAIT_SECONDS * 1000)
                except BrowserTimeout:
                    pass
        if not page_loaded:
            raise RuntimeError("3次加载后登录页仍未就绪")

        if not await diagnostics.wait_for("login_form",
                                          lambda: page.locator('input[placeholder="输入手机号"]').is_visible(),
                                          timeout_s=WAIT_SECONDS):
            raise RuntimeError("登录表单未就绪")
        await page.fill('input[placeholder="输入手机号"]', phone)
        if not await diagnostics.wait_for("phone_filled",
                                          lambda: page.locator('input[placeholder="输入手机号"]').input_value() == phone,
                                          timeout_s=15):
            raise RuntimeError("手机号填入丢失(页面未就绪)")
        if not await real_click(page, "我已阅读并同意", starts=True):
            raise RuntimeError("未找到协议勾选元素")
        if not await real_click(page, "获取验证码"):
            raise RuntimeError("未找到获取验证码按钮")

        n_net_start = len(netlog)
        if not await diagnostics.wait_for("send_code_request",
                                          lambda: any("send_code" in e["url"] for e in netlog[n_net_start:]),
                                          timeout_s=WAIT_SECONDS):
            raise RuntimeError("send_code 请求未发出")

        async def send_code_result():
            for entry in reversed(netlog[n_net_start:]):
                if "send_code" in entry["url"] and entry.get("body") is not None:
                    return entry
            return None
        result = await diagnostics.wait_for("send_code_response", send_code_result, timeout_s=WAIT_SECONDS)
        if not result or result.get("body_error"):
            raise RuntimeError("send_code 响应未完整取得")
        try:
            error_code = (json.loads(result["body"]).get("data") or {}).get("error_code")
        except ValueError:
            error_code = None
        pr(f"[i] send_code 首响应: HTTP {result['status']} error_code={error_code}")
        if str(error_code) == "7":
            raise RuntimeError("手机号被限频(error_code=7): 换号或等冷却")
        if error_code is None:
            pr("[i] 无滑块直接放行(罕见), 短信可能已发出")

        # ── 滑块 ──
        if error_code is not None:
            async def frame_ready():
                return await page.locator('iframe[src*="verifycenter"]').first.is_visible()
            if not await diagnostics.wait_for("verification_frame", frame_ready, timeout_s=WAIT_SECONDS):
                raise RuntimeError("验证 iframe 未出现")
            passed, _verdict = await solve_slider(page, ctx, cdp, diagnostics, attempts)
            if not passed:
                raise RuntimeError(f"{attempts} 次滑块尝试未通过")

        # ── 通过后: SDK 自动重试 send_code → success → 短信已发出 ──
        n_after = len(netlog)

        async def sms_success():
            for entry in netlog[n_after:]:
                if "send_code" in entry["url"] and entry.get("body") \
                        and '"message":"success"' in entry["body"]:
                    return entry
            return None
        success = await diagnostics.wait_for("sms_success", sms_success, timeout_s=45)
        if success:
            sms_sent = True
            pr("[i] ★ 短信已发出(send_code success)")
        else:
            pr("[!] 滑块通过但 45 秒内未见 send_code success; 仍导出会话供人工核查")
            await diagnostics.snapshot("no_sms_success")

        # ── 导出会话(浏览器使命完成) ──
        cookies = await ctx.cookies()
        local_storage = {}
        try:
            local_storage = await page.evaluate(
                "() => Object.fromEntries(Object.entries(localStorage))")
        except Exception:
            pass
        fp_cookie = next((c["value"] for c in cookies if c["name"] == "s_v_web_id"), "")
        csrf_cookie = next((c["value"] for c in cookies if c["name"] == "passport_csrf_token"), "")
        session = {
            "run_id": RUN_ID,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "phone": phone,
            "proxy_index": proxy_index,
            "proxy": f"{ip}:{port}",
            "fingerprint_seed": seed,
            "profile_dir": str(profile_dir),
            "user_agent": await page.evaluate("() => navigator.userAgent"),
            "fp": fp_cookie,
            "csrf_token": csrf_cookie,
            "sms_sent": sms_sent,
            "cookies": cookies,
            "local_storage": local_storage,
        }
        session_path = SESSIONS / f"{RUN_ID}.json"
        # 会话含登录态敏感数据, 只落 tmp/(gitignore)
        session_path.write_text(json.dumps(session, ensure_ascii=False, indent=1), encoding="utf-8")
        pr(f"[i] 会话已导出: {session_path}")
        await diagnostics.finish("stage1_done", f"sms_sent={sms_sent}")
        log_run("stage1_done", f"sms_sent={sms_sent} session={session_path.name}")
        dump_netlog()
        return session_path
    except Exception as error:
        log_run("stage1_error", f"{type(error).__name__}: {error}")
        dump_netlog()
        if diagnostics:
            await diagnostics.finish("stage1_error", str(error), hold=KEEP_OPEN)
        raise
    finally:
        if sms_sent:
            try:
                await ctx.close()
            except Exception:
                pass


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Stage1: 指纹浏览器过滑块拿短信")
    parser.add_argument("proxy_index", type=int)
    parser.add_argument("phone")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--close-on-error", action="store_true")
    args = parser.parse_args()
    await run_stage1(args.proxy_index, args.phone, headless=args.headless,
                     attempts=args.attempts, keep_open_on_error=not args.close_on_error)


if __name__ == "__main__":
    asyncio.run(main())
