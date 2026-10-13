# -*- coding: utf-8 -*-
"""Trae 自动注册流水线 - 共享库(纯 HTTP 侧的全部逻辑 + 浏览器侧的配置派生)

设计对齐 agent2api(desktop-tauri/.../providers/trae/)的 Rust 实现:
  - 授权 URL 构造/回调解析/DeviceInfo 形状: 与 vectors/trae-login-vectors.json 逐字节对拍
  - 凭证导出: credentials.rs from_payload 认的嵌套形状 {type, provider, auth{}, account{}}
  - ExchangeToken(GetUserInfo/refresh) 的 host 候选与请求形状: 同源码

模块划分:
  - 编码/加密: xor05_hex, generate_device_key_pair, pkce_pair, uuid_v4, random_digits
  - OAuth: LoginContext, verification_uri, parse_callback, exchange_token, exchange_via_refresh,
           get_user_info, request_login_guidance
  - Passport HTTP: sms_login(纯HTTP注册/登录), 所需 URL 参数复用抓包静态值
  - 导出: build_agent2api_credential
  - 代理: load_proxies, pick_proxy(带台账记账)
无副作用: 本模块不做任何网络请求与文件写入(除显式传入的函数)。
"""

import base64
import hashlib
import json
import os
import secrets
import time
import uuid as uuidlib
from urllib.parse import urlsplit

import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

# ─────────────────────────── 常量(全部对齐 agent2api trae 模块) ───────────────────────────

PLUGIN_VERSION = "1.0.0"
DEVICE_NAME = "DESKTOP-TRAE"
DEVICE_TYPE = "windows"
DEVICE_BRAND = "Generic"
OS_VERSION = "Windows 11 Pro"
ENV = "prod"
APP_TYPE = "trae"
IDE_VERSION = "0.1.61"
IDE_VERSION_CODE = "20260820"
CLIENT_USER_AGENT = "Trae/0.1.61"
OAUTH_UA = f"Trae/{PLUGIN_VERSION} antigravity-cockpit-tools"

SOLO_CLIENT_ID = "en1oxy7wnw8j9n"
IDE_CLIENT_ID = "ono9krqynydwx5"
CALLBACK_PATH = "/authorize"
AUTH_CODE_EXCHANGE_PATH = "/trae/api/v3/oauth/ExchangeToken"
REFRESH_PATH = "/cloudide/api/v3/trae/oauth/ExchangeToken"
USER_INFO_PATH = "/cloudide/api/v3/trae/GetUserInfo"
GUIDANCE_URLS_CN = [
    "https://api.trae.cn/cloudide/api/v3/trae/GetLoginGuidance",
    "https://api.trae.com.cn/cloudide/api/v3/trae/GetLoginGuidance",
    "https://www.trae.cn/cloudide/api/v3/trae/GetLoginGuidance",
]
DEFAULT_LOGIN_HOST = "https://api.trae.cn"
CN_DOMAIN = "trae.cn"
ACCOUNT_API_ORIGINS = ["https://api.trae.cn", "https://api.trae.com.cn"]
CN_DEFAULT_ORIGINS = ["https://api.trae.cn", "https://api.trae.com.cn", "https://www.trae.cn"]

PASSPORT_AID = 711126
# 公共协议参数；请求现场参数由运行时配置提供。
CAPTURED_PASSPORT_PARAMS = {
    "passport_jssdk_version": "3.0.11",
    "passport_jssdk_type": "normal",
    "aid": str(PASSPORT_AID),
    "language": "zh",
    "account_sdk_source": "web",
    # account_sdk_source_info / qs / sign 见 stage 调用处(很长, 放常量会淹没文件头)
}


# ─────────────────────────── 编码 / 加密材料 ───────────────────────────

def xor05_hex(text: str) -> str:
    """passport mix_mode=1 的手机号/验证码编码: 每字符 XOR 0x05 后转 hex(小写)。"""
    return "".join(f"{ord(c) ^ 0x05:02x}" for c in text)


def xor05_unescape(hexstr: str) -> str:
    return "".join(chr(int(hexstr[i:i + 2], 16) ^ 0x05) for i in range(0, len(hexstr), 2))


def generate_device_key_pair():
    """EC P-256 设备密钥对: 公钥 SPKI PEM / 私钥 PKCS#8 PEM。

    cryptography 的 PEM 折行即 64 字符, 与 Go encoding/pem 一致, 产物可直接互读。
    空公钥会被上游判 20405 设备绑定拒绝, 所以这是登录必带材料。
    """
    key = ec.generate_private_key(ec.SECP256R1())
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode("ascii")
    private_pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode("ascii")
    return {"public_pem": public_pem, "private_pem": private_pem}


def pkce_pair():
    """PKCE: 48 随机字节 → base64url 无填充(64字符); challenge = SHA-256(verifier) 同编码(43字符)。"""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(48)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


def uuid_v4() -> str:
    """RFC 4122 v4(agent2api 的 machine_id/login_trace_id 都是这形状)。"""
    return str(uuidlib.uuid4())


def random_digits(length: int = 16) -> str:
    """纯数字 device_id(官方客户端 16 位数字串)。"""
    return "".join(secrets.choice("0123456789") for _ in range(length))


def url_encode(value: str) -> str:
    """Go url.QueryEscape 同规则: 字母数字与 -_.~ 原样, 空格成 +, 其余 %XX 大写(* 要转义)。"""
    out = []
    for byte in value.encode():
        c = chr(byte)
        if c.isalnum() and byte < 128 or c in "-_.~":
            out.append(c)
        elif c == " ":
            out.append("+")
        else:
            out.append(f"%{byte:02X}")
    return "".join(out)


def percent_decode(value: str) -> str:
    """query 语义还原: + 成空格, %XX 还原; 认不出的 % 原样保留。"""
    out = []
    i = 0
    raw = value.encode()
    while i < len(raw):
        b = raw[i]
        if b == 0x2B:  # +
            out.append(b" ")
            i += 1
        elif b == 0x25 and i + 2 < len(raw):  # %
            try:
                out.append(bytes([int(raw[i + 1:i + 3].decode("ascii"), 16)]))
                i += 3
            except (ValueError, UnicodeDecodeError):
                out.append(b"%")
                i += 1
        else:
            out.append(bytes([b]))
            i += 1
    return b"".join(out).decode("utf-8", "replace")


# ─────────────────────────── OAuth 上下文与 URL 构造 ───────────────────────────

def client_id_for(variant: str) -> str:
    return SOLO_CLIENT_ID if variant in ("solo", "solo-intl") else IDE_CLIENT_ID


def other_client_id(client_id: str):
    if client_id == SOLO_CLIENT_ID:
        return IDE_CLIENT_ID
    if client_id == IDE_CLIENT_ID:
        return SOLO_CLIENT_ID
    return None


def auth_from_for(variant: str) -> str:
    return "solo" if variant == "solo" else "trae"


def platform_code_for(variant: str) -> str:
    return "SOLO_PC" if variant == "solo" else "IDE_PC"


def ensure_https_scheme(raw: str) -> str:
    trimmed = raw.strip()
    if not trimmed:
        return ""
    if trimmed.startswith("http://") or trimmed.startswith("https://"):
        return trimmed
    return "https://" + trimmed.lstrip("/")


def _host_only(url: str) -> str:
    rest = url.split("://", 1)[1] if "://" in url else url
    return next((part for part in rest.replace("?", "/").replace("#", "/").split("/")), "")


class LoginContext:
    """一轮登录的全部状态: PKCE 对 + 设备指纹 + 回调地址。同生共死, 换证都要用它。"""

    def __init__(self, variant: str, callback_port: int, callback_host: str = "127.0.0.1"):
        self.variant = variant
        self.login_trace_id = uuid_v4()
        self.code_verifier, self.code_challenge = pkce_pair()
        self.device_id = random_digits(16)
        self.machine_id = uuid_v4()
        self.callback_url = f"http://{callback_host}:{callback_port}{CALLBACK_PATH}"
        keys = generate_device_key_pair()
        self.device_public_pem = keys["public_pem"]
        self.device_private_pem = keys["private_pem"]
        self.started_at_ms = int(time.time() * 1000)

    def to_json(self) -> dict:
        return {k: getattr(self, k) for k in (
            "variant", "login_trace_id", "code_verifier", "code_challenge", "device_id",
            "machine_id", "callback_url", "device_public_pem", "device_private_pem", "started_at_ms")}

    @classmethod
    def from_json(cls, data: dict) -> "LoginContext":
        obj = cls.__new__(cls)
        for k, v in data.items():
            setattr(obj, k, v)
        return obj


def verification_uri(login_host: str, context: LoginContext) -> str:
    """授权地址。参数顺序/编码由 agent2api 向量钉死: auth_callback_url 刻意不编码。"""
    host = ensure_https_scheme(login_host).rstrip("/")
    q = []
    push = lambda k, v, enc=True: q.append(f"{k}={url_encode(v) if enc else v}")
    push("login_version", "1", False)
    push("auth_from", auth_from_for(context.variant), False)
    push("login_channel", "native_ide", False)
    push("plugin_version", PLUGIN_VERSION)
    push("auth_type", "local", False)
    push("client_id", client_id_for(context.variant), False)
    push("redirect", "0", False)
    push("login_trace_id", context.login_trace_id)
    # 刻意不编码: 官方授权页对它做正则匹配, 编码后匹配不上
    push("auth_callback_url", context.callback_url, False)
    push("machine_id", context.machine_id)
    push("device_id", context.device_id)
    push("x_device_id", context.device_id)
    push("x_machine_id", context.machine_id)
    push("x_device_brand", DEVICE_BRAND)
    push("x_device_type", DEVICE_TYPE)
    push("x_os_version", OS_VERSION)
    push("x_env", ENV)
    push("x_app_version", IDE_VERSION)
    push("x_app_type", APP_TYPE)
    push("code_challenge", context.code_challenge)
    push("code_challenge_method", "S256", False)
    if context.variant == "solo":
        push("hide_saas_login", "true", False)
    return f"{host}/authorization?{'&'.join(q)}"


def device_info(context: LoginContext) -> dict:
    """ExchangeToken 的 DeviceInfo(官方客户端形状; DeviceBrand 与 DeviceModel 不同值)。"""
    return {
        "DeviceID": context.device_id,
        "MachineID": context.machine_id,
        "PlatformCode": platform_code_for(context.variant),
        "DeviceType": "PC",
        "DeviceName": DEVICE_NAME,
        "DeviceModel": DEVICE_BRAND,
        "ClientVersion": IDE_VERSION,
        "DevicePublicKey": context.device_public_pem,
        "DeviceBrand": "Apple" if DEVICE_TYPE == "mac" else "Microsoft",
        "DeviceCPU": "",
        "OSInfo": DEVICE_TYPE,
        "OSVersion": OS_VERSION,
    }


def query_pairs(query: str):
    pairs = []
    for pair in query.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        pairs.append((percent_decode(key), percent_decode(value)))
    return pairs


def _extract_auth_code_from_json(raw: str):
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    keys = ("authCode", "auth_code", "AuthCode", "AuthCodeToken", "code")

    def walk(node):
        if isinstance(node, dict):
            for k in keys:
                v = node.get(k)
                if isinstance(v, str) and v.strip():
                    return v
            for child in node.values():
                found = walk(child)
                if found:
                    return found
        elif isinstance(node, list):
            for child in node:
                found = walk(child)
                if found:
                    return found
        return None

    return walk(parsed)


def parse_callback(query: str) -> dict:
    """解析回调 query → {auth_code, refresh_token, login_host, user_tag, error}。

    键名候选/错误优先级/空值不算命中, 全部按 agent2api oauth.rs Callback::from_query。
    """
    pairs = query_pairs(query)

    def lookup(keys):
        for key in keys:
            for name, value in pairs:
                if name == key and value:
                    return value
        return ""

    for name, value in pairs:
        if name in ("error", "error_code", "err", "errorCode") and value:
            return {"auth_code": "", "refresh_token": "", "login_host": "",
                    "user_tag": "", "error": f"oauth callback error: {name}={value}"}
    if lookup(("isRedirect",)) == "false":
        return {"auth_code": "", "refresh_token": "", "login_host": "",
                "user_tag": "", "error": "oauth callback: isRedirect=false"}

    login_host = lookup(("loginHost", "login_host", "LoginHost", "host", "consoleHost"))
    user_tag = lookup(("userTag", "user_tag", "UserTag"))
    refresh_token = lookup(("refreshToken", "refresh_token", "RefreshToken", "refresh-token"))
    auth_code = lookup(("authCode", "auth_code", "AuthCode", "authorization_code", "code"))
    if not auth_code:
        for key in ("authCodeInfo", "auth_code_info", "AuthCodeInfo"):
            for name, value in pairs:
                if name == key and value:
                    code = _extract_auth_code_from_json(value)
                    if code:
                        auth_code = code
                        break
    return {"auth_code": auth_code, "refresh_token": refresh_token, "login_host": login_host,
            "user_tag": user_tag, "error": ""}


def callback_resolves(query: str) -> bool:
    cb = parse_callback(query)
    return bool(cb["error"] or cb["auth_code"] or cb["refresh_token"])


def _dedup_keep_order(values):
    seen, out = set(), []
    for v in values:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def candidate_api_origins(login_host: str):
    """换证候选 origin: loginHost 派生在前(www.x 额外派生 api.x), CN 默认源垫底。"""
    origins = []
    derived = ensure_https_scheme(login_host)
    host = _host_only(derived)
    if derived and host:
        scheme = derived.split("://", 1)[0]
        origins.append(f"{scheme}://{host}")
        if host.startswith("www."):
            origins.append(f"{scheme}://api.{host[4:]}")
    origins.extend(CN_DEFAULT_ORIGINS)
    return _dedup_keep_order(origins)


def auth_code_exchange_urls(login_host: str):
    """授权码换证地址(官方源钉前, 候选源兜底; 顺序就是语义)。"""
    urls = [o.rstrip("/") + AUTH_CODE_EXCHANGE_PATH for o in ACCOUNT_API_ORIGINS]
    urls += [o.rstrip("/") + AUTH_CODE_EXCHANGE_PATH for o in candidate_api_origins(login_host)]
    return _dedup_keep_order(urls)


def candidate_hosts(api_host: str):
    """续期/GetUserInfo 的 host 候选: 凭据 apiHost → api.trae.cn → api.trae.com.cn。"""
    hosts = []
    for h in [api_host.strip(), "https://api.trae.cn", "https://api.trae.com.cn"]:
        h = h.rstrip("/")
        if not h:
            continue
        if not h.startswith(("http://", "https://")):
            h = "https://" + h
        if h not in hosts:
            hosts.append(h)
    return hosts


def refresh_client_ids(variant: str, auth_client_id: str = ""):
    """续期 ClientID 候选: 记录的归属 → 按 variant 推 → 表里另一把。"""
    ids = []
    variant_id = client_id_for(variant)
    for cid in [auth_client_id.strip(), variant_id, other_client_id(variant_id) or ""]:
        if cid and cid not in ids:
            ids.append(cid)
    return ids


# ─────────────────────────── HTTP 侧(带留痕钩子) ───────────────────────────

GET_REFRESH_TOKEN_PATH = "/cloudide/api/v3/trae/oauth/GetRefreshToken"


def get_refresh_token(client_id: str, session: requests.Session = None, trace=None):
    """带网页登录态(cookie)拿 IDE refreshToken —— 授权页自己的取数方式(逆向自 55521 chunk)。

    授权页(www.trae.cn/authorization)在已登录时根本不跳授权码:
    直接 POST 本接口 {clientID} → Result.RefreshToken, 再把 refreshToken 递给回调。
    纯 HTTP 流水线照抄这条路: sms_login 的 cookie 在手即可, 无需授权页/回调/监听器。
    返回 {"refresh_token":..., "expire_at":..., "raw":...}。
    """
    session = session or requests.Session()
    body = {"clientID": client_id}
    headers = {"User-Agent": CLIENT_USER_AGENT, "Content-Type": "application/json"}
    errors = []
    for host in candidate_hosts(DEFAULT_LOGIN_HOST):
        url = host + GET_REFRESH_TOKEN_PATH
        resp = _post_json(session, url, body, headers, trace=trace)
        if resp.status_code >= 400:
            errors.append(f"{url} => HTTP {resp.status_code} {resp.text[:150]}")
            continue
        try:
            parsed = json.loads(resp.text)
        except ValueError:
            errors.append(f"{url} => 非JSON: {resp.text[:120]}")
            continue
        result = parsed.get("Result") if isinstance(parsed.get("Result"), dict) else parsed
        token = next((result[k] for k in ("RefreshToken", "refreshToken")
                      if isinstance(result.get(k), str) and result[k]), "")
        if token:
            expire = next((int(result[k]) for k in ("RefreshExpireAt", "refreshExpireAt")
                           if isinstance(result.get(k), (int, float))), 0)
            if expire and expire < 100_000_000_000:
                expire *= 1000
            return {"refresh_token": token, "expire_at": expire, "raw": resp.text}
        errors.append(f"{url} => 响应无 RefreshToken: {resp.text[:200]}")
    raise RuntimeError("GetRefreshToken 全部候选失败: " + " | ".join(errors))


def _response_client_id(body: str) -> str:
    try:
        parsed = json.loads(body)
    except ValueError:
        return ""
    for key in ("Result", "result"):
        obj = parsed.get(key)
        if isinstance(obj, dict):
            for name in ("ClientID", "clientId", "client_id"):
                v = obj.get(name)
                if isinstance(v, str) and v:
                    return v
    return ""


def parse_exchange_response(body: str):
    """ExchangeToken(authCode/refresh 两种路径同构)响应解析 → (access, refresh, expires_ms, client_id)。"""
    parsed = json.loads(body)
    for key in ("Result", "result"):
        obj = parsed.get(key)
        if not isinstance(obj, dict):
            continue
        access = next((obj[k] for k in ("Token", "token", "AccessToken", "accessToken")
                       if isinstance(obj.get(k), str) and obj[k]), "")
        refresh = next((obj[k] for k in ("RefreshToken", "refreshToken", "refresh_token")
                        if isinstance(obj.get(k), str) and obj[k]), "")
        expires = next((int(obj[k]) for k in ("TokenExpireAt", "tokenExpireAt", "ExpiresAt", "expiresAt")
                        if isinstance(obj.get(k), (int, float))), 0)
        if expires and expires < 100_000_000_000:
            expires *= 1000
        if access:
            return access, refresh, expires, _response_client_id(body)
    raise ValueError("换证响应里没有 access token: " + body[:300])


def _post_json(session, url, body, headers, timeout=30, trace=None):
    resp = session.post(url, json=body, headers=headers, timeout=timeout)
    if trace is not None:
        trace.append({"t": int(time.time() * 1000), "url": url, "status": resp.status_code,
                      "reqBody": body if isinstance(body, str) else json.dumps(body, ensure_ascii=False),
                      "resBody": resp.text[:4000]})
    return resp


def exchange_auth_code(context: LoginContext, auth_code: str, login_host: str,
                       session: requests.Session = None, trace=None):
    """授权码换凭据。逐候选 URL 尝试, 返回 {access, refresh, expires_ms, auth_client_id, raw}。"""
    session = session or requests.Session()
    body = {
        "ClientID": client_id_for(context.variant),
        "AuthCode": auth_code,
        "CodeVerifier": context.code_verifier,
        "IDEVersion": IDE_VERSION,
        "DeviceInfo": device_info(context),
    }
    headers = {"x-cloudide-token": "", "User-Agent": OAUTH_UA}
    errors = []
    for url in auth_code_exchange_urls(login_host):
        resp = _post_json(session, url, body, headers, trace=trace)
        if resp.status_code >= 400:
            errors.append(f"{url} => HTTP {resp.status_code} {resp.text[:120]}")
            continue
        try:
            access, refresh, expires, client_id = parse_exchange_response(resp.text)
            return {"access": access, "refresh": refresh, "expires_ms": expires,
                    "auth_client_id": client_id, "raw": resp.text}
        except ValueError as e:
            errors.append(f"{url} => {e}")
    raise RuntimeError("authCode 换证全部候选失败: " + " | ".join(errors))


def exchange_via_refresh(refresh_token: str, variant: str, api_host: str = DEFAULT_LOGIN_HOST,
                         auth_client_id: str = "", session: requests.Session = None, trace=None):
    """refreshToken 路径换新凭据(ClientID 逐候选, 10101 归属错才换下一把)。"""
    session = session or requests.Session()
    headers = {"User-Agent": CLIENT_USER_AGENT}
    urls = [h + REFRESH_PATH for h in candidate_hosts(api_host)]
    errors = []
    for cid in refresh_client_ids(variant, auth_client_id):
        body = {"ClientID": cid, "RefreshToken": refresh_token, "ClientSecret": "-", "UserID": ""}
        wrong_attribution = False
        for url in urls:
            resp = _post_json(session, url, body, headers, trace=trace)
            if resp.status_code >= 400:
                errors.append(f"{url}(ClientID {cid}) => HTTP {resp.status_code} {resp.text[:120]}")
                if resp.status_code == 400 and ("10101" in resp.text or "not matched" in resp.text):
                    wrong_attribution = True
                continue
            try:
                access, refresh, expires, ack_id = parse_exchange_response(resp.text)
                return {"access": access, "refresh": refresh or refresh_token, "expires_ms": expires,
                        "auth_client_id": ack_id or cid, "raw": resp.text}
            except ValueError as e:
                errors.append(f"{url}(ClientID {cid}) => {e}")
        if not wrong_attribution:
            break
    raise RuntimeError("refreshToken 换证失败: " + " | ".join(errors[-3:]))


def get_user_info(access_token: str, api_host: str = DEFAULT_LOGIN_HOST,
                  session: requests.Session = None, trace=None):
    """读身份(只读数不是门槛, 失败不阻断登录)。返回 {uid, nickname, enterprise_id, raw}。"""
    session = session or requests.Session()
    body = {"ReqSource": "IDE", "IDEVersion": IDE_VERSION}
    headers = {"X-Cloudide-Token": access_token, "User-Agent": CLIENT_USER_AGENT}
    for host in candidate_hosts(api_host):
        url = host + USER_INFO_PATH
        resp = _post_json(session, url, body, headers, timeout=15, trace=trace)
        if resp.status_code >= 400:
            continue
        try:
            parsed = json.loads(resp.text)
        except ValueError:
            continue
        src = parsed.get("Result") if isinstance(parsed.get("Result"), dict) else parsed
        pick = lambda keys: next((src[k] for k in keys if isinstance(src.get(k), str) and src[k]), "")
        return {"uid": pick(("UserID", "userId", "uid")),
                "nickname": pick(("ScreenName", "screenName", "nickname")),
                "enterprise_id": pick(("EnterpriseID", "enterpriseId", "TenantID", "tenantId")),
                "raw": resp.text}
    return {"uid": "", "nickname": "", "enterprise_id": "", "raw": ""}


def request_login_guidance(session: requests.Session = None, trace=None):
    """问上游该去哪个登录页, 全挂兜到默认 host(CN 不因它阻塞)。"""
    session = session or requests.Session()
    body = {"loginTraceID": "", "login_trace_id": ""}
    headers = {"User-Agent": OAUTH_UA}
    for url in GUIDANCE_URLS_CN:
        try:
            resp = _post_json(session, url, body, headers, timeout=5, trace=trace)
        except requests.RequestException:
            continue
        if resp.status_code < 400:
            host = extract_login_host(resp.text)
            if host:
                return host
    return DEFAULT_LOGIN_HOST


def extract_login_host(body: str) -> str:
    try:
        parsed = json.loads(body)
    except ValueError:
        return ""
    keys = ("LoginHost", "loginHost", "LoginURL", "loginUrl", "login_url")
    layers = [parsed]
    for wrap in ("Result", "result", "data"):
        if isinstance(parsed.get(wrap), dict):
            layers.append(parsed[wrap])
            for inner in ("Result", "result"):
                if isinstance(parsed[wrap].get(inner), dict):
                    layers.append(parsed[wrap][inner])
    for layer in layers:
        for key in keys:
            v = layer.get(key)
            if isinstance(v, str) and v:
                return v
    return ""


# ─────────────────────────── Passport: sms_login(纯 HTTP 注册/登录) ───────────────────────────


CAPTURED_SMS_LOGIN_URL = ""
CAPTURED_CSRF_TOKEN = ""
CAPTURED_TRACE_ID = ""


def sms_login(mobile: str, code: str, cookies: dict, csrf_token: str = None,
              trace_id: str = None, session: requests.Session = None, trace=None,
              proxies: dict = None):
    """短信登录(新号自动注册), 纯 HTTP。

    - mobile/code: XOR 0x05 hex, mix_mode=1
    - cookies: 浏览器导出的全套(至少含 passport_csrf_token 同源的键)
    - csrf_token/trace_id: 请求头 x-tt-passport-csrf-token / x-tt-passport-trace-id
    - proxies: 传给 requests 的 proxies dict(代理行号解析后的形态)
    返回 (parsed_json, resp)。message=success 即注册/登录成功。
    """
    session = session or requests.Session()
    body = ("mix_mode=1"
            f"&mobile={xor05_hex(mobile)}"
            f"&code={xor05_hex(code)}"
            "&fixed_mix_mode=1")
    headers = {
        "Accept": "application/json, text/javascript",
        "Content-Type": "application/x-www-form-urlencoded",
        "x-tt-passport-csrf-token": csrf_token or cookies.get("passport_csrf_token") or CAPTURED_CSRF_TOKEN,
        "x-tt-passport-trace-id": trace_id or CAPTURED_TRACE_ID,
        "Origin": "https://www.trae.cn",
        "Referer": "https://www.trae.cn/login",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/203.0.113.1 Safari/537.36",
    }
    resp = session.post(CAPTURED_SMS_LOGIN_URL, data=body, headers=headers, cookies=cookies,
                        timeout=30, proxies=proxies)
    if trace is not None:
        trace.append({"t": int(time.time() * 1000), "url": CAPTURED_SMS_LOGIN_URL[:120],
                      "status": resp.status_code, "reqBody": "mobile=<masked>&code=<masked>",
                      "resBody": resp.text[:4000], "set_cookie": resp.headers.get("Set-Cookie", "")[:1000]})
    try:
        parsed = json.loads(resp.text)
    except ValueError:
        parsed = {"raw": resp.text}
    return parsed, resp


def extract_login_cookies(resp) -> dict:
    """从 sms_login 响应的 Set-Cookie 提取登录态(sessionid 等)。"""
    out = {}
    for chunk in resp.headers.get("Set-Cookie", "").split(","):
        if "=" not in chunk:
            continue
        name, _, value = chunk.strip().partition("=")
        value = value.split(";")[0]
        if name and value:
            out[name] = value
    return out


# ─────────────────────────── 凭证导出(agent2api 粘贴格式) ───────────────────────────

def build_agent2api_credential(context: LoginContext, exchange: dict, identity: dict,
                               variant: str = "solo") -> dict:
    """组装 agent2api「粘贴凭证」认的嵌套形状(credentials.rs from_payload 兼容平铺/嵌套, 我们给嵌套)。

    expiresAt 落盘统一毫秒; uid/nickname 来自 GetUserInfo。
    """
    return {
        "type": "trae",
        "provider": "trae",
        "auth": {
            "accessToken": exchange["access"],
            "refreshToken": exchange["refresh"],
            "expiresAt": exchange["expires_ms"],
            "domain": CN_DOMAIN,
            "apiHost": DEFAULT_LOGIN_HOST,
            "machineId": context.machine_id,
            "deviceId": context.device_id,
            "variant": variant,
            "devicePublicKey": context.device_public_pem,
            "devicePrivateKey": context.device_private_pem,
            "authClientId": exchange.get("auth_client_id", ""),
        },
        "account": {
            "uid": identity.get("uid", ""),
            "nickname": identity.get("nickname", ""),
            "enterpriseId": identity.get("enterprise_id", ""),
        },
    }


# ─────────────────────────── 代理池(带台账纪律) ───────────────────────────

def load_proxies(proxy_file):
    with open(proxy_file, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def proxy_requests_kwargs(proxy_line: str) -> dict:
    ip, port, user, pwd = proxy_line.split(":", 3)
    return {"proxies": {"http": f"http://{user}:{pwd}@{ip}:{port}",
                        "https": f"http://{user}:{pwd}@{ip}:{port}"}}


def record_proxy_use(ledger_path, proxy_line, use, run_id):
    """台账记账(每代理每半小时最多 3 次注册流程的硬约束由调用方查询实现)。"""
    data = {}
    if os.path.exists(ledger_path):
        try:
            with open(ledger_path, encoding="utf-8") as f:
                data = json.load(f)
        except ValueError:
            data = {}
    data.setdefault(proxy_line, []).append(
        {"time": time.strftime("%Y-%m-%dT%H:%M:%S"), "use": use, "run_id": run_id})
    with open(ledger_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)


def fingerprint_seed_for(proxy_index: int) -> int:
    """指纹 seed 派生自代理号: 同一代理永远同一指纹(cloakbrowser-capability.md 的约定)。"""
    digest = hashlib.sha256(f"trae-{proxy_index}".encode()).hexdigest()[:8]
    return int(digest, 16) % 10**9 + 10**6
