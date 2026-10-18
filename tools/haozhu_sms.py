# -*- coding: utf-8 -*-
"""豪猪接码接入(api.haozhuma.com) —— 把"手机号+收验证码"自动化, 移植自 senseauto-2.0.0。

豪猪客户端用于以下流程:
    取号 getPhone(sid) → 浏览器过滑块发短信 → 轮询 getMessage → 提取验证码
    → 成功 release / 失败 blacklist(收不到码的号拉黑, 防止再取到)

凭据来源(优先级): 命令行参数 > 环境变量(HAOZHU_*) > data/config/haozhu.json(gitignored)
  {"user": "...", "password": "...", "token": "可空,自动登录换", "sid": "项目ID",
   "h5_session": "h5.haozhuma.com 的 PHPSESSID, 仅项目搜索用"}

独立用法(装配/排障):
  python haozhu_sms.py search trae          # H5 搜项目(找 trae 的 sid), 需 h5_session
  python haozhu_sms.py summary              # 余额/占用
  python haozhu_sms.py get                  # 试取一个号(会占用额度, 用完记得 release)
流水线用法: 通过工作台或 run.py 启动。
"""

import json
import os
import pathlib
import re
import sys
import time
from dataclasses import dataclass
import hashlib
import secrets

from haozhu_ledger import Ledger, account_fingerprint, receipt_hash

import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
CRED_FILE = pathlib.Path(os.environ.get("TRAE_HAOZHU_FILE", str(pathlib.Path(os.environ.get("TRAE_DATA_DIR", str(pathlib.Path(__file__).resolve().parents[1] / "data"))) / "config" / "haozhu.json")))
BASE = "https://api.haozhuma.com/sms/"
CODE_RE = re.compile(r"验证码[^0-9]{0,8}([0-9]{4,8})")
ANY_CODE_RE = re.compile(r"(?<!\d)(\d{4,8})(?!\d)")


def extract_code(sms: str) -> str:
    """从短信文本提取验证码: 优先「验证码」后的数字, 其次独立数字串。"""
    sms = (sms or "").strip()
    if not sms:
        return ""
    m = CODE_RE.search(sms)
    if m:
        return m.group(1)
    m = ANY_CODE_RE.search(sms)
    return m.group(1) if m else ""


class HaozhuError(Exception):
    """豪猪 API 错误及轮询、额度、会话失效分类。"""

    def __init__(self, api: str, code: str, msg: str):
        super().__init__(f"豪猪[{api}] code={code} msg={msg}")
        self.api, self.code, self.msg = api, str(code), msg or ""

    def waiting(self) -> bool:
        """「短信还在路上」的正常轮询状态。"""
        return self.code == "-1" and "等待" in self.msg

    def token_invalid(self) -> bool:
        return (self.code in ("401", "403", "101")
                or "token" in self.msg.lower() or "登录" in self.msg)

    def quota(self) -> bool:
        """占用名额满: 释放后可再取, 不算真失败。"""
        return "释放" in self.msg and ("余额不足" in self.msg or "再取号" in self.msg)

    def fatal(self) -> bool:
        """不可恢复: 余额耗尽、无库存、项目不存在、账号被封。"""
        if self.quota():
            return False
        keys = ("余额不足", "充值", "欠费", "无号", "没有可用", "暂无号码",
                "号码不足", "库存不足", "项目不存在", "没有找到项目", "封禁", "账号异常")
        return any(k in self.msg for k in keys)


@dataclass
class HaozhuProject:
    sid: str
    project_id: str
    name: str


def load_credentials(cli: dict = None) -> dict:
    """凭据合成: CLI > 环境变量 > data/config/haozhu.json。至少要有 user+password+sid。"""
    merged = {}
    if CRED_FILE.exists():
        try:
            # utf-8-sig: 容忍 PowerShell 写入的 BOM
            merged.update(json.loads(CRED_FILE.read_text(encoding="utf-8-sig")))
        except (ValueError, OSError):
            pass
    env = {k.lower(): v for k, v in os.environ.items() if k.startswith("HAOZHU_")}
    for k, v in env.items():
        merged[k.replace("haozhu_", "")] = v
    if cli:
        merged.update({k: v for k, v in cli.items() if v})
    return merged


class HaozhuClient:
    """官方 API 客户端(取号/收码/释放/拉黑/余额)。单线程使用。"""

    def __init__(self, user: str, password: str, sid: str, token: str = "",
                 uid: str = "", timeout: float = 20, uids=None, ledger=None,
                 attempt_id: str = "", run_id: str = "", account: str = "", http=None):
        self.user, self.password, self.sid = user, password, str(sid)
        self.token = token or ""
        self.uid = uid or ""  # 对接码(渠道): 锁定低价渠道用, 留空走默认
        self.timeout = float(timeout or 20)
        self.ledger = ledger if ledger is not None else Ledger()
        self.attempt_id = str(attempt_id or os.environ.get("TRAE_ATTEMPT_ID", ""))
        self.run_id = str(run_id or os.environ.get("TRAE_RUN_ID", ""))
        self.account = str(account or os.environ.get("TRAE_ACCOUNT", ""))
        self._sms_sent = False
        self._finished = False
        self._owner = f"{os.getpid()}-{secrets.token_hex(6)}"
        # 对接码池: 依次尝试, 某渠道取号失败自动换下一个(见 get_phone_multi)
        pool = []
        if isinstance(uids, (list, tuple)):
            pool = [str(u).strip() for u in uids if str(u).strip()]
        elif isinstance(uids, str):
            pool = [u.strip() for u in re.split(r"[,\n]", uids) if u.strip()]
        legacy = (self.uid or "").strip()
        if legacy and legacy not in pool:
            pool.append(legacy)
        self.uid_pool = list(dict.fromkeys(pool))  # 去重保序
        self.http = http if http is not None else requests.Session()

    # ---- 底层 ----

    def _record(self, event, **fields):
        identity = {"run_id": self.run_id, "sid": self.sid, "uid": self.uid,
                    "account_fingerprint": account_fingerprint(self.user)}
        return self.ledger.record(event, attempt_id=self.attempt_id, **{**identity, **fields})

    def _request(self, api: str, params: dict, auth: bool = True) -> dict:
        if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
            raise HaozhuError(api, "maintenance", "registration provider is paused")
        query = {"api": api, **params}
        if auth:
            query["token"] = self.token
        try:
            resp = self.http.get(BASE, params=query, timeout=self.timeout)
        except requests.RequestException as exc:
            # {Intent: requests exceptions embed query credentials; retain only the type.}
            self._record("http_error", api=api, error_category="network",
                         last_error=type(exc).__name__)
            raise HaozhuError(api, "net", type(exc).__name__) from None
        if resp.status_code in (401, 403):
            raise HaozhuError(api, str(resp.status_code), "token invalid")
        try:
            data = resp.json()
        except (ValueError, TypeError):
            self._record("http_error", api=api, status=resp.status_code,
                         error_category="invalid_response")
            raise HaozhuError(api, str(resp.status_code), "response is not JSON") from None
        if not isinstance(data, dict):
            raise HaozhuError(api, "invalid_response", "response is not an object")
        code = str(data.get("code", "")).strip()
        msg = str(data.get("msg") or "")
        if api == "getPhone" and code in ("0", "200") and data.get("phone"):
            self._record("phone_acquired", phone=str(data["phone"]).strip(), sid=self.sid,
                         uid=str(params.get("uid") or ""), held=True, state="held", raw_response=data)
        elif api != "getMessage":
            self._record("http_response", api=api, status=resp.status_code,
                         upstream_code=code, raw_response=data)
        if code in ("0", "200"):
            return data
        err = HaozhuError(api, code, _safe_message(msg, self))
        if err.waiting():
            return data
        raise err

    def _call(self, api: str, params: dict, auth: bool = True) -> dict:
        if not auth:
            return self._request(api, params, auth=False)
        self.ensure_login()
        stale_token = self.token
        try:
            return self._request(api, params)
        except HaozhuError as error:
            if not error.token_invalid():
                raise
        self.ensure_login(force=True, stale_token=stale_token)
        return self._request(api, params)

    def _cache_name(self):
        identity = self.user + "\0" + hashlib.sha256(self.password.encode()).hexdigest()
        return "haozhu-token-" + account_fingerprint(identity)

    def ensure_login(self, force=False, stale_token="") -> None:
        if self.token and not force:
            return
        name = self._cache_name()
        deadline = time.monotonic() + max(35, self.timeout * 2 + 5)
        while True:
            cached = self.ledger.load_token(name)
            if cached and (not force or cached != stale_token):
                self.token = cached
                return
            if self.ledger.acquire_lease(name, self._owner, ttl=self.timeout + 15):
                try:
                    cached = self.ledger.load_token(name)
                    if cached and (not force or cached != stale_token):
                        self.token = cached
                        return
                    self.login()
                    return
                finally:
                    self.ledger.release_lease(name, self._owner)
            if time.monotonic() >= deadline:
                raise HaozhuError("login", "lease_timeout", "login coordination timeout")
            time.sleep(0.05)

    # ---- 业务 ----

    def login(self) -> str:
        data = self._call("login", {"user": self.user, "pass": self.password}, auth=False)
        token = str(data.get("token") or "").strip()
        if not token:
            raise HaozhuError("login", data.get("code"), "登录成功但没有 token")
        self.ledger.save_token(self._cache_name(), token)
        self.token = token
        return token

    def begin_attempt(self, attempt_id="", run_id="", phone="", proxy=None, **fields):
        self.attempt_id = str(attempt_id or self.attempt_id or os.environ.get("TRAE_ATTEMPT_ID", "") or secrets.token_hex(12))
        self.run_id = str(run_id or self.run_id or os.environ.get("TRAE_RUN_ID", "") or self.attempt_id)
        self.account = self.user
        return self.ledger.begin(self.attempt_id, self.run_id, self.sid, uid=self.uid,
                                 account=self.user, phone=phone, proxy=proxy, **fields)

    def adopt_phone(self, phone, from_attempt_id=""):
        self.begin_attempt()
        current = self.ledger.get_attempt(self.attempt_id)
        if current["held"] and current["phone"] == phone:
            return current
        if from_attempt_id:
            prior = self.ledger.get_attempt(from_attempt_id)
        else:
            matches = [item for item in self.ledger.held() if item["phone"] == phone
                       and item["account_fingerprint"] == account_fingerprint(self.user)
                       and item["attempt_id"] != self.attempt_id]
            prior = matches[0] if len(matches) == 1 else None
        if not prior:
            raise ValueError("original phone occupancy is missing or ambiguous")
        lease, owner = self.ledger.resource_lease(prior), self._owner
        if not self.ledger.acquire_lease(lease, owner, ttl=max(120, self.timeout * 8 + 120)):
            raise ValueError("phone ownership operation in progress")
        try:
            prior = self.ledger.get_attempt(prior["attempt_id"])
            if prior["account_fingerprint"] != account_fingerprint(self.user):
                raise ValueError("phone account identity mismatch")
            if not prior["held"] or prior["phone"] != phone:
                raise ValueError("phone attempt is no longer held")
            if _pid_alive(prior.get("owner_pid")):
                raise ValueError("phone is owned by a running attempt")
            record = self.ledger.transfer(prior, self.attempt_id, self.run_id, os.getpid())
            self.sid, self.uid = record["sid"], record["uid"]
            return record
        finally:
            self.ledger.release_lease(lease, owner)

    def mark_manual_code(self, code):
        self._record("code_received", code_source="manual", source="manual", charge="unknown",
                     receipt_hash=receipt_hash(code), verification_code=code)

    def finish_attempt(self, **kwargs):
        return finish_attempt(self.attempt_id, ledger=self.ledger, client=self,
                              run_id=self.run_id, **kwargs)

    def get_phone(self, uid: str = "") -> str:
        if not self.attempt_id or not self.ledger.get_attempt(self.attempt_id):
            self.begin_attempt()
        existing = self.ledger.get_attempt(self.attempt_id)
        if existing["phone"]:
            if existing["held"]:
                if (existing["account_fingerprint"] != account_fingerprint(self.user) or existing["sid"] != self.sid
                        or existing["run_id"] != self.run_id or existing["owner_pid"] != os.getpid()):
                    raise ValueError("held attempt identity mismatch")
                return existing["phone"]
            raise ValueError("finished attempt requires a new attempt_id")
        uid = uid or self.uid
        params = {"sid": self.sid}
        if uid:
            params["uid"] = uid
        # {Intent: persist intent before acquisition, then held state before any progress.}
        self._record("acquire_requested", sid=self.sid, uid=uid, state="acquiring")
        data = self._call("getPhone", params)
        phone = str(data.get("phone") or "").strip()
        if not phone:
            raise HaozhuError("getPhone", data.get("code"), "取号成功但没有手机号")
        self.uid = uid
        return phone

    def get_phone_multi(self, uid: str = "") -> str:
        """对接码池取号: 逐个渠道试, 某渠道失败自动换下一个。
        池空或全部失败时回落到 get_phone 原有行为(默认渠道)。"""
        pool = [u for u in ([uid] if uid else self.uid_pool) if u]
        if not pool:
            return self.get_phone()
        last_err = None
        for i, u in enumerate(pool):
            try:
                phone = self.get_phone(uid=u)
                if len(pool) > 1:
                    print(f"[hz] 渠道 {u} 取号成功: {phone}")
                return phone
            except HaozhuError as e:
                stock_empty = any(word in e.msg for word in
                                  ("无号", "没有可用", "暂无号码", "号码不足", "库存不足"))
                if not stock_empty:
                    raise
                last_err = e
                print(f"[hz] 渠道 {u} 取号失败({e.code}): "
                      + ("换下一个" if i < len(pool) - 1 else "渠道池已用尽"))
        if last_err:
            raise last_err
        return self.get_phone()

    def get_message(self, phone: str) -> str:
        data = self._call("getMessage", {"sid": self.sid, "phone": phone})
        sms = str(data.get("sms") or data.get("yzm") or "").strip()
        if extract_code(sms):
            code = extract_code(sms)
            self._record("code_received", phone=phone, code_source="platform", source="platform",
                         charge="unknown", receipt_hash=receipt_hash(sms),
                         verification_code=code, raw_sms=sms)
        return sms

    def release(self, phone: str) -> dict:
        data = self._call("cancelRecv", {"sid": self.sid, "phone": phone})
        if not _release_confirmed(data):
            raise HaozhuError("cancelRecv", "release_unknown", "release confirmation missing")
        self._record("release_confirmed", phone=phone, state="released", held=False,
                     refund="unknown")
        return data

    def blacklist(self, phone: str) -> dict:
        data = self._call("addBlacklist", {"sid": self.sid, "phone": phone})
        if not _release_confirmed(data):
            raise HaozhuError("addBlacklist", "release_unknown", "blacklist confirmation missing")
        self._record("blacklist_confirmed", phone=phone, state="blacklisted", held=False,
                     refund="unknown")
        return data

    def summary(self) -> dict:
        data = self._call("getSummary", {})
        try:
            balance = float(data.get("money"))
        except (TypeError, ValueError):
            balance = None
        raw_num = data.get("num")
        try:
            occupied = int(raw_num)
        except (TypeError, ValueError):
            occupied = None
        self._record("summary", money=data.get("money"), num=raw_num, num_raw=raw_num,
                     num_verified=False, balance=balance, occupied=occupied)
        return {"balance": balance, "occupied": occupied, "money": data.get("money"),
                "num": raw_num, "num_raw": raw_num, "num_verified": False,
                "occupied_semantics": "upstream num; occupancy meaning unverified"}

    def poll_code(self, phone: str, rounds: int = 36, interval: float = 5, log=print):
        """轮询收码(默认 36 轮×5s=3 分钟; 虚拟渠道短信可能慢)。返回验证码字符串。

        网络抖动(code=net)不终止轮询——短信可能在路上, 断网重试即可。"""
        net_errors = 0
        for i in range(rounds):
            try:
                sms = self.get_message(phone)
            except HaozhuError as e:
                if e.code == "net":
                    net_errors += 1
                    log(f"[!] 豪猪网络抖动(第{net_errors}次), 继续轮询")
                    if net_errors >= 12:  # 连续 12 次(约1分钟)网络故障才放弃
                        raise
                    sms = ""
                elif not e.waiting():
                    raise
                else:
                    sms = ""
            code = extract_code(sms)
            if code:
                log(f"[i] 豪猪收码成功(第{i + 1}轮)")
                return code
            if i + 1 < rounds:
                time.sleep(interval)
        raise HaozhuError("getMessage", "-1", f"验证码等待超时(轮询 {rounds} 次未收到短信)")


def _safe_message(message, client=None):
    text = str(message or "")
    for secret in (getattr(client, "password", ""), getattr(client, "token", "")):
        if secret:
            text = text.replace(secret, "[redacted]")
    text = re.sub(r"https?://[^\s]+", "[url]", text)
    text = re.sub(r"(?i)(password|pass|token|cookie|authorization|code)\s*[:=]\s*[^\s,;]+",
                  r"\1=[redacted]", text)
    return text[:200]


def _release_confirmed(data):
    if str(data.get("code", "")) not in ("0", "200"):
        return False
    msg = str(data.get("msg") or "").lower()
    if any(word in msg for word in ("失败", "错误", "异常", "稍后", "未知", "fail", "error", "unknown", "pending")):
        return False
    return True


def _pid_alive(pid):
    try:
        pid = int(pid or 0)
        if pid <= 0:
            return False
        if pid == os.getpid():
            return True
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.restype = wintypes.HANDLE
            kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
            kernel.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
            kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
            handle = kernel.OpenProcess(0x1000, False, pid)
            if not handle:
                return ctypes.get_last_error() == 5
            try:
                code = wintypes.DWORD()
                return bool(kernel.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == 259
            finally:
                kernel.CloseHandle(handle)
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False


def finish_attempt(attempt_id, *, ledger=None, client=None, cli_creds=None,
                   run_id="", allow_blacklist=False, reason="", recovery=False):
    """Release one recorded resource; unknown provider results remain pending.

    recovery=True is for the parent after it has terminated this exact worker.
    The recorded sid is always used, while the current SMS account must match.
    """
    ledger = ledger if ledger is not None else (client.ledger if client else Ledger())
    record = ledger.get_attempt(attempt_id)
    if not record:
        return {"attempt_id": attempt_id, "ok": False, "state": "missing", "held": False}
    if run_id and record["run_id"] != run_id:
        return {**record, "ok": False, "skipped": "run_mismatch"}
    if not record["held"] or not record["phone"]:
        return {**record, "ok": True, "skipped": "not_held"}
    same_worker = bool(client and client.attempt_id == attempt_id
                       and int(record.get("owner_pid") or 0) == os.getpid())
    if _pid_alive(record.get("owner_pid")) and (recovery or not same_worker):
        return {**record, "ok": False, "skipped": "running"}
    client = client or build_client(cli_creds, ledger=ledger)
    if account_fingerprint(client.user) != record["account_fingerprint"]:
        return {**record, "ok": False, "skipped": "account_mismatch"}
    for other in ledger.held():
        if (other["attempt_id"] != attempt_id and other["phone"] == record["phone"]
                and other["sid"] == record["sid"]
                and other["account_fingerprint"] == record["account_fingerprint"]
                and _pid_alive(other.get("owner_pid"))):
            return {**record, "ok": False, "skipped": "resource_owned_by_running_attempt"}
    if recovery and not run_id:
        return {**record, "ok": False, "skipped": "recovery_requires_run_id"}
    lease = ledger.resource_lease(record)
    locked_identity = tuple(record[k] for k in ("account_fingerprint", "sid", "phone"))
    owner = f"{os.getpid()}-{secrets.token_hex(6)}"
    if not ledger.acquire_lease(lease, owner, ttl=max(120, client.timeout * 8 + 120)):
        return {**record, "ok": False, "skipped": "finish_in_progress"}
    original = (client.sid, client.uid, client.attempt_id, client.run_id)
    try:
        record = ledger.get_attempt(attempt_id)
        if not record or not record["held"]:
            return {**(record or {}), "ok": True, "skipped": "not_held"}
        if tuple(record[k] for k in ("account_fingerprint", "sid", "phone")) != locked_identity:
            return {**record, "ok": False, "skipped": "identity_changed"}
        if run_id and record["run_id"] != run_id:
            return {**record, "ok": False, "skipped": "run_mismatch"}
        if account_fingerprint(client.user) != record["account_fingerprint"]:
            return {**record, "ok": False, "skipped": "account_mismatch"}
        same_worker = bool(client.attempt_id == attempt_id and client.run_id == record["run_id"]
                           and int(record.get("owner_pid") or 0) == os.getpid())
        if _pid_alive(record.get("owner_pid")) and (recovery or not same_worker):
            return {**record, "ok": False, "skipped": "running"}
        if any(other["attempt_id"] != attempt_id and
               tuple(other[k] for k in ("account_fingerprint", "sid", "phone")) == locked_identity
               for other in ledger.held()):
            return {**record, "ok": False, "skipped": "ambiguous_resource_owner"}
        client.sid, client.uid = record["sid"], record["uid"]
        client.attempt_id, client.run_id = attempt_id, record["run_id"]
        ledger.record("release_requested", attempt_id=attempt_id, state="release_pending", held=True,
                      reason=_safe_message(reason, client), recovery=bool(recovery))
        try:
            if allow_blacklist:
                client.blacklist(record["phone"])
            else:
                client.release(record["phone"])
        except Exception as error:
            category = "network" if getattr(error, "code", "") == "net" else "release_unknown"
            ledger.record("release_pending", attempt_id=attempt_id, state="release_pending", held=True,
                          error_category=category, last_error=_safe_message(error, client))
            return {**ledger.get_attempt(attempt_id), "ok": False}
        return {**ledger.get_attempt(attempt_id), "ok": True}
    finally:
        client.sid, client.uid, client.attempt_id, client.run_id = original
        ledger.release_lease(lease, owner)


def reclaim_held(*, attempt_id="", run_id="", ledger=None, cli_creds=None,
                 exclude_attempt_ids=(), recovery=False):
    ledger = ledger if ledger is not None else Ledger()
    if not attempt_id and not run_id:
        return []
    records = [r for r in ledger.held() if (not attempt_id or r["attempt_id"] == attempt_id)
               and (not run_id or r["run_id"] == run_id)
               and r["attempt_id"] not in set(exclude_attempt_ids)]
    if not records:
        return []
    client = build_client(cli_creds, ledger=ledger)
    return [finish_attempt(r["attempt_id"], ledger=ledger, client=client, run_id=run_id,
                           recovery=recovery, reason="parent_recovery") for r in records]


def h5_projects(h5_session: str, keyword: str) -> list:
    """H5 项目搜索(至少两个汉字或英文词)。返回 [HaozhuProject]; 需要 PHPSESSID。

    取号用的 sid 就是搜索结果里的数字 ID(如 104918); H5 的 16 位 hex 标识仅对接码用。
    """
    if os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
        raise HaozhuError("h5", "maintenance", "registration provider is paused")
    if not h5_session:
        raise HaozhuError("h5", "0", "没有 h5_session(在 h5.haozhuma.com 登录后复制 PHPSESSID)")
    resp = requests.get("https://h5.haozhuma.com/api.php",
                        params={"type": "30", "gjc": keyword.strip()},
                        headers={"Cookie": f"PHPSESSID={h5_session}", "Accept": "application/json"},
                        timeout=20, allow_redirects=False)
    try:
        data = resp.json()
    except ValueError:
        raise HaozhuError("h5", str(resp.status_code), resp.text[:120])
    msg = str(data.get("msg") or "")
    if "请选择正确的API" in msg:
        raise HaozhuError("h5", "0", "PHPSESSID 已失效, 请重新粘贴")
    if str(data.get("code", "")).strip() not in ("1", "200", "0") and "没有数据" not in msg:
        raise HaozhuError("h5", str(data.get("code")), msg)
    out = []
    for item in data.get("data") or []:
        sid = item.get("sid")
        if not sid:
            continue
        name = item.get("name") or ""
        m = re.search(r"【(\d+)】", name)
        out.append(HaozhuProject(sid=str(sid), project_id=m.group(1) if m else "", name=name))
    return out


def build_client(cli_creds: dict = None, *, ledger=None, attempt_id="", run_id="") -> HaozhuClient:
    creds = load_credentials(cli_creds)
    missing = [k for k in ("user", "password", "sid") if not creds.get(k)]
    if missing:
        raise SystemExit(f"豪猪凭据缺 {missing}: 配置 {CRED_FILE} 或环境变量 HAOZHU_USER/"
                         f"HAOZHU_PASSWORD/HAOZHU_SID(或命令行 --hz-user/--hz-pass/--hz-sid)")
    return HaozhuClient(creds["user"], creds["password"], creds["sid"],
                        token=creds.get("token", ""), uid=creds.get("uid", ""),
                        uids=creds.get("uids", ""), ledger=ledger,
                        attempt_id=attempt_id, run_id=run_id)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="豪猪接码工具(Trae 流水线配套)")
    parser.add_argument("command", choices=["search", "summary", "get"])
    parser.add_argument("keyword", nargs="?", help="search: 项目关键词")
    parser.add_argument("--hz-user"), parser.add_argument("--hz-pass")
    parser.add_argument("--hz-sid"), parser.add_argument("--hz-token")
    parser.add_argument("--h5-session", dest="h5_session")
    args = parser.parse_args()
    cli = {"user": args.hz_user, "password": args.hz_pass, "sid": args.hz_sid,
           "token": args.hz_token, "h5_session": args.h5_session}

    if args.command == "search":
        if not args.keyword:
            parser.error("search 需要关键词")
        session = cli["h5_session"] or load_credentials().get("h5_session", "")
        for p in h5_projects(session, args.keyword):
            print(f"  sid={p.sid}  项目ID={p.project_id}  {p.name}")
        return
    hz = build_client(cli)
    if args.command == "summary":
        print(hz.summary())
    elif args.command == "get":
        phone = hz.get_phone()
        print(f"取号成功: {phone}(已占用额度, 用完 release/blacklist)")


if __name__ == "__main__":
    main()
