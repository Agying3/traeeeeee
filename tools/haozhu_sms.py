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

import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
CRED_FILE = ROOT / "tmp" / "haozhu.json"
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
                 uid: str = "", timeout: float = 20):
        self.user, self.password, self.sid = user, password, str(sid)
        self.token = token or ""
        self.uid = uid or ""  # 对接码(渠道): 锁定低价渠道用, 留空走默认
        self.http = requests.Session()

    # ---- 底层 ----

    def _call(self, api: str, params: dict, auth: bool = True) -> dict:
        query = {"api": api, **params}
        if auth:
            query["token"] = self.token
        try:
            resp = self.http.get(BASE, params=query, timeout=20)
        except requests.RequestException as e:
            raise HaozhuError(api, "net", str(e)[:120])
        if resp.status_code in (401, 403):
            raise HaozhuError(api, str(resp.status_code), "token 失效或账号被拒")
        try:
            data = resp.json()
        except ValueError:
            raise HaozhuError(api, str(resp.status_code), f"响应不是 JSON: {resp.text[:120]}")
        code = str(data.get("code", "")).strip()
        msg = str(data.get("msg") or "")
        if code in ("0", "200"):
            return data
        err = HaozhuError(api, code, msg)
        if err.waiting():
            return data
        raise err

    def ensure_login(self) -> None:
        if not self.token:
            self.login()

    # ---- 业务 ----

    def login(self) -> str:
        data = self._call("login", {"user": self.user, "pass": self.password}, auth=False)
        token = str(data.get("token") or "").strip()
        if not token:
            raise HaozhuError("login", data.get("code"), "登录成功但没有 token")
        self.token = token
        return token

    def get_phone(self, uid: str = "") -> str:
        self.ensure_login()
        params = {"sid": self.sid}
        uid = uid or self.uid
        if uid:
            params["uid"] = uid
        try:
            data = self._call("getPhone", params)
        except HaozhuError as e:
            if e.token_invalid():
                self.login()
                data = self._call("getPhone", params)
            else:
                raise
        phone = str(data.get("phone") or "").strip()
        if not phone:
            raise HaozhuError("getPhone", data.get("code"), "取号成功但没有手机号")
        return phone

    def get_message(self, phone: str) -> str:
        self.ensure_login()
        data = self._call("getMessage", {"sid": self.sid, "phone": phone})
        return str(data.get("sms") or data.get("yzm") or "").strip()

    def release(self, phone: str) -> None:
        self.ensure_login()
        self._call("cancelRecv", {"sid": self.sid, "phone": phone})

    def blacklist(self, phone: str) -> None:
        self.ensure_login()
        self._call("addBlacklist", {"sid": self.sid, "phone": phone})

    def summary(self) -> dict:
        self.ensure_login()
        data = self._call("getSummary", {})
        try:
            balance = float(data.get("money", 0))
        except (TypeError, ValueError):
            balance = -1
        try:
            occupied = int(data.get("num", -1))
        except (TypeError, ValueError):
            occupied = -1
        return {"balance": balance, "occupied": occupied}

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
                log(f"[i] 豪猪收码成功(第{i + 1}轮): {code}  短信: {sms[:60]}")
                return code
            if i + 1 < rounds:
                time.sleep(interval)
        raise HaozhuError("getMessage", "-1", f"验证码等待超时(轮询 {rounds} 次未收到短信)")


def h5_projects(h5_session: str, keyword: str) -> list:
    """H5 项目搜索(至少两个汉字或英文词)。返回 [HaozhuProject]; 需要 PHPSESSID。

    取号用的 sid 就是搜索结果里的数字 ID(如 104918); H5 的 16 位 hex 标识仅对接码用。
    """
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


def build_client(cli_creds: dict = None) -> HaozhuClient:
    creds = load_credentials(cli_creds)
    missing = [k for k in ("user", "password", "sid") if not creds.get(k)]
    if missing:
        raise SystemExit(f"豪猪凭据缺 {missing}: 配置 {CRED_FILE} 或环境变量 HAOZHU_USER/"
                         f"HAOZHU_PASSWORD/HAOZHU_SID(或命令行 --hz-user/--hz-pass/--hz-sid)")
    return HaozhuClient(creds["user"], creds["password"], creds["sid"],
                        token=creds.get("token", ""), uid=creds.get("uid", ""))


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
