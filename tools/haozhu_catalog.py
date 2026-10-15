"""Shared Haozhu H5 catalog and local settings for CLI and WebUI."""
import json
import math
import os
import re
import sys
import threading
from pathlib import Path

import requests
import haozhu_sms as hz

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import atomic_json

BASE = "https://h5.haozhuma.com/api.php"
_LOCK = threading.RLock()


class CatalogError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


def uid_list(value):
    parts = value if isinstance(value, list) else re.split(r"[,\s]+", str(value or ""))
    return list(dict.fromkeys(str(p).strip() for p in parts if str(p).strip()))


def save_config(changes):
    with _LOCK:
        try:
            data = json.loads(hz.CRED_FILE.read_text(encoding="utf-8-sig")) if hz.CRED_FILE.exists() else {}
        except (OSError, ValueError) as error:
            raise CatalogError("Local Haozhu configuration could not be read", 409) from error
        if not isinstance(data, dict):
            raise CatalogError("Local Haozhu configuration must be an object", 409)
        for key, value in changes.items():
            if value is None:
                continue
            if key == "uids":
                value = ",".join(uid_list(value))
            data[key] = value.strip() if isinstance(value, str) and key != "password" else value
        atomic_json(hz.CRED_FILE, data)
    return {"ok": True}


def select_channels(sid, uids, append=False):
    sid = str(sid).strip()
    if not re.fullmatch(r"[0-9]+", sid):
        raise CatalogError("Use the numeric project ID, not the H5 project identifier", 400)
    channels = uid_list(uids)
    if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", u) for u in channels):
        raise CatalogError("Invalid channel UID", 400)
    if any("-" in u and u.split("-", 1)[0].isdecimal() and u.split("-", 1)[0] != sid for u in channels):
        raise CatalogError("Selected channels belong to a different project", 400)
    with _LOCK:
        current = hz.load_credentials()
        if append and str(current.get("sid", "")) != sid:
            raise CatalogError("Select the same project before appending channels", 409)
        if append:
            channels = uid_list(uid_list(current.get("uids", "")) + uid_list(current.get("uid", "")) + channels)
        # {Intent: local selection never joins or deletes a provider-side channel.}
        return save_config({"sid": sid, "uid": "", "uids": channels})


def _parts(value):
    if isinstance(value, list):
        return [s.strip() for s in value if isinstance(s, str) and s.strip()]
    return [s.strip() for s in str(value or "").split("|") if s.strip()]


def _number(value, default, integer=False):
    try:
        text = str(value).strip().split(":", 1)[-1]
        number = int(text) if integer else float(text)
        return number if math.isfinite(number) else default
    except (ValueError, TypeError):
        return default


class Catalog:
    def __init__(self, session=None, http=None):
        self.session = str(session if session is not None else hz.load_credentials().get("h5_session", "")).strip()
        self.http = http or requests

    def call(self, kind, **params):
        # {意图：注册暂停仍允许浏览目录；完全离线由目录开关单独控制，加入渠道仍受注册暂停限制。}
        if os.environ.get("TRAE_HAOZHU_CATALOG_DISABLED") == "1":
            raise CatalogError("豪猪目录查询已暂停（离线模式）", 409)
        if kind == 4 and os.environ.get("TRAE_REGISTRATION_DISABLED") == "1":
            raise CatalogError("注册已暂停，加入豪猪账户操作也已暂停", 409)
        if not self.session:
            raise CatalogError("请先填写并保存豪猪 H5 PHPSESSID，再搜索项目或查询渠道", 503)
        if re.search(r"[;\s]", self.session):
            raise CatalogError("Paste only the PHPSESSID value", 400)
        try:
            response = self.http.get(BASE, params={"type": str(kind), **params},
                headers={"Cookie": "PHPSESSID=" + self.session, "Accept": "application/json"},
                timeout=20, allow_redirects=False)
            if response.status_code != 200:
                raise CatalogError("Haozhu H5 HTTP " + str(response.status_code))
            data = response.json()
        except (requests.RequestException, ValueError) as error:
            raise CatalogError("Haozhu H5 connection or JSON response failed") from error
        if not isinstance(data, dict):
            raise CatalogError("Haozhu H5 response must be an object")
        code, message = str(data.get("code", "")).strip(), str(data.get("msg") or "")
        if any(word in message for word in ("请选择正确的API", "请先登录", "登录失效", "登录过期")):
            raise CatalogError("H5 会话已失效，请重新填写并保存 PHPSESSID", 410)
        if kind == 4 and "已添加" in message:
            return {"data": []}
        if code not in ("1", "200") and not (kind != 4 and code == "0"):
            raise CatalogError("Haozhu H5: " + message.replace(self.session, "[redacted]")[:160])
        return data

    def rows(self, kind, **params):
        data = self.call(kind, **params).get("data")
        if data in (None, ""):
            return []
        if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
            raise CatalogError("Haozhu H5 data must be a list")
        return data

    def projects(self, keyword):
        keyword = keyword.strip()
        if not keyword or len(keyword) > 100:
            raise CatalogError("Enter a project keyword (1-100 characters)", 400)
        result = []
        for row in self.rows(30, gjc=keyword):
            if not row.get("sid"):
                continue
            name = str(row.get("name") or "").strip()
            match = re.search(r"【(\d+)】", name)
            result.append({"sid": str(row["sid"]), "project_id": match[1] if match else "", "name": name})
        return result

    def channels(self, sid="", keyword="", mine=False):
        if not mine and not str(sid).strip():
            raise CatalogError("A project identifier is required", 400)
        query = ({"gjc": keyword.strip()} if keyword.strip() else {}) if mine else {"sid": str(sid).strip()}
        result = []
        for row in self.rows(3 if mine else 8, **query):
            if not row.get("uid"):
                continue
            result.append({"uid": str(row["uid"]), "name": str(row.get("mc") or "").strip(),
                "price": _number(row.get("yhj"), None), "stock": _number(row.get("zxky"), -1, True),
                "isps": _parts(row.get("yyy")), "provinces": _parts(row.get("sheng")),
                "segments": _parts(row.get("hd")), "segment_type": str(row.get("haoduan") or "").strip(),
                "pinned": row.get("zd") in (1, "1", True), "state": str(row.get("djzt") or "").strip(),
                "updated_at": row.get("time", "") if isinstance(row.get("time", ""), str) else ""})
        if not mine:
            result.sort(key=lambda row: (not row["pinned"], -row["stock"]))
        return result

    def add_uid(self, uid):
        uid = uid.strip()
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", uid):
            raise CatalogError("Invalid channel UID", 400)
        self.call(4, djm=uid)
        return {"added": uid}
