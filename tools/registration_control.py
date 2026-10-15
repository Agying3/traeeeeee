"""File-based control shared by the service and its registration child."""
import asyncio
import json
import os
import re
import time
import uuid
from pathlib import Path


def _ledger_for_env():
    try:
        from haozhu_ledger import Ledger
        return Ledger()
    except Exception:
        return None


def register_held(phone, *, attempt_id="", run_id="", sid="", uid="", account="", proxy=None):
    """Durably register ownership before an irreversible registration action."""
    ledger = _ledger_for_env()
    if ledger is None:
        raise RuntimeError("Haozhu ledger unavailable before held resource use")
    attempt_id = attempt_id or os.environ.get("TRAE_ATTEMPT_ID", "")
    run_id = run_id or os.environ.get("TRAE_RUN_ID", "")
    sid = sid or os.environ.get("TRAE_HAOZHU_SID", "")
    uid = uid or os.environ.get("TRAE_HAOZHU_UID", "")
    account = account or os.environ.get("TRAE_HAOZHU_USER", "")
    if not (phone and attempt_id and run_id and sid):
        raise RuntimeError("held registration requires attempt_id, run_id, sid and phone")
    record = ledger.begin(attempt_id, run_id, sid, uid=uid, account=account,
                          phone=phone, proxy=proxy, state="held", held=True)
    if not record["held"] or record["phone"] != phone or record["owner_pid"] != os.getpid():
        raise RuntimeError("held ownership must be acquired or explicitly transferred")
    return record



def public_log(message):
    text = str(message)
    secret = r"(?:[a-z_\-]*(?:token|cookie|password|secret|ticket|sessionid)|pass|authorization|auth_?code|verification_?code|sms|code|yzm)"
    pattern = r'''(?i)(["']?''' + secret + r'''["']?\s*[:=]\s*)(?:"[^"\r\n]*(?:"|$)|'[^'\r\n]*(?:'|$)|[^\s,;}&]+)'''
    text = re.sub(pattern, lambda match: match.group(1) + "[redacted]", text)
    return re.sub(r"https?://[^\s]+", "[url]", text)


def options():
    return json.loads(os.environ.get("TRAE_RUN_OPTIONS", "{}"))


def emit(stage, **fields):
    name = os.environ.get("TRAE_PROGRESS_FILE")
    if not name:
        return False
    temporary = None
    try:
        path = Path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(previous, dict):
                previous = {}
        except (OSError, ValueError):
            previous = {}
        identity = {key: os.environ[env] for key, env in (
            ("attempt_id", "TRAE_ATTEMPT_ID"), ("run_id", "TRAE_RUN_ID")) if os.environ.get(env)}
        from haozhu_ledger import _redact
        payload = _redact({**previous, **identity, "stage": stage, "updated": time.time(), **fields})
        with temporary.open("x", encoding="utf-8") as out:
            json.dump(payload, out, ensure_ascii=False)
            out.flush()
            os.fsync(out.fileno())
        temporary.replace(path)
        return True
    except (OSError, ValueError, TypeError):
        return False
    finally:
        if temporary:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def manual_code():
    name = os.environ.get("TRAE_CODE_FILE")
    if not name:
        return ""
    path = Path(name)
    try:
        value = path.read_text(encoding="utf-8").strip()
        path.unlink(missing_ok=True)
        return value if re.fullmatch(r"[0-9]{4,8}", value) else ""
    except OSError:
        return ""


async def wait_sms(client, phone, timeout_s=None, interval=5):
    from haozhu_sms import extract_code
    timeout_s = timeout_s if timeout_s is not None else options().get("code_timeout_minutes", 5) * 60
    emit("等待短信", status="waiting_code", phone=phone, wait_code_since=time.time())
    deadline = time.monotonic() + timeout_s
    pending = None
    next_poll = 0
    try:
        while time.monotonic() < deadline:
            code = manual_code()
            if code:
                marker = getattr(client, "mark_manual_code", None)
                if callable(marker):
                    marker(code)
                emit("验证码已接收", status="running", phone=phone, wait_code_since=None,
                     code_source="manual", charge="unknown")
                return code
            if pending is None and time.monotonic() >= next_poll:
                pending = asyncio.create_task(asyncio.to_thread(client.get_message, phone))
            if pending and pending.done():
                try:
                    sms = pending.result()
                    code = extract_code(sms)
                except Exception as error:
                    # {意图：临时等码和网络错误继续轮询；明确的账号或库存错误直接结束。}
                    if not (getattr(error, "code", "") == "net" or
                            callable(getattr(error, "waiting", None)) and error.waiting()):
                        raise
                    code = ""
                pending = None
                if code:
                    emit("验证码已接收", status="running", phone=phone, wait_code_since=None,
                         code_source="platform", charge="unknown")
                    return code
                next_poll = time.monotonic() + interval
            await asyncio.sleep(0.1)
        emit("等码超时", status="timeout", phone=phone)
        raise TimeoutError("SMS wait timeout")
    finally:
        if pending:
            pending.cancel()
