"""File-based control shared by the service and its registration child."""
import asyncio
import json
import os
import re
import time
from pathlib import Path


def options():
    return json.loads(os.environ.get("TRAE_RUN_OPTIONS", "{}"))


def emit(stage, **fields):
    name = os.environ.get("TRAE_PROGRESS_FILE")
    if name:
        path = Path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        try:
            previous = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            previous = {}
        temporary.write_text(json.dumps({**previous, "stage": stage, "updated": time.time(), **fields}),
                             encoding="utf-8")
        temporary.replace(path)


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
                emit("验证码已接收", status="running", phone=phone, wait_code_since=None)
                return code
            if pending is None and time.monotonic() >= next_poll:
                pending = asyncio.create_task(asyncio.to_thread(client.get_message, phone))
            if pending and pending.done():
                try:
                    code = extract_code(pending.result())
                except Exception as error:
                    # {意图：临时等码和网络错误继续轮询；明确的账号或库存错误直接结束。}
                    if not (getattr(error, "code", "") == "net" or
                            callable(getattr(error, "waiting", None)) and error.waiting()):
                        raise
                    code = ""
                pending = None
                if code:
                    emit("验证码已接收", status="running", phone=phone, wait_code_since=None)
                    return code
                next_poll = time.monotonic() + interval
            await asyncio.sleep(0.1)
        emit("等码超时", status="timeout", phone=phone)
        raise TimeoutError("SMS wait timeout")
    finally:
        if pending:
            pending.cancel()
