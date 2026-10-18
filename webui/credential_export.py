"""Read-only, gateway-independent export of nested registration credentials."""
from __future__ import annotations

import hashlib
import json
import math
import time
from pathlib import Path


def _expiry_ms(value) -> int:
    number = float(value or 0)
    if not math.isfinite(number) or number < 0:
        raise ValueError("invalid expiry")
    return int(number * 1000 if 0 < number < 100_000_000_000 else number)


def _variant(auth: dict) -> str:
    value = str(auth.get("variant") or "solo").strip().lower()
    return value if value in ("solo", "cn", "intl", "solo-intl") else "solo"


def build_credentials_export(directory: str | Path, *, exported_at: int | None = None) -> dict:
    selected: dict[tuple[str, str, str], tuple[tuple[int, int, str], dict]] = {}
    skipped = []
    paths = sorted(Path(directory).glob("*.json"))
    valid_files = 0
    for path in paths:
        try:
            stat = path.stat()
            credential = json.loads(path.read_text(encoding="utf-8-sig"))
            if not isinstance(credential, dict):
                raise ValueError("invalid credential")
            auth, account = credential.get("auth"), credential.get("account")
            if not isinstance(auth, dict) or not isinstance(account, dict):
                raise ValueError("invalid nested credential")
            if not any(isinstance(auth.get(key), str) and auth[key].strip()
                       for key in ("accessToken", "refreshToken")):
                raise ValueError("missing token")
            uid = str(account.get("uid") or "").strip()
            if not uid:
                raise ValueError("missing uid")
            expiry = _expiry_ms(auth.get("expiresAt"))
            provider = str(credential.get("provider") or credential.get("type") or "trae")
            if provider != "trae":
                raise ValueError("not a Trae credential")
            json.dumps(credential, allow_nan=False)
        except (OSError, ValueError, TypeError, OverflowError):
            skipped.append(path.name)
            continue
        valid_files += 1
        identity = (provider, _variant(auth), uid)
        rank = (expiry, stat.st_mtime_ns, path.name)
        if identity not in selected or rank > selected[identity][0]:
            selected[identity] = (rank, credential)

    accounts = [item[1] for item in selected.values()]
    return {
        "format": "trae-credentials",
        "version": 1,
        "exportedAt": exported_at if exported_at is not None else int(time.time() * 1000),
        "count": len(accounts),
        "accounts": accounts,
        "summary": {
            "sourceFiles": len(paths),
            "duplicatesRemoved": valid_files - len(accounts),
            "skippedFiles": skipped,
        },
    }


def build_agent2api_export(directory: str | Path, *, exported_at: int | None = None) -> dict:
    source = build_credentials_export(directory, exported_at=exported_at)
    accounts = []
    for index, credential in enumerate(source["accounts"], 1):
        auth, identity = credential["auth"], credential["account"]
        uid, variant = str(identity["uid"]).strip(), _variant(auth)
        excluded = {"auth", "account", "type", "id", "provider", "rateLimits", "desktop"}
        record = {key: value for section in (credential, auth, identity)
                  for key, value in section.items()
                  if key not in excluded and not key.startswith("_")}
        # {意图：批量文件导入直接读平铺字段，不走单账号 Credential::from_payload；
        # id 与 account_store/trae_accounts.rs 一致，不能只给 auth/account 嵌套对象。}
        record.update({
            "id": f"trae-{variant}-" + hashlib.sha256(f"{variant}-{uid}".encode()).hexdigest(),
            "provider": "trae",
            "uid": uid,
            "variant": variant,
            "accessToken": str(auth.get("accessToken") or "").strip(),
            "refreshToken": str(auth.get("refreshToken") or "").strip(),
            "expiresAt": _expiry_ms(auth.get("expiresAt")),
            "name": str(identity.get("nickname") or credential.get("phone") or "Trae")[:100],
            "enabled": True,
            "desktop": False,
            "priority": index,
            "proxy": None,
            "source": "import",
        })
        accounts.append(record)
    return {
        "version": 2,
        "exportedAt": source["exportedAt"],
        "customProviders": [],
        "accounts": accounts,
        "count": len(accounts),
        "summary": source["summary"],
    }
