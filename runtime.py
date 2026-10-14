"""Directory-local paths, migration records, and durable service logging."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
WORKSPACE = APP_DIR.parent
DATA_DIR = Path(os.environ.get("TRAE_DATA_DIR", APP_DIR / "data")).resolve()
STATE_DIR = DATA_DIR / "state"
CONFIG_DIR = DATA_DIR / "config"
LOG_DIR = DATA_DIR / "logs"
CRED_DIR = DATA_DIR / "credentials"
RUN_DIR = DATA_DIR / "runs"
PID_FILE = STATE_DIR / "service.json"
VERSION = "1.0.0"


def atomic_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def initialize(migrate: bool = True) -> dict:
    for path in (STATE_DIR, CONFIG_DIR, LOG_DIR, CRED_DIR, RUN_DIR,
                 DATA_DIR / "http-trace", DATA_DIR / "register-diagnostics",
                 DATA_DIR / "cloak-profiles"):
        path.mkdir(parents=True, exist_ok=True)
    # {意图：通过子进程继承路径；旧研究命令未带这些变量时继续使用旧目录。}
    os.environ["TRAE_DATA_DIR"] = str(DATA_DIR)
    os.environ["TRAE_PROXY_FILE"] = str(CONFIG_DIR / "proxies.txt")
    os.environ["TRAE_HAOZHU_FILE"] = str(CONFIG_DIR / "haozhu.json")
    os.environ["TRAE_SLIDER_LIB"] = str(APP_DIR / "vendor" / "slider")
    os.environ["TRAE_HEADLESS"] = "1"
    os.environ["PYTHONUNBUFFERED"] = "1"
    marker = STATE_DIR / "migration.json"
    if marker.exists() or not migrate:
        return {"migrated": False}
    sources = [(APP_DIR / "webui" / name, STATE_DIR / name)
               for name in ("queue.json", "metrics.json", "continuous.json")]
    sources += [(APP_DIR / "webui" / "config.json", CONFIG_DIR / "webui.json"),
                (WORKSPACE / "tmp" / "haozhu.json", CONFIG_DIR / "haozhu.json")]
    proxy = APP_DIR / "tools" / "proxies.txt"
    if not proxy.exists() or not proxy.read_text(encoding="utf-8-sig").strip():
        proxy = WORKSPACE / "Webshare 250 proxies.txt"
    sources.append((proxy, CONFIG_DIR / "proxies.txt"))
    sources.extend((src, CRED_DIR / src.name)
                   for src in (WORKSPACE / "tmp" / "register-credentials").glob("*.json"))
    copied = []
    for source, dest in sources:
        if source.is_file() and not dest.exists():
            shutil.copy2(source, dest)
            copied.append({"source": str(source), "destination": str(dest),
                           "bytes": dest.stat().st_size,
                           "sha256": hashlib.sha256(dest.read_bytes()).hexdigest()})
    record = {"time": datetime.now(timezone.utc).isoformat(), "files": copied}
    atomic_json(marker, record)
    return {"migrated": True, "files": len(copied)}


def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("trae.registration")
    if not logger.handlers:
        handler = RotatingFileHandler(LOG_DIR / "application.log", maxBytes=5_000_000,
                                      backupCount=5, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger
