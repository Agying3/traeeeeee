"""Standalone headless registration service: start | stop | status | doctor | serve."""
from __future__ import annotations

import argparse
import importlib
import importlib.metadata
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = ROOT / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
if PYTHON.exists() and Path(sys.prefix).resolve() != PYTHON.parent.parent.resolve():
    raise SystemExit(subprocess.call([str(PYTHON), "-X", "utf8", str(Path(__file__).resolve()),
                                     *sys.argv[1:]], cwd=ROOT))

from runtime import (APP_DIR, DATA_DIR, LOG_DIR, PID_FILE, VERSION, atomic_json,
                     configure_logging, initialize)


def health(port: int) -> dict:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
            return json.loads(response.read())
    except (OSError, ValueError, urllib.error.URLError):
        return {}


def owner(port: int, instance: str = "") -> dict:
    result = health(port)
    if result.get("service") != "trae-registration" or result.get("root") != str(ROOT):
        return {}
    if instance and result.get("instance") != instance:
        return {}
    return result


def read_pid() -> dict:
    try:
        return json.loads(PID_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def doctor() -> int:
    checks = {}
    modules = {"fastapi": "fastapi", "uvicorn": "uvicorn", "pydantic": "pydantic",
               "requests": "requests", "cryptography": "cryptography", "numpy": "numpy",
               "opencv-python": "cv2", "captcha-recognizer": "captcha_recognizer.slider",
               "onnxruntime": "onnxruntime", "cloakbrowser": "cloakbrowser",
               "geoip2": "geoip2", "playwright": "playwright.async_api"}
    for package, module in modules.items():
        try:
            importlib.import_module(module)
            checks[package] = {"ok": True, "version": importlib.metadata.version(package)}
        except Exception as error:
            checks[package] = {"ok": False, "error": str(error)}
    sys.path.insert(0, str(ROOT / "tools"))
    sys.path.insert(0, str(ROOT / "vendor" / "slider"))
    for module in ("human_track", "haozhu_sms", "trae_pipeline_lib", "page_diagnostics",
                   "stage1_browser_captcha"):
        try:
            importlib.import_module(module)
            checks[module] = {"ok": True}
        except Exception as error:
            checks[module] = {"ok": False, "error": str(error)}
    checks["curl"] = {"ok": __import__("shutil").which("curl") is not None}
    checks["own_venv"] = {"ok": Path(sys.prefix).resolve() == ROOT / ".venv"}
    try:
        from cloakbrowser import binary_info
        info = binary_info()
        checks["browser"] = {"ok": bool(info.get("installed")), "details": info}
    except Exception as error:
        checks["browser"] = {"ok": False, "error": str(error)}
    result = {"python": sys.executable, "root": str(ROOT), "data": str(DATA_DIR),
              "checks": checks}
    atomic_json(LOG_DIR / "doctor.json", result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if all(item["ok"] for item in checks.values()) else 1


def serve(port: int, instance: str) -> int:
    sys.path.insert(0, str(ROOT / "webui"))
    import uvicorn
    from trae_reg_api import app
    instance = instance or uuid.uuid4().hex
    os.environ["TRAE_SERVICE_INSTANCE"] = instance
    atomic_json(PID_FILE, {"pid": os.getpid(), "port": port, "instance": instance,
                           "root": str(ROOT), "python": sys.executable, "started": time.time()})
    logger = configure_logging()
    logger.info("service start pid=%s port=%s python=%s", os.getpid(), port, sys.executable)
    try:
        server = uvicorn.Server(uvicorn.Config(app, host=os.environ.get("TRAE_LISTEN_HOST", "127.0.0.1"), port=port,
                                log_level="info", timeout_graceful_shutdown=12))
        app.state.shutdown = lambda: setattr(server, "should_exit", True)
        server.run()
        return 0
    finally:
        if read_pid().get("instance") == instance:
            PID_FILE.unlink(missing_ok=True)
        logger.info("service stopped pid=%s", os.getpid())


def start(port: int) -> int:
    current = owner(port)
    if current:
        print(f"Already running: http://127.0.0.1:{port} PID={current['pid']}")
        return 0
    existing = read_pid()
    if existing and owner(int(existing.get("port", port)), existing.get("instance", "")):
        print(f"Already running on port {existing['port']}; use status or stop first.")
        return 1
    with socket.socket() as probe:
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            print(f"Port {port} is occupied; no other process was stopped.")
            return 1
    instance = uuid.uuid4().hex
    options = ({"creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP}
               if os.name == "nt" else {"start_new_session": True})
    with (LOG_DIR / "server.log").open("ab") as output:
        process = subprocess.Popen([sys.executable, "-X", "utf8", str(ROOT / "run.py"),
                                    "serve", "--port", str(port), "--instance", instance],
                                   cwd=ROOT, env=os.environ.copy(), stdin=subprocess.DEVNULL,
                                   stdout=output, stderr=subprocess.STDOUT, **options)
    for _ in range(60):
        if owner(port, instance):
            print(f"Started: http://127.0.0.1:{port} PID={health(port)['pid']}")
            print(f"Logs: {LOG_DIR}")
            return 0
        if process.poll() is not None:
            break
        time.sleep(0.25)
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=10)
    print(f"Startup failed; see {LOG_DIR / 'server.log'}")
    return 1


def stop(port: int) -> int:
    record = read_pid()
    if not record:
        print("No managed registration service is running.")
        return 0
    port = int(record.get("port", port))
    current = owner(port, record.get("instance", ""))
    if not current:
        print("No matching service on this port; no process was stopped.")
        PID_FILE.unlink(missing_ok=True)
        return 0
    request = urllib.request.Request(f"http://127.0.0.1:{port}/api/program/stop", method="POST",
                                     headers={"X-Service-Instance": record["instance"]})
    with urllib.request.urlopen(request, timeout=15) as response:
        response.read()
    for _ in range(60):
        if not owner(port, record["instance"]):
            PID_FILE.unlink(missing_ok=True)
            print("Registration service stopped; queue retained.")
            return 0
        time.sleep(0.25)
    print("Stop is still pending; inspect server.log before retrying.")
    return 1


def main() -> int:
    if sys.argv[1:2] == ["haozhu"]:
        initialize(migrate=os.environ.get("TRAE_SKIP_MIGRATION") != "1")
        sys.path.insert(0, str(ROOT / "tools"))
        from haozhu_cli import main as haozhu_main
        return haozhu_main(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=("start", "stop", "status", "doctor", "serve"),
                        default="start")
    parser.add_argument("--port", type=int, default=8898)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--instance", default="", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.data_dir:
        # The runtime module is imported before arguments, so re-enter with the chosen directory.
        target = str(args.data_dir.resolve())
        if os.environ.get("TRAE_DATA_DIR") != target:
            return subprocess.call([sys.executable, "-X", "utf8", str(ROOT / "run.py"),
                                    *sys.argv[1:]], env={**os.environ, "TRAE_DATA_DIR": target})
    if not PYTHON.exists():
        print("Run install.cmd to create this program's .venv first.")
        return 1
    if args.data_dir:
        os.environ["TRAE_SKIP_MIGRATION"] = "1"
    initialize(migrate=os.environ.get("TRAE_SKIP_MIGRATION") != "1")
    if args.command == "doctor":
        return doctor()
    if args.command == "serve":
        return serve(args.port, args.instance)
    if args.command == "start":
        return start(args.port)
    if args.command == "stop":
        return stop(args.port)
    record = read_pid()
    result = owner(int(record.get("port", args.port)))
    print(json.dumps(result or {"status": "stopped", "data": str(DATA_DIR)},
                     ensure_ascii=False, indent=2))
    return 0 if result else 1


if __name__ == "__main__":
    raise SystemExit(main())
