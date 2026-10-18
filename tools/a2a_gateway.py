"""HTTP-only adapter for an independently deployed external gateway."""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE = os.environ.get('A2A_BASE_URL', 'http://127.0.0.1:3065').rstrip('/')
ADMIN_USER = os.environ.get('A2A_ADMIN_USER', '').strip()
ADMIN_PASS = os.environ.get('A2A_ADMIN_PASS', '')


def req(method, path, body=None, token=None, timeout=15):
    request = urllib.request.Request(BASE + path, method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={'content-type':'application/json'})
    if token:
        request.add_header('authorization', 'Bearer ' + token)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, {'error':'gateway returned an HTTP error'}


def _login():
    if not ADMIN_USER or not ADMIN_PASS:
        raise RuntimeError('Set A2A_ADMIN_USER and A2A_ADMIN_PASS for the external gateway')
    status, result = req('POST', '/api/panel/login', {'username':ADMIN_USER,'password':ADMIN_PASS})
    token = (result.get('data') or {}).get('accessToken') or ''
    if status not in (200,201) or not token:
        raise RuntimeError('External gateway login failed')
    return token


def alive():
    try:
        status, _ = req('GET', '/health', timeout=3)
        return status == 200
    except (OSError, ValueError):
        return False


def cmd_start():
    # {意图：本程序只对接外部 HTTP 服务，不附带或启动第三方网关二进制。}
    print('External gateway ready' if alive() else 'Deploy the external gateway separately and set A2A_BASE_URL')
    return 0 if alive() else 1


def cmd_login():
    _login()
    print('External gateway login succeeded; credentials are not printed')
    return 0


def cmd_import(path):
    credential = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    status, _ = req('POST', '/api/accounts', credential, token=_login())
    print('Account imported' if status in (200,201) else 'Account import failed')
    return 0 if status in (200,201) else 1


def cmd_health():
    ready = alive()
    print(json.dumps({'gateway_ready':ready,'admin_configured':bool(ADMIN_USER and ADMIN_PASS)}))
    return 0 if ready else 1


if __name__ == '__main__':
    try:
        command = sys.argv[1] if len(sys.argv)>1 else 'health'
        if command == 'import':
            code = cmd_import(sys.argv[2])
        else:
            code = {'start':cmd_start,'login':cmd_login,'health':cmd_health}[command]()
    except Exception as error:
        print(type(error).__name__ + ': external gateway operation failed; check local configuration')
        code = 1
    raise SystemExit(code)
