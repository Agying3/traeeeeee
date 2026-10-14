@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  if errorlevel 1 exit /b 1
)
".venv\Scripts\python.exe" -m pip install --index-url https://pypi.org/simple -r requirements.txt
if errorlevel 1 exit /b 1
".venv\Scripts\python.exe" -c "from cloakbrowser import ensure_binary; ensure_binary()"
if errorlevel 1 exit /b 1
".venv\Scripts\python.exe" -X utf8 run.py doctor
exit /b %errorlevel%
