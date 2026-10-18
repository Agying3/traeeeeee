@echo off
setlocal
cd /d "%~dp0"
set "VENV_PY=.venv\Scripts\python.exe"

call "%~dp0ensure-python.cmd"
if errorlevel 1 exit /b 1

"%VENV_PY%" -m pip install --index-url https://pypi.org/simple -r requirements.txt
if errorlevel 1 exit /b 1
"%VENV_PY%" -c "from cloakbrowser import ensure_binary; ensure_binary()"
if errorlevel 1 exit /b 1
"%VENV_PY%" -X utf8 run.py doctor
exit /b %errorlevel%
