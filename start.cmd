@echo off
setlocal
cd /d "%~dp0"
set "VENV_PY=.venv\Scripts\python.exe"

if not exist "%VENV_PY%" (
  call "%~dp0install.cmd"
  if errorlevel 1 exit /b 1
) else (
  call "%~dp0ensure-python.cmd"
  if errorlevel 1 exit /b 1
  "%VENV_PY%" -c "import fastapi, uvicorn" >nul 2>&1
  if errorlevel 1 (
    echo Python dependencies are missing; running install.cmd...
    call "%~dp0install.cmd"
    if errorlevel 1 exit /b 1
  )
)

"%VENV_PY%" -X utf8 run.py start %*
exit /b %errorlevel%
