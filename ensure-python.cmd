@echo off
setlocal
cd /d "%~dp0"
set "VENV_PY=.venv\Scripts\python.exe"
if not exist "%VENV_PY%" goto repair
"%VENV_PY%" -c "import sys" >nul 2>&1
if not errorlevel 1 exit /b 0

:repair
set "VENV_VERSION=3.13"
if not exist ".venv\pyvenv.cfg" goto rebuild
for /f "tokens=3" %%V in ('findstr /b /c:"version = " ".venv\pyvenv.cfg"') do (
  for /f "tokens=1,2 delims=." %%A in ("%%V") do set "VENV_VERSION=%%A.%%B"
)

:rebuild
echo Repairing .venv with Python %VENV_VERSION%...
py -%VENV_VERSION% -m venv --upgrade .venv
if errorlevel 1 (
  echo Install Python %VENV_VERSION%, then run install.cmd again.
  exit /b 1
)
"%VENV_PY%" -c "import sys" >nul 2>&1
exit /b %errorlevel%
