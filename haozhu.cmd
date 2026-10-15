@echo off
setlocal
cd /d "%~dp0"
call "%~dp0ensure-python.cmd"
if errorlevel 1 exit /b 1
".venv\Scripts\python.exe" -X utf8 run.py haozhu %*
exit /b %errorlevel%
