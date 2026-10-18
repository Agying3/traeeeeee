@echo off
setlocal
cd /d "%~dp0"
".venv\Scripts\python.exe" -X utf8 run.py stop %*
exit /b %errorlevel%
