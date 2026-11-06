@echo off
rem 日增量采集包装脚本：给 Windows 计划任务用。
rem 关键点：代理与令牌不硬编码进代码，只在此处给默认值；日志留在 data\logs 便于事后查因。
setlocal
set "ROOT=%~dp0.."
if "%HF_PROXY%"=="" set "HF_PROXY=http://127.0.0.1:7897"
if not exist "%ROOT%\data\logs" mkdir "%ROOT%\data\logs"
cd /d "%ROOT%"

set "UV=uv"
where uv >nul 2>&1 || set "UV=%USERPROFILE%\.local\bin\uv.exe"

"%UV%" run hf-data-crawl collect --refresh-top 20000 --skip-if-done %* >> "data\logs\collect.log" 2>&1
exit /b %ERRORLEVEL%
