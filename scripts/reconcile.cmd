@echo off
rem 全量对账包装脚本：给 Windows 计划任务用。
rem 注意：不加 --max-pages，只有扫到末页才会判定下架。
setlocal
set "ROOT=%~dp0.."
if "%HF_PROXY%"=="" set "HF_PROXY=http://127.0.0.1:7897"
if not exist "%ROOT%\data\logs" mkdir "%ROOT%\data\logs"
cd /d "%ROOT%"

set "UV=uv"
where uv >nul 2>&1 || set "UV=%USERPROFILE%\.local\bin\uv.exe"

"%UV%" run hf-data-crawl reconcile %* >> "data\logs\reconcile.log" 2>&1
exit /b %ERRORLEVEL%
