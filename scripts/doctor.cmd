@echo off
rem 体检包装脚本：给 Windows 计划任务用。有问题时退出码为 1，并在 data\ALERT.txt 留下说明。
setlocal
set "ROOT=%~dp0.."
if not exist "%ROOT%\data\logs" mkdir "%ROOT%\data\logs"
cd /d "%ROOT%"

set "UV=uv"
where uv >nul 2>&1 || set "UV=%USERPROFILE%\.local\bin\uv.exe"

"%UV%" run hf-data-crawl doctor >> "data\logs\doctor.log" 2>&1
exit /b %ERRORLEVEL%
