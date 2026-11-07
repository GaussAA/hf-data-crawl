@echo off
rem 规模补齐包装脚本：给 Windows 计划任务用。
rem
rem 这是全流程里最慢的一步：每条要单独请求 datasets-server，并发 16 时约 26 req/s。
rem 故一周只跑一次，且只补全站中还没数据的那些——fetch-sizes 是增量合并，
rem 已有 ok 记录会跳过，重复执行不会重抓。
rem
rem 默认给 4 小时时限；中断也没关系，已抓到的都已落库，下周接着补。
setlocal
rem 撞上别的写命令时最多等 15 分钟（fetch-sizes 可能要跑一小时），
rem 而不是直接失败——一次时间重叠不该白丢一整轮。
set "ROOT=%~dp0.."
if "%HF_PROXY%"=="" set "HF_PROXY=http://127.0.0.1:7897"
rem 三个小时的任务，日志不能等进程结束才刷出来——出问题时要能实时看到进度。
set "PYTHONUNBUFFERED=1"
if not exist "%ROOT%\data\logs" mkdir "%ROOT%\data\logs"
cd /d "%ROOT%"

set "UV=uv"
where uv >nul 2>&1 || set "UV=%USERPROFILE%\.local\bin\uv.exe"

"%UV%" run hf-data-crawl --lock-wait 900 fetch-sizes --top 1122000 --workers 16 %* >> "data\logs\sizes.log" 2>&1
exit /b %ERRORLEVEL%
