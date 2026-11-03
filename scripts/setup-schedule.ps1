<#
.SYNOPSIS
    注册 / 卸载 hf-data-crawl 的 Windows 计划任务。

.DESCRIPTION
    每日 03:10 跑日增量；每周日 04:00 跑全量对账。
    幂等：同名任务会被 /F 覆盖，可反复执行。
    用 PowerShell 7 运行（脚本为 UTF-8 无 BOM，5.1 会乱码）：
        pwsh -File scripts\setup-schedule.ps1
        pwsh -File scripts\setup-schedule.ps1 -Uninstall

.NOTES
    采集日志落 data\logs\，运行摘要落 runs\，任务本身不弹窗。
#>
[CmdletBinding()]
param(
    [string]$CollectTime = "03:10",
    [string]$ReconcileTime = "04:00",
    [switch]$Uninstall
)

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "Please run with PowerShell 7: pwsh -File scripts\setup-schedule.ps1"
    exit 1
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$collectCmd = Join-Path $root "scripts\collect.cmd"
$reconcileCmd = Join-Path $root "scripts\reconcile.cmd"
$taskNames = @("hf-collect", "hf-reconcile")

if ($Uninstall) {
    foreach ($name in $taskNames) {
        schtasks /Delete /TN $name /F 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "已删除任务 $name"
        }
        else {
            Write-Host "任务 $name 不存在，跳过"
        }
    }
    return
}

foreach ($path in @($collectCmd, $reconcileCmd)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "缺少包装脚本：$path"
    }
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Warning "PATH 中找不到 uv；包装脚本会回退到 %USERPROFILE%\.local\bin\uv.exe，请确认该文件存在。"
}

schtasks /Create /F /SC DAILY /ST $CollectTime /TN "hf-collect" /TR "`"$collectCmd`"" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "注册 hf-collect 失败（schtasks 退出码 $LASTEXITCODE）"
}

schtasks /Create /F /SC WEEKLY /D SUN /ST $ReconcileTime /TN "hf-reconcile" /TR "`"$reconcileCmd`"" | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "注册 hf-reconcile 失败（schtasks 退出码 $LASTEXITCODE）"
}

Write-Host "已注册："
Write-Host "  hf-collect    每日 $CollectTime"
Write-Host "  hf-reconcile  每周日 $ReconcileTime"
Write-Host "日志目录：$(Join-Path $root 'data\logs')"
Write-Host "运行摘要：$(Join-Path $root 'runs')"
Write-Host "核对：schtasks /Query /TN hf-collect /V /FO LIST"
