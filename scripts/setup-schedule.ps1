<#
.SYNOPSIS
    注册 / 卸载 hf-data-crawl 的 Windows 计划任务。

.DESCRIPTION
    hf-collect    每日 03:10 日增量，另在登录后 2 分钟补跑（collect 带 --skip-if-done，
                  当天已采过会直接跳过）。
    hf-reconcile  每周日 04:00 全量对账；错过则开机后补跑一次。
    hf-doctor     每日 09:00 体检，另在登录后 10 分钟再查一次；有问题写 data\ALERT.txt。
    hf-sizes      每周日 06:00 补数据集规模（行数/列数），最慢的一步，增量可续。
    hf-archive    每周一 05:00 归档满 30 天的 Raw（去重小分块、冻结写入序），原文件移入 backup。
    hf-compact    每周一 06:00 压缩满 7 天的冷区 Raw（跨天去重成基线），原分块移入 backup 不删。

    与旧版的差别（旧版是笔记本上三天漏采的原因）：
      - StartWhenAvailable：关机错过的任务，开机后补跑；
      - 允许在电池供电时启动（旧版 DisallowStartIfOnBatteries 会让任务被拒绝，
        结果码 0x800710E0），且切到电池不中断；
      - 设执行时限：写命令持有数据目录的锁，挂死的进程会一直占着它。

    幂等：同名任务会被覆盖，可反复执行。用 PowerShell 7 运行（脚本为 UTF-8 无 BOM）：
        pwsh -File scripts\setup-schedule.ps1
        pwsh -File scripts\setup-schedule.ps1 -Uninstall

.NOTES
    采集日志落 data\logs\（按轮命名 `时间戳-任务名.log`，自动保留最近 200 个），
    运行摘要落 data\runs\（同样是时间戳-命令，但为结构化 JSON），任务本身不弹窗。
    两者同键不同扩展名：查某一轮的现场过程与结论，取同前缀即可。
#>
[CmdletBinding()]
param(
    [string]$CollectTime = "03:10",
    [string]$ReconcileTime = "04:00",
    [string]$DoctorTime = "09:00",
    [string]$SizesTime = "06:00",
    [string]$ArchiveTime = "05:00",
    [string]$CompactTime = "06:00",
    [switch]$Uninstall
)

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "Please run with PowerShell 7: pwsh -File scripts\setup-schedule.ps1"
    exit 1
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$taskNames = @("hf-collect", "hf-reconcile", "hf-doctor", "hf-sizes", "hf-archive", "hf-compact")

if ($Uninstall) {
    foreach ($name in $taskNames) {
        if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false
            Write-Host "已删除任务 $name"
        }
        else {
            Write-Host "任务 $name 不存在，跳过"
        }
    }
    return
}

$jobs = @(
    @{ Name = "hf-collect"; Script = "collect.ps1"; Hours = 4
       Triggers = @(
           (New-ScheduledTaskTrigger -Daily -At $CollectTime),
           (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME)
       ); LogonDelay = "PT2M" },
    @{ Name = "hf-reconcile"; Script = "reconcile.ps1"; Hours = 8
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $ReconcileTime))
       LogonDelay = $null },
    @{ Name = "hf-doctor"; Script = "doctor.ps1"; Hours = 1
       Triggers = @(
           (New-ScheduledTaskTrigger -Daily -At $DoctorTime),
           (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME)
       ); LogonDelay = "PT10M" },
    # 规模补齐：全流程最慢的一步（每条单独请求，约 26 req/s），故一周一次，
    # 放在对账之后两小时。给足 4 小时；中途被打断也不要紧——已抓到的都已落库，
    # 下周从缺口接着补，不会重抓。
    @{ Name = "hf-sizes"; Script = "sizes.ps1"; Hours = 4
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $SizesTime))
       LogonDelay = $null },
    # 归档放周一，避开周日的对账与规模补齐（写命令互斥锁）。
    @{ Name = "hf-archive"; Script = "archive.ps1"; Hours = 2
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At $ArchiveTime))
       LogonDelay = $null },
    # 压缩排在归档之后：两者都是写命令、互斥锁串行执行；compact 的 7 天门槛
    # 会先移走冷区，archive 的 30 天门槛通常空转，保留作单日手动去重工具。
    @{ Name = "hf-compact"; Script = "compact.ps1"; Hours = 2
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At $CompactTime))
       LogonDelay = $null }
)

foreach ($job in $jobs) {
    $cmd = Join-Path $root ("scripts\" + $job.Script)
    if (-not (Test-Path -LiteralPath $cmd)) {
        throw "缺少包装脚本：$cmd"
    }
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    Write-Warning "PATH 中找不到 uv；包装脚本会回退到 %USERPROFILE%\.local\bin\uv.exe，请确认该文件存在。"
}

# 计划任务的环境不保证有 PATH，pwsh 的绝对路径须在注册时定下来。
$pwshCommand = Get-Command pwsh -ErrorAction SilentlyContinue
if (-not $pwshCommand) {
    throw "找不到 pwsh：请安装 PowerShell 7（winget install Microsoft.PowerShell）"
}
$pwsh = $pwshCommand.Source

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive -RunLevel Limited

foreach ($job in $jobs) {
    $cmd = Join-Path $root ("scripts\" + $job.Script)
    foreach ($trigger in $job.Triggers) {
        if ($job.LogonDelay -and $trigger.CimClass.CimClassName -eq "MSFT_TaskLogonTrigger") {
            $trigger.Delay = $job.LogonDelay
        }
    }
    $settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Hours $job.Hours)
    $arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$cmd`""
    $action = New-ScheduledTaskAction -Execute $pwsh -Argument $arguments -WorkingDirectory $root
    Register-ScheduledTask -TaskName $job.Name -Action $action -Trigger $job.Triggers `
        -Settings $settings -Principal $principal -Force | Out-Null
}

Write-Host "已注册："
Write-Host "  hf-collect    每日 $CollectTime + 登录后补跑（当天已采过则跳过）"
Write-Host "  hf-reconcile  每周日 $ReconcileTime，错过则开机后补跑"
Write-Host "  hf-doctor     每日 $DoctorTime + 登录后体检，问题写 data\ALERT.txt"
Write-Host "  hf-sizes      每周日 $SizesTime 补规模数据，增量合并、中断可续"
Write-Host "  hf-archive    每周一 $ArchiveTime 归档满 30 天的 Raw，原文件移入 backup"
Write-Host "  hf-compact    每周一 $CompactTime 压缩满 7 天的冷区 Raw，原分块移入 backup 不删"
Write-Host "日志目录：$(Join-Path $root 'data\logs')"
Write-Host "运行摘要：$(Join-Path $root 'data\runs')"
Write-Host "核对：Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo"
