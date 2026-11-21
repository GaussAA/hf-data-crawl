#Requires -Version 7.0
<#
.SYNOPSIS
    注册 / 卸载 hf-data-crawl 的 Windows 计划任务。

.DESCRIPTION
    hf-refresh  每日 00:30 起跑完全部例行，一条链，不分钟点：
                compact → collect --full-scan → build --stale
                → 〔reconcile ‖ fetch-sizes / fetch-readme〕
                → parse-readme → export-wide → request run-due
                → archive → prune → doctor
                顺序与并发由 `ops/refresh.py` 保证，不由钟点猜。09:00 是软边界：
                到点不再启动新的抓取，已在跑的收尾，`doctor` 照常跑——它是唯一
                能说明「今天哪里没跑完」的环节（ADR-0008）。

    为什么不拆成多个定时任务（2026-11-09 实测，教训保留）
    ----------------------------------------------------
    原先是采集 03:10 / 对账 04:00 / 导出 04:50 / 交付 07:00 / 体检 09:00 各一条，
    靠时刻先后表达依赖。机器一直开着时看不出问题；一旦关机错过，
    `-StartWhenAvailable` 会在开机后**把错过的实例在同一分钟全部补跑**：

        11-09 00:30 之后关机，19:03:26 开机
        19:06  采集（登录 +2 分钟）
        19:07  导出先完成（1,126,351 行），对账才开始、8 分钟后才收工
        ⇒ 导出一出来就落后，体检随之报「宽表导出落后：缺 2,466 行」
        19:14  体检第二次触发，撞上对账持锁 → 降级非快照 → 退出码 1

    所以依赖必须由编排器表达，而不是由「谁先到点」。2026-11-19 起进一步把编排
    从 PowerShell 挪进 Python（ADR-0008）：锁、runs 台账、退出码本来都在 CLI 里，
    `_common.ps1` 又实现过一遍墙钟与摘要，再叠一层 `Start-Job` 只会得到两份事实。
    现在这个 ps1 只负责**注册**，跑什么由 `hf-data-crawl refresh` 决定。

    **不补跑**（`CatchUp = $false`）：错过就错过，次日从 `runs/` 台账与 staging
    分片续。补跑整条链既抢写锁，也会把「昨天的口径」跑成今天的日期——昨天那条
    链的产物日期不对，比缺一天更麻烦。

    其余与旧版一致：
      - 允许在电池供电时启动（旧版 DisallowStartIfOnBatteries 会让任务被拒绝，
        结果码 0x800710E0），且切到电池不中断；
      - 设执行时限：写命令持有数据目录的锁，挂死的进程会一直占着它。

    幂等：同名任务会被覆盖，可反复执行。**升级时 11 个旧任务会被一并清掉**
    （`hf-daily` / `hf-sizes` / `hf-files` / `hf-archive` / `hf-compact` /
    `hf-readme` 及更早的 `hf-collect` 等五个）——它们的脚本已删，留着只会在夜里
    报「找不到文件」，且补跑仍会各自抢锁。
    用 PowerShell 7 运行（脚本为 UTF-8 无 BOM）：
        pwsh -File scripts\setup-schedule.ps1
        pwsh -File scripts\setup-schedule.ps1 -Uninstall

.NOTES
    采集日志落 data\logs\（按轮命名 `时间戳-任务名.log`，自动保留最近 200 个），
    运行摘要落 data\runs\（同样是时间戳-命令，但为结构化 JSON），任务本身不弹窗。
    两者同键不同扩展名：查某一轮的现场过程与结论，取同前缀即可。链条另写一份
    `时间戳-refresh.log`，逐步行首尾与跳过原因，一眼看出断在哪。
#>
[CmdletBinding()]
param(
    [string]$RefreshTime = "00:30",
    [switch]$Uninstall
)

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "Please run with PowerShell 7: pwsh -File scripts\setup-schedule.ps1"
    exit 1
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

#: 现行任务。每日 00:30 一条链（ADR-0008）。
$taskNames = @("hf-refresh")

#: 历史任务，升级时一并清掉：留着就与链抢写锁，且补跑仍会各自抢跑。
#: `hf-collect` 等五个是 2026-11-09 前按钟点拆分的；`hf-daily` / `hf-sizes` /
#: `hf-files` / `hf-archive` / `hf-compact` / `hf-readme` 六个在 2026-11-19 并入
#: 刷新鲜——每日增量已覆盖它们各自的周期，留着只是同一批工作被排两次。
$legacyNames = @(
    "hf-collect", "hf-reconcile", "hf-export", "hf-deliver", "hf-doctor",
    "hf-daily", "hf-sizes", "hf-files", "hf-archive", "hf-compact", "hf-readme"
)

if ($Uninstall) {
    foreach ($name in ($taskNames + $legacyNames)) {
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
    # 一条链跑完全部例行（ADR-0008）。00:30 起跑，09:00 软边界；错过不补跑——
    # 次日从 runs 台账 + staging 分片续，不需要把整条链重放一遍。
    @{ Name = "hf-refresh"; Script = "refresh.ps1"; Hours = 10; CatchUp = $false
       LogonDelay = $null
       Triggers = @((New-ScheduledTaskTrigger -Daily -At $RefreshTime)) }
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
    # StartWhenAvailable 按任务给：只有链需要补跑，纯周期维护不必（理由见 .DESCRIPTION）。
    $settingArgs = @{
        AllowStartIfOnBatteries     = $true
        DontStopIfGoingOnBatteries  = $true
        MultipleInstances           = "IgnoreNew"
        ExecutionTimeLimit          = (New-TimeSpan -Hours $job.Hours)
    }
    if ($job.CatchUp) { $settingArgs["StartWhenAvailable"] = $true }
    $settings = New-ScheduledTaskSettingsSet @settingArgs
    $arguments = "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$cmd`""
    $action = New-ScheduledTaskAction -Execute $pwsh -Argument $arguments -WorkingDirectory $root
    Register-ScheduledTask -TaskName $job.Name -Action $action -Trigger $job.Triggers `
        -Settings $settings -Principal $principal -Force | Out-Null
}

# 清掉旧的按钟点拆分的任务：留着就与链争锁，且补跑仍会各自抢跑。
foreach ($name in $legacyNames) {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "已移除旧任务 $name（已并入 hf-refresh）"
    }
}

Write-Host "已注册："
Write-Host "  hf-refresh    每日 $RefreshTime 起跑完全部例行：采集 → 派生 → 对账 ‖ enrich → 导出 → 交付 → 清理 → 体检"
Write-Host "                （顺序与并发由 ops/refresh.py 保证；09:00 软边界，未完成的下次从断点续）"
Write-Host "日志目录：$(Join-Path $root 'data\logs')　刷新鲜摘要：同上，文件名以 -refresh.log 结尾"
Write-Host "运行摘要：$(Join-Path $root 'data\runs')"
Write-Host "核对：Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo"
