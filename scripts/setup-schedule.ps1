#Requires -Version 7.0
<#
.SYNOPSIS
    注册 / 卸载 hf-data-crawl 的 Windows 计划任务。

.DESCRIPTION
    hf-daily    每日 03:10 一条链跑完当日全部例行，登录后 3 分钟再补一次：
                采集 → 派生重放 → 全站对账 → 导出宽表 → 到期交付 → 体检。
                顺序由 scripts\daily.ps1 保证，不由钟点猜（见下）。
    hf-compact  每日 00:30 压缩满 7 天的冷区 Raw（跨天去重成基线），原分块移入 backup。
    hf-sizes    每周日 06:00 补数据集规模（行数/列数），最慢的一步，增量可续。
    hf-files    每周日 07:30 补下载量前 1000 名的文件清单（首屏，子集而非全量）。
    hf-archive  每周一 05:00 归档满 30 天的 Raw（去重小分块、冻结写入序），移入 backup。
    hf-readme   每周六 09:00 重取 commit 已变的 README 全文，默认前 8000 名的量。
                全量那趟（约 21 小时）仍留人工：11-17 跑过一次，113 万条已入库。

    为什么把五个任务收成一条链（2026-11-09 实测）
    ----------------------------------------------
    原先是采集 03:10 / 对账 04:00 / 导出 04:50 / 交付 07:00 / 体检 09:00 各一条，
    靠时刻先后表达依赖。机器一直开着时看不出问题；一旦关机错过，
    `-StartWhenAvailable` 会在开机后**把错过的实例在同一分钟全部补跑**：

        11-09 00:30 之后关机，19:03:26 开机
        19:06  采集（登录 +2 分钟）
        19:07  导出先完成（1,126,351 行），对账才开始、8 分钟后才收工
        ⇒ 导出一出来就落后，体检随之报「宽表导出落后：缺 2,466 行」
        19:14  体检第二次触发，撞上对账持锁 → 降级非快照 → 退出码 1

    补跑本身要留着（不补就断更），要改的是**依赖的表达方式**：把五步写进一条
    脚本按序执行，补跑时整条链重跑一遍。故本次只给 hf-daily 保留
    `-StartWhenAvailable`；compact / sizes / archive 是互不相干的纯周期维护，
    错过一轮就等下一轮（compact 次日、sizes 与 archive 下周），不必开机补跑——
    它们补跑也只会跟链抢写锁。

    其余与旧版一致：
      - 允许在电池供电时启动（旧版 DisallowStartIfOnBatteries 会让任务被拒绝，
        结果码 0x800710E0），且切到电池不中断；
      - 设执行时限：写命令持有数据目录的锁，挂死的进程会一直占着它。

    幂等：同名任务会被覆盖，可反复执行；升级时旧的五个任务会被一并清掉。
    用 PowerShell 7 运行（脚本为 UTF-8 无 BOM）：
        pwsh -File scripts\setup-schedule.ps1
        pwsh -File scripts\setup-schedule.ps1 -Uninstall

.NOTES
    采集日志落 data\logs\（按轮命名 `时间戳-任务名.log`，自动保留最近 200 个），
    运行摘要落 data\runs\（同样是时间戳-命令，但为结构化 JSON），任务本身不弹窗。
    两者同键不同扩展名：查某一轮的现场过程与结论，取同前缀即可。链条另写一份
    `时间戳-daily.log`，逐步行首尾与跳过原因，一眼看出断在哪。
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
        Write-Host "已移除旧任务 $name（已并入 hf-daily）"
    }
}

Write-Host "已注册："
Write-Host "  hf-refresh    每日 $RefreshTime 起跑完全部例行：采集 → 派生 → 对账 ‖ enrich → 导出 → 交付 → 清理 → 体检"
Write-Host "                （顺序与并发由 ops/refresh.py 保证；09:00 软边界，未完成的下次从断点续）"
Write-Host "日志目录：$(Join-Path $root 'data\logs')　刷新鲜摘要：同上，文件名以 -refresh.log 结尾"
Write-Host "运行摘要：$(Join-Path $root 'data\runs')"
Write-Host "核对：Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo"
