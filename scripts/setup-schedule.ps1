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
    hf-archive  每周一 05:00 归档满 30 天的 Raw（去重小分块、冻结写入序），移入 backup。

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
    [string]$DailyTime = "03:10",
    [string]$SizesTime = "06:00",
    [string]$ArchiveTime = "05:00",
    [string]$CompactTime = "00:30",
    [switch]$Uninstall
)

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "Please run with PowerShell 7: pwsh -File scripts\setup-schedule.ps1"
    exit 1
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

#: 现行任务。
$taskNames = @("hf-daily", "hf-sizes", "hf-archive", "hf-compact")

#: 2026-11-09 之前按钟点拆分的五个任务。升级时一并清掉：留着它们就会与链抢写锁，
#: 且补跑依旧抢在彼此前面——正是这次要治的病。
$legacyNames = @("hf-collect", "hf-reconcile", "hf-export", "hf-deliver", "hf-doctor")

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
    # 一条链跑完全部例行。`CatchUp = $true`：错过（关机 / 休眠）则开机补跑，
    # 补的是整条链，顺序仍在脚本里，不会像旧版那样各抢各的。
    @{ Name = "hf-daily"; Script = "daily.ps1"; Hours = 8; CatchUp = $true
       Triggers = @(
           (New-ScheduledTaskTrigger -Daily -At $DailyTime),
           (New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME)
       ); LogonDelay = "PT3M" },
    @{ Name = "hf-sizes"; Script = "sizes.ps1"; Hours = 4; CatchUp = $false
       LogonDelay = $null
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At $SizesTime)) },
    # 归档放周一，与周日那条错开一天，省得争锁。
    @{ Name = "hf-archive"; Script = "archive.ps1"; Hours = 2; CatchUp = $false
       LogonDelay = $null
       Triggers = @((New-ScheduledTaskTrigger -Weekly -DaysOfWeek Monday -At $ArchiveTime)) },
    # 压缩每日 00:30：门槛仍是 7 天，实际每天最多压掉一个刚满期的分区，把
    # 「周一才清一次、平日热区一路上涨」摊平。错过一轮次日接着压，不必补跑。
    @{ Name = "hf-compact"; Script = "compact.ps1"; Hours = 2; CatchUp = $false
       LogonDelay = $null
       Triggers = @((New-ScheduledTaskTrigger -Daily -At $CompactTime)) }
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
Write-Host "  hf-daily      每日 $DailyTime + 登录后 3 分钟补跑：采集 → 派生 → 对账 → 导出 → 交付 → 体检"
Write-Host "                （顺序由 scripts\daily.ps1 保证；错过则开机补跑整条链）"
Write-Host "  hf-compact    每日 $CompactTime 压缩满 7 天的冷区 Raw（不补跑，次日接着压）"
Write-Host "  hf-sizes      每周日 $SizesTime 补规模数据，增量合并、中断可续（不补跑）"
Write-Host "  hf-archive    每周一 $ArchiveTime 归档满 30 天的 Raw，原文件移入 backup（不补跑）"
Write-Host "日志目录：$(Join-Path $root 'data\logs')　链条摘要：同上，文件名以 -daily.log 结尾"
Write-Host "运行摘要：$(Join-Path $root 'data\runs')"
Write-Host "核对：Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo"
