#Requires -Version 7.0
<#
.SYNOPSIS
    注册 / 卸载 hf-data-crawl 的 Windows 计划任务。

.DESCRIPTION
    hf-refresh  每日按**班次起点**起跑完全部例行，一条链，不分钟点：
                compact → collect --full-scan → build --stale
                → 〔reconcile ‖ fetch-sizes / fetch-readme〕
                → parse-readme → export-wide → request run-due
                → archive → prune → doctor
                → 〔fetch-files ‖ fetch-preview〕（`--top 0`；收工到次日起点前十分钟）
                顺序与并发由 `ops/refresh.py` 保证，不由钟点猜。09:00 是软边界：
                到点不再启动新的抓取，已在跑的收尾，`doctor` 照常跑——它是唯一
                能说明「今天哪里没跑完」的环节（ADR-0008）。

    班次起点默认取 `.env` 的 `HF_DAILY_START`（再缺省 00:30）。两台机器共用一份
    代码、各有各的 `.env`，靠这一个值把任务错开；`-RefreshTime` 仍可临时覆盖。

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

    **补跑**（`CatchUp = $true`，2026-11-21 改）：开机后系统把错过的那次补上。
    原先写的是「不补跑」，理由是「补跑整条链既抢写锁，也会把昨天的口径跑成今天的
    日期」。这两个理由在合并之后都不成立了：

      - 抢锁：现在只有一条链，`MultipleInstances = IgnoreNew` 保证不会并发；
        `refresh` 本身还带 `runs/` 台账续跑与写锁，补跑不会与谁抢。
      - 口径：`collect --full-scan` 是**全站扫当前状态**，不是「重放昨天」。
        14:39 补跑拿到的是 14:39 的底库，日期写着今天，本来就该是今天。

    而不补跑的代价在 2026-11-21 实测到了：机器前一晚关机，00:30 那次没跑，
    开机后也不补，于是**整天零采集、底库停在昨天，且没有任何提醒**——唯一会写
    `ALERT.txt` 的 `doctor` 自己就在链里，链不跑它也不跑。断更本身不可怕，
    静默才可怕。

    唯一要留意的边界：临近零点开机补跑会跨日，分区可能一半算今天一半算明天。
    无害（`build` 按 raw 全量重放，重复的行会去重），次日 00:30 那次被 IgnoreNew
    挡掉，数据并不缺。

    **登录补跑**（2026-11-25 加）：`StartWhenAvailable` 只补「**没启动过**」的实例。
    如果链启动了、却在半路被关机杀掉，系统认为它已经跑过，不再补——2026-11-25 实测：
    00:30 起跑，00:40 关机，collect 只写了 408/1159 片，当天零采集，开机后毫无动静
    （任务结果是 0x40010004，不是「错过」）。所以另加一条登录触发，延迟十分钟：

      - 十分钟是等网络与代理起来。链的第一步就要连 HF，早启动只会拿到超时；
      - 这条触发不判断「今天跑没跑」，交给 `refresh` 自己按 `runs/` 台账决定：
        已完成的步骤一律「今天已完成」跳过，全做完时几秒就退出，不写任何数据；
      - 与每日触发共用同一个任务，`MultipleInstances = IgnoreNew` 保证与正在跑的
        链不会并发；登录时若链正在跑，这次触发直接作废。

    其余与旧版一致：
      - 允许在电池供电时启动（旧版 DisallowStartIfOnBatteries 会让任务被拒绝，
        结果码 0x800710E0），且切到电池不中断；
      - 设执行时限：写命令持有数据目录的锁，挂死的进程会一直占着它。**时限按兜底段
        开关派生**（`HF_FILL_ENABLED`）：开 → 25 小时，关 → 10 小时。这个数曾写死 10 小时，
        那是兜底段还没挂回链上时的口径；挂回之后它会在半路把兜底段斩掉——常开那台
        04:40 起跑，14:00 就被杀，而兜底段存在的理由正是把链外的整段时间用掉。
        `_common.ps1` 的 `Get-HfWallClockSeconds` 用的是同一个键、同一个阈值，两处必须一起改。

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
    # 留空则读 `.env` 的 `HF_DAILY_START`（再缺省 00:30）。两台机器各写各的 `.env`，
    # 代码保持一份——把时间写进脚本参数里，另一台 pull 下来就被覆盖了。
    [string]$RefreshTime = "",
    [switch]$Uninstall
)

if ($PSVersionTable.PSVersion.Major -lt 7) {
    Write-Host "Please run with PowerShell 7: pwsh -File scripts\setup-schedule.ps1"
    exit 1
}

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

<#
.SYNOPSIS
    从仓库根的 `.env` 读一个值（只读，不写回）。
.DESCRIPTION
    只做 `KEY=VALUE` 的最小解析：跳过空行与注释、容忍 `export ` 前缀与成对引号，
    与 Python 侧 `platform/env.py` 的口径一致。解析不了就返回默认值——这份文件的
    用途是「给两台机器各留一份不同的班次」，读不到时不该把注册流程卡死。
#>
function Read-HfEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Key,
        [Parameter(Mandatory = $true)][string]$Default
    )
    $path = Join-Path $root ".env"
    if (-not (Test-Path -LiteralPath $path)) { return $Default }
    foreach ($line in (Get-Content -LiteralPath $path -Encoding utf8)) {
        $text = $line.Trim()
        if (-not $text -or $text.StartsWith("#")) { continue }
        if ($text.StartsWith("export ")) { $text = $text.Substring(7).TrimStart() }
        $split = $text.IndexOf("=")
        if ($split -lt 1) { continue }
        if ($text.Substring(0, $split).Trim() -ne $Key) { continue }
        $value = $text.Substring($split + 1).Trim().Trim("'").Trim('"')
        if ($value) { return $value }
    }
    return $Default
}

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

#: 班次起点。命令行不给就读 `.env` 的 `HF_DAILY_START`，再缺省 00:30。
#:
#: 两台机器共用同一份代码，靠这一个值错开：一台 00:30、一台 04:00。**动它之前先看
#: `HF_FILL_ENABLED`**——兜底段会从链尾一直跑到次日起点前十分钟，如果两台都开着它，
#: 错开起跑并不能错开窗口（它们是同一段配额的两半）。
$refreshFrom = "命令行参数"
if (-not $RefreshTime) {
    $RefreshTime = Read-HfEnvValue -Key "HF_DAILY_START" -Default "00:30"
    $refreshFrom = ".env 的 HF_DAILY_START（未配置则 00:30）"
}
if ($RefreshTime -notmatch '^([01]?\d|2[0-3]):[0-5]\d$') {
    Write-Host "RefreshTime 需为 HH:MM（当前：$RefreshTime，来源：$refreshFrom）"
    exit 1
}
Write-Host "班次起点：$RefreshTime（来源：$refreshFrom）"

# 任务时限（小时）必须盖住整条链最长能跑多久，口径与 `_common.ps1` 的
# `Get-HfWallClockSeconds` 是同一份：跑兜底段 25 小时，不跑 10 小时。两处都从
# `HF_FILL_ENABLED` 派生，改一处必须改另一处，否则任务时限会把墙钟没跑完的链强杀。
# 10 小时是兜底段还没挂回链上时的口径；挂回之后常开那台 04:40 起跑，14:00 就会被斩。
$fillEnabled = (Read-HfEnvValue -Key "HF_FILL_ENABLED" -Default "1") -match '^(1|true|yes|on)$'
$wallHours = if ($fillEnabled) { 25 } else { 10 }
Write-Host ("兜底段：$(if ($fillEnabled) { '开' } else { '关' })（任务时限 $wallHours 小时）")

$jobs = @(
    # 一条链跑完全部例行（ADR-0008）。按班次起点起跑，09:00 软边界；**错过要补跑**——
    # 2026-11-21 实测：前一晚关机，00:30 没跑、开机也不补，整天零采集且无提醒。
    # 补跑安全的前提是全站扫描（拿的是扫描那一刻的底库，不是重放昨天）与
    # IgnoreNew（不会与下一班并发）。
    @{ Name = "hf-refresh"; Script = "refresh.ps1"; Hours = $wallHours; CatchUp = $true
       LogonDelay = "PT10M"
       # 两条触发，同一个任务：每日班次 + 登录后十分钟。
       # 登录那条补的是 `StartWhenAvailable` 补不到的洞——链启动了却被中途杀掉，
       # 系统认为「跑过了」，不会再补（2026-11-25：00:40 关机，当天零采集）。
       # 多跑一次的代价接近于零：`refresh` 按 `runs/` 台账跳过已完成的步骤。
       Triggers = @(
           (New-ScheduledTaskTrigger -Daily -At $RefreshTime),
           (New-ScheduledTaskTrigger -AtLogOn)
       ) }
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
