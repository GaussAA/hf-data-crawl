#Requires -Version 7.0
<#
.SYNOPSIS
    刷新鲜：每日 00:30 起跑完全部例行，09:00 是软边界。
.DESCRIPTION
    编排在 `ops/refresh.py`（ADR-0008）：串行头（compact → collect --full-scan →
    build）把 `current` 定稿，然后对账与三个锁外抓取并行，最后导出、交付、清理、体检。
    本脚本只负责一件事：把任务名、墙钟上限与日志前缀交给 `Invoke-HfTask`。

    为什么不再用 `daily.ps1`：那条是纯串行 foreach，四个抓取只能排队；并行段必须在
    编排器里开子进程，而锁与 runs 台账仍由每个子命令自己管——`daily.ps1` 的
    `Requires` 依赖表达不了「并行分支失败不影响另一支」。

    退出码：编排器把失败的步骤汇总成非零；到 09:00 仍未启动的步骤记为「留给次日」，
    不算失败。次日 00:30 从 runs 台账与 staging 分片续跑。
.EXAMPLE
    pwsh -File scripts\refresh.ps1
    pwsh -File scripts\refresh.ps1 --budget-hours 4
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

# 墙钟上限 10 小时：编排器内部是 8.5 小时预算，留 1.5 小时给「到点时正在跑的抓取
# 自然收尾」。超出这个上限说明有一步真的卡住了，`Invoke-HfTask` 会强杀进程树。
exit (Invoke-HfTask -Name "refresh" -Arguments (@(
    "--lock-wait", "900",
    "refresh"
) + $Rest) -TimeoutSeconds 36000)
