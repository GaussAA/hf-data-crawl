#Requires -Version 7.0
<#
.SYNOPSIS
    交付：把到期需求的当前口径各跑一次，写出带底库指纹的档案。
.DESCRIPTION
    扫 requests\ 下所有 spec，按各自 cadence 判断「今天是否到期」（据上一次交付
    的时刻，不据上一次运行成功与否——后者在空结果或失败时会让它每天重试）。

    once 交过即永久跳过；manual 从不自动跑；daily / weekly / monthly 按日历日差
    判定。每条交付写三件：data.*（字节）、manifest.json（机器读）、README.md
    （人读），三者靠 sha256 绑定。

    排在 04:50 导出之后、09:00 体检之前：拿到的必然是当天的底库，且若某条需求
    0 行被阻断，体检能立刻看见。

    --lock-wait 900：撞上别的长任务（周四的 sizes 可能跑数小时）时最多等 15 分钟。
    交付本身很轻（一条 SQL + 写几个文件），等锁几乎总是划算的。
.EXAMPLE
    pwsh -File scripts\deliver.ps1
    pwsh -File scripts\deliver.ps1 --requests-dir D:\other\requests
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "deliver" -Arguments (@(
    "--lock-wait", "900",
    "request", "run-due"
) + $Rest))
