#Requires -Version 7.0
<#
.SYNOPSIS
    把 gold.datasets 宽视图导出成单文件 parquet。
.DESCRIPTION
    导出是**物化投影**：不参与派生、不登记为表，删掉重跑本命令即可。但它不会
    跟着派生自动更新——current 每轮都重写，而导出文件只在跑本命令时刷新。
    2026-11-08 实测：对账在 04:11 把底库刷到 1,123,235 行，而导出仍是前夜
    02:33 的 1,123,090 行，差 145 行且没有任何提示。

    故每日 04:50 在对账之后跑一次，让宽表跟着底库走。成本约 1-2 分钟、
    重写约 340 MB；放这个点是避开 04:00 的对账（实测 7-40 分钟）与 06:00
    的规模补齐——三者都是写命令，靠互斥锁串行，撞上会有一个失败。

.EXAMPLE
    pwsh -File scripts\export.ps1
    pwsh -File scripts\export.ps1 --out data\exports\custom.parquet
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "export" -Arguments (@(
    "--lock-wait", "900",
    "export-wide"
) + $Rest))
