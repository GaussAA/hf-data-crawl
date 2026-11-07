#Requires -Version 7.0
<#
.SYNOPSIS
    日增量采集：扫「最近更新」的那一段，把新数据接上。
.DESCRIPTION
    按 lastModified 倒序扫到水位线（连续若干页没有新记录）即停，另有 --refresh-top
    补扫下载量前两万名——否则增速榜永远只有零星读数。

    --skip-if-done：登录后补跑时若当天已采过则直接跳过，避免一天抓两遍。

    --lock-wait 900：撞上别的写命令时最多等 15 分钟。fetch-sizes 一次可能跑一小时，
    直接失败会白丢一整轮（2026-11-07 实测踩过）。

.EXAMPLE
    pwsh -File scripts\collect.ps1
    pwsh -File scripts\collect.ps1 --max-pages 3
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "collect" -Arguments (@(
    "--lock-wait", "900",
    "collect", "--refresh-top", "20000", "--skip-if-done"
) + $Rest))
