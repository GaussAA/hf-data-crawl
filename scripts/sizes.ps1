#Requires -Version 7.0
<#
.SYNOPSIS
    规模补齐：逐条问 datasets-server「这个数据集多少行、多少列」。
.DESCRIPTION
    全流程最慢的一步：每条要单独请求，并发 16 时约 26 req/s。全量实测 3 小时 38 分。

    增量合并：已拿到 ok 的记录会跳过，重复执行不会重抓（实测全命中时 0.14 秒结束）。
    中断也不白费——已抓到的都已落库，下次从缺口接着补。

    失败分两类：500/401/501 记 unavailable 落库并默认跳过（上游永久缺失，重试是白烧
    配额）；429/502/503 不算失败，下轮自然重试。想重试那批永久缺失的，加
    --retry-unavailable（约再花 3 小时）。

    --top 1122000 覆盖全站：现在 100% 的数据集都有记录，其中约 74% 能拿到实测行数。

.EXAMPLE
    pwsh -File scripts\sizes.ps1
    pwsh -File scripts\sizes.ps1 --retry-unavailable
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "sizes" -Arguments (@(
    "--lock-wait", "900",
    "fetch-sizes", "--top", "1122000", "--workers", "16"
) + $Rest))
