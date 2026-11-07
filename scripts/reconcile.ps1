#Requires -Version 7.0
<#
.SYNOPSIS
    全量对账：按 createdAt 稳定序遍历全站，产出新增 / 下架 / 转私有 / 改名 / sha 变更。
.DESCRIPTION
    **不要加 --max-pages。** 只有扫到末页才允许判定下架；有界扫描属于没扫完，
    此时若照常判定，会把整站未扫到的部分全标成 disappeared——不可逆的误判。

    中断可续：游标、已见集合与基线由会话文件维护，重跑从断点接。

    全站约 1117 页、112 万条，实测 30–40 分钟（含派生开销）。

.EXAMPLE
    pwsh -File scripts\reconcile.ps1
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "reconcile" -Arguments (@(
    "--lock-wait", "900",
    "reconcile"
) + $Rest))
