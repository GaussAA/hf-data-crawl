#Requires -Version 7.0
<#
.SYNOPSIS
    归档满 30 天的 Raw 分区：合并小分块、去掉同天被覆盖的重复记录。
.DESCRIPTION
    归档先验证前后 Bronze 逐行相等，不等即撤销、原文件原地不动；原文件只移入
    data\backup\raw\dt=<日期>\，不删除。真实写入序冻结进分片的 groups，此后
    build 不再依赖文件 mtime。

    清理 backup 须人工确认（见 docs\operations\RECOVERY.md），本脚本不删任何数据。

.EXAMPLE
    pwsh -File scripts\archive.ps1
    pwsh -File scripts\archive.ps1 --date 2026-11-07
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "archive" -Arguments (@(
    "--lock-wait", "900",
    "archive"
) + $Rest))
