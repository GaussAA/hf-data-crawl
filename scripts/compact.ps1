#Requires -Version 7.0
<#
.SYNOPSIS
    压缩满 7 天的冷区 Raw：跨天去重成一份基线，主目录不再线性膨胀。
.DESCRIPTION
    先逐个 id 校验胜出者，不等即撤销新分片、原文件不动；原分块只移入
    data\backup\raw\dt=<日期>\，不删除。已压缩日期登记在
    data\cold\raw\manifest.json，doctor 据此避免误报断日。

    清理 backup 由 scripts\prune.ps1 单独负责（每日链内跑，只动 backup\），
    本脚本不删任何数据——搬运与销毁分开，谁出事都能定位。

.EXAMPLE
    pwsh -File scripts\compact.ps1
    pwsh -File scripts\compact.ps1 --date 2026-10-01
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "compact" -Arguments (@(
    "--lock-wait", "900",
    "compact"
) + $Rest))
