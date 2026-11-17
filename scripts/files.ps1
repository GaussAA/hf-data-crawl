#Requires -Version 7.0
<#
.SYNOPSIS
    文件清单补齐：按下载量取前 N 名，抓 `/tree/main` 首屏，汇总文件数与体积。
.DESCRIPTION
    列表接口不返回 `siblings`，单库详情又厚重到不可行（头部实测 13-25 MB / 44-76 s），
    故这条链路走 `/tree/main?recursive=true&expand=false`：每页固定 1000 条、
    约 110-230 KB / 0.4-0.7 s。默认每库只取第一页，有后续页时标 `truncated=true`
    ——此时文件数与体积是**这一页的下限**，不是全库真值。

    所以这张表是「头部子集」，不是全站分布。它每周日随规模任务之后跑一趟，
    目的是让这 1000 个最常用的库有一份不至于过期的清单，而不是承诺覆盖。

.EXAMPLE
    pwsh -File scripts\files.ps1
    pwsh -File scripts\files.ps1 --top 3000
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "files" -Arguments (@(
    "--lock-wait", "900",
    "fetch-files", "--top", "1000", "--workers", "8"
) + $Rest) -TimeoutSeconds 1800)
