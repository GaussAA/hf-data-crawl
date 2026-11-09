#Requires -Version 7.0
<#
.SYNOPSIS
    清理备份：把 backup\ 里超过保留期的分片真正删掉，释放磁盘。
.DESCRIPTION
    compact / archive 刻意只移不删——删除不可逆，不能混在搬运里顺手发生。但
    「永不删除」等于把磁盘交给时间：每天约 330 MB 原始分块进 backup\raw\，
    一年上百 GB，而它们早已被冷区基线（按 id 去重、体量恒定）取代。

    本脚本是那条「销毁」的出口：只动 data\backup\ 子树，权威层（raw 热区、
    silver、observations、current、cold 基线）一律不碰。阈值与 compact 共用
    retention.BACKUP_PRUNE_AFTER_DAYS（7 天）。

    首次启用或想先看账，加 --dry-run：只列会删什么、能释放多少，不动磁盘。

.EXAMPLE
    pwsh -File scripts\prune.ps1
    pwsh -File scripts\prune.ps1 --dry-run
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "prune" -Arguments (@(
    "--lock-wait", "900",
    "prune"
) + $Rest))
