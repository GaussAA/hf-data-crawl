#Requires -Version 7.0
<#
.SYNOPSIS
    体检：采集与对账是否按时、Raw 有没有断日、有没有残留的临时文件。
.DESCRIPTION
    有计划任务失败或被错过时不会有任何痕迹——笔记本关机错过 03:10，任务不补跑，
    runs\ 里也不会多出一条失败记录，因为进程根本没起来。「没有记录」本身就是故障
    信号，只有拿当前时间去比最近一次成功运行的时间才看得出来。

    有问题时退出码为 1，并把说明写进 data\ALERT.txt；一切正常则删掉它。

    不持 --lock-wait：体检只读数据、写 ALERT，撞上别的写命令失败也无妨，
    而且要尽快出结果而不是干等。

.EXAMPLE
    pwsh -File scripts\doctor.ps1
    if ($LASTEXITCODE -ne 0) { Get-Content data\ALERT.txt }
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

exit (Invoke-HfTask -Name "doctor" -Arguments (@("doctor") + $Rest))
