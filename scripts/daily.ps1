#Requires -Version 7.0
<#
.SYNOPSIS
    一条链跑完当日全部例行：采集 → 派生 → 对账 → 导出 → 交付 → 清理备份 → 体检。
.DESCRIPTION
    为什么要有这条链（2026-11-09 实测的教训）
    ----------------------------------------
    原先五个任务各按钟点排：采集 03:10、对账 04:00、导出 04:50、交付 07:00、
    体检 09:00。机器一直开着时，先后由时刻保证，看不出问题；一旦关机错过，
    `-StartWhenAvailable` 会把它们**在同一分钟全部补跑**，先后关系当场崩塌：

        19:06  采集（登录后 2 分钟触发）
        19:07  导出于 19:07:27 完成，导出 1,126,351 行
        19:07  对账才开始，19:21:49 才收工（新增 106 / 删除 1,266 / 改名 219）
        ⇒ 导出一出来就落后，体检随即报「宽表导出落后：缺 2,466 行」
        19:14  体检第二次触发，撞上对账持写锁 → 降级非快照 → 退出码 1

    根子不在于补跑（不补就断更），而在于**用钟点表达依赖**。钟点是给人看的，
    依赖必须由程序保证。故五个任务收成一条链：顺序写死在这里，一次只跑一步，
    上一步不成就别做下一步；补跑时整条链重跑一遍，而不是五条各自抢跑。

    两步之间的依赖只在「依赖真实存在」时才拦：
      - 导出、交付依赖 `build`——派生层没跟上就发布，等于把旧值当新值交出去；
      - 对账是独立的全站扫描，采集失败也照样该跑；
      - 体检永远跑，它是唯一能说明「前面哪一步出了问题」的环节。

.NOTES
    退出码语义分两层：前五步任一非零 = 故障，链条返回该码；体检非零只代表
    **有告警**（它把「导出落后」「Raw 断日」这类待办也计入非零），详情在
    data\ALERT.txt，不让整条链报红——告警与故障挤进同一个码，铃声很快就没人听。

.EXAMPLE
    pwsh -File scripts\daily.ps1
#>
[CmdletBinding()]
param()

. (Join-Path $PSScriptRoot "_common.ps1")

# 摘要日志的路径、命名与「同一秒不覆盖」全由 _common.ps1 的 Write-HfChain 决定：
# 日志约定只许有一个出口，否则早晚有人拼回「单文件永久追加」那一版。
function Write-Summary {
    param([string]$Text)
    $null = Write-HfChain -Text $Text
}

# 顺序即依赖。`Requires` 只写「真的必须先成」的前置：前置失败则跳过本步，
# 而不是带着半截状态硬跑——那正是 11-09 那次导出抢跑的病根。
$steps = @(
    @{ Name = "collect"; Label = "日增量采集"; Requires = @(); Report = $false
        Arguments = @(
            "--lock-wait", "900",
            "collect", "--refresh-top", "20000", "--cards", "--skip-if-done"
        ) },
    @{ Name = "build"; Label = "派生层重放"; Requires = @(); Report = $false
        Arguments = @("--lock-wait", "900", "build", "--stale") },
    @{ Name = "reconcile"; Label = "全站对账"; Requires = @(); Report = $false
        Arguments = @("--lock-wait", "900", "reconcile") },
    @{ Name = "export"; Label = "导出宽表"; Requires = @("build", "reconcile"); Report = $false
        Arguments = @("--lock-wait", "900", "export-wide") },
    @{ Name = "deliver"; Label = "到期需求交付"; Requires = @("build", "reconcile"); Report = $false
        Arguments = @("--lock-wait", "900", "request", "run-due") },
    # 清理紧跟导出：本地产物刚定稿，正是把上一批备份释放掉的时候。
    # 只动 backup/，与搬运分开，删错也不可能伤到权威层。
    @{ Name = "prune"; Label = "清理备份"; Requires = @(); Report = $false
        Arguments = @("--lock-wait", "900", "prune") },
    # 体检放最后：前面任一步出事，它才看得见。不计入链条成败，理由见 .NOTES。
    @{ Name = "doctor"; Label = "体检"; Requires = @(); Report = $true
        Arguments = @("doctor") }
)

# 首次调用顺带建出摘要日志，取回路径供收尾时点明「日志在哪」。
$summary = Write-HfChain -Text "== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  daily  $(($steps.Label) -join ' → ')"

$codes = @{}
$failure = 0

foreach ($step in $steps) {
    $blocked = @($step.Requires | Where-Object { $codes.ContainsKey($_) -and $codes[$_] -ne 0 })
    if ($blocked.Count -gt 0) {
        $codes[$step.Name] = -1
        Write-Summary "  [跳过] $($step.Label)（$($step.Name)）：前置 $($blocked -join ' / ') 未成功"
        continue
    }

    Write-Summary "  [开始] $($step.Label)（$($step.Name)）"
    $code = Invoke-HfTask -Name $step.Name -Arguments $step.Arguments
    $codes[$step.Name] = $code

    if ($code -eq 0) {
        Write-Summary "  [完成] $($step.Label)"
        continue
    }
    if ($step.Report) {
        # 体检非零＝有告警，不等于故障；它自己已写 ALERT.txt。
        Write-Summary "  [告警] $($step.Label) 退出码 $code，详情见 data\ALERT.txt 与对应日志"
        continue
    }
    Write-Summary "  [失败] $($step.Label) 退出码 $code"
    if ($failure -eq 0) { $failure = $code }
}

Write-Summary "== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  链条结束，退出码 $failure（日志：$summary）"

exit $failure
