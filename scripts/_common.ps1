#Requires -Version 7.0
<#
.SYNOPSIS
    计划任务的公共包装：环境、日志、uv 定位、退出码，一处定义。
.DESCRIPTION
    四个任务脚本原本各写一遍同样的准备动作（找 uv、设代理、建日志目录、切目录、
    回传退出码），任何一处改动都要同步四份。抽到这里之后，各脚本只声明「跑哪个
    子命令、带什么参数」。

    日志用 *>> 追加：PS 7 下所有流一起进文件，且默认 UTF-8——CMD 的 >> 在中文系统
    是 GBK，日志里的中文会变成乱码（2026-11-07 实测）。

    日志文件按轮命名（时间戳-任务名.log），不再按任务名覆盖：此前 `collect.log`
    一直是同一个文件往后追加，既对不上 `runs/` 里那轮摘要，也从不清理——目录会
    无限长大，只能靠人手工改名成 `.bak`（现存 3 个命令就只剩 `.bak` 了）。现在与
    `runs/` 取同一套键：查某轮的过程 ＝ 取同前缀、把扩展名换掉。每次写完修剪到
    最近 $script:LogKeep 个，与 `runs.RUNS_KEEP` 同一哲学：有界才不会悄悄撑爆。
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

#: logs/ 保留最近多少个日志文件。按目前每天十余轮、每轮一个文件，200 个约两周；
#: 与 runs.RUNS_KEEP 同一哲学：有界才不会被计划任务悄悄撑爆。
$script:LogKeep = 200

function Remove-StaleLogs {
    <#
    .SYNOPSIS
        只保留最近 Keep 个日志文件，多余的删掉。
    .DESCRIPTION
        按**文件名**排序，不按修改时间：前缀就是开跑时的时间戳，字典序即时间序；
        而 mtime 会被同步盘、备份、解压等外部动作整体改掉，排序会失真。
        与 runs.prune_runs 刻意不同（那边按记录里的 started_at）：日志文件没有
        结构化的时间字段，文件名里的时间戳就是它唯一可靠的顺序依据。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$LogDir,
        [int]$Keep = $script:LogKeep
    )

    if ($Keep -le 0) { throw "Keep 必须为正数" }
    if (-not (Test-Path -LiteralPath $LogDir)) { return }

    $files = @(Get-ChildItem -LiteralPath $LogDir -Filter "*.log" -File | Sort-Object Name)
    if ($files.Count -le $Keep) { return }

    foreach ($file in $files[0..($files.Count - $Keep - 1)]) {
        Remove-Item -LiteralPath $file.FullName -Force -ErrorAction SilentlyContinue
    }
}

function New-RunLogPath {
    <#
    .SYNOPSIS
        生成一份按轮命名的日志路径：data\logs\<时间戳>-<Name>[-序号].log。
    .DESCRIPTION
        命名与去重规则只写在这里：时间戳前缀让字典序即时间序；同一秒连跑两条
        加序号，不覆盖上一轮现场（与 `runs.write_run` 同一套约定）。
        新建目录与修剪留在调用方——本函数只回答「该写哪个文件」。
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][string]$Name)

    $root = Split-Path -Parent $PSScriptRoot
    $logDir = Join-Path $root "data\logs"
    $null = New-Item -ItemType Directory -Force -Path $logDir

    $stamp = Get-Date -Format "yyyyMMddTHHmmss"
    $log = Join-Path $logDir "$stamp-$Name.log"
    $suffix = 1
    while (Test-Path -LiteralPath $log) {
        $suffix++
        $log = Join-Path $logDir "$stamp-$Name-$suffix.log"
    }
    return $log
}

#: 链条摘要日志：一个进程一份，逐步日志各一份，同一目录、同一命名模板。
$script:ChainLogPath = $null

function Write-HfChain {
    <#
    .SYNOPSIS
        往「链条摘要」日志追加一行，同时打到控制台。
    .DESCRIPTION
        为什么归在这里而不是写在链条脚本里：日志路径与命名是公共约定，
        允许某个脚本自己拼路径，早晚会有人拼回「单文件永久追加」那一版，
        而且从外部看不出异常。故只有本文件可以决定写到哪个文件。
    .OUTPUTS
        摘要日志的路径（首次调用时创建）。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Text,
        [string]$Name = "daily"
    )

    if (-not $script:ChainLogPath) {
        $script:ChainLogPath = New-RunLogPath -Name $Name
    }
    $Text | Out-File -FilePath $script:ChainLogPath -Append -Encoding utf8
    Write-Host $Text
    return $script:ChainLogPath
}

function Invoke-HfTask {
    <#
    .SYNOPSIS
        跑一条 hf-data-crawl 子命令，输出追加到 data\logs\<时间戳>-<Name>.log。
    .OUTPUTS
        进程退出码。计划任务据此判定成败。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string[]]$Arguments
    )

    $root = Split-Path -Parent $PSScriptRoot

    # 代理与令牌不硬编码进代码，只在此处给默认值；用户环境已有则以用户为准。
    if (-not $env:HF_PROXY) { $env:HF_PROXY = "http://127.0.0.1:7897" }
    # 最慢的一步要跑几小时，日志不能等进程结束才刷出来——出问题时要能实时看到进度。
    $env:PYTHONUNBUFFERED = "1"

    # 编码统一成 UTF-8，三处缺一不可：
    #   PYTHONIOENCODING  让 Python 不按控制台默认（中文系统是 CP936）输出
    #   Console::OutputEncoding  让 PowerShell 按 UTF-8 解读原生命令的输出
    #   OutputEncoding    让重定向写文件时也用 UTF-8
    # 只改其中一处，日志就还是 GBK——中文系统上打开是乱码，且与 PS 7 的默认期望不符。
    $env:PYTHONIOENCODING = "utf-8"
    $utf8 = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = $utf8
    $OutputEncoding = $utf8

    # 命名与去重规则见 New-RunLogPath：键与 runs/ 一致（时间戳-任务名），
    # 同一秒连跑两条加序号，不覆盖上一轮现场。
    $log = New-RunLogPath -Name $Name
    $logDir = Split-Path -Parent $log

    $uvCommand = Get-Command uv -ErrorAction SilentlyContinue
    $uv = if ($uvCommand) { $uvCommand.Source } else { Join-Path $env:USERPROFILE ".local\bin\uv.exe" }
    if (-not (Test-Path -LiteralPath $uv)) {
        throw "找不到 uv：$uv（请安装或确认它在 PATH 中）"
    }

    $started = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "== $started  $Name  $($Arguments -join ' ')" | Out-File -FilePath $log -Append -Encoding utf8

    Push-Location $root
    try {
        & $uv run hf-data-crawl @Arguments *>> $log
        $code = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    $finished = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    "== $finished  退出码 $code" | Out-File -FilePath $log -Append -Encoding utf8

    Remove-StaleLogs -LogDir $logDir
    return $code
}
