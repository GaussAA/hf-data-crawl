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

#: 被墙钟掐掉时的退出码，取 coreutils `timeout` 的惯例值。与真实失败区分开：
#: hf-data-crawl 自己只返回 0/1，124 不会撞车，摘要里一眼能认出「是被我们杀的」。
$script:TimeoutExitCode = 124

function Stop-ProcessTree {
    <#
    .SYNOPSIS
        连子进程一起杀。
    .DESCRIPTION
        只杀父进程会留下持锁的 python：`uv` 是启动器，真正的 python 与
        hf-data-crawl.exe 都在它下面，`Stop-Process` 只杀得掉 uv，那两个会变成
        孤儿继续跑——手里还攥着写锁，后面每一步都因「拿不到锁」而失败，比不杀更糟。
        `taskkill /T` 按进程树杀，是这里唯一可靠的办法。
    #>
    [CmdletBinding()]
    param([Parameter(Mandatory)][int]$ProcessId)

    $taskkill = Join-Path $env:SystemRoot "System32\taskkill.exe"
    if (Test-Path -LiteralPath $taskkill) {
        & $taskkill /PID $ProcessId /T /F *> $null
    }

    # 强杀可能让我们自己先死：等它真的消失再返回，否则后续步骤抢锁会撞上一个
    # 「还活着但已经没人管」的进程。15 秒足够，超时就用 Stop-Process 再补一刀。
    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) { return }
        Start-Sleep -Milliseconds 200
    }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

function Invoke-HfTask {
    <#
    .SYNOPSIS
        跑一条 hf-data-crawl 子命令，输出进 data\logs\<时间戳>-<Name>.log，带墙钟上限。
    .DESCRIPTION
        为什么要墙钟上限：2026-11-16 的 `fetch-sizes` 与 11-17 的 `reconcile` 各卡死
        一次，症状一模一样——进程留着、线程挂着、CPU 与读写计数零增长、`Get-NetTCPConnection`
        一个连接都没有（卡在 DNS 解析，httpx 的超时覆盖不到 getaddrinfo）。心跳只让它
        可见，救不了链条：进程不退出，后面每一步都在排队等锁。
        这里给每一步一个总时长上限，到点杀进程树、记 124、链条照常往下走。

        为什么用 `Start-Process` 重定向文件而不是 `& ... *>> $log`：前者由 OS 直接
        写文件句柄，跑到一半也能 tail；后者在 PowerShell 里是同一进程内的管道，
        我们没法既实时读又在超时后掐断。管道还有个额外风险：没人读满缓冲就死锁。
    .OUTPUTS
        进程退出码（超时为 124）。计划任务据此判定成败。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][string[]]$Arguments,
        # 墙钟上限（秒）。默认一小时，各脚本按自己那步的正常耗时给值。
        [ValidateRange(1, 86400)][int]$TimeoutSeconds = 3600
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
    # stderr 不能与 stdout 同一个文件（Start-Process 不允许），先分着，收尾时合流。
    $stderrLog = "$log.stderr"

    $uvCommand = Get-Command uv -ErrorAction SilentlyContinue
    $uv = if ($uvCommand) { $uvCommand.Source } else { Join-Path $env:USERPROFILE ".local\bin\uv.exe" }
    if (-not (Test-Path -LiteralPath $uv)) {
        throw "找不到 uv：$uv（请安装或确认它在 PATH 中）"
    }

    $started = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $startedAt = Get-Date
    $argumentsWithCommand = @("run", "hf-data-crawl") + $Arguments

    $timedOut = $false
    $code = -1
    $process = $null
    try {
        # 头部两行不在这里写：`-RedirectStandardOutput` 会**截断**目标文件，
        # 先写必被冲掉。收尾时把「头 + 正文 + 尾」一次性重写，格式与旧版一致。
        $process = Start-Process -FilePath $uv -ArgumentList $argumentsWithCommand `
            -WorkingDirectory $root -NoNewWindow -PassThru `
            -RedirectStandardOutput $log -RedirectStandardError $stderrLog

        if ($process.WaitForExit($TimeoutSeconds * 1000)) {
            $code = $process.ExitCode
        }
        else {
            $timedOut = $true
            $code = $script:TimeoutExitCode
            Stop-ProcessTree -ProcessId $process.Id
        }
    }
    finally {
        if ($process) { $process.Dispose() }
    }

    $finished = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $elapsed = [int]((Get-Date) - $startedAt).TotalSeconds

    $body = ""
    if (Test-Path -LiteralPath $log) {
        $body = [string](Get-Content -LiteralPath $log -Raw -Encoding utf8)
    }
    if (Test-Path -LiteralPath $stderrLog) {
        $stderrText = [string](Get-Content -LiteralPath $stderrLog -Raw -Encoding utf8)
        if ($stderrText) { $body = "$body$stderrText" }
        Remove-Item -LiteralPath $stderrLog -Force -ErrorAction SilentlyContinue
    }

    $header = "== $started  $Name  $($Arguments -join ' ')`r`n"
    $footer = if ($timedOut) {
        "== $finished  退出码 $code（墙钟 ${TimeoutSeconds}s 用尽、实耗 ${elapsed}s，已强杀进程树）`r`n"
    }
    else {
        "== $finished  退出码 $code（实耗 ${elapsed}s）`r`n"
    }
    # 正文若不以换行收尾，尾行会粘在最后一行输出后面；空正文时更不该留一行空白。
    if ($body -and -not $body.EndsWith("`n")) { $body = "$body`r`n" }
    # 去掉尾部换行再由 Out-File 补一个：否则每份日志末尾会多出一整行空白，
    # 而 MONITORING 承诺「末行带实耗秒数」——多出空行就变成末行什么都没有。
    ($header + $body + $footer).TrimEnd("`r", "`n") | Out-File -FilePath $log -Encoding utf8

    Remove-StaleLogs -LogDir $logDir
    return $code
}
