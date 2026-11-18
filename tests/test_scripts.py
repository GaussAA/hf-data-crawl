"""计划任务脚本的契约：日志按轮命名、有界修剪，且不许绕过公共包装。

为什么是文本断言而不是真跑一边：这些脚本只在 Windows 计划任务里执行，CI 上不
一定有 `pwsh`；而它们真正容易退化的地方恰好是**命名模板**——有人图省事改回
`$Name.log`，单文件永久追加就会悄悄回来，从外部看不出任何异常，直到某天发现
日志里混着几个月前的内容、且永远对不上 `runs/` 里那轮摘要。
"""

from __future__ import annotations

from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
COMMON = SCRIPTS / "_common.ps1"

#: 会落到文件上的写法。它们都该只出现在 `_common.ps1` 的公共包装里。
WRITE_HINTS = ("Out-File", "Add-Content", "Set-Content", "Tee-Object", "*>>", "Join-Path")

#: 只往控制台打的写法。`setup-schedule.ps1` 会 `Write-Host (Join-Path ... 'data\logs')`
#: 报一下日志目录，那是告知而非落盘，不该算绕过。
CONSOLE_HINTS = ("Write-Host", "Write-Output", "Write-Information", "Write-Verbose")


def text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def job_block(source: str, name: str) -> str:
    """截出 `@{ Name = "hf-<name>" ... }` 这一段。

    按 `},` 收尾而不是按下一个任务名切：任务之间的字段顺序并不统一
    （compact 把 `LogonDelay` 写在 `Triggers` 之前），按固定字段切会切错段。
    """
    blocks = source.split('Name = "hf-')
    block = next(item for item in blocks if item.startswith(name))
    return block.split("},")[0]


def test_日志文件名含时间戳而不是按任务名覆盖() -> None:
    source = text(COMMON)
    assert 'Get-Date -Format "yyyyMMddTHHmmss"' in source
    assert '"$stamp-$Name.log"' in source
    # 旧写法是 Join-Path $logDir "$Name.log"：同一个命令一个文件、永久追加，
    # 既对不上 runs/ 里那轮的摘要，也没有任何上限。
    assert 'Join-Path $logDir "$Name.log"' not in source


def test_同一秒连跑两条不覆盖() -> None:
    """覆盖会让上一轮的现场记录静默消失——runs.write_run 用同一套加序号约定。"""
    source = text(COMMON)
    assert "while (Test-Path -LiteralPath $log)" in source
    assert '"$stamp-$Name-$suffix.log"' in source


def test_日志有上限且每轮修剪() -> None:
    source = text(COMMON)
    assert "$script:LogKeep = 200" in source
    assert "function Remove-StaleLogs" in source
    assert "Remove-StaleLogs -LogDir $logDir" in source
    # 上限为 0 或负数会把整个目录清空，必须挡在函数入口。
    assert "if ($Keep -le 0)" in source


def test_修剪按文件名排序而非修改时间() -> None:
    """文件名前缀就是开跑时的时间戳；mtime 会被同步盘/备份/解压整体改掉。"""
    source = text(COMMON)
    assert "Sort-Object Name" in source
    assert "Sort-Object LastWriteTime" not in source


def test_没有脚本绕过公共包装自己写日志() -> None:
    offenders = []
    for path in sorted(SCRIPTS.glob("*.ps1")):
        if path == COMMON:
            continue
        for line in text(path).splitlines():
            if "data\\logs" not in line:
                continue
            if not any(hint in line for hint in WRITE_HINTS):
                continue
            if any(hint in line for hint in CONSOLE_HINTS):
                continue
            offenders.append(f"{path.name}: {line.strip()}")
    assert offenders == [], f"这些脚本绕过了 Invoke-HfTask 的日志约定：{offenders}"


def test_数据说明写明日志按轮命名() -> None:
    from hf_data_crawl.ops.datadoc import SIDE_FILES

    entry = next(item for item in SIDE_FILES if item[0] == "logs/")
    assert "按轮命名" in entry[1]
    assert "LOG_KEEP" in entry[1]


def test_全量对账与压缩按日跑() -> None:
    """日增量按 lastModified 走只覆盖约 6%，长尾靠全量对账刷新；退回按周就会让
    底库在两次对账之间拿旧值充数。这里把频率钉死，改回 Weekly 必须有人来解释。

    对账已并入 `hf-daily` 链（2026-11-09），故要同时盯两头：链本身按日跑，
    且链里确实有对账这一步——只在其中一个上做文章，都能悄悄把频率改回去。
    """
    source = text(SCRIPTS / "setup-schedule.ps1")
    for name in ("daily", "compact"):
        block = job_block(source, name)
        assert "-Daily" in block, f"hf-{name} 应每日触发"
        assert "Weekly" not in block, f"hf-{name} 不应再按周触发"

    chain = text(SCRIPTS / "daily.ps1")
    assert 'Name = "reconcile"' in chain, "链条里必须还有全量对账这一步"


def test_链条按序且前置失败会跳过() -> None:
    """顺序是这条链存在的理由：补跑时若各步并起，导出会抢在对账前完成。"""
    chain = text(SCRIPTS / "daily.ps1")
    order = [
        chain.index(f'Name = "{name}"')
        for name in ("collect", "build", "reconcile", "export", "deliver", "doctor")
    ]
    assert order == sorted(order), "链条步骤顺序被打乱"
    # 前置失败要跳过并写明原因，而不是带着半截状态硬跑。
    assert "Requires" in chain
    assert "[跳过]" in chain


def test_体检在链条里只算告警不算失败() -> None:
    """体检把「导出落后」「Raw 断日」这类待办也计入非零；若让它拖垮链条，
    整条链会天天报红，铃声很快就没人听。"""
    chain = text(SCRIPTS / "daily.ps1")
    assert "Report = $true" in chain
    assert "$step.Report" in chain
    assert "if ($failure -eq 0) { $failure = $code }" in chain


def test_补跑只留给链条() -> None:
    """错过时补跑整条链是对的；其余纯周期维护补跑只会跟链抢写锁。"""
    source = text(SCRIPTS / "setup-schedule.ps1")
    for block in source.split('Name = "hf-')[1:]:
        name = block.split('"')[0]
        catch_up = "CatchUp = $true" in job_block(source, name)
        assert catch_up == (name == "daily"), f"hf-{name} 的补跑设置不对"


def test_升级时清除旧的钟点任务() -> None:
    """留着旧的五个任务，它们仍会各自补跑——正是这次要治的病。"""
    source = text(SCRIPTS / "setup-schedule.ps1")
    for legacy in ("hf-collect", "hf-reconcile", "hf-export", "hf-deliver", "hf-doctor"):
        assert f'"{legacy}"' in source, f"缺少旧任务 {legacy} 的清理"
    assert "Unregister-ScheduledTask" in source


def test_规模补齐不写死全站行数() -> None:
    """`--top 1122000` 会随底库增长过期，长尾永远补不到规模且不报任何错。"""
    source = text(SCRIPTS / "sizes.ps1")

    assert '"--top", "0"' in source
    assert "1122000" not in source
    assert "全量" in source


# ---------------------------------------------------------------- 墙钟上限


def test_每一步都有墙钟上限且退出码可辨() -> None:
    """卡死时不该靠人守着：进程留着不走，后面每一步都在排队等锁。

    2026-11-16 的 `fetch-sizes` 卡 1 小时 35 分、11-17 的 `reconcile` 卡 2 小时，
    症状一模一样——CPU 与读写计数零增长、`Get-NetTCPConnection` 一个连接都没有
    （卡在 DNS 解析，httpx 的超时覆盖不到 getaddrinfo）。心跳只能让它可见，
    救不了链条；必须有一个总时长上限，到点掐掉、记 124、继续往下走。
    """
    source = text(COMMON)
    assert "$script:TimeoutExitCode = 124" in source
    assert "[ValidateRange(1, 86400)][int]$TimeoutSeconds = 3600" in source

    chain = text(SCRIPTS / "daily.ps1")
    steps = ("collect", "build", "reconcile", "sizes", "export", "deliver", "prune", "doctor")
    assert chain.count("TimeoutSeconds = ") == len(steps), "有步骤没声明墙钟上限"
    # 首次调用与 collect 的重试都要把上限传下去，漏一处等于漏一条。
    assert chain.count("-TimeoutSeconds $step.TimeoutSeconds") == 2


def test_超时要杀进程树而不是只杀父进程() -> None:
    """`uv` 是启动器，python 与 hf-data-crawl.exe 都在它下面。

    只杀 uv 会留下攥着写锁的孤儿——后面每一步都会因「拿不到锁」而失败，
    比不杀更糟。故必须 `taskkill /T`（整棵进程树）。
    """
    source = text(COMMON)
    assert "function Stop-ProcessTree" in source
    assert "/T /F" in source, "taskkill 必须带 /T，否则只杀得掉启动器"
    assert "Start-Process" in source
    assert "-RedirectStandardOutput" in source
    # 旧的同步调用拿不到进程句柄，超时无从谈起；管道还可能在缓冲满时死锁。
    assert "@Arguments *>> $log" not in source


def test_超时与失败在摘要里分开写() -> None:
    """一个是「这步自己出错」，一个是「这步卡住了」——处置完全不同。"""
    chain = text(SCRIPTS / "daily.ps1")
    assert "[超时]" in chain
    assert "$script:TimeoutExitCode" in chain
    # 超时也要计入链条失败，不能静默放过。
    assert "if ($failure -eq 0) { $failure = $code }" in chain


def test_所有任务脚本都传了墙钟上限() -> None:
    """漏一个脚本，就等于留了一处无限等。"""
    for path in sorted(SCRIPTS.glob("*.ps1")):
        if path.name == COMMON.name:
            continue
        source = text(path)
        if "Invoke-HfTask" not in source:
            continue
        assert "-TimeoutSeconds" in source, f"{path.name} 调了 Invoke-HfTask 却没给上限"


def test_日志头部在收尾时补写() -> None:
    """`Start-Process` 的重定向会截断目标文件，先写头必被冲掉。

    所以运行期由 OS 直接写正文（能实时 tail），收尾再拼「头 + 正文 + 尾」，
    格式与旧版一致。这条断言钉住那个拼接，别让头部悄悄消失。
    """
    source = text(COMMON)
    assert "$header + $body + $footer" in source
    assert "已强杀进程树" in source
    # 末尾不能多出空行：MONITORING 承诺「末行带实耗秒数」，多一行空白就食言了。
    assert '.TrimEnd("`r", "`n") | Out-File' in source
    # 正文不以换行收尾时，尾行会粘在最后一行输出后面。
    assert '$body.EndsWith("`n")' in source
