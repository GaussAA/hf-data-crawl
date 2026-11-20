"""计划任务脚本的契约：日志按轮命名、有界修剪，且不许绕过公共包装。

为什么是文本断言而不是真跑一边：这些脚本只在 Windows 计划任务里执行，CI 上不
一定有 `pwsh`；而它们真正容易退化的地方恰好是**命名模板**——有人图省事改回
`$Name.log`，单文件永久追加就会悄悄回来，从外部看不出任何异常，直到某天发现
日志里混着几个月前的内容、且永远对不上 `runs/` 里那轮摘要。
"""

from __future__ import annotations

import re
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

    按花括号配平取整块，不按固定字段或 `},` 切：任务字段顺序并不统一
    （compact 把 `LogonDelay` 写在 `Triggers` 之前），而 `$jobs` 数组末尾的任务
    以单独的 `}` 收尾、没有逗号——按 `},` 切会让整块一直延伸到文件末尾，
    块内任何字样都成立，判据就恒真了。
    """
    marker = f'Name = "hf-{name}"'
    position = source.index(marker)
    start = source.rindex("@{", 0, position)
    depth = 0
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]
    raise AssertionError(f"没找到 hf-{name} 的任务块")


def test_日志文件名含时间戳而不是按任务名覆盖() -> None:
    source = text(COMMON)
    assert 'Get-Date -Format "yyyyMMddTHHmmss"' in source
    assert '"$stamp-$Name.log"' in source


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

    2026-11-19 起调度收成一条 `hf-refresh`（ADR-0008）：对账在并行段、压缩在
    串行头，都由 `ops/refresh.py` 定义，不再散在多个计划任务里。
    """
    from hf_data_crawl.ops import refresh

    block = job_block(text(SCRIPTS / "setup-schedule.ps1"), "refresh")
    assert "-Daily" in block, "hf-refresh 应每日触发"
    assert "Weekly" not in block, "刷新链不应再按周触发"

    names = {step.name for step in (*refresh.SERIAL_HEAD, *refresh.PARALLEL)}
    assert "reconcile" in names, "链条里必须还有全量对账"
    assert "compact" in names, "压缩必须仍在链里"


def test_链条按序且前置失败会跳过() -> None:
    """顺序是这条链存在的理由：补跑时若各步并起，导出会抢在对账前完成。"""
    from hf_data_crawl.ops import refresh

    assert [step.name for step in refresh.SERIAL_HEAD] == ["compact", "collect", "build"]
    assert [step.name for step in refresh.SERIAL_TAIL] == [
        "parse",
        "export",
        "report",
        "deliver",
        "archive",
        "prune",
        "doctor",
    ]
    export = next(step for step in refresh.SERIAL_TAIL if step.name == "export")
    assert set(export.needs) == {"build", "reconcile", "sizes", "parse"}


def test_补跑只留给链条() -> None:
    """错过时补跑整条链是对的；其余纯周期维护补跑只会跟链抢写锁。

    2026-11-19 起只有一个任务，且它**不补跑**：续跑靠 runs 台账 + staging 分片，
    比把整条链重放一遍精细（ADR-0008）。
    """
    source = text(SCRIPTS / "setup-schedule.ps1")
    for block in source.split('Name = "hf-')[1:]:
        name = block.split('"')[0]
        assert "CatchUp = $true" not in job_block(source, name), f"hf-{name} 不该补跑"


def test_升级时清除旧的钟点任务() -> None:
    """留着旧任务，它们仍会各自补跑——正是这次要治的病。

    名单与 `setup-schedule.ps1` 的 `$legacyNames` 逐项对齐：少钉一个，升级时就
    可能漏清一个，而漏掉的那个仍会按旧钟点抢写锁。
    """
    source = text(SCRIPTS / "setup-schedule.ps1")
    legacy = (
        "hf-collect",
        "hf-reconcile",
        "hf-export",
        "hf-deliver",
        "hf-doctor",
        "hf-daily",
        "hf-sizes",
        "hf-files",
        "hf-archive",
        "hf-compact",
        "hf-readme",
    )
    for name in legacy:
        assert f'"{name}"' in source, f"缺少旧任务 {name} 的清理"
    assert "Unregister-ScheduledTask" in source


def test_规模补齐不写死全站行数() -> None:
    """`--top 1122000` 会随底库增长过期，长尾永远补不到规模且不报任何错。

    2026-11-19 起不看 ps1 的默认参数（那 11 个单命令包装已删），改看刷新链里
    `sizes` 这一步：它用 `--changed-all`（陈旧 ∪ 缺口），本来就不依赖行数常量。
    """
    from hf_data_crawl.ops import refresh

    sizes = next(step for step in refresh.PARALLEL if step.name == "sizes")

    assert "--changed-all" in sizes.args
    assert not [arg for arg in sizes.args[1:] if arg.isdigit()]


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
    assert re.search(
        r"\[ValidateRange\(1,\s*86400\)\]\[int\]\$TimeoutSeconds\s*=\s*3600", source
    ), "超时上限必须带 1..86400 的取值范围，且默认 1 小时"

    chain = text(SCRIPTS / "refresh.ps1")
    assert "-TimeoutSeconds 36000" in chain, "刷新链必须有 10 小时墙钟上限"


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
    assert re.search(r"\$header\s*\+\s*\$body\s*\+\s*\$footer", source), (
        "收尾必须把「头 + 正文 + 尾」拼回去，先写头会被 Start-Process 截断"
    )
    # 末尾不能多出空行：MONITORING 承诺「末行带实耗秒数」，多一行空白就食言了。
    assert re.search(r"\.TrimEnd\(", source)
    # 正文不以换行收尾时，尾行会粘在最后一行输出后面。
    assert '$body.EndsWith("`n")' in source
