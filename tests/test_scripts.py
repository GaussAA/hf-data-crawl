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
    from hf_data_crawl.datadoc import SIDE_FILES

    entry = next(item for item in SIDE_FILES if item[0] == "logs/")
    assert "按轮命名" in entry[1]
    assert "LOG_KEEP" in entry[1]
