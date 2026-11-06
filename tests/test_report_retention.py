"""对账差异清单的保留策略：只留最新几份，只动自己的文件。"""

from __future__ import annotations

from pathlib import Path

from hf_data_crawl.reconcile import REPORT_KEEP, prune_reports


def touch(directory: Path, name: str) -> Path:
    path = directory / name
    path.write_text("kind,id,detail\n", encoding="utf-8")
    return path


def test_超出份数时删最旧的(tmp_path: Path) -> None:
    for day in range(1, 6):
        touch(tmp_path, f"reconcile-2026-11-0{day}.csv")

    removed = prune_reports(tmp_path, keep=3)

    assert [path.name for path in removed] == [
        "reconcile-2026-11-01.csv",
        "reconcile-2026-11-02.csv",
    ]
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "reconcile-2026-11-03.csv",
        "reconcile-2026-11-04.csv",
        "reconcile-2026-11-05.csv",
    ]


def test_不足份数时一份不删(tmp_path: Path) -> None:
    touch(tmp_path, "reconcile-2026-11-01.csv")

    assert prune_reports(tmp_path, keep=3) == []
    assert len(list(tmp_path.iterdir())) == 1


def test_按文件名日期排序而不是修改时间(tmp_path: Path) -> None:
    old = touch(tmp_path, "reconcile-2026-10-01.csv")
    touch(tmp_path, "reconcile-2026-11-01.csv")
    # 让最旧日期的文件拥有最新的 mtime，模拟复制目录后时间戳被打乱。
    old.touch()

    prune_reports(tmp_path, keep=1)

    assert [path.name for path in tmp_path.iterdir()] == ["reconcile-2026-11-01.csv"]


def test_不碰榜单页与其他文件(tmp_path: Path) -> None:
    touch(tmp_path, "dashboard.html")
    touch(tmp_path, "reconcile-notes.csv")
    for day in range(1, 4):
        touch(tmp_path, f"reconcile-2026-11-0{day}.csv")

    prune_reports(tmp_path, keep=1)

    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "dashboard.html",
        "reconcile-2026-11-03.csv",
        "reconcile-notes.csv",
    ]


def test_默认保留份数为八() -> None:
    assert REPORT_KEEP == 8
