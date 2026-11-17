"""对账差异清单的保留策略：只留最新几份，只动自己的文件。"""

from __future__ import annotations

from pathlib import Path

from hf_data_crawl.acquire.reconcile import REPORT_KEEP, REPORT_MAX_BYTES, prune_reports


def touch(directory: Path, name: str) -> Path:
    path = directory / name
    path.write_text("kind,id,detail\n", encoding="utf-8")
    return path


def fill(directory: Path, name: str, size: int) -> Path:
    """造一份指定字节数的清单，用来测「份数没超、体积超了」那种日子。"""
    path = directory / name
    path.write_bytes(b"x" * size)
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


def test_默认保留份数为一月() -> None:
    """对账改为每日后，份数即天数：30 份约一个月，8 份只够八天。"""
    assert REPORT_KEEP == 30


def test_份数没超但总量超限时从最旧的删起(tmp_path: Path) -> None:
    """首次全量对账那天的清单实测 50.5 MB，30 份就是 1.5 GB。

    只按份数判，份数永远合规、磁盘照样被吃光。体积上限从最旧的删起——
    反过来会先删掉几天前的小文件，只为给今天的大文件腾地方。
    """
    for day in range(1, 5):
        fill(tmp_path, f"reconcile-2026-11-0{day}.csv", 100)
    fill(tmp_path, "reconcile-2026-11-05.csv", 1000)

    removed = prune_reports(tmp_path, keep=30, max_bytes=1100)

    assert [path.name for path in removed] == [
        "reconcile-2026-11-01.csv",
        "reconcile-2026-11-02.csv",
        "reconcile-2026-11-03.csv",
    ]
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "reconcile-2026-11-04.csv",
        "reconcile-2026-11-05.csv",
    ]


def test_先按份数删再按体积删(tmp_path: Path) -> None:
    """两道判据串起来：份数超限的那批先走，剩下的再看总量。"""
    for day in range(1, 6):
        fill(tmp_path, f"reconcile-2026-11-0{day}.csv", 100)

    removed = prune_reports(tmp_path, keep=4, max_bytes=250)

    # 份数先删 11-01；剩下 4 份共 400 字节，仍超 250，再从 11-02 起删到只剩 2 份。
    assert [path.name for path in removed] == [
        "reconcile-2026-11-01.csv",
        "reconcile-2026-11-02.csv",
        "reconcile-2026-11-03.csv",
    ]
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "reconcile-2026-11-04.csv",
        "reconcile-2026-11-05.csv",
    ]


def test_体积上限是给全量对账日留的余量() -> None:
    """200 MB 能装四个 50 MB 的全量对账清单，日常几十 KB 的月份也在内。"""
    assert REPORT_MAX_BYTES == 200 * 1024 * 1024
