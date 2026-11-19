"""运维命令层的三条破坏性动作：归档、压缩、清理。

这三条与其它命令的根本差别是**它们会删东西**。所以本文件的判据不是「跑通了」，
而是三条更硬的：

- **预演必须真的不动磁盘**。人按预演的报告决定要不要执行，报告错了就是错删。
- **没有可做的对象时要说清是「没有」而不是「做完零个」**。
- **一处失败不能连坐**：某天归档验证不过，其它天照旧归档，失败单独计数。

底层算法（`plan` / `apply` / `compact` / `archive_day`）各有自己的测试；这里只管
命令层怎么调用它们、怎么把结果说给人听。
"""

from __future__ import annotations

from argparse import Namespace
from datetime import date
from pathlib import Path

from hf_data_crawl.ops import commands
from hf_data_crawl.platform.store import DatasetStore

OLD = "2020-01-01"


def args(**overrides: object) -> Namespace:
    base: dict[str, object] = {"date": None, "older_than_days": 30, "dry_run": False}
    base.update(overrides)
    return Namespace(**base)


def backup_partition(store: DatasetStore, day: str = OLD, *, size: int = 1024) -> Path:
    """造一处过期的 Raw 备份分区。天数远早于保留阈值，判龄必然命中。"""
    path = store.root / "backup" / "raw" / f"dt={day}"
    path.mkdir(parents=True)
    (path / "part-0.parquet").write_bytes(b"x" * size)
    return path


def test_清理预演只报不动磁盘(tmp_path: Path, capsys) -> None:
    """人按这份报告决定要不要真删；报告里列了却在别处删了，等于骗人。"""
    store = DatasetStore(tmp_path / "data")
    kept = backup_partition(store)

    outcome = commands.run_prune(args(dry_run=True), store)

    out = capsys.readouterr().out
    assert "将清理 backup/raw/dt=2020-01-01" in out
    assert "预演：1 处可清理" in out
    assert "未动磁盘" in out
    assert outcome.code == 0
    assert outcome.payload["dry_run"] is True
    assert outcome.payload["removed"] == []
    assert kept.is_dir(), "预演之后分区必须原样还在"


def test_清理真跑会删掉并报释放量(tmp_path: Path, capsys) -> None:
    store = DatasetStore(tmp_path / "data")
    gone = backup_partition(store)

    outcome = commands.run_prune(args(), store)

    out = capsys.readouterr().out
    assert "已清理 backup/raw/dt=2020-01-01" in out
    assert "清理完成：1 处" in out
    assert not gone.exists()
    assert outcome.payload["removed"] == ["dt=2020-01-01"]
    assert outcome.payload["freed"] > 0


def test_清理只碰过期的分区(tmp_path: Path, capsys) -> None:
    """留够天数是为了还能回滚；把新备份一起清了，回滚就成了空话。"""
    store = DatasetStore(tmp_path / "data")
    backup_partition(store, OLD)
    fresh = backup_partition(store, date.today().isoformat())

    commands.run_prune(args(dry_run=True), store)

    assert fresh.is_dir()
    assert "1 处可清理" in capsys.readouterr().out


def test_压缩遇到没有分块的日期时说清无需压缩(tmp_path: Path, capsys) -> None:
    """清单里的日期在 raw 里没有分块，是「不用压」而不是「压失败了」。"""
    store = DatasetStore(tmp_path / "data")

    outcome = commands.run_compact(args(date=OLD), store)

    assert outcome.code == 0
    assert outcome.payload == {"compacted": [], "failures": []}
    assert "没有分块" in capsys.readouterr().out


def test_压缩按天数筛日期时没有目标就直说(tmp_path: Path, capsys) -> None:
    """全量筛一遍也是常态：没有够冷的日期时要一句话收工。"""
    store = DatasetStore(tmp_path / "data")

    outcome = commands.run_compact(args(older_than_days=30), store)

    assert outcome.code == 0
    assert "没有符合条件的冷区日期" in capsys.readouterr().out


def test_归档没有匹配日期时不谎报成功(tmp_path: Path, capsys) -> None:
    store = DatasetStore(tmp_path / "data")

    outcome = commands.run_archive(args(older_than_days=30), store)

    assert outcome.code == 0
    assert outcome.payload == {"archived": [], "failures": []}
    assert "没有符合条件的日期" in capsys.readouterr().out


def test_归档一天失败不影响其它天(tmp_path: Path, monkeypatch, capsys) -> None:
    """某天验证不过就记一条失败、继续下一天；中断其余天会让「已归档几天」说不清。"""
    store = DatasetStore(tmp_path / "data")
    ok_day = date(2020, 1, 2)
    bad_day = date(2020, 1, 1)

    class Result:
        source_files = 2
        shards = 1
        records_before = 10
        records_after = 10
        bytes_before = 2_000_000
        bytes_after = 1_000_000
        backup_dir = store.root / "backup" / "raw"

    def fake_archive(_store: DatasetStore, day: date) -> Result:
        if day == bad_day:
            raise commands_archive_error(f"{day} 归档前后 Bronze 不一致")
        return Result()

    monkeypatch.setattr(commands, "compare", lambda *_a, **_k: [])
    monkeypatch.setattr(commands, "restamp", lambda *_a, **_k: None)
    monkeypatch.setattr("hf_data_crawl.ops.archive.archive_day", fake_archive)
    monkeypatch.setattr(
        "hf_data_crawl.pipeline.rebuild.discover_raw_dates",
        lambda _store: [bad_day, ok_day],
    )

    outcome = commands.run_archive(args(older_than_days=30), store)

    out = capsys.readouterr().out
    assert outcome.code == 1, "有失败就要非零，否则定时任务看不出今天出过问题"
    assert outcome.payload["failures"] == [f"{bad_day} 归档前后 Bronze 不一致"]
    assert ok_day.isoformat() in out and "归档：" in out
    assert len(outcome.payload["archived"]) == 1


def commands_archive_error(message: str) -> Exception:
    """取归档模块自己的异常类型：命令层只捕它，捕宽了会连 bug 一起吞。"""
    from hf_data_crawl.ops.archive import ArchiveMismatch

    return ArchiveMismatch(message)


def test_归档单日时直接按参数走(tmp_path: Path, monkeypatch) -> None:
    """`--date` 给了就不该再去扫全库的日期——扫一遍几秒钟，且结果用不上。"""
    store = DatasetStore(tmp_path / "data")

    def boom(_store: object) -> list[date]:
        raise AssertionError("给了 --date 就不该调用 discover_raw_dates")

    monkeypatch.setattr("hf_data_crawl.pipeline.rebuild.discover_raw_dates", boom)
    monkeypatch.setattr(commands, "compare", lambda *_a, **_k: [])

    outcome = commands.run_archive(args(date=OLD), store)

    assert outcome.code == 0
