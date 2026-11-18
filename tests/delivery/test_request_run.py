"""`request run` 与 `run-due` 的分支测试。

这两条是交付链的入口，却一直没有直接测试：`test_delivery` 测的是 `deliver` /
`preview` / `run_due` 这些下游函数，命令层自己那四条出口——预演、预览、正式交付、
被闸门拦下——只有「正式交付」被间接走到过一次。

四条出口的差别不是文案差别，是「写不写盘、占不占名额、发不发生」的差别：

- `--dry-run` 一行都不写，也不该占掉一次性需求的「已交付」名额；
- `--preview` 把真产物写进草稿区，但不进台账；
- 已关闭的需求必须拒交，并指出退路；
- 两者同给是语义冲突，不能挑一个执行。

`run-due` 另有三种汇总结果（交付 / 跳过 / 失败），此前一条都没测。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from support import write_method

from hf_data_crawl.delivery.delivery import PREVIEW_DIR, SCRATCH_DIR
from hf_data_crawl.delivery.requirement import SPEC_NAME
from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform import DatasetStore

DAY = date(2026, 11, 1)


def seed(tmp_path: Path) -> DatasetStore:
    """底库：三个库，下载量 2000 / 1500 / 500，供 `下载量 > 1000` 筛出两条。"""
    made = DatasetStore(tmp_path / "data")
    raw.write(
        made,
        snapshot_date=DAY,
        page_index=1,
        records=[
            {
                "id": f"acme/ds-{index}",
                "author": "acme",
                "lastModified": "2026-11-01T00:00:00.000Z",
                "createdAt": "2025-01-01T00:00:00.000Z",
                "sha": f"sha-{index}",
                "downloads": downloads,
                "downloadsAllTime": downloads * 10,
                "likes": 2,
                "private": False,
                "description": "d",
                "tags": ["license:mit"],
            }
            for index, downloads in ((1, 2000), (2, 1500), (3, 500))
        ],
        meta={},
    )
    derive_day(made, DAY)
    return made


def write_spec(root: Path, request_id: str = "REQ-0001", **overrides: Any) -> Path:
    body: dict[str, Any] = {
        "id": request_id,
        "title": "高下载数据集",
        "requester": "张三",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    path = root / request_id / SPEC_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    # 交付链要求过程记录齐全：骨架走真实入口生成，再抹掉「待填」标记。
    write_method(path.parent, request_id=request_id, title=str(body["title"]))
    return path


def run(tmp_path: Path, *argv: str) -> int:
    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def deliveries(root: Path, request_id: str = "REQ-0001") -> list[Path]:
    base = root / request_id / "deliveries"
    return sorted(path for path in base.iterdir() if path.is_dir()) if base.is_dir() else []


# ---------------------------------------------------------------- 四条出口


def test_预演不写盘也不占一次性名额(store: DatasetStore, tmp_path: Path) -> None:
    """`--dry-run` 若要占掉「once 已交付」的名额，先看一眼就再也不能交了。

    判据分两半：跑完没有任何交付目录；紧接着的正式交付仍然成功——只验前者，
    命令层即使把名额悄悄记上也能过。
    """
    root = tmp_path / "req"
    write_spec(root)

    assert run(tmp_path, "request", "run", "REQ-0001", "--dry-run") == 0
    assert deliveries(root) == []
    assert run(tmp_path, "request", "run", "REQ-0001") == 0
    assert len(deliveries(root)) == 1


def test_预演与预览同时给出被拒(store: DatasetStore, tmp_path: Path, capsys) -> None:
    """两者都是「只看不写」，但一个不落盘、一个落草稿区；同时给出时先后都是猜。"""
    root = tmp_path / "req"
    write_spec(root)

    code = run(tmp_path, "request", "run", "REQ-0001", "--dry-run", "--preview")

    assert code == 1
    assert "选一个" in capsys.readouterr().out
    assert deliveries(root) == []


def test_预览把产物写进草稿区但不记账(store: DatasetStore, tmp_path: Path, capsys) -> None:
    root = tmp_path / "req"
    write_spec(root)

    assert run(tmp_path, "request", "run", "REQ-0001", "--preview") == 0
    out = capsys.readouterr().out

    assert "产物落在草稿区" in out
    assert deliveries(root) == []
    assert any((root / "REQ-0001" / SCRATCH_DIR / PREVIEW_DIR).iterdir())


def test_已关闭的需求拒绝交付并指出退路(store: DatasetStore, tmp_path: Path, capsys) -> None:
    root = tmp_path / "req"
    write_spec(root)
    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "对方撤单") == 0

    code = run(tmp_path, "request", "run", "REQ-0001")

    assert code == 1
    out = capsys.readouterr().out
    assert "已关闭" in out and "对方撤单" in out
    assert "request reopen" in out
    assert deliveries(root) == []


def test_需求号不存在时如实报错而非抛栈(store: DatasetStore, tmp_path: Path, capsys) -> None:
    """`resolve_dir` 抛的是 `SpecError`；命令层若漏接，用户看到的是 traceback。"""
    (tmp_path / "req").mkdir()

    assert run(tmp_path, "request", "run", "REQ-9999") == 1
    assert "失败" in capsys.readouterr().out


# ---------------------------------------------------------------- 批量


def test_批量交付在没有任何需求时直接说明(tmp_path: Path, capsys) -> None:
    """首次部署就会走到这里：不该报错，也不该假装跑过一轮。"""
    assert run(tmp_path, "request", "run-due") == 0
    assert "还没有需求" in capsys.readouterr().out


def test_批量交付按交付与跳过两类汇总(store: DatasetStore, tmp_path: Path, capsys) -> None:
    """一条正常、一条已关闭：正常的交付，关闭的记为跳过，两者都不算失败。"""
    root = tmp_path / "req"
    write_spec(root, "REQ-0001")
    write_spec(root, "REQ-0002")
    assert run(tmp_path, "request", "close", "REQ-0002", "--reason", "需求方推迟") == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "run-due") == 0
    out = capsys.readouterr().out

    assert "共 1 交付 / 1 跳过 / 0 失败" in out
    assert "需求方推迟" in out
    assert len(deliveries(root, "REQ-0001")) == 1
    assert deliveries(root, "REQ-0002") == []


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return seed(tmp_path)
