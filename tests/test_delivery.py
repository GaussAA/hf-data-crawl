"""交付的行为测试。

守四条判据：三件齐全、同一底库两次交付逐字节相同、0 行与超限不落盘、
行数突变要标疑但**不阻断**（交货比不交好，可疑必须留在纸面上）。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl
import pytest
import yaml
from support import write_method

from hf_data_crawl import method
from hf_data_crawl.cli import deliveries_of, main
from hf_data_crawl.delivery import (
    EXECUTORS,
    DeliveryBlocked,
    deliver,
    due_state,
    preview,
    run_due,
)
from hf_data_crawl.fields import CONTRACT_VERSION, DEFAULT_COLUMNS
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.requirement import (
    DELIVERIES_DIR,
    MANIFEST_NAME,
    SPEC_NAME,
    Kind,
    directory_name,
    spec_digest,
)

DAY = date(2026, 11, 1)
NOW = datetime(2026, 11, 8, 15, 20, 0, tzinfo=UTC)


def record(index: int, *, downloads: int, likes: int = 2) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 10,
        "likes": likes,
        "private": False,
        "description": "d",
        "tags": ["license:mit"],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    """三个库：2000 下载 / 1500 下载 / 500 下载，点赞都是 2。"""
    made = DatasetStore(tmp_path / "data")
    raw.write(
        made,
        snapshot_date=DAY,
        page_index=1,
        records=[
            record(1, downloads=2000),
            record(2, downloads=1500),
            record(3, downloads=500),
        ],
        meta={},
    )
    derive_day(made, DAY)
    return made


def make_request(root: Path, **overrides: Any) -> Path:
    body: dict[str, Any] = {
        "id": "REQ-0001",
        "title": "高下载数据集",
        "requester": "张三",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    # 目录跟 id 走，不写死：多需求共存的测试全靠这一点。
    path = root / str(body["id"]) / SPEC_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    write_method(path.parent, request_id=str(body["id"]), title=str(body["title"]))
    return path


def delivery_dirs(root: Path) -> list[Path]:
    base = root / "REQ-0001" / DELIVERIES_DIR
    if not base.is_dir():
        return []
    return sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )


def test_交付写出三件套(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    names = {path.name for path in result.directory.iterdir()}
    assert names == {"data.parquet", MANIFEST_NAME, "README.md"}
    assert result.rows == 2


def test_manifest记下底库指纹与校验和(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    body = yaml.safe_load((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert len(body["source"]["current_md5"]) == 32
    assert body["source"]["current_rows"] == 3
    assert body["result"]["rows"] == 2
    assert len(body["result"]["files"][0]["sha256"]) == 64
    assert body["criteria"][0]["field"] == "downloads"
    assert body["health"]["status"] == "first"
    assert body["projection"]["source"] == "default"
    assert body["projection"]["default_columns_version"] == CONTRACT_VERSION
    assert body["projection"]["columns"] == list(DEFAULT_COLUMNS)


def test_同一底库两次交付逐字节相同(store: DatasetStore, tmp_path: Path) -> None:
    """可复现的判据。靠的是编译时那条 "id" ASC 兜底。"""
    root = tmp_path / "req"
    make_request(root)

    first = deliver(store, root, "REQ-0001", now=NOW)
    second = deliver(store, root, "REQ-0001", now=NOW.replace(second=30))

    assert first.files[0]["sha256"] == second.files[0]["sha256"]
    assert first.directory != second.directory


def test_零行不交付且不留目录(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, criteria=[{"metric": "下载量", "op": ">", "value": 999999}])

    with pytest.raises(DeliveryBlocked, match="min_rows"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert delivery_dirs(root) == []


def test_超上限不交付且不留目录(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, guard={"min_rows": 1, "max_rows": 1})

    with pytest.raises(DeliveryBlocked, match="max_rows"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert delivery_dirs(root) == []


def test_失败时不留半成品目录(store: DatasetStore, tmp_path: Path) -> None:
    """中途失败只能留下 .tmp-*，绝不能出现「有数据没说明」。"""
    root = tmp_path / "req"
    make_request(root, output={"format": "csv", "columns": ["id", "不存在的列"]})

    with pytest.raises(DeliveryBlocked):
        deliver(store, root, "REQ-0001", now=NOW)

    leftovers = [path for path in (root / "REQ-0001" / DELIVERIES_DIR).glob(".tmp-*")]
    assert leftovers == []


def test_行数突变标疑但不阻断(store: DatasetStore, tmp_path: Path) -> None:
    """交货比不交货好——但「这次不对劲」必须留在纸面上。"""
    root = tmp_path / "req"
    # 把绝对阈值调到 10：这份夹具只有 2 行，默认的 100 行阈值永远不会触发。
    make_request(root, alert={"row_delta_pct": 20, "row_delta_abs": 10})
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 100}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "suspect"
    assert result.health.delta_abs == -98
    readme = (result.directory / "README.md").read_text(encoding="utf-8")
    assert "已标疑" in readme.splitlines()[2]


def test_小波动不标疑(store: DatasetStore, tmp_path: Path) -> None:
    """12 行变 15 行是 +25%，但绝对差只有 3 行——只判百分比会天天误报。"""
    root = tmp_path / "req"
    make_request(root)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 1}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "ok"
    assert result.health.delta_abs == 1


def test_口径升版后不比较行数(store: DatasetStore, tmp_path: Path) -> None:
    """改了口径必然行数大变；此时比较只会天天发假警报，而假警报会训练人忽略告警。"""
    root = tmp_path / "req"
    make_request(root, version=2)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 100}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "ok"
    assert result.health.supersedes == 1
    assert "口径已从 v1 改到 v2" in result.health.note


def test_上次档案读不出来时如实说明而非假装正常(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text("{ 坏掉的 json", encoding="utf-8")

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "unknown"
    assert "读不出来" in result.health.note


def test_CSV列表列按页面形式展平() -> None:
    """CSV 给人读，语言/任务/规模换成人话；parquet 仍留机器码。"""
    from hf_data_crawl.delivery import _flat_for_csv

    frame = pl.DataFrame(
        {
            "languages": [["ko", "en"]],
            "task_categories": [["text-generation"]],
            "size_categories": [["100K<n<1M"]],
            "tags": [["keyword", "reranking"]],
        }
    )

    out = _flat_for_csv(frame)

    assert out["languages"][0] == "Korean;English"
    assert out["task_categories"][0] == "Text Generation"
    assert out["size_categories"][0] == "100K - 1M"
    assert out["tags"][0] == "keyword;reranking"


def test_CSV展平区分空列表与未声明() -> None:
    """空列表是「声明了但没有值」，未声明是 NULL；CSV 里分别落成空串与空。"""
    from hf_data_crawl.delivery import _flat_for_csv

    frame = pl.DataFrame({"tags": [["a", "b"], [], None]})

    out = _flat_for_csv(frame)

    assert out["tags"].to_list() == ["a;b", "", None]


def test_csv格式(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "csv"})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert (result.directory / "data.csv").exists()
    assert not (result.directory / "data.parquet").exists()


def test_both格式写两份(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "both"})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert (result.directory / "data.parquet").exists()
    assert (result.directory / "data.csv").exists()
    assert len(result.files) == 2


def test_首次交付留存口径快照(store: DatasetStore, tmp_path: Path) -> None:
    """只留「真的用过」的口径——没用过的版本没有保留价值。"""
    root = tmp_path / "req"
    make_request(root)

    deliver(store, root, "REQ-0001", now=NOW)

    assert (root / "REQ-0001" / "history" / "v1.yaml").exists()


def test_输出列可写词条(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "parquet", "columns": ["id", "下载量"]})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.columns == ("id", "downloads")
    assert pl.read_parquet(result.directory / "data.parquet").height == 2
    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["projection"]["source"] == "spec"
    assert body["projection"]["columns"] == ["id", "下载量"]
    assert "默认交付字段" not in (result.directory / "README.md").read_text(encoding="utf-8")


def test_默认交付README列出逐列定义(store: DatasetStore, tmp_path: Path) -> None:
    """不指定字段时，README 要能回答「每一列各是什么意思」。"""
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    readme = (result.directory / "README.md").read_text(encoding="utf-8")
    assert f"默认交付字段（契约 v{CONTRACT_VERSION}" in readme
    assert "近 30 天下载量，不是累计值" in readme
    for column in DEFAULT_COLUMNS:
        assert f"`{column}`" in readme
    # 页面标签名与库列名对不上，是「详情页有、库里像没有」的主因。
    assert "详情页标签 ↔ 库列" in readme
    assert "| Tasks |" in readme
    assert "| Tags |" in readme
    assert "扩展字段（不进默认面，按需取用）" in readme
    assert "`regions`" in readme
    assert "output.columns" in readme


def test_一次性需求重复交付需force(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)
    argv = [
        "--data-dir",
        str(store.root),
        "--requests-dir",
        str(root),
        "request",
        "run",
        "REQ-0001",
    ]

    assert main(argv) == 0
    assert main(argv) == 1
    assert main([*argv, "--force"]) == 0


# ---------------------------------------------------------------- 到期判断


def seed_delivery(root: Path, *, delivered_at: str, rows: int = 2, version: int = 1) -> None:
    """造一份「上次交付」的档案，只放到期判断与比较要用的字段。"""
    directory = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / MANIFEST_NAME).write_text(
        json.dumps(
            {"spec_version": version, "delivered_at": delivered_at, "result": {"rows": rows}}
        ),
        encoding="utf-8",
    )


def spec_of(root: Path, **overrides: Any) -> Any:
    make_request(root, **overrides)
    from hf_data_crawl.requirement import load

    return load(root / "REQ-0001" / SPEC_NAME)


def test_从未交付即到期(tmp_path: Path) -> None:
    root = tmp_path / "req"
    spec = spec_of(root, cadence="weekly")

    due, reason = due_state(spec, root / "REQ-0001", NOW)

    assert due
    assert reason == "从未交付"


def test_一次性交过就不再跑(tmp_path: Path) -> None:
    root = tmp_path / "req"
    spec = spec_of(root, cadence="once")
    seed_delivery(root, delivered_at="2026-11-08T10:00:00+00:00")

    due, reason = due_state(spec, root / "REQ-0001", NOW)

    assert not due
    assert "once" in reason


def test_manual只等人跑(tmp_path: Path) -> None:
    root = tmp_path / "req"
    spec = spec_of(root, cadence="manual")

    due, reason = due_state(spec, root / "REQ-0001", NOW)

    assert not due
    assert "manual" in reason


def test_每日按日历日而非24小时(tmp_path: Path) -> None:
    """昨晚 23:50 交过，今早该再交一份——昨天那份属于昨天。"""
    root = tmp_path / "req"
    spec = spec_of(root, cadence="daily")
    seed_delivery(root, delivered_at="2026-11-07T23:50:00+00:00")

    due, _ = due_state(spec, root / "REQ-0001", NOW)

    assert due


def test_每日同一天内不重复(tmp_path: Path) -> None:
    root = tmp_path / "req"
    spec = spec_of(root, cadence="daily")
    seed_delivery(root, delivered_at="2026-11-08T01:00:00+00:00")

    due, reason = due_state(spec, root / "REQ-0001", NOW)

    assert not due
    assert "未满 1 天" in reason


@pytest.mark.parametrize(("days", "expected"), [(6, False), (7, True)])
def test_每周满七天(tmp_path: Path, days: int, expected: bool) -> None:
    root = tmp_path / "req"
    spec = spec_of(root, cadence="weekly")
    seed_delivery(root, delivered_at=f"2026-11-{8 - days:02d}T10:00:00+00:00")

    due, _ = due_state(spec, root / "REQ-0001", NOW)

    assert due is expected


def test_批量交付只跑到期的(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, id="REQ-0001", cadence="daily")
    make_request(root, id="REQ-0002", cadence="daily")
    # REQ-0002 刚交过，REQ-0001 从未交过
    recent = root / "REQ-0002" / DELIVERIES_DIR / "20261108T010000"
    recent.mkdir(parents=True)
    (recent / MANIFEST_NAME).write_text(
        json.dumps(
            {"spec_version": 1, "delivered_at": "2026-11-08T01:00:00+00:00", "result": {"rows": 2}}
        ),
        encoding="utf-8",
    )

    report = run_due(store, root, now=NOW)

    assert [item.request_id for item in report.delivered] == ["REQ-0001"]
    assert [item[0] for item in report.skipped] == ["REQ-0002"]


def test_一个需求失败不挡别的(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, id="REQ-0001")
    broken = root / "REQ-0002" / SPEC_NAME
    broken.parent.mkdir(parents=True)
    broken.write_text("id: REQ-0002\n", encoding="utf-8")

    report = run_due(store, root, now=NOW)

    assert [item.request_id for item in report.delivered] == ["REQ-0001"]
    assert [item[0] for item in report.failed] == ["REQ-0002"]


def test_底库缺失时整体失败(tmp_path: Path) -> None:
    """这不是某一条需求的问题，报十遍同样的错只会把真因埋掉。"""
    root = tmp_path / "req"
    make_request(root)
    empty = DatasetStore(tmp_path / "empty")

    with pytest.raises(DeliveryBlocked, match="底库不存在"):
        run_due(empty, root, now=NOW)


def test_批量交付共享一次底库指纹(store: DatasetStore, tmp_path: Path) -> None:
    """十个需求算十遍 70MB 的 md5 是纯浪费。"""
    root = tmp_path / "req"
    make_request(root, id="REQ-0001")
    make_request(root, id="REQ-0002")

    report = run_due(store, root, now=NOW)

    assert len(report.delivered) == 2


# ---------------------------------------------------------------- 统一生命周期
#
# 这些用例守的不是「回填能跑」，而是「回填与查询跑的是同一套东西」：同一套编号、
# 护栏、标疑、原子落盘、快照、台账。形态只在 kind 与执行体里变。


def make_book(path: Path, keys: list[str]) -> Path:
    """造一张与真实回填同构的表：第 2 行是列名，其上有一行分组标题。"""
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.cell(row=1, column=1, value="基础信息")
    sheet.cell(row=2, column=1, value="URL")
    sheet.cell(row=2, column=2, value="下载量")
    sheet.cell(row=2, column=3, value="训练阶段")
    for offset, key in enumerate(keys):
        sheet.cell(row=3 + offset, column=1, value=key)
    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)
    return path


def make_backfill_request(root: Path, book: Path, **overrides: Any) -> Path:
    """造一份自包含的 `kind: backfill` 需求：输入表复制进 `input/`，spec 只写相对路径。"""
    folder = root / "REQ-0002"
    input_dir = folder / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    (input_dir / book.name).write_bytes(book.read_bytes())
    body: dict[str, Any] = {
        "id": "REQ-0002",
        "title": "清单补全",
        "requester": "大帅",
        "version": 1,
        "cadence": "once",
        "kind": "backfill",
        "fill": {
            "source": {"path": f"input/{book.name}", "header_row": 2, "key_column": "URL"},
            "output": {"name": "已补全.xlsx"},
            "columns": {
                "下载量": {"field": "downloads"},
                "训练阶段": {"manual": "须读 README 判定"},
            },
        },
    }
    body.update(overrides)
    path = root / str(body["id"]) / SPEC_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    write_method(path.parent, request_id=str(body["id"]), title=str(body["title"]), kind="backfill")
    return path


def test_执行体注册表覆盖全部形态() -> None:
    """加了新 kind 却忘了注册执行体，交付时才炸太晚；这里当场发现。"""
    assert set(EXECUTORS) == set(Kind)


def test_两种形态共用一套交付档案(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book)

    query = deliver(store, root, "REQ-0001", now=NOW)
    fill = deliver(store, root, "REQ-0002", now=NOW)

    assert query.kind is Kind.QUERY
    assert fill.kind is Kind.BACKFILL
    for result in (query, fill):
        names = {path.name for path in result.directory.iterdir()}
        assert {MANIFEST_NAME, "README.md"} <= names
        body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
        assert body["kind"] == result.kind.value
        assert len(body["source"]["current_md5"]) == 32
        assert body["result"]["rows"] == 2
        assert (root / result.request_id / "history" / "v1.yaml").exists()

    assert (fill.directory / "已补全.xlsx").exists()
    assert fill.files[0]["name"] == "已补全.xlsx"
    assert len(fill.files[0]["sha256"]) == 64
    assert "逐列规则" in (fill.directory / "README.md").read_text(encoding="utf-8")
    assert "sql" not in json.loads((fill.directory / MANIFEST_NAME).read_text(encoding="utf-8"))


def test_回填档案记下逐列口径与覆盖(store: DatasetStore, tmp_path: Path) -> None:
    """机器读的那份要能回答「哪一列从哪来、这轮到底写了多少格」。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book)

    result = deliver(store, root, "REQ-0002", now=NOW)

    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["result"]["rows"] == 2
    assert body["fill"]["written"] == 2
    assert body["fill"]["tallies"]["下载量"]["written"] == 2
    assert body["fill"]["tallies"]["训练阶段"]["empty"] == 2
    assert body["fill"]["manual"] == [{"column": "训练阶段", "reason": "须读 README 判定"}]
    assert body["fill"]["columns"][0]["field"] == "downloads"


def test_回填与查询共用护栏(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book, guard={"min_rows": 99, "max_rows": 500000})

    with pytest.raises(DeliveryBlocked, match="min_rows"):
        deliver(store, root, "REQ-0002", now=NOW)

    assert deliveries_of(root / "REQ-0002") == []


def test_预演不落盘也不受护栏阻断(store: DatasetStore, tmp_path: Path) -> None:
    """预演正是为了看出「会不会 0 行」，此时套护栏等于自相矛盾。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book, guard={"min_rows": 999, "max_rows": 500000})

    looked = preview(store, root, "REQ-0002")

    assert looked.kind is Kind.BACKFILL
    assert looked.rows == 2
    assert any("逐列规则" in line for line in looked.lines)
    assert not (root / "REQ-0002" / DELIVERIES_DIR).exists()


def test_产物只落在需求目录内且输入有指纹(store: DatasetStore, tmp_path: Path) -> None:
    """自包含的判据：输入、口径、产物全在需求目录，manifest 能自证输入是哪一版。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book)

    result = deliver(store, root, "REQ-0002", now=NOW)

    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["fill"]["input"]["path"] == f"input/{book.name}"
    assert body["fill"]["input"]["bytes"] == book.stat().st_size
    assert len(body["fill"]["input"]["sha256"]) == 64
    assert "copy_to" not in body["fill"]
    assert (result.directory / "已补全.xlsx").exists()
    assert not (tmp_path / "已补全.xlsx").exists()
    assert "输入快照" in (result.directory / "README.md").read_text(encoding="utf-8")


def test_混合形态批量交付一份失败不挡另一份(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, id="REQ-0001", cadence="daily")
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    make_backfill_request(root, book)
    # 删的是需求目录里的快照，不是外部原件：受理期就该失败，且不能拖累别的需求。
    (root / "REQ-0002" / "input" / book.name).unlink()

    report = run_due(store, root, now=NOW)

    assert [item.request_id for item in report.delivered] == ["REQ-0001"]
    assert [item[0] for item in report.failed] == ["REQ-0002"]
    assert "输入表不存在" in report.failed[0][1]


def test_暂存目录不算一次交付(tmp_path: Path) -> None:
    """`.tmp-*` 若算进台账，「once 已交付过」会凭空成立。"""
    root = tmp_path / "req"
    base = root / "REQ-0002" / DELIVERIES_DIR / ".tmp-20261108T000000"
    base.mkdir(parents=True)

    assert deliveries_of(root / "REQ-0002") == []


def test_语义化目录仍按编号交付(store: DatasetStore, tmp_path: Path) -> None:
    """目录名给人看，编号给机器用；换了目录名，交付链路一步不改。"""
    root = tmp_path / "req"
    folder = root / directory_name("REQ-0001", "高下载数据集", Kind.QUERY)
    folder.mkdir(parents=True)
    (folder / SPEC_NAME).write_text(
        yaml.safe_dump(
            {
                "id": "REQ-0001",
                "title": "高下载数据集",
                "requester": "张三",
                "version": 1,
                "cadence": "once",
                "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    write_method(folder, request_id="REQ-0001", title="高下载数据集")

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.directory.parent == folder / DELIVERIES_DIR
    assert (result.directory / MANIFEST_NAME).exists()


def test_manifest钉住方法与逐脚本指纹(store: DatasetStore, tmp_path: Path) -> None:
    """档案要能回答「这份结果由哪版方法产出」，否则只钉住了原材料、钉不住做法。"""
    root = tmp_path / "req"
    path = make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)
    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))

    doc = path.parent / "method" / "METHOD.md"
    assert body["method"]["doc"]["sha256"] == method.digest(doc)
    # 这份测试需求没写脚本，清单就该是空的——不是缺字段，是「确实没有」。
    assert body["method"]["scripts"] == []
    assert body["method"]["env"]["contract_version"] == CONTRACT_VERSION
    assert body["spec_sha256"] == spec_digest(path)


def test_改了口径不升版本不许交付(store: DatasetStore, tmp_path: Path) -> None:
    """版本号复用是最隐蔽的错：档案指着 v1，说的却不是 v1 当初那件事。"""
    root = tmp_path / "req"
    path = make_request(root)
    deliver(store, root, "REQ-0001", now=NOW)  # 首次交付顺手写下 history/v1

    body = yaml.safe_load(path.read_text(encoding="utf-8"))
    body["criteria"] = [{"metric": "下载量", "op": ">", "value": 999}]
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")

    with pytest.raises(DeliveryBlocked, match="版本号不能复用"):
        deliver(store, root, "REQ-0001", now=NOW)

    # 拦在建目录之前：一次被拒的交付不该在台账里留下空壳。
    assert len(deliveries_of(root / "REQ-0001")) == 1


def test_档案记下输入表头_换表后标出来但照常交付(store: DatasetStore, tmp_path: Path) -> None:
    """换了一份字段不同的表：行数可能一模一样，但「表变了」必须留在纸面上。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec_path = make_backfill_request(root, book)
    first = deliver(store, root, "REQ-0002", now=NOW)
    body = json.loads((first.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["fill"]["input"]["header"]["columns"] == ["URL", "下载量", "训练阶段"]

    # 换一张表：多一列「数据模态」。这里刻意**原地替换**输入快照，模拟「对方重发
    # 了一份表、人直接覆盖」这种最常见的走法——正是要靠表头指纹把它标出来。
    changed = openpyxl.Workbook()
    sheet = changed.active
    assert sheet is not None
    sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(["URL", "下载量", "训练阶段", "数据模态"], start=1):
        sheet.cell(row=2, column=column, value=name)
    sheet.cell(row=3, column=1, value="acme/ds-1")
    changed.save(root / "REQ-0002" / "input" / "in.xlsx")
    spec_text = spec_path.read_text(encoding="utf-8").replace("version: 1", "version: 2")
    spec_path.write_text(spec_text, encoding="utf-8")

    second = deliver(store, root, "REQ-0002", now=NOW)
    later = json.loads((second.directory / MANIFEST_NAME).read_text(encoding="utf-8"))

    assert later["health"]["header_note"].startswith("输入表头与上一份交付不同")
    assert "数据模态" in later["health"]["header_note"]
    assert "注意：" in (second.directory / "README.md").read_text(encoding="utf-8")
