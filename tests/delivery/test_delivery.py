"""交付的行为测试。

守四条判据：三件齐全、同一底库两次交付逐字节相同、0 行与超限不落盘、
行数突变要标疑但**不阻断**（交货比不交好，可疑必须留在纸面上）。
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl
import pytest
import yaml
from support import write_method

from hf_data_crawl.delivery import method
from hf_data_crawl.delivery.commands import deliveries_of
from hf_data_crawl.delivery.compare import CompareError, compare_deliveries
from hf_data_crawl.delivery.contract import DeliveryUnchanged
from hf_data_crawl.delivery.delivery import (
    EXECUTORS,
    DeliveryBlocked,
    deliver,
    due_state,
    index_problem,
    preview,
    prune,
    pruned_facts,
    run_due,
    write_index,
)
from hf_data_crawl.delivery.fields import CONTRACT_VERSION, DEFAULT_COLUMNS
from hf_data_crawl.delivery.requirement import (
    DELIVERIES_DIR,
    HISTORY_DIR,
    MANIFEST_NAME,
    SPEC_NAME,
    Kind,
    directory_name,
    loads,
    spec_body,
    spec_body_digest,
    spec_digest,
)
from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.store import DatasetStore

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


def test_换名失败时把本次新建的版本快照收回(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """「先快照、后改名」的顺序不变，但**可处理的失败要回滚那一份快照**。

    顺序反过来的风险是：改名后崩溃会留下「产物已出土、history 没有这版」，于是这版
    静默隐身、版本号能被复用。所以快照仍先落。

    但落下的快照要有代价意识：它会让 `version_conflict` 认为 v1 已被占用，人改好
    口径后必须跳到 v2——明明什么都没交出去。崩溃（来不及收）与失败（收得回）是两件
    事：前者只能靠「先落」兜底，后者应当收回。判据是「这次运行有没有发布产物」。
    """
    root = tmp_path / "req"
    make_request(root)

    def boom(source: Path, target: Path) -> None:
        raise OSError("模拟换名失败")

    monkeypatch.setattr("hf_data_crawl.delivery.delivery.replace", boom)

    with pytest.raises(OSError):
        deliver(store, root, "REQ-0001", now=NOW)

    assert not (root / "REQ-0001" / "history" / "v1.yaml").exists(), (
        "没有任何档案声称用过 v1，就不该让 v1 变成「已占用」"
    )
    assert delivery_dirs(root) == []


def test_交付期间口径被改动不影响这次档案(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """交付只读一次 spec：校验、指纹、快照必须来自同一份字节。

    原先 `load` / `spec_digest` / `snapshot_history` 各读一次文件。有人在跑的过程中
    改了 spec，档案就会记下「校验的是旧版、快照与指纹的是新版」——两边都自洽，
    合起来却是假的。这里让「执行完之后」那一刻把 spec 改掉，模拟最坏的重叠。
    """
    root = tmp_path / "req"
    make_request(root)
    spec_path = root / "REQ-0001" / SPEC_NAME
    original = spec_path.read_bytes()

    def tamper(*_args: Any) -> None:
        body = yaml.safe_load(original.decode("utf-8"))
        body["criteria"] = [{"metric": "下载量", "op": ">", "value": 800}]
        spec_path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")

    monkeypatch.setattr("hf_data_crawl.delivery.delivery._worktree_gate", tamper)

    result = deliver(store, root, "REQ-0001", now=NOW)

    # 快照与档案都指向最初那份，而不是被改过的磁盘现状。
    history = (root / "REQ-0001" / HISTORY_DIR / "v1.yaml").read_bytes()
    assert history == original
    manifest = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["spec_sha256"] == spec_body_digest(
        spec_body(loads(original.decode("utf-8"), spec_path))
    )


def test_索引写不出来不影响交付落盘(
    store: DatasetStore,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """索引是台账的视图，不是交付的一部分。

    交付已换名成功、然后索引写失败，如果因此报「交付失败」，人只会再跑一次，
    再被重复闸门拦下——两个错叠在一起，现场就没人说得清了。
    """
    root = tmp_path / "req"
    make_request(root)

    def boom(_request_dir: Path) -> Path:
        raise OSError("模拟索引写失败")

    monkeypatch.setattr("hf_data_crawl.delivery.delivery.write_index", boom)

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert (result.directory / MANIFEST_NAME).is_file()
    assert "索引没写成" in capsys.readouterr().out


def test_批量交付里一条炸了不带走其余需求(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """整批因为一条未预期异常中断，其余需求就永远轮不到——坏的报出来，好的照常出。"""
    root = tmp_path / "req"
    make_request(root, cadence="daily")
    make_request(root, id="REQ-0002", title="另一份", cadence="daily")

    original = deliver

    def sometimes(
        store_arg: DatasetStore, requests_root: Path, request_id: str, **kwargs: Any
    ) -> Any:
        if request_id == "REQ-0001":
            raise ValueError("模拟未预期异常")
        return original(store_arg, requests_root, request_id, **kwargs)

    monkeypatch.setattr("hf_data_crawl.delivery.delivery.deliver", sometimes)

    report = run_due(store, root, now=NOW)

    assert [item.request_id for item in report.delivered] == ["REQ-0002"]
    assert report.failed and report.failed[0][0] == "REQ-0001"
    assert "ValueError" in report.failed[0][1]


def test_关闭的需求在批量里记跳过且不交付(store: DatasetStore, tmp_path: Path) -> None:
    """`run-due` 是没人盯着的路径：关闭的需求要明确跳过，不该每天报一次失败。"""
    root = tmp_path / "req"
    make_request(root, cadence="daily")
    (root / "REQ-0001" / "STATUS.json").write_text(
        '{"status": "closed", "reason": "对方只要 v1"}', encoding="utf-8"
    )

    report = run_due(store, root, now=NOW)

    assert [item[0] for item in report.skipped] == ["REQ-0001"]
    assert "已关闭" in report.skipped[0][1]
    assert report.failed == [] and report.delivered == []


def test_直接调deliver也不放行已关闭的需求(store: DatasetStore, tmp_path: Path) -> None:
    """`request run` 拦了一道，档案层再兜一层：别的调用方不该绕过这个状态。"""
    root = tmp_path / "req"
    make_request(root)
    (root / "REQ-0001" / "STATUS.json").write_text(
        '{"status": "closed", "reason": "换供应商"}', encoding="utf-8"
    )

    with pytest.raises(DeliveryBlocked, match="已关闭"):
        deliver(store, root, "REQ-0001", now=NOW)


def test_护栏拦下的交付不占版本号(store: DatasetStore, tmp_path: Path) -> None:
    """0 行被护栏拦下是最常见的失败：改完口径应当还能用同一个版本号重交。"""
    root = tmp_path / "req"
    make_request(root)

    spec_path = root / "REQ-0001" / "spec.yaml"
    body = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    body["criteria"] = [{"metric": "下载量", "op": ">", "value": 10**9}]
    spec_path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")

    with pytest.raises(DeliveryBlocked, match="低于 min_rows"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert not (root / "REQ-0001" / "history" / "v1.yaml").exists(), (
        "这次没有任何产物，不该让 v1 变成「已占用」"
    )
    # 改回口径、版本号不动，照样能交——失败不该逼人跳版本。
    body["criteria"] = [{"metric": "下载量", "op": ">", "value": 1000}]
    spec_path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    result = deliver(store, root, "REQ-0001", now=NOW)
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


def test_手写SQL的交付不谎称按默认契约投影(store: DatasetStore, tmp_path: Path) -> None:
    """`mode: sql` 的列集由 SELECT 决定，projection 说 31 列默认契约就是自相矛盾。"""
    root = tmp_path / "req"
    make_request(
        root,
        mode="sql",
        sql='SELECT "id", "downloads" FROM datasets ORDER BY "id"',
        criteria=[],
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    body = yaml.safe_load((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["projection"]["source"] == "sql"
    assert body["projection"]["columns"] == ["id", "downloads"]
    assert "default_columns_version" not in body["projection"]


def test_同一底库两次交付逐字节相同(store: DatasetStore, tmp_path: Path) -> None:
    """可复现的判据。靠的是编译时那条 "id" ASC 兜底。

    第二次交付会被重复闸门拦下（同底库、同字节），所以这里先把上一条从台账里摘掉
    ——测的是「同样的输入给同样的字节」，不是「能不能重复记账」。
    """
    root = tmp_path / "req"
    make_request(root)

    first = deliver(store, root, "REQ-0001", now=NOW)
    path = first.directory
    digest = first.files[0]["sha256"]
    shutil.rmtree(path)
    second = deliver(store, root, "REQ-0001", now=NOW.replace(second=30))

    assert first.files[0]["sha256"] == second.files[0]["sha256"] == digest


def test_同底库同字节的重复交付被拦下(store: DatasetStore, tmp_path: Path) -> None:
    """没有新信息就不记新条目：这是交付目录堆条目的主要来源（2026-11-17 复盘）。"""
    root = tmp_path / "req"
    make_request(root)
    deliver(store, root, "REQ-0001", now=NOW)

    with pytest.raises(DeliveryUnchanged, match="没有新信息"):
        deliver(store, root, "REQ-0001", now=NOW.replace(second=30))

    assert len(delivery_dirs(root)) == 1


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


def test_失败时不留半成品目录(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """中途失败只能留下 .tmp-*，绝不能出现「有数据没说明」。

    以前靠「指名不存在的列」制造这个现场：SQL 跑起来才报错。契约 v6 起这类列在
    **受理期**就被 `_supported_columns` 拦下（SpecError），所以要显式让执行阶段
    炸一次，才测得到清理逻辑。
    """
    root = tmp_path / "req"
    make_request(root, output={"format": "csv", "columns": ["id", "url"]})

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("执行中途炸了")

    monkeypatch.setattr("hf_data_crawl.delivery.delivery._execute", boom)

    with pytest.raises(RuntimeError):
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
    """1 行变 2 行是 +100%，但绝对差只有 1 行——只判百分比会天天误报。"""
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
    from hf_data_crawl.delivery.query_exec import _flat_for_csv

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
    from hf_data_crawl.delivery.query_exec import _flat_for_csv

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


def test_交付README点出整列为空的列(store: DatasetStore, tmp_path: Path) -> None:
    """交付方拿到一份整列为空的 `server_*` 会以为数据坏了。

    `size` / `carddata` 没采集时，这几个来源的列必然全空——这是「计划内还没抓」，
    不是「这个数据集没有这一属性」。两者在表格里都是空，只能靠这段文字区分。
    """
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    readme = (result.directory / "README.md").read_text(encoding="utf-8")
    assert "本次输出的列覆盖" in readme
    # 夹具没有 size / carddata 表，这两列的来源必然全空。
    empty_line = next(line for line in readme.splitlines() if "整列为空" in line)
    assert "`server_num_rows`" in empty_line
    assert "`pretty_name`" in empty_line
    # 扩展字段的覆盖率按当期底库现算，不是抄在源码里的固定档位。
    assert "| 字段 | 定义 | 全库覆盖 |" in readme


def test_一次性需求重复交付需force(store: DatasetStore, tmp_path: Path) -> None:
    """`--force` 只解除「once 已交付」的名额，解不开重复闸门：同一份东西不记第二条。

    真要让对方再拿一份，直接取旧目录里的文件——重交出来也只是多一条一模一样的账。
    """
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
    # --force 放行「once 已交付」这一关，但产物没变，重复闸门仍然拒绝。
    assert main([*argv, "--force"]) == 1
    assert len(delivery_dirs(root)) == 1


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
    from hf_data_crawl.delivery.requirement import load

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


def test_批量交付共享一次底库指纹(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """十个需求算十遍 70MB 的 md5 是纯浪费。"""
    from hf_data_crawl.delivery import delivery as delivery_module

    calls: list[int] = []
    original = delivery_module._stamp

    def counted(current: DatasetStore):
        calls.append(1)
        return original(current)

    monkeypatch.setattr(delivery_module, "_stamp", counted)
    root = tmp_path / "req"
    make_request(root, id="REQ-0001")
    make_request(root, id="REQ-0002")

    report = run_due(store, root, now=NOW)

    assert len(report.delivered) == 2
    assert len(calls) == 1, "底库指纹必须整批算一次，不能每个需求各算一遍"


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


def test_回填档案记下追加列而输入表头只描述对方那张表(store: DatasetStore, tmp_path: Path) -> None:
    """对方只给两列、需求要「把能拿到的字段都拼进来」时，档案必须分开答两件事：
    对方那张表长什么样（`input.header`）、我们往右加了什么（`appended`）。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(
        root,
        book,
        fill={
            "source": {"path": f"input/{book.name}", "header_row": 2, "key_column": "URL"},
            "output": {"name": "已补全.xlsx"},
            "columns": {
                "下载量": {"field": "downloads"},
                "点赞量": {"field": "likes", "append": True},
            },
        },
    )

    result = deliver(store, root, "REQ-0002", now=NOW)

    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["fill"]["appended"] == ["点赞量"]
    assert body["fill"]["input"]["header"]["columns"] == ["URL", "下载量", "训练阶段"]
    assert "追加列 1 列" in (result.directory / "README.md").read_text(encoding="utf-8")


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

    looked = preview(store, root, "REQ-0002", keep_files=False)

    assert looked.kind is Kind.BACKFILL
    assert looked.rows == 2
    assert any("逐列规则" in line for line in looked.lines)
    assert not (root / "REQ-0002" / DELIVERIES_DIR).exists()
    assert not (root / "REQ-0002" / "scratch").exists()
    assert looked.path is None


def test_预览把产物落进草稿区且不进台账(store: DatasetStore, tmp_path: Path) -> None:
    """「想看一眼真表」不该以多一条交付为代价——这是本次治理要拆的那个结。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/ds-2"])
    make_backfill_request(root, book)

    looked = preview(store, root, "REQ-0002")

    scratch = root / "REQ-0002" / "scratch" / "preview"
    assert looked.path == scratch
    assert (scratch / "已补全.xlsx").exists()
    assert (scratch / "PREVIEW.md").exists()
    note = (scratch / "PREVIEW.md").read_text(encoding="utf-8")
    assert "不是交付" in note
    assert deliveries_of(root / "REQ-0002") == [], "预览不能变成一条交付"


def test_预览会写出正式交付会被拦下的原因(store: DatasetStore, tmp_path: Path) -> None:
    """跑通了不等于能交；预览必须把闸门的结论说出来，而不是留到正式那一刻。"""
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    make_backfill_request(root, book, guard={"min_rows": 99, "max_rows": 500000})

    looked = preview(store, root, "REQ-0002")

    assert looked.notes and "min_rows" in looked.notes[0]
    assert "正式交付会被拦下的原因" in (
        root / "REQ-0002" / "scratch" / "preview" / "PREVIEW.md"
    ).read_text(encoding="utf-8")


def test_预览会说明草稿来自脏代码(
    store: DatasetStore, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """草稿长得像交付，但正式交时会被脏闸门拦下——不写这一句，人会拿它去对数字。"""
    root = tmp_path / "req"
    make_request(root)
    monkeypatch.setattr(
        "hf_data_crawl.delivery.method.worktree_state",
        lambda: {"checked": True, "clean": False, "paths": ["src/x.py"], "note": ""},
    )

    result = preview(store, root, "REQ-0001")

    assert any("未提交改动" in note for note in result.notes)
    note = (root / "REQ-0001" / "scratch" / "preview" / "PREVIEW.md").read_text(encoding="utf-8")
    assert "未提交改动" in note


def test_交付成功后草稿区被清掉(store: DatasetStore, tmp_path: Path) -> None:
    """客已交完，草稿留着只会让人以为「这里还有一份可能要交的东西」。"""
    root = tmp_path / "req"
    make_request(root)
    preview(store, root, "REQ-0001")

    deliver(store, root, "REQ-0001", now=NOW)

    assert not (root / "REQ-0001" / "scratch" / "preview").exists()


def test_批量交付把重复的记为跳过而不是失败(store: DatasetStore, tmp_path: Path) -> None:
    """「今天没事发生」和「今天出事了」混成一种，日志里的红点就失去意义。"""
    root = tmp_path / "req"
    make_request(root, cadence="daily")
    deliver(store, root, "REQ-0001", now=NOW - timedelta(days=1))

    report = run_due(store, root, now=NOW)

    assert report.delivered == []
    assert report.failed == []
    assert [item[0] for item in report.skipped] == ["REQ-0001"]
    assert "没有新信息" in report.skipped[0][1]
    assert len(delivery_dirs(root)) == 1


def test_脏代码不许交付(store: DatasetStore, tmp_path: Path, monkeypatch: Any) -> None:
    """manifest 记 HEAD，产物就必须真出自那个提交；否则档案从写下的那一刻就是假的。"""
    root = tmp_path / "req"
    make_request(root)
    monkeypatch.setattr(
        method,
        "worktree_state",
        lambda: {"checked": True, "clean": False, "paths": ["src/x.py"], "note": ""},
    )

    with pytest.raises(DeliveryBlocked, match="未提交"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert deliveries_of(root / "REQ-0001") == []


def test_档案记下代码是否干净(store: DatasetStore, tmp_path: Path) -> None:
    """「查没查」与「干不干净」都要留痕：半年后没人能从 commit 号倒推出这件事。"""
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["worktree"] == {"checked": True, "clean": True, "paths": [], "note": ""}
    # 同一份档案还要能跨机器回答「跑的是哪份代码」：commit 是本机事实，内容指纹才是。
    assert len(str(body["method"]["env"]["code_sha256"])) == 64


# ---------------------------------------------------------------- 交付索引


def seed_manifest(root: Path, name: str, **fields: Any) -> Path:
    """造一条只放到索引要用的字段的交付目录。"""
    directory = root / "REQ-0001" / DELIVERIES_DIR / name
    directory.mkdir(parents=True, exist_ok=True)
    body = {
        "spec_version": 1,
        "source": {"current_md5": "aaa"},
        "result": {"rows": 2, "columns": ["id"], "files": [{"name": "d.parquet", "sha256": "s1"}]},
        "method": {"env": {"commit": "c1"}},
    }
    body.update(fields)
    (directory / MANIFEST_NAME).write_text(json.dumps(body), encoding="utf-8")
    return directory


def test_清理只删旧产物且当前那条永不动(tmp_path: Path) -> None:
    """产物是可重算的副本；台账（manifest / README）一份不动，清掉的 sha256 记进清单。"""
    root = tmp_path / "req"
    older = seed_manifest(root, "20261101T000000")
    newer = seed_manifest(
        root,
        "20261102T000000",
        source={"current_md5": "bbb"},
        result={"rows": 2, "columns": ["id"], "files": [{"name": "d.parquet", "sha256": "s2"}]},
    )
    for directory in (older, newer):
        (directory / "d.parquet").write_bytes(b"payload")
        (directory / "README.md").write_text("说明", encoding="utf-8")

    report = prune(root / "REQ-0001", keep=1, moment=NOW)

    assert [item.name for item in report.pruned] == ["20261101T000000"]
    assert report.freed == 7
    assert not (older / "d.parquet").exists(), "旧产物应被清掉"
    assert (older / MANIFEST_NAME).exists() and (older / "README.md").exists(), "档案一份不动"
    assert (newer / "d.parquet").exists(), "当前有效那条的产物永远留着"
    facts = pruned_facts(root / "REQ-0001")
    assert facts["20261101T000000"]["files"][0]["sha256"] == "s1"


def test_清理干跑不动磁盘(tmp_path: Path) -> None:
    root = tmp_path / "req"
    older = seed_manifest(root, "20261101T000000")
    seed_manifest(root, "20261102T000000", source={"current_md5": "bbb"})
    (older / "d.parquet").write_bytes(b"payload")

    report = prune(root / "REQ-0001", keep=1, moment=NOW, dry_run=True)

    assert [item.name for item in report.pruned] == ["20261101T000000"]
    assert (older / "d.parquet").exists(), "--dry-run 只报不动"
    assert pruned_facts(root / "REQ-0001") == {}


def test_清理后索引与比对都记得住账(tmp_path: Path) -> None:
    """清理不是「文件丢了」：索引标已清理，比对给出去哪看 sha256 的话。"""
    root = tmp_path / "req"
    older = seed_manifest(root, "20261101T000000")
    seed_manifest(root, "20261102T000000", source={"current_md5": "bbb"})
    (older / "d.parquet").write_bytes(b"payload")

    prune(root / "REQ-0001", keep=1, moment=NOW)
    write_index(root / "REQ-0001")

    text = (root / "REQ-0001" / DELIVERIES_DIR / "INDEX.md").read_text(encoding="utf-8")
    assert "| 已清理 |" in text
    with pytest.raises(CompareError, match="留存策略"):
        compare_deliveries(older, root / "REQ-0001" / DELIVERIES_DIR / "20261102T000000")


def test_样例需求在批量里记跳过(store: DatasetStore, tmp_path: Path) -> None:
    """夹具可以手工 `run`，但不该由定时链每天生产交付。"""
    root = tmp_path / "req"
    make_request(root, cadence="daily")
    (root / "REQ-0001" / "STATUS.json").write_text(
        '{"sample": true, "sample_reason": "开发夹具"}', encoding="utf-8"
    )

    report = run_due(store, root, now=NOW)

    assert [item[0] for item in report.skipped] == ["REQ-0001"]
    assert "样例需求" in report.skipped[0][1]
    assert report.delivered == [] and report.failed == []


def test_索引标出样例需求(tmp_path: Path) -> None:
    """样例常常交得比真实需求还整齐；不标出来，它会被当成业务事实引用。"""
    root = tmp_path / "req"
    seed_manifest(root, "20261101T000000")
    (root / "REQ-0001" / "STATUS.json").write_text(
        '{"sample": true, "sample_reason": "开发夹具"}', encoding="utf-8"
    )

    write_index(root / "REQ-0001")

    text = (root / "REQ-0001" / DELIVERIES_DIR / "INDEX.md").read_text(encoding="utf-8")
    assert "**样例需求**" in text and "开发夹具" in text


def test_索引把重钉与内容变化分开(tmp_path: Path) -> None:
    """台账里最容易混淆的一件事：这条是「又交了一份一样的」还是「真的变过」。"""
    root = tmp_path / "req"
    seed_manifest(root, "20261101T000000")
    seed_manifest(root, "20261102T000000")  # 同底库同字节 → 重钉
    seed_manifest(
        root,
        "20261103T000000",
        spec_version=2,
        source={"current_md5": "bbb"},
        result={
            "rows": 2,
            "columns": ["id", "作者"],
            "files": [{"name": "d.parquet", "sha256": "s2"}],
        },
    )

    write_index(root / "REQ-0001")

    text = (root / "REQ-0001" / DELIVERIES_DIR / "INDEX.md").read_text(encoding="utf-8")
    assert "| 首版 |" in text
    assert "| 重钉 |" in text
    assert "| 结构扩展 |" in text
    assert "当前有效：`20261103T000000`" in text
    assert "<!-- deliveries: 20261103T000000 · entries: 3 -->" in text


def test_索引的可复现列认档案与AUDIT(tmp_path: Path) -> None:
    """新交付由闸门保证干净；历史交付的结论只能来自 AUDIT.md，不能靠猜。"""
    root = tmp_path / "req"
    seed_manifest(root, "20261101T000000", worktree={"checked": True, "clean": False, "paths": []})
    seed_manifest(
        root,
        "20261102T000000",
        source={"current_md5": "bbb"},
        result={"rows": 2, "columns": ["id"], "files": [{"name": "d.parquet", "sha256": "s2"}]},
    )
    (root / "REQ-0001" / DELIVERIES_DIR / "AUDIT.md").write_text(
        '```json\n{"20261101T000000": {"reproducible": false, "note": "代码未提交"}}\n```\n',
        encoding="utf-8",
    )

    write_index(root / "REQ-0001")

    text = (root / "REQ-0001" / DELIVERIES_DIR / "INDEX.md").read_text(encoding="utf-8")
    assert "✗（见 AUDIT）" in text
    # 没有 worktree 段、也没进 AUDIT 的历史条目：只能写「未核」，不替它下结论。
    assert "| ? |" in text


def test_索引过期能被指出来(tmp_path: Path) -> None:
    """过期 INDEX 比没有更坏：读者会拿一份长得完全正常的旧清单当现状。"""
    root = tmp_path / "req"
    request_dir = root / "REQ-0001"
    assert index_problem(request_dir) is None, "还没有交付时不报"

    seed_manifest(root, "20261101T000000")
    assert "没有 INDEX.md" in (index_problem(request_dir) or "")

    write_index(request_dir)
    assert index_problem(request_dir) is None

    # 台账多了一条、索引没跟上：判据是机读标记，不看 mtime。
    seed_manifest(
        root,
        "20261102T000000",
        source={"current_md5": "bbb"},
        result={"rows": 2, "columns": ["id"], "files": [{"name": "d.parquet", "sha256": "s2"}]},
    )
    assert "台账实际是 2 条" in (index_problem(request_dir) or "")


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


def test_判读表放在子目录时档案记相对路径并钉住(store: DatasetStore, tmp_path: Path) -> None:
    """`overrides` 原先只记 basename，`stamp` 又只认根目录的 `overrides.json`。

    判读表挪进子目录后，档案既说不清它在哪、也没有它的指纹——「脚本 → 判读表
    → 交付」那条链断在中间，且不报任何错。
    """
    root = tmp_path / "req"
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    judge = root / "REQ-0002" / "judge"
    judge.mkdir(parents=True)
    (judge / "ov.json").write_text('{"acme/ds-1": {"训练阶段": "后训练"}}', encoding="utf-8")
    make_backfill_request(
        root,
        book,
        fill={
            "source": {"path": f"input/{book.name}", "header_row": 2, "key_column": "URL"},
            "output": {"name": "已补全.xlsx"},
            "overrides": "judge/ov.json",
            "columns": {
                "下载量": {"field": "downloads"},
                "训练阶段": {"manual": "须读 README 判定"},
            },
        },
    )

    result = deliver(store, root, "REQ-0002", now=NOW)

    body = json.loads((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert body["fill"]["overrides"] == "judge/ov.json"
    assert body["method"]["inputs"]["judge/ov.json"]["sha256"] == method.digest(judge / "ov.json")


def test_判读表换了子目录就算换了口径(tmp_path: Path) -> None:
    """`spec_digest` 原先只吃 basename：`judge/ov.json` 改成 `ov.json` 指纹不变。

    于是版本闸门会放行「输入换了却复用 v1」——那正是它存在要拦的东西。
    """
    book = make_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    base = {
        "source": {"path": f"input/{book.name}", "header_row": 2, "key_column": "URL"},
        "output": {"name": "已补全.xlsx"},
        "columns": {"下载量": {"field": "downloads"}},
    }
    # 两份需求各放一个根目录：判读表存在与否在 `load` 时才校验，先建 spec 再补文件。
    root_level = make_backfill_request(
        tmp_path / "flat", book, fill={**base, "overrides": "ov.json"}
    )
    in_subdir = make_backfill_request(
        tmp_path / "nested", book, fill={**base, "overrides": "judge/ov.json"}
    )
    (root_level.parent / "ov.json").write_text("{}", encoding="utf-8")
    (in_subdir.parent / "judge").mkdir()
    (in_subdir.parent / "judge" / "ov.json").write_text("{}", encoding="utf-8")

    assert spec_digest(root_level) != spec_digest(in_subdir)


# ---------------------------------------------------------------- 档案的读法只有一处


def test_台账把缺档案与坏档案分开说(tmp_path: Path) -> None:
    """「缺 manifest」与「读不出来」引向两种不同的处置，不能折成一句话。"""
    from hf_data_crawl.delivery.commands import manifest_summary

    missing = tmp_path / "20261101-120000"
    missing.mkdir()
    assert "缺 manifest" in manifest_summary(missing)

    broken = tmp_path / "20261102-120000"
    broken.mkdir()
    (broken / MANIFEST_NAME).write_text("{不是 json", encoding="utf-8")
    assert "读不出来" in manifest_summary(broken)

    good = tmp_path / "20261103-120000"
    good.mkdir()
    (good / MANIFEST_NAME).write_text(
        '{"result": {"rows": 42}, "health": {"status": "first"}}', encoding="utf-8"
    )
    assert "42 行" in manifest_summary(good)
