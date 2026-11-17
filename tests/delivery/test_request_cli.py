"""request 子命令的行为测试。

验的是三件事：**建出来的东西一定合法**（建完立即按真规矩回读一遍）、
**一个坏 spec 不挡住其余需求**、**坏输入不留半成品**。
"""

from __future__ import annotations

import json
from pathlib import Path

import openpyxl
import pytest
import yaml

from hf_data_crawl.delivery.requirement import SPEC_NAME, Kind, load, resolve_dir
from hf_data_crawl.ops.cli import main


def run(tmp_path: Path, *argv: str) -> int:
    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def make(tmp_path: Path, request_id: str = "REQ-0001") -> Path:
    code = run(
        tmp_path,
        "request",
        "new",
        request_id,
        "--title",
        "高热度数据集清单",
        "--requester",
        "张三",
        "--purpose",
        "选型参考",
    )
    assert code == 0
    # 目录名带语义后缀，按编号解析才找得到。
    return resolve_dir(tmp_path / "req", request_id) / SPEC_NAME


def test_新建后文件落盘且可读(tmp_path: Path) -> None:
    path = make(tmp_path)

    assert path.exists()
    spec = load(path)
    assert spec.id == "REQ-0001"
    assert spec.requester == "张三"


def test_重复需求号被拒且不覆盖原文件(tmp_path: Path) -> None:
    path = make(tmp_path)
    original = path.read_bytes()

    code = run(tmp_path, "request", "new", "REQ-0001", "--title", "改个名", "--requester", "李四")

    assert code == 1
    assert path.read_bytes() == original


def test_需求号不合法时不留半成品(tmp_path: Path) -> None:
    """建错就删掉：留一个读不出来的 spec，比没有更麻烦。"""
    code = run(tmp_path, "request", "new", "REQ-1", "--title", "x", "--requester", "y")

    assert code == 1
    root = tmp_path / "req"
    assert not (root / "REQ-1").exists()
    if root.is_dir():
        assert list(root.glob("REQ-1*")) == []


def test_空目录给出下一步提示(tmp_path: Path) -> None:
    assert run(tmp_path, "request", "list") == 0
    assert not (tmp_path / "req").exists()


def test_列出已建需求(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "REQ-0001" in printed
    assert "高热度数据集清单" in printed
    assert "从未交付" in printed


def test_坏spec不挡住其余需求(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    broken = tmp_path / "req" / "REQ-0002" / SPEC_NAME
    broken.parent.mkdir(parents=True)
    broken.write_text(yaml.safe_dump({"id": "REQ-0002"}), encoding="utf-8")
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "[!] REQ-0002" in printed
    assert "REQ-0001" in printed


def test_show打印口径与编译后的SQL(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    printed = capsys.readouterr().out
    assert "落成列 downloads" in printed
    assert '"downloads" > 0' in printed
    assert "ORDER BY" in printed
    assert "尚未快照" in printed


def test_show不存在的需求返回非零(tmp_path: Path) -> None:
    assert run(tmp_path, "request", "show", "REQ-9999") == 1


# ---------------------------------------------------------------- 单一入口


def make_book(path: Path) -> Path:
    openpyxl.Workbook().save(path)
    return path


def test_fill命令已退役(tmp_path: Path) -> None:
    """回填不是另一套命令，它只是 `request` 的一种 kind。"""
    with pytest.raises(SystemExit) as caught:
        run(tmp_path, "fill")

    assert caught.value.code == 2


def test_新建回填需求走统一入口(tmp_path: Path) -> None:
    book = make_book(tmp_path / "in.xlsx")
    code = run(
        tmp_path,
        "request",
        "new",
        "REQ-0002",
        "--title",
        "清单补全",
        "--requester",
        "大帅",
        "--kind",
        "backfill",
        "--source",
        str(book),
    )

    assert code == 0
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    spec = load(folder / SPEC_NAME)
    assert spec.kind is Kind.BACKFILL
    assert spec.fill is not None
    assert spec.fill.source.relative == f"input/{book.name}"
    snapshot = folder / "input" / book.name
    assert snapshot.read_bytes() == book.read_bytes()
    assert spec.fill.source.path == snapshot.resolve()
    assert "backfill" in folder.name and "清单补全" in folder.name


def test_回填需求缺输入表被拒且不留半成品(tmp_path: Path) -> None:
    code = run(
        tmp_path,
        "request",
        "new",
        "REQ-0002",
        "--title",
        "清单补全",
        "--requester",
        "大帅",
        "--kind",
        "backfill",
    )

    assert code == 1
    root = tmp_path / "req"
    assert not (root / "REQ-0002").exists()
    if root.is_dir():
        assert list(root.glob("REQ-0002*")) == []


def test_列表同时显示两种形态(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    book = make_book(tmp_path / "in.xlsx")
    assert (
        run(
            tmp_path,
            "request",
            "new",
            "REQ-0002",
            "--title",
            "清单补全",
            "--requester",
            "大帅",
            "--kind",
            "backfill",
            "--source",
            str(book),
        )
        == 0
    )
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "query" in printed
    assert "backfill" in printed
    assert "REQ-0001" in printed
    assert "REQ-0002" in printed


def test_取材后外部文件改动不影响本需求(tmp_path: Path) -> None:
    """取完即断：外部原件被删被改，需求目录仍是能独立复现的封闭单元。"""
    book = make_book(tmp_path / "in.xlsx")
    assert (
        run(
            tmp_path,
            "request",
            "new",
            "REQ-0002",
            "--title",
            "清单补全",
            "--requester",
            "大帅",
            "--kind",
            "backfill",
            "--source",
            str(book),
        )
        == 0
    )
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    snapshot = folder / "input" / book.name
    before = snapshot.read_bytes()

    book.write_bytes(b"changed by someone else")

    assert snapshot.read_bytes() == before
    assert load(folder / SPEC_NAME).fill is not None


def test_新建即带过程记录骨架且标明未填(tmp_path: Path) -> None:
    """新建就把 method/ 的坑位占好：想不起来记「怎么做的」，是这类记录最常见的死法。"""
    path = make(tmp_path)

    doc = path.parent / "method" / "METHOD.md"
    index = path.parent / "method" / "method.json"
    assert doc.is_file() and index.is_file()
    assert "<!-- 待填：" in doc.read_text(encoding="utf-8")


def test_method命令只判不写且以退出码表达(tmp_path: Path) -> None:
    path = make(tmp_path)
    assert run(tmp_path, "request", "method", "REQ-0001", "--check") == 1

    doc = path.parent / "method" / "METHOD.md"
    doc.write_text(
        doc.read_text(encoding="utf-8").replace("<!-- 待填：", "<!-- 已填："), encoding="utf-8"
    )

    assert run(tmp_path, "request", "method", "REQ-0001", "--check") == 0


def test_缺需求成立性一节只提示不拦(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """判断的价值在于有人真读到不同的东西；做成必填，最先发生的总是抄一句过闸。"""
    path = make(tmp_path)
    doc = path.parent / "method" / "METHOD.md"
    doc.write_text(
        doc.read_text(encoding="utf-8")
        .replace("<!-- 待填：", "<!-- 已填：")
        .replace("## 需求成立性", "## 需求成立性（已删）"),
        encoding="utf-8",
    )

    assert run(tmp_path, "request", "method", "REQ-0001", "--check") == 0
    assert "需求成立性" in capsys.readouterr().out


def test_交付失败也留运行台账(tmp_path: Path) -> None:
    """需求侧的失败原先只活在一次终端输出里，体检的「近 7 天失败」完全看不见。

    这里用空底库触发一次必然失败：记录要落进 `runs/`，且 reason 说得出为什么。
    """
    make(tmp_path)

    assert run(tmp_path, "request", "run", "REQ-0001") == 1

    runs = list((tmp_path / "data" / "runs").glob("*-request-run.json"))
    assert len(runs) == 1, "交付失败必须留痕，否则「这轮到底跑没跑过」没人答得上来"
    body = json.loads(runs[0].read_text(encoding="utf-8"))
    assert body["status"] == "failed"
    assert body["reason"]


def test_预演与预览不写运行台账(tmp_path: Path) -> None:
    """`--dry-run` / `--preview` 是草稿：写进台账只会让「最近的失败运行」混进一堆草稿。"""
    make(tmp_path)

    assert run(tmp_path, "request", "run", "REQ-0001", "--dry-run") == 1

    assert not list((tmp_path / "data" / "runs").glob("*-request-run.json"))


def test_预演与预览同时给出被拒(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """两个开关说的是不同的「只看不写」；同时给出时谁先谁后都是猜。"""
    make(tmp_path)

    assert run(tmp_path, "request", "run", "REQ-0001", "--dry-run", "--preview") == 1

    assert "选一个" in capsys.readouterr().out


def test_show区分交过的版本与只有快照的版本(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """失败尝试也会留下快照（崩溃窗口靠它兜底），但那一版从没交付过。

    两者混着列，读的人会以为每一版都真的交过——「可复现」的前提就不成立了。
    """
    path = make(tmp_path)
    (path.parent / "history").mkdir()
    (path.parent / "history" / "v1.yaml").write_bytes(path.read_bytes())

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "历史版本：v1" in out
    assert "没有对应交付" in out


def test_usage按词条反查需求(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """改一个词条的定义前，先看清谁会受影响；口径散在 spec 里，grep 认不出语义。"""
    make(tmp_path)

    assert run(tmp_path, "request", "usage", "下载量") == 0
    out = capsys.readouterr().out
    assert "REQ-0001" in out
    assert "条件" in out, "要指出具体引用点，不只报个需求号"

    assert run(tmp_path, "request", "usage", "查无此词") == 1


def test_prune命令清旧产物但留当前与档案(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """产物是可重算的副本；清掉旧的那些，manifest / README 与当前那条一份不动。"""
    path = make(tmp_path)
    deliveries = path.parent / "deliveries"
    for name in ("20261101T000000", "20261102T000000"):
        directory = deliveries / name
        directory.mkdir(parents=True)
        (directory / "d.parquet").write_bytes(b"payload")
        (directory / "manifest.json").write_text(
            json.dumps(
                {
                    "spec_version": 1,
                    "result": {
                        "rows": 1,
                        "columns": ["id"],
                        "files": [{"name": "d.parquet", "sha256": name}],
                    },
                }
            ),
            encoding="utf-8",
        )

    assert run(tmp_path, "request", "prune", "REQ-0001") == 0
    assert not (deliveries / "20261101T000000" / "d.parquet").exists()
    assert (deliveries / "20261102T000000" / "d.parquet").exists(), "当前有效那条永远留着"
    assert (deliveries / "PRUNED.json").is_file()
    assert (deliveries / "20261101T000000" / "manifest.json").is_file(), "档案一份不动"
    assert "已清理" in (deliveries / "INDEX.md").read_text(encoding="utf-8")

    # `--keep` 至少 1：当前有效那条不能清。
    assert run(tmp_path, "request", "prune", "REQ-0001", "--keep", "0") == 1
    assert "至少是 1" in capsys.readouterr().out


def test_关闭的需求不交付也不报错(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """关闭是运营决定，不是故障：run 明确拒绝并指路 reopen。"""
    make(tmp_path)
    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "对方只要 v1") == 0

    assert run(tmp_path, "request", "run", "REQ-0001") == 1
    out = capsys.readouterr().out
    assert "已关闭" in out and "--dry-run" not in out


def test_关闭必须写理由否则不落盘(tmp_path: Path) -> None:
    """半年后只看到「已关闭」，没人知道为什么——那这句状态就只是噪音。"""
    path = make(tmp_path)

    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "  ") == 1
    assert not (path.parent / "STATUS.json").exists()


def test_恢复后又能交付(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = make(tmp_path)
    run(tmp_path, "request", "close", "REQ-0001", "--reason", "换供应商")

    assert run(tmp_path, "request", "reopen", "REQ-0001") == 0
    assert not (path.parent / "STATUS.json").exists()
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0
    assert "已关闭" not in capsys.readouterr().out


def test_index命令重写交付索引(tmp_path: Path) -> None:
    """补建历史索引不该靠再交付一份——那正是这次治理要断掉的循环。"""
    path = make(tmp_path)
    assert run(tmp_path, "request", "index", "REQ-0001") == 1, "没有交付时无事可写"

    delivery = path.parent / "deliveries" / "20261101T000000"
    delivery.mkdir(parents=True)
    (delivery / "manifest.json").write_text(
        '{"spec_version": 1, "source": {"current_md5": "aaa"},'
        ' "result": {"rows": 2, "columns": ["id"], "files": [{"name": "d.parquet", "sha256": "s"}]}}',
        encoding="utf-8",
    )

    assert run(tmp_path, "request", "index", "REQ-0001") == 0
    index = path.parent / "deliveries" / "INDEX.md"
    assert "当前有效：`20261101T000000`" in index.read_text(encoding="utf-8")


def test_show打印过程记录状态(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "过程记录：" in out
    assert "不合规" in out
    assert "口径指纹：" in out


def make_header_book(path: Path, headers: list[str], *, sheet_name: str = "Sheet1") -> Path:
    """造一张「第 1 行分组、第 2 行列名」的表，与回填骨架的 header_row: 2 对齐。"""
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.title = sheet_name
    sheet.cell(row=1, column=1, value="基础信息")
    for index, name in enumerate(headers, start=1):
        sheet.cell(row=2, column=index, value=name)
    sheet.cell(row=3, column=headers.index("URL") + 1, value="https://huggingface.co/datasets/a/b")
    book.save(path)
    return path


def test_收新输入按版本归档并报字段差异(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """换表不是覆盖：旧快照要留着，否则上一份档案里的 sha256 就对不上磁盘了。"""
    old = make_header_book(tmp_path / "旧表.xlsx", ["URL", "数据集名称", "下载量", "备注"])
    assert (
        run(
            tmp_path,
            "request",
            "new",
            "REQ-0002",
            "--title",
            "清单补全",
            "--requester",
            "大帅",
            "--kind",
            "backfill",
            "--source",
            str(old),
        )
        == 0
    )
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    before = (folder / SPEC_NAME).read_text(encoding="utf-8")

    new = make_header_book(tmp_path / "新表.xlsx", ["URL", "数据集名称", "下载量", "新增列"])
    assert run(tmp_path, "request", "input", "REQ-0002", "--add", str(new)) == 0

    out = capsys.readouterr().out
    assert (folder / "input" / "v2" / "新表.xlsx").is_file()
    assert (folder / "input" / "旧表.xlsx").is_file()  # 旧快照不动
    assert "新增 1 列" in out and "新增列" in out
    assert "少了 1 列" in out and "备注" in out
    # 换输入等于换口径：命令只搬文件报差异，不替人改 spec。
    assert (folder / SPEC_NAME).read_text(encoding="utf-8") == before


def test_新输入缺标识列时拒收且不留目录(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    old = make_header_book(tmp_path / "旧表.xlsx", ["URL", "数据集名称"])
    assert (
        run(
            tmp_path,
            "request",
            "new",
            "REQ-0002",
            "--title",
            "清单补全",
            "--requester",
            "大帅",
            "--kind",
            "backfill",
            "--source",
            str(old),
        )
        == 0
    )
    folder = resolve_dir(tmp_path / "req", "REQ-0002")

    broken = tmp_path / "缺列.xlsx"
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.cell(row=2, column=1, value="链接")
    book.save(broken)

    assert run(tmp_path, "request", "input", "REQ-0002", "--add", str(broken)) == 1
    assert "标识列" in capsys.readouterr().out
    assert not (folder / "input" / "v2").exists()


def write_versions(tmp_path: Path, *bodies: dict[str, object]) -> None:
    """造一份带 n 版快照的需求；最后一版同时作为当前 spec。"""
    folder = tmp_path / "req" / "REQ-0001-query-高热度"
    (folder / "history").mkdir(parents=True, exist_ok=True)
    for index, body in enumerate(bodies, start=1):
        text = yaml.safe_dump(body, allow_unicode=True)
        (folder / "history" / f"v{index}.yaml").write_text(text, encoding="utf-8")
        (folder / "spec.yaml").write_text(text, encoding="utf-8")


def version_body(version: int, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "id": "REQ-0001",
        "title": "高热度",
        "requester": "大帅",
        "version": version,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    return body


def test_diff默认比最近两版(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(
        tmp_path,
        version_body(1),
        version_body(2, criteria=[{"metric": "下载量", "op": ">", "value": 800}]),
    )

    assert run(tmp_path, "request", "diff", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "口径 v1 → v2" in out
    assert "criteria[下载量 > #0].value" in out
    assert "1000" in out and "800" in out


def test_diff可以点名两版(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1), version_body(2), version_body(3))

    assert run(tmp_path, "request", "diff", "REQ-0001", "v1", "v3") == 0

    assert "口径 v1 → v3" in capsys.readouterr().out


def test_diff只有一版时说清没有可比对象(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1))

    assert run(tmp_path, "request", "diff", "REQ-0001") == 1
    assert "没有可比对象" in capsys.readouterr().out


def test_diff只改标题时明说口径未变(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1), version_body(2, title="高热度（改名）"))

    assert run(tmp_path, "request", "diff", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "口径未变" in out
    assert "标题" in out and "改名" in out
