"""需求生命周期里「出错时说什么」的那几条分支。

`request close` / `reopen` / `input` 的成功路径早已被别的测试走到过，但它们的
**拒绝路径**没有：写不出原因时的态度、对本来就开着的需求重复恢复、拿查询型需求
去换输入表。这些分支各自只有几行，共同点是「用户已经走错一步，命令得说清楚错在
哪、下一步怎么办」——说错了，人就会沿着错的方向继续走。

`run-due` 的那条整体失败同样在此：底库本身不成立时，它不该安静地报「0 交付」。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import openpyxl
import pytest
import yaml
from support import write_method

from hf_data_crawl.delivery.requirement import HANDOFF_NAME, SPEC_NAME, resolve_dir
from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform import DatasetStore

HEADERS = ["URL", "数据集名称", "下载量"]
DAY = date(2026, 11, 1)


def run(tmp_path: Path, *argv: str) -> int:
    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def book(path: Path, keys: tuple[str, ...] = ("acme/ds-1",), header_row: int = 2) -> Path:
    """造一张最小可用的输入表：第 header_row 行是列名，其上留一行分组。"""
    made = openpyxl.Workbook()
    sheet = made.active
    assert sheet is not None
    sheet.title = "数据"
    if header_row > 1:
        sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(HEADERS, start=1):
        sheet.cell(row=header_row, column=column, value=name)
    for offset, key in enumerate(keys):
        sheet.cell(row=header_row + 1 + offset, column=1, value=key)
    path.parent.mkdir(parents=True, exist_ok=True)
    made.save(path)
    return path


def new_query(tmp_path: Path, request_id: str = "REQ-0001") -> int:
    return run(
        tmp_path, "request", "new", request_id, "--title", "高下载数据集", "--requester", "张三"
    )


def new_backfill(tmp_path: Path, source: Path, request_id: str = "REQ-0001") -> int:
    return run(
        tmp_path,
        "request",
        "new",
        request_id,
        "--title",
        "补一批库的信息",
        "--requester",
        "张三",
        "--kind",
        "backfill",
        "--source",
        str(source),
    )


# ---------------------------------------------------------------- 关闭与恢复


def test_关闭必须写原因(tmp_path: Path, capsys) -> None:
    """半年后只看到「已关闭」，没人知道为什么——这条就是为此而拒。

    参数层确实 `required=True`，但 `--reason "   "` 能绕过去，所以命令层还得再判一次。
    """
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "   ") == 1
    out = capsys.readouterr().out
    assert "写一句关闭原因" in out
    assert "半年后" in out


def test_本来就开着的需求无需恢复(tmp_path: Path, capsys) -> None:
    """这是幂等操作：重复 `reopen` 不该报错，但也不该假装做了什么。"""
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "reopen", "REQ-0001") == 0
    assert "本来就是开着的" in capsys.readouterr().out


# ---------------------------------------------------------------- 换输入表


def test_查询型需求没有输入表可换(tmp_path: Path, capsys) -> None:
    """查询型的输入来自底库，没有「对方给的表」这回事；说清楚比抛字段缺失有用。"""
    assert new_query(tmp_path) == 0
    given = book(tmp_path / "in.xlsx")
    capsys.readouterr()

    assert run(tmp_path, "request", "input", "REQ-0001", "--add", str(given)) == 1
    assert "没有输入表可换" in capsys.readouterr().out


def test_换输入表把字段差异摆到明面上(tmp_path: Path, capsys) -> None:
    """命令只搬文件、只报差异，不改 spec：换输入等于换口径，那是人的判断。

    判据：新表归档进 `input/v2/`，且输出里点出 spec 声明了新表没有的列。
    """
    first = book(tmp_path / "第一版.xlsx")
    assert new_backfill(tmp_path, first) == 0
    write_method(resolve_dir(tmp_path / "req", "REQ-0001"), title="补一批库的信息", kind="backfill")
    capsys.readouterr()

    second = tmp_path / "第二版.xlsx"
    made = openpyxl.Workbook()
    sheet = made.active
    assert sheet is not None
    sheet.title = "数据"
    # 表头行必须与第一版一致（spec 里 header_row: 2），否则读出来的「表头」是数据行。
    sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(["URL", "数据集名称", "论文URL"], start=1):
        sheet.cell(row=2, column=column, value=name)
    sheet.cell(row=3, column=1, value="acme/ds-1")
    made.save(second)

    code = run(tmp_path, "request", "input", "REQ-0001", "--add", str(second), "--sheet", "数据")
    out = capsys.readouterr().out

    assert code == 0
    assert "已收进" in out
    assert "下载量" in out
    assert (resolve_dir(tmp_path / "req", "REQ-0001") / "input" / "v2" / "第二版.xlsx").is_file()
    # 原件一字未动
    assert first.is_file()


# ---------------------------------------------------------------- 批量失败


def test_底库缺失时批量交付整体失败(tmp_path: Path, capsys) -> None:
    """这不是某一条需求的问题，而是底库不成立：报「0 交付」会让人以为都跑过了。"""
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "run-due") == 1
    assert "失败" in capsys.readouterr().out


# ---------------------------------------------------------------- 外发记录


def test_外发可指定具体交付并带备注(tmp_path: Path, capsys) -> None:
    """默认记最新一次；给了 --delivery 就必须记那一版，否则「上周发的那版给谁了」还是答不上。"""
    assert new_query(tmp_path) == 0
    request_dir = resolve_dir(tmp_path / "req", "REQ-0001")
    # 直接造两条交付目录：这里测的是外发记账，不是交付本身。
    for name in ("20261101-120000", "20261102-120000"):
        (request_dir / "deliveries" / name).mkdir(parents=True)
    capsys.readouterr()

    code = run(
        tmp_path,
        "request",
        "handoff",
        "REQ-0001",
        "--to",
        "李四",
        "--delivery",
        "20261101-120000",
        "--note",
        "邮件附件",
    )

    assert code == 0
    assert "20261101-120000" in capsys.readouterr().out
    lines = (request_dir / HANDOFF_NAME).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1 and "李四" in lines[0] and "邮件附件" in lines[0]


def test_外发记录指向不存在的交付时报错(tmp_path: Path, capsys) -> None:
    """报错要给出最近三条的名字，否则用户不知道该填什么。"""
    assert new_query(tmp_path) == 0
    request_dir = resolve_dir(tmp_path / "req", "REQ-0001")
    (request_dir / "deliveries" / "20261101-120000").mkdir(parents=True)
    capsys.readouterr()

    assert (
        run(tmp_path, "request", "handoff", "REQ-0001", "--to", "李四", "--delivery", "nope") == 1
    )
    out = capsys.readouterr().out
    assert "找不到交付" in out and "20261101-120000" in out
    assert (request_dir / SPEC_NAME).is_file()


# ---------------------------------------------------------------- 统一早退与批量失败


def seed(tmp_path: Path, *, downloads: int = 2000) -> None:
    """最小底库：一个库，够让 `request run` 真的跑起来。"""
    made = DatasetStore(tmp_path / "data")
    raw.write(
        made,
        snapshot_date=DAY,
        page_index=1,
        records=[
            {
                "id": "acme/ds-1",
                "author": "acme",
                "lastModified": "2026-11-01T00:00:00.000Z",
                "createdAt": "2025-01-01T00:00:00.000Z",
                "sha": "sha-1",
                "downloads": downloads,
                "downloadsAllTime": downloads * 10,
                "likes": 2,
                "private": False,
                "description": "d",
                "tags": ["license:mit"],
            }
        ],
        meta={},
    )
    derive_day(made, DAY)


def write_spec(root: Path, request_id: str, threshold: int) -> None:
    body = {
        "id": request_id,
        "title": "高下载数据集",
        "requester": "张三",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": threshold}],
    }
    path = root / request_id / SPEC_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    write_method(path.parent, request_id=request_id, title="高下载数据集")


@pytest.mark.parametrize(
    ("action", "extra"),
    [
        ("close", ("--reason", "测试")),
        ("reopen", ()),
        ("sample", ()),
        ("handoff", ("--to", "李四")),
        ("prune", ()),
        ("input", ("--add", "x.xlsx")),
    ],
)
def test_对不存在的需求号一律如实报错(
    tmp_path: Path, capsys, action: str, extra: tuple[str, ...]
) -> None:
    """五条命令都先 `resolve_dir`，各自抄了一遍 `except SpecError`——但没人验证过。

    抄漏一条的表现不是崩溃，而是 traceback 直接糊到终端：用户看到栈，不知道
    「是我输错了需求号」。
    """
    (tmp_path / "req").mkdir()

    assert run(tmp_path, "request", action, "REQ-9999", *extra) == 1
    assert "失败" in capsys.readouterr().out


def test_没有需求时清理如实说明(tmp_path: Path, capsys) -> None:
    (tmp_path / "req").mkdir()

    assert run(tmp_path, "request", "prune") == 1
    assert "还没有需求" in capsys.readouterr().out


def test_没有可清理产物时清理不谎报(tmp_path: Path, capsys) -> None:
    """需求存在但零交付：不是错误，但也不能报「已清理 0 条」了事。"""
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "prune") == 0
    assert "没有可清理的产物" in capsys.readouterr().out


def test_批量交付把失败单独归类(tmp_path: Path, capsys) -> None:
    """零行是失败，不是跳过：跳过意味着「这单不用跑」，失败意味着「跑了但没成」。"""
    seed(tmp_path, downloads=2000)
    write_spec(tmp_path / "req", "REQ-0001", threshold=5000)
    capsys.readouterr()

    assert run(tmp_path, "request", "run-due") == 1
    out = capsys.readouterr().out

    assert "共 0 交付 / 0 跳过 / 1 失败" in out
    assert "0 行" in out or "行数为 0" in out


# ---------------------------------------------------------------- 口径视图


def test_diff在只有一版时说明去路(tmp_path: Path, capsys) -> None:
    """「只有 1 版」不是错误，但必须告诉人怎么才会出现第二版——否则会以为命令坏了。"""
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "diff", "REQ-0001") == 1
    out = capsys.readouterr().out
    assert "没有可比对象" in out
    assert "交付一次" in out


def test_diff指定不存在的版本时列出已有的(tmp_path: Path, capsys) -> None:
    assert new_query(tmp_path) == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "diff", "REQ-0001", "v1", "v9") == 1
    out = capsys.readouterr().out
    assert "没有 v1 的快照" in out and "现有：无" in out


def test_show把有快照但没交付的版本标出来(tmp_path: Path, capsys) -> None:
    """快照在 ≠ 交过：失败尝试也会留下快照。混着列，读的人会以为每一版都真的交过。"""
    assert new_query(tmp_path) == 0
    request_dir = resolve_dir(tmp_path / "req", "REQ-0001")
    spec = (request_dir / SPEC_NAME).read_text(encoding="utf-8")
    history = request_dir / "history"
    history.mkdir(parents=True, exist_ok=True)
    (history / "v2.yaml").write_text(spec.replace("version: 1", "version: 2"), encoding="utf-8")
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0
    out = capsys.readouterr().out
    assert "历史版本" in out and "v2" in out
    assert "没有对应交付" in out


def test_show对已关闭的需求明说(tmp_path: Path, capsys) -> None:
    assert new_query(tmp_path) == 0
    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "对方撤单") == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0
    out = capsys.readouterr().out
    assert "状态：已关闭" in out and "对方撤单" in out


def test_show在方法合规时明说合规(tmp_path: Path, capsys) -> None:
    """「一处问题都没列」与「合规」在终端里长得一样；后者要主动说出来，否则读者
    分不清是没检查还是检查通过。"""
    assert new_query(tmp_path) == 0
    write_method(resolve_dir(tmp_path / "req", "REQ-0001"), title="高下载数据集")
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0
    assert "合规：步骤齐备" in capsys.readouterr().out


# ---------------------------------------------------------------- 换输入表的三种拒绝


def backfill_request(tmp_path: Path) -> Path:
    """建一份回填型需求，返回需求目录。

    骨架里的 sheet 恒为 `Sheet1`——那是待人工核对的占位，不是从源表读出来的。
    测试用表叫「数据」，所以按受理完成后提示的那一步改一次，这正是真实用法。
    """
    first = book(tmp_path / "第一版.xlsx")
    assert new_backfill(tmp_path, first) == 0
    request_dir = resolve_dir(tmp_path / "req", "REQ-0001")
    spec = request_dir / SPEC_NAME
    text = spec.read_text(encoding="utf-8").replace("sheet: Sheet1", "sheet: 数据")
    spec.write_text(text, encoding="utf-8")
    return request_dir


def test_换输入表时新文件不存在要当场拒绝(tmp_path: Path, capsys) -> None:
    backfill_request(tmp_path)
    capsys.readouterr()

    assert run(tmp_path, "request", "input", "REQ-0001", "--add", str(tmp_path / "无.xlsx")) == 1
    assert "不存在或不是文件" in capsys.readouterr().out


def test_换输入表缺标识列时拒绝并列出已有列(tmp_path: Path, capsys) -> None:
    """没有标识列就接不进来：填出来的行对不上任何库。列出已有列，人好判断是表给错了。"""
    backfill_request(tmp_path)
    capsys.readouterr()
    wrong = tmp_path / "没有URL.xlsx"
    made = openpyxl.Workbook()
    sheet = made.active
    assert sheet is not None
    sheet.title = "数据"
    sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(["数据集名称", "下载量"], start=1):
        sheet.cell(row=2, column=column, value=name)
    made.save(wrong)

    code = run(tmp_path, "request", "input", "REQ-0001", "--add", str(wrong), "--sheet", "数据")
    out = capsys.readouterr().out

    assert code == 1
    assert "没有标识列" in out
    assert "数据集名称" in out


def test_换输入表表头完全一致时明说(tmp_path: Path, capsys) -> None:
    """「没有差异」也是结论。不打印的话，人分不清是没比对还是比对通过。"""
    backfill_request(tmp_path)
    capsys.readouterr()
    same = book(tmp_path / "同名新表.xlsx", keys=("acme/ds-2",))

    assert run(tmp_path, "request", "input", "REQ-0001", "--add", str(same), "--sheet", "数据") == 0
    assert "表头与当前完全一致" in capsys.readouterr().out


# ---------------------------------------------------------------- 清单与样例


def test_清单里标出已关闭的需求(tmp_path: Path, capsys) -> None:
    assert new_query(tmp_path) == 0
    assert run(tmp_path, "request", "close", "REQ-0001", "--reason", "对方撤单") == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0
    out = capsys.readouterr().out
    assert "已关闭：对方撤单" in out


def test_取消样例标记后状态里不残留样例键(tmp_path: Path, capsys) -> None:
    """`--undo` 之后 STATUS.json 里再留着 sample 的键，下一次读它的人会以为还是样例。"""
    assert new_query(tmp_path) == 0
    assert run(tmp_path, "request", "sample", "REQ-0001", "--reason", "夹具") == 0
    request_dir = resolve_dir(tmp_path / "req", "REQ-0001")
    capsys.readouterr()

    assert run(tmp_path, "request", "sample", "REQ-0001", "--undo") == 0
    status = request_dir / "STATUS.json"
    body = json.loads(status.read_text(encoding="utf-8")) if status.is_file() else {}

    assert not {"sample", "sample_reason", "sample_marked_at"} & set(body)
    assert "已取消样例标记" in capsys.readouterr().out
