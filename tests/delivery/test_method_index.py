"""过程记录索引的坏数据：手改 `method.json` 之后会撞到的每一种。

索引是给人改的（脚本用途、跑法都得人工填），所以「改坏了」是常态而非意外。
判据有两条：

- **每条错误都要说清是哪一处、怎么改**。只抛一句「格式不对」，人只能去翻源码。
- **错误要一次收全**（`problems` 的设计），而不是改一处跑一次。这条已由
  `test_method.py` 覆盖，这里补的是各条错误本身。

另有一类只在 `advisories` 里出现：提醒不阻断交付，所以它自己绝不能抛异常——
索引坏了、文档缺了，它都只是少说一句或换个说法。
"""

from __future__ import annotations

import json
from pathlib import Path

from support import write_method

from hf_data_crawl.delivery import method


def prepared(tmp_path: Path) -> Path:
    """一份合规的过程记录，供各用例只改坏其中一处。"""
    request_dir = tmp_path / "REQ-0001"
    write_method(request_dir)
    return request_dir


def index_of(request_dir: Path) -> Path:
    return request_dir / method.METHOD_DIR / method.METHOD_INDEX


def break_index(request_dir: Path, text: str) -> None:
    index_of(request_dir).write_text(text, encoding="utf-8")


def assert_problem(request_dir: Path, *fragments: str) -> None:
    found = " / ".join(method.problems(request_dir))
    for fragment in fragments:
        assert fragment in found, f"期望错误信息里出现 {fragment!r}，实得：{found}"


# ---------------------------------------------------------------- 索引读不出来


def test_索引不是合法JSON时指出文件在哪(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(request_dir, "{ 这不是 json")

    assert_problem(request_dir, "读不出来", method.METHOD_INDEX)


def test_索引顶层不是对象时说清实得什么(tmp_path: Path) -> None:
    """顶层写成列表是最常见的误改——直接说「实得 list」，比「格式错误」有用。"""
    request_dir = prepared(tmp_path)
    break_index(request_dir, "[]")

    assert_problem(request_dir, "顶层应是对象", "list")


def test_索引版本过新时让人升级而不是硬读(tmp_path: Path) -> None:
    """新版本写的索引可能带本程序不认识的字段；按旧规矩读会得出错误结论。"""
    request_dir = prepared(tmp_path)
    break_index(request_dir, json.dumps({"version": method.INDEX_VERSION + 1}))

    assert_problem(request_dir, "本程序只认", "请升级后再读")


def test_版本号写成字符串也不算数(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(request_dir, json.dumps({"version": "1"}))

    assert_problem(request_dir, "本程序只认")


# ---------------------------------------------------------------- 条目结构


def test_scripts不是列表时拒绝(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(request_dir, json.dumps({"version": 1, "scripts": {}}))

    assert_problem(request_dir, "scripts 应是列表")


def test_条目不是对象时拒绝(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(request_dir, json.dumps({"version": 1, "scripts": ["a.py"]}))

    assert_problem(request_dir, "每一项应是对象")


def test_条目没写name时点名(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(request_dir, json.dumps({"version": 1, "scripts": [{"purpose": "干什么"}]}))

    assert_problem(request_dir, "没写 name")


def test_脚本重复登记时列出是哪几个(tmp_path: Path) -> None:
    """重复登记会让「指纹对不上」的报错指向两份不同的文件，先在这里拦住。"""
    request_dir = prepared(tmp_path)
    break_index(
        request_dir,
        json.dumps(
            {"version": 1, "scripts": [{"name": "a.py"}, {"name": "a.py"}]},
            ensure_ascii=False,
        ),
    )

    assert_problem(request_dir, "重复登记", "a.py")


def test_inputs不是列表时拒绝(tmp_path: Path) -> None:
    request_dir = prepared(tmp_path)
    break_index(
        request_dir,
        json.dumps({"version": 1, "scripts": [{"name": "a.py", "inputs": "清单.xlsx"}]}),
    )

    assert_problem(request_dir, "inputs 应是列表")


def test_路径写成空字符串时拒绝(tmp_path: Path) -> None:
    """空路径看着无害，到了 `scripts_dir / ""` 就指向目录本身。"""
    request_dir = prepared(tmp_path)
    break_index(
        request_dir,
        json.dumps({"version": 1, "scripts": [{"name": "a.py", "inputs": ["  "]}]}),
    )

    assert_problem(request_dir, "空路径")


# ---------------------------------------------------------------- 目录里多出来的东西


def test_脚本目录里的副产物不算脚本(tmp_path: Path) -> None:
    """`__pycache__`、隐藏文件、字节码都是跑出来的；登记它们等于逼人每次同步。"""
    request_dir = prepared(tmp_path)
    scripts = request_dir / method.METHOD_DIR / method.SCRIPTS_DIR
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "__pycache__").mkdir()
    (scripts / "__pycache__" / "x.pyc").write_bytes(b"\x00")
    (scripts / ".hidden.py").write_text("x", encoding="utf-8")
    (scripts / "stale.pyc").write_bytes(b"\x00")
    (scripts / "临时目录").mkdir()

    found = method.problems(request_dir)

    assert found == [], f"副产物不该被当成未登记的脚本：{found}"


def test_登记了却没写指纹时提示同步(tmp_path: Path) -> None:
    """手抄一条条目进索引，忘了算 sha256——此时「文件在、指纹空」不该算合规。"""
    request_dir = prepared(tmp_path)
    scripts = request_dir / method.METHOD_DIR / method.SCRIPTS_DIR
    scripts.mkdir(parents=True, exist_ok=True)
    (scripts / "统计.py").write_text("print(1)", encoding="utf-8")
    break_index(
        request_dir,
        json.dumps({"version": 1, "scripts": [{"name": "统计.py", "purpose": "数一下"}]}),
    )

    assert_problem(request_dir, "没有指纹", "method sync")


# ---------------------------------------------------------------- 提醒不拦交付


def test_缺方法文档时提醒补需求成立性(tmp_path: Path) -> None:
    """没有 METHOD.md 时 `advisories` 读不到正文，但不能因此把提醒也算错。"""
    request_dir = tmp_path / "REQ-0001"
    request_dir.mkdir()

    found = method.advisories(request_dir)

    assert len(found) == 1
    assert "需求成立性" in found[0]
    assert "不影响交付" in found[0]


def test_索引坏了提醒也不抛异常(tmp_path: Path) -> None:
    """`advisories` 是提醒，不是闸门；它抛异常就会把交付整条链带崩。"""
    request_dir = prepared(tmp_path)
    break_index(request_dir, "{ 坏掉的 json")

    found = method.advisories(request_dir)

    assert all("待补" not in item for item in found), "索引读不出来时不能凭空报「待补」"
