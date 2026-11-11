"""过程自包含的行为测试。

守的判据只有一条：**方法不在目录里，就不许出货**。其余都是围绕它的边界——
骨架没填算不算有方法、脚本改了索引跟不跟得上、路径能不能爬出目录。

为什么这条要拦在交付入口
------------------------
事后补写方法几乎必然失败：三个月后没人记得当初为什么判「后训练」，能补的只有
结论。所以校验点必须放在「货出门」那一刻，而不是放在体检里等下次再看。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from hf_data_crawl import method
from hf_data_crawl.delivery import DeliveryBlocked, _preview_lines, deliver
from hf_data_crawl.fields import CONTRACT_VERSION
from hf_data_crawl.platform.store import DatasetStore

NOW = datetime(2026, 11, 9, 21, 0, 0, tzinfo=UTC)

#: 一份填好的过程记录。测试要的是「填写之后合规」，不是骨架长什么样。
DOC = """\
# 过程记录：REQ-0001 测试需求

## 步骤

1. 读 spec.yaml 核对口径，再跑 `hf-data-crawl request run REQ-0001`。
"""

SCRIPT = """\
\"\"\"拉 README 原文，供人工判读训练阶段。\"\"\"

print("readme")
"""


def request_dir(tmp_path: Path, request_id: str = "REQ-0001") -> tuple[Path, Path]:
    """造一份最小需求目录，返回（需求根, 需求目录）。"""
    root = tmp_path / "req"
    folder = root / request_id
    folder.mkdir(parents=True)
    body = {
        "id": request_id,
        "title": "测试需求",
        "requester": "大帅",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1}],
    }
    (folder / "spec.yaml").write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    return root, folder


def build_method(folder: Path, *, request_id: str = "REQ-0001") -> None:
    """建一份合规的过程记录（空脚本清单）。"""
    method.sync(folder, request_id=request_id, title="测试需求", kind="query")
    (folder / method.METHOD_DIR / method.METHOD_DOC).write_text(DOC, encoding="utf-8")


def add_script(folder: Path, name: str = "pull_readme.py") -> Path:
    path = folder / method.METHOD_DIR / method.SCRIPTS_DIR / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SCRIPT, encoding="utf-8")
    return path


def index_of(folder: Path) -> dict[str, Any]:
    return json.loads(
        (folder / method.METHOD_DIR / method.METHOD_INDEX).read_text(encoding="utf-8")
    )


def edit_index(folder: Path, **changes: Any) -> None:
    """直接改索引里第一条脚本登记——模拟人手写错，或改了脚本却没同步。"""
    path = folder / method.METHOD_DIR / method.METHOD_INDEX
    body = index_of(folder)
    body["scripts"][0].update(changes)
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- 骨架与填写


def test_骨架未填不算方法(tmp_path: Path) -> None:
    """生成骨架不等于写了方法：标记还在，就必须拦住。"""
    _, folder = request_dir(tmp_path)

    report = method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")

    assert report.doc_created is True
    assert any("未填的骨架" in text for text in method.problems(folder))


def test_填写后即合规(tmp_path: Path) -> None:
    _, folder = request_dir(tmp_path)

    build_method(folder)

    assert method.problems(folder) == []


def test_缺步骤一节不算合规(tmp_path: Path) -> None:
    """散文可以随意写，但没有「步骤」就复跑不了，等于没解决它要解决的问题。"""
    _, folder = request_dir(tmp_path)
    build_method(folder)
    (folder / method.METHOD_DIR / method.METHOD_DOC).write_text(
        "# 过程记录\n\n反正就是跑了一下。\n", encoding="utf-8"
    )

    assert any("## 步骤" in text for text in method.problems(folder))


def test_缺方法时把补法说清楚(tmp_path: Path) -> None:
    _, folder = request_dir(tmp_path)

    with pytest.raises(method.MethodError) as caught:
        method.read(folder)

    message = str(caught.value)
    assert "METHOD.md" in message
    assert "怎么算出来的" in message


# ---------------------------------------------------------------- 脚本与索引


def test_新脚本自动登记但用途待补(tmp_path: Path) -> None:
    """机器算得出 sha256，算不出这个脚本为什么存在——所以新增时留占位逼人回来写。"""
    _, folder = request_dir(tmp_path)
    build_method(folder)
    script = add_script(folder)

    assert any("未登记" in text for text in method.problems(folder))

    report = method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    entry = index_of(folder)["scripts"][0]
    assert report.added == ("pull_readme.py",)
    # 指纹按磁盘字节算：Windows 上 write_text 会写 \r\n，不能拿源码字符串去比。
    assert entry["sha256"] == method.digest(script)
    assert "待补" in entry["purpose"]
    assert method.problems(folder) == []


def test_脚本改动后指纹不符_同步后保留人工写的用途(tmp_path: Path) -> None:
    _, folder = request_dir(tmp_path)
    build_method(folder)
    add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    edit_index(folder, purpose="拉 16 份 README 原文，供判训练阶段")

    add_script(folder).write_text(SCRIPT + "\n# 加了重试\n", encoding="utf-8")
    assert any("指纹与索引不符" in text for text in method.problems(folder))

    report = method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    assert report.refreshed == ("pull_readme.py",)
    # 重算只换指纹，不许把人写的用途冲掉。
    assert index_of(folder)["scripts"][0]["purpose"] == "拉 16 份 README 原文，供判训练阶段"
    assert method.problems(folder) == []


def test_删掉脚本后条目一并移除(tmp_path: Path) -> None:
    _, folder = request_dir(tmp_path)
    build_method(folder)
    script = add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")

    script.unlink()
    report = method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")

    assert report.dropped == ("pull_readme.py",)
    assert index_of(folder)["scripts"] == []


def test_同步幂等(tmp_path: Path) -> None:
    """内容没变就不重写：否则每次 sync 都动一次文件，真改动反而看不出来。"""
    _, folder = request_dir(tmp_path)
    build_method(folder)
    add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")

    again = method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")

    assert again.written is False
    assert again.changed is False


def test_索引里的绝对路径被拒(tmp_path: Path) -> None:
    """绝对路径一进来，这个目录就搬不走了——而「能搬走」正是自包含的定义。"""
    _, folder = request_dir(tmp_path)
    build_method(folder)
    add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    edit_index(folder, name="/home/someone/pull_readme.py")

    assert any("绝对路径" in text for text in method.problems(folder))


def test_用点号爬出目录被拒(tmp_path: Path) -> None:
    _, folder = request_dir(tmp_path)
    build_method(folder)
    add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    edit_index(folder, inputs=["../外面的表.xlsx"])

    assert any("爬出" in text for text in method.problems(folder))


# ---------------------------------------------------------------- 交付闸门


def test_缺方法时交付被拦且不留目录(tmp_path: Path) -> None:
    root, folder = request_dir(tmp_path)

    with pytest.raises(DeliveryBlocked, match="过程记录"):
        deliver(DatasetStore(tmp_path / "data"), root, "REQ-0001", now=NOW)

    # 连 deliveries/ 都不该建：方法是交付成立的一部分，不是交付之后的补录。
    assert not (folder / "deliveries").exists()


def test_预演放行但把拦下的理由写在最上面(tmp_path: Path) -> None:
    """预演若静默跑通，人会以为可以交付了，然后在正式那一刻才撞墙。"""
    _, folder = request_dir(tmp_path)

    lines = _preview_lines(folder, ["原样口径"])

    assert lines[0].startswith("> ⚠")
    assert lines[-1] == "原样口径"


def test_stamp钉住文档脚本判读表与环境(tmp_path: Path) -> None:
    root, folder = request_dir(tmp_path)
    build_method(folder)
    add_script(folder)
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    overrides = folder / "overrides.json"
    overrides.write_text('{"acme/ds-1": {"训练阶段": "后训练"}}', encoding="utf-8")

    stamp = method.stamp(folder, method.read(folder))

    doc = folder / method.METHOD_DIR / method.METHOD_DOC
    assert stamp["doc"]["sha256"] == method.digest(doc)
    assert stamp["scripts"][0]["name"] == "pull_readme.py"
    assert stamp["inputs"]["overrides.json"]["sha256"] == method.digest(overrides)
    # 环境不复制源码，只钉版本：变了由 doctor 报出来。
    assert stamp["env"]["contract_version"] == CONTRACT_VERSION
    assert "package" in stamp["env"]
    assert root.exists()  # 只有需求目录被读，没有任何东西写到外面


def test_换行风格不同不算脚本改过(tmp_path: Path) -> None:
    """git 检出会把 CRLF/LF 换个样；把它算成「改过」，目录一 clone 下来就交不出去。"""
    _, folder = request_dir(tmp_path)
    build_method(folder)
    script = folder / method.METHOD_DIR / method.SCRIPTS_DIR / "pull_readme.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_bytes(b'print("lf")\n')
    method.sync(folder, request_id="REQ-0001", title="测试需求", kind="query")
    registered = index_of(folder)["scripts"][0]["sha256"]

    script.write_bytes(b'print("lf")\r\n')

    assert method.problems(folder) == []
    assert index_of(folder)["scripts"][0]["sha256"] == registered
