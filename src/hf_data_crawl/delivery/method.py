"""过程自包含：把「怎么做的」钉在需求目录里。

为什么输入、口径、产物三件齐了还不够
------------------------------------
`input/` 回答「拿什么算的」，`spec.yaml` 回答「按什么规则算」，`deliveries/`
回答「算出什么」。三者合起来仍回答不了**怎么算出来的**——`spec.yaml` 能表达
「取 downloads 列」，表达不了「这 24 个库的『训练阶段』是逐个读 README 判的，
判据是 README 明写 pretrain / fine-tune / post-training 才算」。

缺了这一段，三个月后有人问「这一格凭什么这么填」，只有两条路：翻聊天记录，
或者整件事重做一遍。两条路都不叫自包含。

三层，只有两层放得进目录
------------------------
- **文本**：步骤、判据、哪步机器算哪步人判 —— `method/METHOD.md`
- **代码**：实际跑过的脚本原件 —— `method/scripts/`，逐份 sha256 钉住
- **环境**：脚本依赖的底库代码 —— **复制不进来，也不该复制**。每个需求目录塞
  一份 `src/` 只会得到 N 份迟早各不相同的副本，且没人会去升级它们。这一层用
  版本号钉住，记进交付档案的 `method.env`，版本一变就对不上账——与用
  `current_md5` 钉底库是同一手法：不复制，只钉住，能核对。

为什么索引要单独一份机器读
--------------------------
`METHOD.md` 给人看，散文写不出一份可靠清单。`method.json` 给校验用：哪个脚本、
干什么、怎么跑、sha256 是多少。脚本改了而索引没跟上，交付必须被拦下——否则
档案里钉的是旧指纹，等于钉了个谎。

一条链：脚本 → 判读表 → 交付
---------------------------
回填类需求的 `overrides.json` 是人工判读的结论，它的上游是脚本（拉 README、
核 splits），下游是交付。所以 `stamp()` 把 `overrides.json` 的 sha256 一并钉进
交付档案：任一份历史交付都能顺着「哪版脚本、哪版判读表、哪版代码」倒查回去。
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .fields import CONTRACT_VERSION

#: 需求目录下的固定位置。
METHOD_DIR = "method"
METHOD_DOC = "METHOD.md"
METHOD_INDEX = "method.json"
SCRIPTS_DIR = "scripts"

#: 索引格式版本。字段只增不减，旧档案永远读得懂。
INDEX_VERSION = 1

#: 骨架里的待办标记。留着它就是「还没写」——交付一律拦下，免得一份空模板
#: 被当成过程记录蒙混过关。
DRAFT_MARK = "<!-- 待填："

#: 过程记录必须具备的骨架。散文可以随意写，但「步骤」这一节不能省：没有它，
#: 这份文档就没法照着复跑，也就没解决它本来要解决的问题。
REQUIRED_HEADING = "## 步骤"

#: 随方法一起钉住的中间件：脚本产出、交付直接吃进去的东西。
PINNED_INPUTS = ("overrides.json",)


class MethodError(ValueError):
    """过程记录缺失或不自洽。信息要说清「缺什么、怎么补」，不能只说「不合规」。"""


@dataclass(frozen=True)
class ScriptEntry:
    """`method.json` 里的一条脚本登记。"""

    name: str
    purpose: str = ""
    run: str = ""
    inputs: tuple[str, ...] = ()
    outputs: tuple[str, ...] = ()
    sha256: str = ""


@dataclass(frozen=True)
class Method:
    """一份通过校验的过程记录。拿到它即代表目录里的方法与索引对得上。"""

    directory: Path
    doc_sha256: str
    index_sha256: str
    scripts: tuple[ScriptEntry, ...]


@dataclass(frozen=True)
class SyncReport:
    """`sync` 的一次结果。`written=False` 表示索引本来就已经一致。"""

    doc_created: bool = False
    added: tuple[str, ...] = ()
    dropped: tuple[str, ...] = ()
    refreshed: tuple[str, ...] = ()
    written: bool = False

    @property
    def changed(self) -> bool:
        return self.doc_created or bool(self.added or self.dropped or self.refreshed)


# ---------------------------------------------------------------- 路径与指纹


def digest(path: Path) -> str:
    """文件内容的指纹。

    按**内容**算，不按换行风格算：同一个脚本 git 检出到 Windows 是 CRLF、到 Linux
    是 LF，若把这当成「脚本改过」，目录一 clone 下来第一次交付就被拦，而人根本没动
    过它。真正的改动（多一行、改一个字符）照样算得出来——规范化只抹掉 `\\r`。

    这也是为什么交付档案里记的 doc / index / 判读表指纹都用它：一份档案不该因为
    在另一台机器上打开过就「对不上」。
    """
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def _relative(text: str, where: str) -> str:
    """索引里的路径必须是相对需求目录的。

    绝对路径一旦写进来，这个目录就不再能搬走——而「能搬走」正是自包含的定义。
    用 `..` 爬出去同样不行，那等于指着目录外面的一份东西，哪天它变了也没人知道。
    """
    value = text.strip()
    if not value:
        raise MethodError(f"{where} 里有一处空路径")
    path = Path(value)
    if path.is_absolute() or path.drive:
        raise MethodError(f"{where} 里的 {value!r} 是绝对路径；目录要能整体搬走，请写相对路径")
    if ".." in path.parts:
        raise MethodError(f"{where} 里的 {value!r} 用 `..` 爬出了需求目录，这不算自包含")
    return value


def _script_files(scripts_dir: Path) -> list[str]:
    """`method/scripts/` 下的脚本清单，相对该目录。

    `__pycache__`、隐藏文件与字节码不算脚本：它们是跑出来的副产物，登记进索引
    只会让每次重算都报「多了一份」，久了就没人看这份清单了。
    """
    if not scripts_dir.is_dir():
        return []
    found: list[str] = []
    for path in scripts_dir.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(scripts_dir)
        if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
            continue
        if path.suffix in {".pyc", ".pyo"}:
            continue
        found.append(relative.as_posix())
    return sorted(found)


# ---------------------------------------------------------------- 读与校验


def _index_path(request_dir: Path) -> Path:
    return request_dir / METHOD_DIR / METHOD_INDEX


def _doc_path(request_dir: Path) -> Path:
    return request_dir / METHOD_DIR / METHOD_DOC


def _load_index(request_dir: Path) -> dict[str, Any]:
    path = _index_path(request_dir)
    if not path.is_file():
        raise MethodError(
            f"{path} 不存在。缺索引就等于没有可校验的清单——"
            f"跑 `request method sync` 生成，再把每个脚本的用途补上。"
        )
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MethodError(f"{path} 读不出来：{error}") from None
    if not isinstance(body, dict):
        raise MethodError(f"{path} 顶层应是对象，实得 {type(body).__name__}")
    version = body.get("version")
    if not isinstance(version, int) or version > INDEX_VERSION:
        raise MethodError(
            f"{path} 的 version 是 {version!r}，本程序只认 <= {INDEX_VERSION}；"
            "这份索引来自更新的版本，请升级后再读"
        )
    return body


def _entries(body: dict[str, Any], path: Path) -> tuple[ScriptEntry, ...]:
    raw = body.get("scripts")
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise MethodError(f"{path}: scripts 应是列表")
    entries: list[ScriptEntry] = []
    for item in raw:
        if not isinstance(item, dict):
            raise MethodError(f"{path}: scripts 的每一项应是对象")
        name = str(item.get("name") or "").strip()
        if not name:
            raise MethodError(f"{path}: scripts 里有一条没写 name")
        entries.append(
            ScriptEntry(
                name=_relative(name, f"{path} 的 scripts"),
                purpose=str(item.get("purpose") or ""),
                run=str(item.get("run") or ""),
                inputs=tuple(
                    _relative(str(value), f"{path} 里 {name} 的 inputs")
                    for value in _string_list(item.get("inputs"), path, name, "inputs")
                ),
                outputs=tuple(
                    _relative(str(value), f"{path} 里 {name} 的 outputs")
                    for value in _string_list(item.get("outputs"), path, name, "outputs")
                ),
                sha256=str(item.get("sha256") or ""),
            )
        )
    repeated = sorted(name for name, count in Counter(e.name for e in entries).items() if count > 1)
    if repeated:
        raise MethodError(f"{path}: 脚本重复登记：{'、'.join(repeated)}")
    return tuple(entries)


def _string_list(raw: Any, path: Path, name: str, key: str) -> list[str]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise MethodError(f"{path}: {name} 的 {key} 应是列表")
    return [str(value) for value in raw]


def problems(request_dir: Path) -> list[str]:
    """列出全部不合规之处。空列表表示这份过程记录是自洽的。

    一次性收全而不是遇到第一个错就抛：改一处跑一次、报一处，改三遍才通过，
    这种体验会让人宁可不写方法。
    """
    found: list[str] = []
    doc = _doc_path(request_dir)
    if not doc.is_file():
        return [
            f"缺 {METHOD_DIR}/{METHOD_DOC}：这份需求没有过程记录，交付后将无法回答「怎么算出来的」"
        ]
    try:
        text = doc.read_text(encoding="utf-8")
    except OSError as error:
        return [f"{doc} 读不出来：{error}"]
    if DRAFT_MARK in text:
        found.append(f"{METHOD_DIR}/{METHOD_DOC} 还是未填的骨架（含 `{DRAFT_MARK.strip()}`）")
    if REQUIRED_HEADING not in text:
        found.append(f"{METHOD_DIR}/{METHOD_DOC} 缺 `{REQUIRED_HEADING}` 一节，没法照着复跑")

    try:
        body = _load_index(request_dir)
        entries = _entries(body, _index_path(request_dir))
    except MethodError as error:
        found.append(str(error))
        return found

    scripts_dir = request_dir / METHOD_DIR / SCRIPTS_DIR
    actual = _script_files(scripts_dir)
    declared = [entry.name for entry in entries]
    for name in actual:
        if name not in declared:
            found.append(f"脚本 {name} 未登记进 {METHOD_INDEX}；跑 `request method sync` 补上")
    for entry in entries:
        path = scripts_dir / entry.name
        if not path.is_file():
            found.append(f"{METHOD_INDEX} 登记了 {entry.name}，但文件不在 {scripts_dir}")
            continue
        if not entry.sha256:
            found.append(f"{entry.name} 没有指纹；跑 `request method sync` 重算")
        elif digest(path) != entry.sha256:
            found.append(
                f"{entry.name} 的指纹与索引不符（脚本改过但索引没同步）；"
                "确认改动无误后跑 `request method sync`，并在 METHOD.md 里记一句改了什么"
            )
    return found


def read(request_dir: Path) -> Method:
    """校验并读出一份过程记录。任何不合规都抛 `MethodError`，消息可直接给人看。"""
    found = problems(request_dir)
    if found:
        raise MethodError(f"{request_dir.name} 的过程记录不合规：\n  - " + "\n  - ".join(found))
    body = _load_index(request_dir)
    return Method(
        directory=request_dir / METHOD_DIR,
        doc_sha256=digest(_doc_path(request_dir)),
        index_sha256=digest(_index_path(request_dir)),
        scripts=_entries(body, _index_path(request_dir)),
    )


# ---------------------------------------------------------------- 骨架与同步


def skeleton(request_id: str, title: str, kind: str) -> str:
    """过程记录骨架。写完必须删掉 `DRAFT_MARK` 那一行，否则交付会被拦下。"""
    extra = (
        "回填类多一层：判读结论进 `overrides.json`，"
        "所以「哪些列人判、判据是什么」必须写死，否则换个人会判出另一套结果。"
        if kind == "backfill"
        else "查询类的方法通常就是口径本身；这里写清「为什么定这个口径」「试跑时看到什么」。"
    )
    return f"""\
# 过程记录：{request_id} {title}

{DRAFT_MARK}把下面几节写成「换个人照着做也能重来一遍」的样子，填完删掉本行。 -->

## 目标

（一句话：这份需求要产出什么，判「做完了」的标准是什么。）

{extra}

## 步骤

1. （第一步做什么；用哪条命令、读哪个文件）
2. （第二步……）

## 机器算的 / 人判的

| 环节 | 谁做 | 判据 |
| --- | --- | --- |
|  | 引擎 |  |
|  | 人 |  |

## 脚本

放在 `method/scripts/`，改完跑 `hf-data-crawl request method sync {request_id}` 重算指纹，
并在本节记一句「改了什么、为什么改」——索引只认指纹，不认理由。

## 环境

- 底库：`data/exports/datasets.parquet`；本次基于哪一版，见交付档案 `manifest.source.current_md5`
- 代码：`hf-data-crawl` 的版本与提交号，见交付档案 `manifest.method.env`

## 已知边界

（哪些做不到、哪些是近似、哪些依赖外部不可控的东西。写清楚，比留白强。）
"""


def _empty_index() -> dict[str, Any]:
    return {"version": INDEX_VERSION, "scripts": []}


def _entry_body(entry: ScriptEntry) -> dict[str, Any]:
    return {
        "name": entry.name,
        "purpose": entry.purpose,
        "run": entry.run,
        "inputs": list(entry.inputs),
        "outputs": list(entry.outputs),
        "sha256": entry.sha256,
    }


def sync(
    request_dir: Path,
    *,
    request_id: str,
    title: str,
    kind: str,
    now: datetime | None = None,
) -> SyncReport:
    """按目录实际内容重算索引；缺骨架就补一份。

    只改指纹与增删条目，**不动人写的 purpose / run / inputs / outputs**：那些是
    判断，机器猜不出来。新冒出来的脚本给一条「待补」占位，逼人回来说清楚它干什么。
    """
    method_dir = request_dir / METHOD_DIR
    scripts_dir = method_dir / SCRIPTS_DIR
    doc_created = False
    doc = method_dir / METHOD_DOC
    if not doc.is_file():
        method_dir.mkdir(parents=True, exist_ok=True)
        doc.write_text(skeleton(request_id, title, kind), encoding="utf-8", newline="\n")
        doc_created = True
    scripts_dir.mkdir(parents=True, exist_ok=True)

    try:
        body = _load_index(request_dir)
    except MethodError:
        body = _empty_index()
    try:
        previous = {entry.name: entry for entry in _entries(body, _index_path(request_dir))}
    except MethodError:
        # 索引本身坏了：以目录实际内容为准重建，坏的条目不再沿用。
        previous = {}

    actual = _script_files(scripts_dir)
    entries: list[ScriptEntry] = []
    added: list[str] = []
    refreshed: list[str] = []
    for name in actual:
        # 局部变量别叫 digest：那会和模块级函数同名，读的人以为在调它。
        fingerprint = digest(scripts_dir / name)
        old = previous.get(name)
        if old is None:
            added.append(name)
            entries.append(
                ScriptEntry(
                    name=name,
                    purpose="（待补：这个脚本干什么、为什么需要它）",
                    run="（待补：在需求目录下怎么运行它）",
                    sha256=fingerprint,
                )
            )
            continue
        if old.sha256 != fingerprint:
            refreshed.append(name)
        entries.append(
            ScriptEntry(
                name=name,
                purpose=old.purpose,
                run=old.run,
                inputs=old.inputs,
                outputs=old.outputs,
                sha256=fingerprint,
            )
        )
    dropped = sorted(set(previous) - set(actual))

    new_body: dict[str, Any] = {
        "version": INDEX_VERSION,
        "scripts": [_entry_body(entry) for entry in entries],
    }
    path = _index_path(request_dir)
    # 比对时剔掉 `updated_at`：它只是「最后一次重算在何时」，落了它，内容没变
    # 也会被判成变了，于是每次 sync 都重写一次，谁也没法用 diff 看真改动。
    on_disk: dict[str, Any] | None = None
    if path.is_file():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            loaded = None
        if isinstance(loaded, dict):
            on_disk = {key: value for key, value in loaded.items() if key != "updated_at"}
    written = on_disk != new_body
    if written:
        moment = (now or datetime.now().astimezone()).isoformat(timespec="seconds")
        payload = {"version": INDEX_VERSION, "updated_at": moment, **new_body}
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
            # 固定 LF：换行风格属于平台，不属于内容；写死它，`git diff` 里就不会
            # 每次只有行尾在动。
            newline="\n",
        )

    return SyncReport(
        doc_created=doc_created,
        added=tuple(added),
        dropped=tuple(dropped),
        refreshed=tuple(refreshed),
        written=written,
    )


# ---------------------------------------------------------------- 交付档案用的指纹


def _package_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("hf-data-crawl")
    except PackageNotFoundError:
        return "unknown"


def _commit() -> str | None:
    """当前代码的提交号，取不到就返回 None。

    这是尽力而为：包可能装在只读环境、可能不在 git 仓库里。取不到时如实记 None，
    不要编一个看起来像提交号的东西——档案里最怕的就是「像真的」的假值。
    """
    start = Path(__file__).resolve().parent
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            try:
                done = subprocess.run(
                    ["git", "rev-parse", "HEAD"],
                    cwd=candidate,
                    capture_output=True,
                    text=True,
                    timeout=3,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError):
                return None
            text = done.stdout.strip()
            return text or None
    return None


def env() -> dict[str, Any]:
    """脚本依赖的环境。不复制源码，只钉版本——变了就由 doctor 报出来。"""
    return {
        "package": _package_version(),
        "contract_version": CONTRACT_VERSION,
        "commit": _commit(),
    }


def stamp(request_dir: Path, method: Method) -> dict[str, Any]:
    """写进交付档案的 `method` 段：文档、索引、逐脚本指纹、被钉住的中间件、环境。"""
    pinned: dict[str, Any] = {}
    for name in PINNED_INPUTS:
        path = request_dir / name
        if path.is_file():
            pinned[name] = {"bytes": path.stat().st_size, "sha256": digest(path)}
    return {
        "doc": {"path": f"{METHOD_DIR}/{METHOD_DOC}", "sha256": method.doc_sha256},
        "index": {"path": f"{METHOD_DIR}/{METHOD_INDEX}", "sha256": method.index_sha256},
        "scripts": [
            {"name": entry.name, "purpose": entry.purpose, "sha256": entry.sha256}
            for entry in method.scripts
        ],
        "inputs": pinned,
        "env": env(),
    }
