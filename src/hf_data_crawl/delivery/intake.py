"""需求受理：把「建一份需求」从命令层提出来，收成一次调用。

命令层只该回答「参数对不对、打印什么」，不该亲自建目录、拷快照、失败回滚。
这条用例横跨 spec 骨架（`requirement`）与方法记录（`method`），放在两者之外，
CLI、将来的批量导入与测试才能走同一个入口。

失败一律抛 `IntakeError`：它带给人看的中文原因，命令层只负责加「失败：」前缀。
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from ..platform.atomic import write_bytes
from .header import HeaderError
from .method import sync as sync_method
from .requirement import (
    INPUT_DIR,
    SPEC_NAME,
    Cadence,
    Kind,
    SpecError,
    directory_name,
    load,
    skeleton,
    write_status,
)
from .sheet import first_sheet_name


class IntakeError(ValueError):
    """受理不成立：编号重复、输入表缺失、参数与 kind 不配，或建到一半失败。"""


@dataclass(frozen=True)
class CreatedRequest:
    """一份刚建好的需求。`snapshot` 是输入快照落点，查询型为 None。

    `sheet_name` 是从源表读到的第一个 sheet 名，已写进骨架；查询型为空串。
    带上它，命令层才能如实说「填的是哪一页」，而不是笼统地说「已填好」。
    """

    folder: Path
    kind: Kind
    snapshot: Path | None
    sheet_name: str = ""


def create_request(
    root: Path,
    *,
    request_id: str,
    title: str,
    requester: str,
    purpose: str = "",
    cadence: Cadence = Cadence.ONCE,
    kind: Kind = Kind.QUERY,
    source: str | None = None,
    header_row: int | None = None,
    sample: bool = False,
    now: datetime | None = None,
) -> CreatedRequest:
    """建一份需求：目录语义化，外部输入表当场复制进目录，之后与外部再无瓜葛。

    目录名带语义后缀，判重不能只看同名目录，得看编号前缀——否则同一个 REQ-0002
    换个标题就能再建一份，编号的唯一性就废了。

    `now` 只为测试可注入；样例标记的时刻取它，其余时间戳不在此处产生。
    """
    if header_row is not None:
        if kind is not Kind.BACKFILL:
            raise IntakeError("--header-row 只对 kind=backfill 有意义——查询型没有输入表。")
        if header_row < 1:
            raise IntakeError("--header-row 从 1 起算。")

    folder = root / directory_name(request_id, title, kind)
    taken = (
        [
            path
            for path in root.iterdir()
            if path.is_dir() and (path.name == request_id or path.name.startswith(f"{request_id}-"))
        ]
        if root.is_dir()
        else []
    )
    if taken or folder.exists():
        names = "、".join(path.name for path in taken) or folder.name
        raise IntakeError(
            f"需求号 {request_id} 已有目录（{names}）。要改口径请直接编辑它并把 version 加一。"
        )

    given: Path | None = None
    sheet_name = ""
    if kind is Kind.BACKFILL:
        if not source:
            raise IntakeError("kind=backfill 需要 --source 指向对方给的原始表格（文件须已存在）。")
        given = Path(source).expanduser()
        if not given.is_file():
            raise IntakeError(f"输入表不存在或不是文件：{given}")
        # 趁文件在手边把 sheet 名读出来写进骨架。放到「交付时再说」的代价是：
        # 骨架里的占位一旦与实际不符，`request input` 读不到旧表头，字段差异只剩
        # 一半，而那时人已经离开受理现场，得回去翻对方给的原表。
        try:
            sheet_name = first_sheet_name(given)
        except HeaderError as error:
            raise IntakeError(f"读不出输入表的 sheet 名：{error}") from error

    folder.mkdir(parents=True)
    snapshot: Path | None = None
    try:
        if given is not None:
            # 取材一次，取完即断：外部文件之后被删被改，都影响不到这份需求。
            snapshot = folder / INPUT_DIR / given.name
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            write_bytes(snapshot, given.read_bytes())
        target = folder / SPEC_NAME
        target.write_text(
            skeleton(
                request_id=request_id,
                title=title,
                requester=requester,
                purpose=purpose,
                cadence=cadence,
                kind=kind,
                source_path=given.name if given else "",
                header_row=header_row,
                sheet_name=sheet_name,
            ),
            encoding="utf-8",
        )
        # 建完立刻按真规矩读一遍：不合法就整目录收走，好过等下次 run 才发现。
        load(target)
        # 过程记录一并建好骨架：新需求从第一天起就有 method/ 这个坑位，
        # 不必等人想起来该记「怎么做的」——想不起来，是这类记录最常见的死法。
        sync_method(folder, request_id=request_id, title=title, kind=kind.value)
        if sample:
            # 样例标记从建目录那一刻就写下：等它有了交付再补，那之前的台账就已经
            # 把夹具当真实需求算过一次影响面了。
            moment = now or datetime.now().astimezone()
            write_status(
                folder,
                sample=True,
                sample_reason="开发与验证用，不是真实需求（--sample 创建）",
                sample_marked_at=moment.isoformat(timespec="seconds"),
            )
    except (SpecError, OSError) as error:
        shutil.rmtree(folder, ignore_errors=True)
        raise IntakeError(str(error)) from error
    return CreatedRequest(folder=folder, kind=kind, snapshot=snapshot, sheet_name=sheet_name)
