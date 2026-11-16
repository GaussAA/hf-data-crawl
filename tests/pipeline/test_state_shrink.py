"""观测骤降的判据：重放路径下「这轮之前有多少行」只能由调用方在清理前传来。

原先 `carry` 分支把 `before` 硬写成 0，护栏在最需要它的时候——`build` 一次性
清掉 7 天分区再逐日重写——结构性失效。而重放是唯一会批量覆盖观测的路径，
缺一批 Raw 就会把历史观测静默删掉，且删掉之后不留任何痕迹。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from hf_data_crawl.pipeline.state import Carry, update
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import BRONZE_SCHEMA, CURRENT_SCHEMA

DAY = date(2026, 11, 10)

#: `_rebuild_current` 会把 Bronze 的带入列整体搬进 current，所以当日帧必须带全。
_CARRIED = {name: kind for name, kind in BRONZE_SCHEMA.items() if name in CURRENT_SCHEMA}


def _frame() -> pl.DataFrame:
    """当日帧：一行，指标与锚点不同，因此必然产生一条观测。"""
    row: dict[str, object] = dict.fromkeys(_CARRIED)
    row.update(
        {
            "id": "a/x",
            "url": "https://huggingface.co/datasets/a/x",
            "author": "a",
            "sha": "s",
            "downloads": 1,
            "downloads_all_time": 1,
            "likes": 1,
        }
    )
    return pl.DataFrame([row], schema=_CARRIED)


def _carry() -> Carry:
    """重放顺推的锚点：该 id 上一日三个指标都是 0。"""
    return Carry(
        previous=pl.DataFrame(
            {
                "id": ["a/x"],
                "downloads_prev": [0],
                "downloads_all_time_prev": [0],
                "likes_prev": [0],
            }
        ),
        current=pl.DataFrame(schema=CURRENT_SCHEMA),
    )


def test_重放时传了旧行数才判得出骤降(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)

    result = update(store, DAY, frame=_frame(), carry=_carry(), observation_before=100)

    assert result.shrink == (100, 1)


def test_没传旧行数时不误报(tmp_path: Path) -> None:
    """拿不到旧行数就退回 0：宁可漏报一次，也不要拿错尺子天天报警。"""
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)

    result = update(store, DAY, frame=_frame(), carry=_carry())

    assert result.shrink is None


def test_行数没掉一半不算骤降(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)

    result = update(store, DAY, frame=_frame(), carry=_carry(), observation_before=2)

    assert result.shrink is None
