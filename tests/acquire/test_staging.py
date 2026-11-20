"""四条长耗时抓取链路的暂存目录契约。

分片先落 `data/.staging/<链路>/`，收尾才并进正式表；停在中途时 doctor 的
「抓取暂存」检查靠这个路径发现残留，清理也按它找。目录名因此必须真的落在
`.staging` 下——`batch.staging_root()` 只做 `store.root / subdir`，传裸目录名
（如 `"preview"`）会把分片丢到 `data/preview/`，上面两处都看不见它。

2026-11-20 测试审计发现 preview 正是这么写的：旧用例只比了 readme 与 sizes，
「按链路分开」的名字下没有一条判据管 preview。
"""

from __future__ import annotations

from pathlib import Path

from hf_data_crawl.acquire.batch import STAGING_ROOT, staging_root
from hf_data_crawl.acquire.files import FILES_STAGING_DIR
from hf_data_crawl.acquire.preview import PREVIEW_STAGING_DIR
from hf_data_crawl.acquire.readme import README_STAGING_DIR
from hf_data_crawl.acquire.sizes import STAGING_DIR as SIZES_STAGING_DIR
from hf_data_crawl.platform.store import DatasetStore

#: 链路名 → 暂存目录（相对数据根）。
STAGING_DIRS: dict[str, str] = {
    "sizes": SIZES_STAGING_DIR,
    "files": FILES_STAGING_DIR,
    "readme": README_STAGING_DIR,
    "preview": PREVIEW_STAGING_DIR,
}


def test_暂存目录都落在staging根下(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path)
    staging = store.root / STAGING_ROOT

    for chain, directory in STAGING_DIRS.items():
        resolved = staging_root(store, directory)
        assert resolved.is_relative_to(staging), f"{chain} 的分片落在 {resolved}，不在 {staging}"
        assert resolved != staging, f"{chain} 不该直接写在 .staging 根下"


def test_每条链路各占一个暂存目录(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path)
    resolved = [staging_root(store, directory) for directory in STAGING_DIRS.values()]

    assert len(set(resolved)) == len(resolved), "两条链路共用一个暂存目录，会互相清掉分片"
