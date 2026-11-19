"""性能与体积基准：只打印，不判红。

为什么单独一个脚本
------------------
这些数字回答的是「改压缩参数、改分区、换存储方式之后，究竟变好还是变坏」。
把它放进 `doctor` 会让每次体检多花几十秒，还会写进体检流水；放进 pytest 又会
把「基准」与「断言」混成一种东西——基准本来就会随机器、负载、数据量漂，硬阈值
只会训练人忽略红。

所以它是一份**可手动重跑的报告**：跑一次，把输出的 Markdown 贴进
`docs/operations/PERFORMANCE.md` 对应日期的段落，与上一次比。不 exit 1。

用法：
    uv run python scripts/tools/bench.py
    uv run python scripts/tools/bench.py --data-dir path/to/data
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pyarrow.parquet as pq

from hf_data_crawl.delivery.fields import projection_sql
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import TABLES
from hf_data_crawl.query.gold import GoldCatalog

#: 需要报告的目录（相对数据目录）。
LAYERS = ("raw", "cold", "backup", "bronze", "silver", "state", "enrich", "exports", "reports")

#: 表体积报告的对象：数据量最大的几张，以及热路径上的全部。
#: 只放登记表——`exports/` 不在 `TABLES` 里（它是可重建投影），体积在上面的
#: 「各层体积」表里报。
WATCHED_TABLES = ("current", "silver", "observations", "readme", "size", "carddata")

#: 查询基准：标签 → SQL。榜单走 `board()`，其余走 `run()`。
BOARDS = ("growth", "downloads", "modalities")
QUERIES = (
    ("视图 count(*)", "SELECT count(*) FROM datasets"),
    ("readme 按 id 查", "SELECT id, bytes FROM readme WHERE id = '0-hero/MATH'"),
)

REPEAT = 3


def tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def layer_rows(root: Path) -> list[tuple[str, int]]:
    return [(name, tree_bytes(root / name)) for name in LAYERS]


def table_rows(root: Path) -> list[tuple[str, int, int, int]]:
    """(表名, 行数, 压缩后字节, 原始字节)。分区表取全部分区之和。"""
    rows: list[tuple[str, int, int, int]] = []
    for name in WATCHED_TABLES:
        spec = TABLES[name]
        files = sorted(root.glob(spec.glob())) if spec.partitioned else [root / spec.path]
        files = [path for path in files if path.exists()]
        if not files:
            rows.append((name, 0, 0, 0))
            continue
        total_rows = compressed = uncompressed = 0
        for path in files:
            metadata = pq.ParquetFile(path).metadata
            total_rows += metadata.num_rows
            for group in range(metadata.num_row_groups):
                row_group = metadata.row_group(group)
                for column in range(row_group.num_columns):
                    compressed += row_group.column(column).total_compressed_size
                    uncompressed += row_group.column(column).total_uncompressed_size
        rows.append((name, total_rows, compressed, uncompressed))
    return rows


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[len(ordered) // 2]


def timings(catalog: GoldCatalog, workdir: Path) -> list[tuple[str, float]]:
    """每条基准跑 REPEAT 次取中位数；导出只跑一次（要写 119 MB）。"""
    results: list[tuple[str, float]] = []
    for name in BOARDS:
        spent = []
        for _ in range(REPEAT):
            start = time.perf_counter()
            catalog.board(name, 50)
            spent.append(time.perf_counter() - start)
        results.append((f"榜单 {name}", _median(spent)))

    for label, sql in QUERIES:
        spent = []
        for _ in range(REPEAT):
            start = time.perf_counter()
            catalog.run(sql)
            spent.append(time.perf_counter() - start)
        results.append((label, _median(spent)))

    target = workdir / "exports" / "_bench.parquet"
    target.unlink(missing_ok=True)
    try:
        start = time.perf_counter()
        catalog.copy_wide(target, projection=projection_sql())
        results.append(("COPY 全量 31 列导出", time.perf_counter() - start))
    finally:
        target.unlink(missing_ok=True)
    return results


def markdown(store: DatasetStore) -> str:
    root = store.root
    lines: list[str] = [f"# bench · {store.root}", ""]

    lines += ["## 各层体积", "", "| 目录 | 体积 |", "| --- | --- |"]
    for name, size in layer_rows(root):
        lines.append(f"| `{name}` | {size / 1e6:.1f} MB |")

    lines += [
        "",
        "## 表体积与压缩率",
        "",
        "| 表 | 行数 | 压缩后 | 原始 | 压缩率 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for name, rows, compressed, uncompressed in table_rows(root):
        if rows == 0:
            lines.append(f"| `{name}` | 0 | — | — | — |")
            continue
        ratio = compressed / uncompressed if uncompressed else 0.0
        lines.append(
            f"| `{name}` | {rows:,} | {compressed / 1e6:.1f} MB | "
            f"{uncompressed / 1e6:.1f} MB | {ratio:.1%} |"
        )

    catalog = GoldCatalog(store)
    try:
        measured = timings(catalog, root)
    finally:
        catalog.close()

    lines += ["", "## 查询与导出耗时（3 次中位数）", "", "| 基准 | 耗时 |", "| --- | --- |"]
    for label, elapsed in measured:
        lines.append(f"| {label} | {elapsed:.2f}s |")
    lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="性能与体积基准：只打印，不判红")
    parser.add_argument("--data-dir", default="data", help="数据目录，默认 data")
    args = parser.parse_args(argv)

    print(markdown(DatasetStore(Path(args.data_dir))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
