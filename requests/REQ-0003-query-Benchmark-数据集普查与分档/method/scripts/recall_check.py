"""12 个公认榜单的召回自检。

口径改动不会让查询报错，只会让某些榜单静默掉进 U 档——本脚本就是那道把门的检查。
判据：任一标杆的 tier 落在 U（证据不足）即视为口径失效，退出码 1。

用法（仓库根目录）：
    uv run python requests/REQ-0003-*/method/scripts/recall_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

# 12 个标杆：跨文本多选 / 数学 / 代码 / Agent / 无划分形态各取代表。
# 特意纳入 SWE-bench（无 test 划分）与 AIME_2024（30 行、无描述）——这两类
# 只有名录档能救，正是「必须保留 L2」的证据。
ANCHORS: tuple[str, ...] = (
    "cais/mmlu",
    "openai/gsm8k",
    "truthfulqa/truthful_qa",
    "allenai/ai2_arc",
    "Rowan/hellaswag",
    "TIGER-Lab/MMLU-Pro",
    "Idavidrein/gpqa",
    "lukaemon/mmlu",
    "HuggingFaceH4/MATH-500",
    "Maxwell-Jia/AIME_2024",
    "openai/openai_humaneval",
    "SWE-bench/SWE-bench",
)

ACCEPTED = {"L1", "L2"}


def repo_root() -> Path:
    """向上找 pyproject.toml：脚本可从任意工作目录被调用。"""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise SystemExit("找不到仓库根（pyproject.toml）")


def load_sql(root: Path) -> str:
    """取本需求 spec.yaml 里的 sql 字段——自检的必须是被交付的那条查询。"""
    spec_path = Path(__file__).resolve().parents[2] / "spec.yaml"
    spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    sql = (spec or {}).get("sql")
    if not sql or not str(sql).strip():
        raise SystemExit(f"{spec_path} 里没有 sql，形态应为 mode: sql")
    return str(sql).strip()


def main() -> int:
    root = repo_root()
    sys.path.insert(0, str(root / "src"))

    from hf_data_crawl.platform.store import DatasetStore
    from hf_data_crawl.query.gold import GoldCatalog

    sql = load_sql(root)
    catalog = GoldCatalog(DatasetStore(root / "data"))
    frame = catalog.run(sql).select("id", "判定档")

    hit = frame.filter(frame["id"].is_in(ANCHORS))
    found = {row["id"]: row["判定档"] for row in hit.iter_rows(named=True)}

    missed: list[str] = []
    for anchor in ANCHORS:
        tier = found.get(anchor)
        if tier is None:
            missed.append(f"{anchor}（底库中不存在该 id）")
            print(f"  MISS  {anchor:<30} 底库无此 id")
            continue
        print(f"  {tier:<5} {anchor:<30} {'ok' if tier in ACCEPTED else '不达标'}")
        if tier not in ACCEPTED:
            missed.append(f"{anchor}（{tier}）")

    print(f"\n召回 {len(ANCHORS) - len(missed)}/{len(ANCHORS)}，总行数 {frame.height}")
    if missed:
        print("未达 L1/L2 的标杆：" + "、".join(missed))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
