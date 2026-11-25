"""组合根必须先补 `.env`：配置读不到不会报错，只会默默按缺省值办。

2026-11-24 的实证：`.env` 里写着 `HF_FILL_ENABLED=0`，链尾兜底段照样跑了 100 分钟。
原因是只有 `acquire` 的两三个命令自己调过 `load_env_file()`，`refresh` 这条链上一个
都没调——编排器进程里 `HF_*` 全是「没配」，于是班次起点、兜底开关、让路窗口三样
全部退化成缺省值，而且一句提示都没有。
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import hf_data_crawl.ops.cli as cli


def test_命令入口先补dotenv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """顺序不能反：补 .env 必须早于任何读配置的代码。"""
    order: list[str] = []

    def record_env() -> tuple[str, ...]:
        order.append("env")
        return ()

    def record_execute(*_args: object, **_kwargs: object) -> int:
        order.append("execute")
        return 0

    monkeypatch.setattr(cli, "load_env_file", record_env)
    monkeypatch.setattr(cli, "_execute", record_execute)

    cli.main(["--data-dir", str(tmp_path), "report"])

    assert order == ["env", "execute"]


def test_真的读到了dotenv里的值(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """端到端确认：工作目录里的 `.env` 能影响同一个进程里后续的配置读取。"""
    (tmp_path / ".env").write_text("HF_FILL_ENABLED=0\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HF_FILL_ENABLED", raising=False)

    def stop(*_args: object, **_kwargs: object) -> int:
        return 0

    monkeypatch.setattr(cli, "_execute", stop)

    cli.main(["--data-dir", str(tmp_path), "report"])

    assert os.environ["HF_FILL_ENABLED"] == "0"
