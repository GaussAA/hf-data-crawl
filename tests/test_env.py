"""本机私密配置加载的契约测试。

钉住四件事，任一失守都会让「凭据只在本机」这个承诺悄悄失效：

1. 解析只认 `KEY=VALUE`，注释/空行/脏行一律跳过，脏了不许污染 `os.environ`；
2. **已存在的环境变量优先**——命令行或 CI 显式给的值，不能被本地文件盖掉；
3. 文件缺失、坏编码都只是静默跳过，不能让任何命令因为它起不来；
4. `.env` 必须在根 `.gitignore` 显式登记，且真的被 git 忽略。
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import cast

from hf_data_crawl.acquire.commands import _open_transport
from hf_data_crawl.acquire.hf.api import Transport
from hf_data_crawl.platform.env import ENV_FILENAME, default_env_path, load_env_file

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / ENV_FILENAME
    path.write_text(text, encoding="utf-8")
    return path


def test_默认路径取当前工作目录(tmp_path: Path, monkeypatch) -> None:
    """命令一律在仓库根执行，故不做父目录回溯——回溯会让「加载了哪份」难以解释。"""
    monkeypatch.chdir(tmp_path)

    assert default_env_path() == tmp_path / ENV_FILENAME


def test_解析基本键值并跳过注释空行(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("HF_TEST_ALPHA", raising=False)
    monkeypatch.delenv("HF_TEST_BETA", raising=False)
    path = _write(
        tmp_path,
        "# 注释行\n\nHF_TEST_ALPHA=alpha-value\nexport HF_TEST_BETA=beta-value\n",
    )

    applied = load_env_file(path)

    assert applied == ("HF_TEST_ALPHA", "HF_TEST_BETA")
    assert os.environ["HF_TEST_ALPHA"] == "alpha-value"
    assert os.environ["HF_TEST_BETA"] == "beta-value"


def test_成对引号被剥掉(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("HF_TEST_QUOTED", raising=False)
    path = _write(tmp_path, 'HF_TEST_QUOTED="hf_quoted"\n')

    load_env_file(path)

    assert os.environ["HF_TEST_QUOTED"] == "hf_quoted"


def test_已存在的环境变量优先(tmp_path: Path, monkeypatch) -> None:
    """本地文件不许盖掉调用方显式给的值，否则 CI 与临时覆盖都会莫名失效。"""
    monkeypatch.setenv("HF_TEST_KEEP", "from-shell")
    path = _write(tmp_path, "HF_TEST_KEEP=from-file\n")

    applied = load_env_file(path)

    assert os.environ["HF_TEST_KEEP"] == "from-shell"
    assert "HF_TEST_KEEP" not in applied


def test_文件缺失只是跳过(tmp_path: Path) -> None:
    assert load_env_file(tmp_path / "不存在.env") == ()


def test_坏编码不抛错(tmp_path: Path) -> None:
    """cp1252 之类写坏的文件不该让采集命令起不来（Windows 上真出过同类事故）。"""
    path = tmp_path / ENV_FILENAME
    path.write_bytes(b"HF_TEST_X=\xff\xfe\x00bad\n")

    assert load_env_file(path) == ()


def test_脏行被跳过(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("HF_TEST_CLEAN", raising=False)
    path = _write(
        tmp_path,
        "没有等号的行\n1BAD=leading-digit\n带 空格=rubbish\nHF_TEST_CLEAN=ok\n",
    )

    applied = load_env_file(path)

    assert applied == ("HF_TEST_CLEAN",)
    assert "1BAD" not in os.environ
    assert "带 空格" not in os.environ


def test_返回值只含键名不含值(tmp_path: Path, monkeypatch) -> None:
    """返回的东西可能被打进日志，故必须只有键名——漏了值，令牌就进了运行摘要。"""
    monkeypatch.delenv("HF_TEST_SECRET", raising=False)
    path = _write(tmp_path, "HF_TEST_SECRET=hf_s3cr3t_value\n")

    applied = load_env_file(path)

    assert applied == ("HF_TEST_SECRET",)
    assert all("s3cr3t" not in item for item in applied)


def test_env_在根_gitignore_显式登记且确实被忽略() -> None:
    """只靠约定不算数：规则要在文件里，git 也要真的判为忽略。"""
    rules = {
        line.strip().rstrip("/")
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert ENV_FILENAME in rules, f"根 .gitignore 未登记 {ENV_FILENAME}"

    result = subprocess.run(
        ["git", "check-ignore", "-q", ENV_FILENAME],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, f"{ENV_FILENAME} 未被 git 忽略"


def test_构造真实传输时才加载凭据(tmp_path: Path, monkeypatch) -> None:
    """凭据只在真联网那一刻读盘，`main` 开头统一加载会污染注入假传输的用例。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HF_TEST_TOKEN", raising=False)
    _write(tmp_path, "HF_TEST_TOKEN=loaded-here\n")

    _open_transport(None)

    assert os.environ["HF_TEST_TOKEN"] == "loaded-here"


def test_注入传输时不读本机凭据(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("HF_TEST_TOKEN", raising=False)
    _write(tmp_path, "HF_TEST_TOKEN=should-not-load\n")
    injected = cast(Transport, object())

    assert _open_transport(injected) is injected
    assert "HF_TEST_TOKEN" not in os.environ
