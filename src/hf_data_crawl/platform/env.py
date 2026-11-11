"""本机私有配置的加载：仓库根 `.env`。

只做一件事——把 `KEY=VALUE` 塞进 `os.environ`，且**绝不覆盖已存在的变量**：
命令行显式 `HF_TOKEN=... cmd`、系统环境或 CI 注入的值永远优先，免得一份本地
文件悄悄盖掉调用方本意（`HF_PROXY` 被本地代理顶掉这类事故极难排查）。

文件缺失、不可读、行格式不对都只跳过、不抛错：它是可选便利，不该让任何命令
因为它起不来。加载结果只回报**键名**，值绝不打印、绝不进 `runs/` 摘要——
令牌一旦落进日志或运行记录，忽略规则就形同虚设。
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from pathlib import Path

#: 私密配置的文件名。根 `.gitignore` 已登记，实现在 `tests/test_env.py` 里钉死。
ENV_FILENAME = ".env"

#: 允许的变量名形态：拒绝 `1BAD=x`、`带空格 的键=x` 这类脏行，免得污染 os.environ。
_KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

_QUOTES = ("'", '"')


def default_env_path() -> Path:
    """默认取**当前工作目录**下的 `.env`：命令一律在仓库根执行，不做父目录回溯。"""
    return Path.cwd() / ENV_FILENAME


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in _QUOTES:
        return value[1:-1]
    return value


def _parse(text: str) -> Iterator[tuple[str, str]]:
    """逐行解析 `KEY=VALUE`：跳过空行与注释，容忍 `export ` 前缀与成对引号。"""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        key, separator, value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        if not _KEY_PATTERN.fullmatch(key):
            continue
        yield key, _unquote(value.strip())


def load_env_file(path: Path | None = None) -> tuple[str, ...]:
    """把文件里的键值补进环境，返回**实际写入的键名**（不含值，可安全打日志）。"""
    target = default_env_path() if path is None else path
    try:
        text = target.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        # OSError 覆盖不存在/是目录/无权限；编码坏掉也不值得让命令失败。
        return ()

    applied: list[str] = []
    for key, value in _parse(text):
        if key in os.environ:
            continue
        os.environ[key] = value
        applied.append(key)
    return tuple(applied)
