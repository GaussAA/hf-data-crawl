# Coding Standards

本文档记录需要人工判断的编码标准。机械可检查的规则已由自动化工具强制执行（ruff、mypy、pre-commit）。

---

## 工具链口径

检查只有一个入口，工具版本全部由 `uv.lock` 锁定，pre-commit 与 GitHub Actions
调用同一组命令；CI 里任何检查都不用 `continue-on-error` 掩盖失败。

| 检查 | 命令 | 范围 |
| --- | --- | --- |
| 类型 | `uv run mypy` | `src` + `tests`（`[tool.mypy] files`） |
| 类型（补充） | `uv run pyright` | `src`（`[tool.pyright] include`） |
| 静态 | `uv run ruff check .` / `uv run ruff format --check .` | 全仓 |
| 测试 | `uv run pytest -q` | 全量 |
| 文档链接 | `python scripts/check_docs_links.py` | 全仓 Markdown |

类型错误必须改真实类型（显式注解、Protocol、让类型承载不变式），
不得用 `type: ignore` 或临时豁免绕过。

**新增工具的收尾清单**：工具装进 `uv.lock` 后，须同步三处——
`pyproject.toml` 的检查范围、pre-commit 与 CI 的调用命令、根目录
`.gitignore` 的缓存目录。`tests/test_gitignore.py` 的 `TOOL_CACHES`
是这份清单的机器可读版本，新增工具一并登记。

缓存目录只靠工具自建的目录内 `.gitignore` 不够（mypy 就会这么做）：
那是工具行为，不是仓库契约；换 `--cache-dir` 或工具版本行为变化时就会漏。
而缓存里往往带本机绝对路径（实测 `.mypy_cache` 有 1017/1020 个文件含
`C:\Users\...`），一旦入库便永久留在历史里，清理需改写历史并强推。

---

## 测试命名

函数名使用英文，docstring 使用中文：

```python
def test_reasonable_timestamp() -> None:
    """测试合理的时间戳。"""
    # ...
```

**不要**：`def test_合理时间戳()` — 函数名是标识符（ASCII），docstring 是文档（中文）

---

## 错误处理

多文件操作使用标记文件保护原子性：

```python
mark_operation(store, date)  # 写标记
try:
    update_files()
    clear_operation(store, date)  # 删除标记
except Exception:
    raise  # 保留标记供下次恢复
```

启动时检查并恢复。见 `pipeline/marker.py` 和 ADR-0001。

---

## 数据验证

外部 API 数据必须验证合理性，过滤异常值：

```python
def _is_reasonable(value):
    # 检查范围、格式、时间戳等
    if is_anomaly(value):
        return False
    return True
```

见 `acquire/hf/watermark.py` 的时间戳过滤（过滤未来 7 天以外的异常值）。

---

## 新增术语

新术语先入 `GLOSSARY.md` 再使用：

```python
def foo():
    """做 XXX（见 GLOSSARY #30）。"""
```

---

## 派生顺序

派生层有硬性依赖，必须按顺序：bronze → silver → observations → current

**不要**改变顺序或并行执行。见 GLOSSARY #4。

---

## Review 职责

- **实现 Agent**：探索、编写、调试（上下文压力大）
- **Review Agent**：审查 diff，强制本文档标准（上下文压力小）

**Review 清单**：
- [ ] 测试函数名是否用英文
- [ ] 新术语是否已在 GLOSSARY 定义
- [ ] 派生顺序是否正确
- [ ] 异常值是否已过滤

机械检查（代码格式、导入排序、测试通过）已由自动化工具完成，**不需要**在 Review 中重复。
