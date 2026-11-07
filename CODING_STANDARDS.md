# Coding Standards

本文档记录需要人工判断的编码标准。机械可检查的规则已由自动化工具强制执行（ruff、mypy、pre-commit）。

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

启动时检查并恢复。见 `deriving.py` 和 ADR-0001。

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

见 `watermark.py` 的时间戳过滤（过滤未来 7 天以外的异常值）。

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
