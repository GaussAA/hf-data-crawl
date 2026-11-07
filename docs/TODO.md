# TODO

## 可选改进（优先级低）

### Q10: 快照损坏诊断工具
如果遇到快照损坏，可以添加诊断工具：
```bash
uv run hf-data-crawl doctor --fix-snapshot
```

### Q5: 观测覆盖前行数警告
如果观测行数突然减少，发出警告：
```python
if after_count < before_count * 0.5:
    logger.warning("观测行数大幅减少，可能丢失数据")
```

### Q9: 探测失败降级
记录探测失败历史，连续 3 轮失败 → 单独报告

---

## 自动化改进

### 类型检查维护
类型检查已是单一入口，工具版本由 `uv.lock` 锁定，口径见
[CODING_STANDARDS.md](../CODING_STANDARDS.md) 的「工具链口径」：

```bash
uv run mypy      # src + tests，当前 0 错
uv run pyright   # src 补充检查
```

新增模块若出现类型错误，须改真实类型（显式注解、Protocol、让类型承载不变式），
不得用 `type: ignore` 或临时豁免绕过。

### 监控 CI 性能
如果 CI 运行时间过长，考虑：
- 缓存依赖
- 并行运行测试

**不要移除 Windows job**：它曾抓出 cp1252 控制台下打印中文导致的
`UnicodeEncodeError`（文档链接校验器「校验通过」被误判为失败），本机 UTF-8
终端复现不到，见 `scripts/check_docs_links.py`。

---

## 文档维护

- GLOSSARY.md：新增术语时更新
- ADR：重大架构决策时添加
- CODING_STANDARDS.md：发现新规则时更新
