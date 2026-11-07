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

### 逐步修复 Mypy 类型错误
```bash
# 当前有 14 个类型错误，可以逐步修复
uv run mypy src/hf_data_crawl/watermark.py
uv run mypy src/hf_data_crawl/deriving.py
```

### 监控 CI 性能
如果 CI 运行时间过长，考虑：
- 缓存依赖
- 并行运行测试
- Windows CI 改为可选

---

## 文档维护

- GLOSSARY.md：新增术语时更新
- ADR：重大架构决策时添加
- CODING_STANDARDS.md：发现新规则时更新
