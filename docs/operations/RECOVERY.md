# 故障恢复

## 自动恢复机制

### 派生中断自动恢复

启动写命令时，会自动检查是否有未完成的派生（标记超过 10 分钟）。

**观察**：
```
⚠️  检测到 1 个未完成的派生，已自动恢复：[date(2026, 11, 7)]
   提示：这些日期的派生层已清理，下次运行时会重新派生
```

**手动检查**：
```bash
ls data/state/deriving-*.flag
```

---

## 常见故障

### 1. 采集中断

**恢复**：直接重新运行，会从断点续跑
```bash
uv run hf-data-crawl collect
```

### 2. 对账中断

**恢复**：直接重新运行，会从断点续跑
```bash
uv run hf-data-crawl reconcile
```

### 3. 派生层不一致

**自动**：下次写命令会自动恢复（如果标记文件还在且超过 10 分钟）

**手动**：
```bash
uv run hf-data-crawl build  # 从 raw 重建所有派生层
```

### 4. Watermark 异常

**症状**：collect 过早停止

**说明**：正常完成不会清除水线文件（否则下轮时间戳边界失效），此处的 `rm` 是人工复位手段，仅在确认水线被异常时间戳污染时使用。

**恢复**：
```bash
rm data/state/collect-watermark.txt
uv run hf-data-crawl collect --pages-without-new 5
```

### 5. 锁文件冲突

**症状**：
```
数据目录正被另一个写命令占用
```

**恢复**：
- 等待其他命令完成
- 或使用 `--lock-wait 900`（最多等 15 分钟）

---

## 诊断工具

### 查看运行记录
```bash
cat data/runs/collect-*.json | tail -n 5
```

### 查看当前状态
```bash
uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current"
uv run hf-data-crawl query --sql "SELECT status, COUNT(*) FROM current GROUP BY status"
```

### 检查数据完整性
```bash
ls data/raw/dt=*
uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM bronze WHERE dt='2026-11-07'"
```

---

## 维护建议

- **每日**：检查 collect 是否成功
- **每周**：运行 reconcile 全量对账
- **每月**：运行 build 重建所有派生层
