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

## Raw 归档与备份清理

**症状**：`doctor` 报「Raw 归档积压」或「backup/raw 超过 7 天未清理」。

**归档**（每周计划任务自动执行；也可手动指定某一天）：
```bash
uv run hf-data-crawl archive --older-than-days 30
uv run hf-data-crawl archive --date 2026-11-07
```
归档先验证前后 Bronze 逐行相等，不等则撤销、原文件原地不动；原文件移入
`data/backup/raw/dt=<日期>/`，**不删除**。归档分片的 `groups` 冻结了真实写入序，
此后 `build` 不再依赖文件 mtime。

**回滚**（对归档结果有疑问时，把原分块移回并删除分片）：
```powershell
$day = '2026-11-07'
Move-Item "data\backup\raw\dt=$day\*" "data\raw\dt=$day\"
Remove-Item "data\raw\dt=$day\*-archive-*.json.gz"
```
执行前务必确认 backup 中该日原分块齐全，否则该日观测点会丢。

**确认后清理**（不可逆，须人工执行）：先跑一次 `build` 或核对 Bronze，确认派生无误：
```powershell
Remove-Item -Recurse "data\backup\raw\dt=$day"
```

---

## 维护建议

- **每日**：检查 collect 是否成功
- **每周**：reconcile 全量对账；archive 自动归档满 30 天的 Raw，确认无误后清理 backup
- **每月**：运行 build 重建所有派生层
