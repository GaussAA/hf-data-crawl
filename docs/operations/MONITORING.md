# 监控指标

## 核心指标

### 1. Watermark 触发统计

**采集方法**：
```bash
jq -r '.summary.stopped_reason' data/runs/collect-*.json | tail -7 | sort | uniq -c
```

**正常范围**：
- `end`: 60-80%
- `watermark_timestamp`: < 10%
- `watermark_no_new`: 10-30%

**告警**：`watermark_timestamp` > 50%

---

### 2. 派生恢复次数

**采集方法**：
```bash
grep "已自动恢复" data/logs/$(date +%Y-%m-%d).log | wc -l
```

**正常范围**：0 次/天

**告警**：> 0 次/天

---

### 3. 无法检测变更的库数量

**采集方法**：
```bash
grep "undetectable_change" data/reports/reconcile-*.csv | tail -1 | wc -l
```

**正常范围**：< 5% (相对 current 总行数)

**告警**：> 20%

---

### 4. Collect 执行时间

**采集方法**：
```bash
jq -r '.elapsed_seconds' data/runs/collect-*.json | tail -7
```

**正常范围**：60-300 秒（日增量）

**告警**：> 600 秒

---

### 5. Current 表行数

**采集方法**：
```bash
uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current" --format tsv
```

**正常范围**：110 万行左右，每天 +100 ~ +500 行

**告警**：骤降 > 10%

---

### 6. 命令成功率

**采集方法**：
```bash
jq -r '.status' data/runs/collect-*.json | tail -30 | grep -c "success"
```

**正常范围**：> 95%

**告警**：< 90% 或连续 3 次失败

---

### 7. Raw 规模与归档积压

**采集方法**：
```bash
du -sh data/raw data/backup/raw 2>/dev/null
find data/raw -name '*.json.gz' | wc -l
uv run hf-data-crawl doctor      # 直接给出「Raw 规模 / 归档 / 备份」三项结论
uv run python -c "from pathlib import Path; from hf_data_crawl import raw; print(sorted({raw.source_of(p) for p in Path('data/raw').rglob('*.json.gz')}))"  # 当天有哪些扫描来源
```

**正常范围**：
- 热区（30 天内）每天 1000~2500 个原始分块；归档后每个历史分区约 100~130 个分片
- 来源清单应只含 `lean_lastModified` / `top_refresh_downloads` / `enrich_downloads` / `reconcile_createdAt` / `legacy`；出现陌生来源说明新增扫描未登记
- `data/backup/raw/` 无超过 7 天的分区

**告警**（`doctor` 自动判定并写 `ALERT.txt`）：
- `data/raw` > 5 GB 或 > 20,000 文件
- 30 天前的分区仍是原始分块（写入序未冻结、同天重复未压缩）
- `backup/raw` 分区超过 7 天未清理（归档不删原文件，磁盘没真正释放）

---

## 告警规则

### Critical（严重）

- Current 行数骤降 > 10%
- 连续 3 次命令失败
- Watermark timestamp 比例 > 50%

### Warning（警告）

- 派生恢复次数 > 0
- Collect 耗时 > 600 秒
- 成功率 < 90%
- Raw 规模超阈值 / 归档积压 / backup 超期未清理

---

## 监控脚本示例

```bash
#!/bin/bash
# simple_monitor.sh

DATA_DIR="data"

# 1. Current 行数
COUNT=$(uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current" --format tsv)
echo "Current rows: $COUNT"

# 2. 最近一次 collect 状态
LAST_RUN=$(ls -t $DATA_DIR/runs/collect-*.json | head -1)
STATUS=$(jq -r .status "$LAST_RUN")
echo "Last collect: $STATUS"

# 3. Watermark 触发原因
STOPPED=$(jq -r '.summary.stopped_reason' "$LAST_RUN")
echo "Stopped by: $STOPPED"

# 4. 执行时间
ELAPSED=$(jq -r '.elapsed_seconds' "$LAST_RUN")
echo "Elapsed: ${ELAPSED}s"
```
