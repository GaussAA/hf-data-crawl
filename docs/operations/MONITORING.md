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

## 告警规则

### Critical（严重）

- Current 行数骤降 > 10%
- 连续 3 次命令失败
- Watermark timestamp 比例 > 50%

### Warning（警告）

- 派生恢复次数 > 0
- Collect 耗时 > 600 秒
- 成功率 < 90%

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
