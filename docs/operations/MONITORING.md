# 监控指标

> 口径以 `src/hf_data_crawl/ops/cli.py` 与 `src/hf_data_crawl/platform/runs.py` 为准。
> `runs/` 与 `logs/` 按同一时间戳命名：前者是机读台账（JSON），后者是人读现场（纯文本，
> 按轮命名 `<时间戳>-<任务名>.log`）。改动调度或台账结构后请同步本页。

## 台账速查

`data/runs/<yyyyMMddTHHmmss>-<命令>[-序号].json` 的结构：

| 字段 | 含义 |
|---|---|
| `command` | 命令名 |
| `status` | `success` / `failed`，由 `Outcome.code` 推出，不接受外部传入 |
| `reason` | 失败原因（成功时为 `null`） |
| `started_at` / `finished_at` | 微秒精度 ISO 时间戳；**耗时由二者相减**，台账里没有独立的耗时字段 |
| `payload` | 命令自己的摘要；`collect` 的 `stopped_reason` 在这里，不在 `.summary` |

`doctor` 的退出码 1 表示「有告警」而非「命令失败」，它的结论**不进 `runs/`**：
看 `ALERT.txt`（覆盖式，只存最新一次）、`data/state/health-history.jsonl`（一行一次，
可追「这条告警连续多少天」）与当轮 `logs/`。

## 核心指标

### 1. Watermark 触发统计

**采集方法**：
```bash
jq -r '.payload.stopped_reason // "none"' data/runs/*-collect*.json | tail -7 | sort | uniq -c
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
grep -l "已自动恢复" data/logs/*.log | wc -l
```

**正常范围**：0 次/天

**告警**：> 0 次/天

> 日志按轮命名、最近保留 200 个（`scripts/_common.ps1`），没有「按日期一个文件」这回事。

---

### 3. 无法检测变更的库数量

**采集方法**：
```bash
grep "undetectable_change" data/reports/reconcile-*.csv | tail -1 | wc -l
```

**正常范围**：< 5%（相对 current 总行数）

**告警**：> 20%

---

### 4. Collect 执行时间

**采集方法**：
```bash
jq -r '[.started_at, .finished_at] | @tsv' data/runs/*-collect*.json | tail -7
```

**正常范围**：60-300 秒（日增量）

**告警**：> 600 秒

---

### 5. Current 表行数

**采集方法**：
```bash
uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current"
```

**正常范围**：110 万行左右，每天 +100 ~ +500 行

**告警**：骤降 > 10%

> `query` 只接受 `--board` / `--sql` / `--limit`，没有 `--format` 参数。

---

### 6. 命令成功率

**采集方法**：
```bash
jq -r '.status' data/runs/*-collect*.json | tail -30 | grep -c "success"
```

**正常范围**：> 95%

**告警**：< 90% 或连续 3 次失败

---

### 7. Raw 规模与冷区压缩

**采集方法**：
```bash
du -sh data/raw data/backup/raw 2>/dev/null
find data/raw \( -name '*.json.zst' -o -name '*.json.gz' \) | wc -l
uv run hf-data-crawl doctor      # 直接给出「Raw 规模 / 冷区 / 归档 / 备份」结论
du -sh data/cold/raw 2>/dev/null
cat data/cold/raw/manifest.json  # 已压缩进冷区基线的日期
uv run python -c "from pathlib import Path; from hf_data_crawl.pipeline.raw import SUFFIXES, source_of; print(sorted({source_of(p) for s in SUFFIXES for p in Path('data/raw').rglob(f'*{s}')}))"  # 当天有哪些扫描来源
```

**正常范围**：
- 热区（近 7 天）每天 1000~2500 个原始分块；`data/raw` 里没有超过 7 天的 `dt=` 分区
- `cold/raw/manifest.json` 的日期与已压缩分区一致；`build` 默认只重放热区
- 来源清单应只含 `lean_lastModified` / `top_refresh_downloads` / `enrich_downloads` / `reconcile_createdAt` / `legacy`；出现陌生来源说明新增扫描未登记
- `data/backup/raw/` 无超过 7 天的分区

**告警**（`doctor` 自动判定并写 `ALERT.txt`）：
- `data/raw` > 5 GB 或 > 20,000 文件
- 超过 7 天仍留在 `data/raw` 的冷区分区（compact 没跟上，主目录会线性膨胀）
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
- Raw 规模超阈值 / 冷区未压缩 / 归档积压 / backup 超期未清理

---

## 调度与依赖

日常链只有一个补跑单元 `hf-daily`（03:10 + 登录后 3 分钟），由
`scripts/daily.ps1` 按固定顺序跑：
`collect → build --stale → reconcile → fetch-sizes --changed 8000 → export-wide → request run-due → prune → doctor`。
依赖由**脚本顺序**表达，不由钟点表达；前置失败时下游记 `[跳过]`。
独立任务：`hf-sizes`（周日 06:00）、`hf-files`（周日 07:30）、`hf-archive`（周一 05:00）、
`hf-compact`（每日 00:30），四者刻意不带补跑，免得与日常链抢写锁。

---

## 监控脚本示例

```bash
#!/bin/bash
# simple_monitor.sh

DATA_DIR="data"
LAST_RUN=$(ls -t "$DATA_DIR"/runs/*-collect*.json | head -1)

# 1. Current 行数
COUNT=$(uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current")
echo "Current rows: $COUNT"

# 2. 最近一次 collect 状态
echo "Last collect: $(jq -r .status "$LAST_RUN")"

# 3. Watermark 触发原因（在 payload 里）
echo "Stopped by: $(jq -r '.payload.stopped_reason // "none"' "$LAST_RUN")"

# 4. 起止时间；耗时由二者相减，台账不存 elapsed_seconds
echo "Started:  $(jq -r .started_at "$LAST_RUN")"
echo "Finished: $(jq -r .finished_at "$LAST_RUN")"
```
