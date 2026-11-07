# 故障恢复

## 自动恢复机制

### 派生中断自动恢复

启动写命令时，会自动检查是否有未完成的派生（标记超过 10 分钟）。

**观察**：
```
⚠️  检测到 1 个未完成的派生，已自动恢复：[date(2026, 11, 7)]
   提示：残留派生层已清理；有 Raw 的日期已重新派生，无 Raw 的仅清理
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
uv run hf-data-crawl build        # 重放近 7 天的 Raw，重建热区派生层
uv run hf-data-crawl build --full # 清空派生层并按全部 Raw 重放（逃生舱）
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
uv run hf-data-crawl query --sql "SELECT COUNT(*) FROM current WHERE last_seen_at = DATE '2026-11-07'"
```
`query` 只挂了 `current` / `silver` / `observations` / `card` / `dataset_size` 等视图，
没有 `bronze` 视图；按日核对用 `current.last_seen_at`。

---

## Raw 归档与备份清理

**症状**：`doctor` 报「Raw 归档积压」「冷区未压缩」或「backup/raw 超过 7 天未清理」。

**归档**（每周计划任务自动执行；也可手动指定某一天）：
```bash
uv run hf-data-crawl archive --older-than-days 30
uv run hf-data-crawl archive --date 2026-11-07
```
归档先验证前后 Bronze 逐行相等，不等则撤销、原文件原地不动；原文件移入
`data/backup/raw/dt=<日期>/`，**不删除**。归档分片的 `groups` 冻结了真实写入序，
此后 `build` 不再依赖文件 mtime。

**来源可追溯**：归档按 (类别, 来源) 分流，分片名与 `groups.source` 都保留
`source`（如 `lean_lastModified`、`top_refresh_downloads`）；命名空间引入前的历史
分块归为 `legacy`，沿用旧分片名。排障时看文件名第二段即可认出「这批是谁扫的」，
不同来源互不合并。

**冷区压缩**（每周计划任务自动执行；也可手动）：
```bash
uv run hf-data-crawl compact                       # 默认压缩超过 7 天的日期
uv run hf-data-crawl compact --date 2026-10-01    # 只压指定日期
```
压缩把多个日期的全量快照按 id 跨天去重，合并成 `data/cold/raw/baseline-*.json.gz`
一份基线；分片 `groups` 仍冻结原写入序与来源命名空间。写入前后逐个 id 校验
胜出者，不等即撤销新分片、原文件不动。原分块移入 `data/backup/raw/dt=<日期>/`
**不删除**；已压缩日期登记在 `data/cold/raw/manifest.json`，`doctor` 据此避免
压缩后误报断日。

**压缩回滚**（权威层完好时通常不需要）：
```powershell
$day = '2026-10-01'
Move-Item "data\backup\raw\dt=$day\*" "data\raw\dt=$day\"
Remove-Item "data\cold\raw\baseline-*.json.gz"
```
再把 `manifest.json` 的 `compressed_dates` 去掉该日，随后 `build --full` 会按
老办法全量重放。**注意**：基线是「所有冷区日期合并」的，移除它会让其它日期也
失去登记，须把 `backup/raw/` 里**全部**冷区分块移回后再全量重建。

**归档回滚**（对归档结果有疑问时，把原分块移回并删除分片）：
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
**清理前须知**：`backup/raw/` 的原分块是 `build --full` 全量重放的唯一依据。
默认 `build` 只重放近 7 天，冷区靠 `silver` / `observations` / `current` 权威层；
一旦删除原分块且权威层损坏，历史观测流水无法重建。故清理前先备份 `state/` 与
`silver/`，并确认 `doctor` 无告警。

---

## 维护建议

- **每日**：检查 collect 是否成功
- **每周**：reconcile 全量对账；compact 自动压缩满 7 天的冷区 Raw，archive 可对单日做同天去重；确认无误后清理 backup
- **每月**：运行 `build`（默认只重放近 7 天热区）核对派生层；需要整体重算时用 `build --full`
