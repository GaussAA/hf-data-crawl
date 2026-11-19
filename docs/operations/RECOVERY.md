# 故障恢复

## 自动恢复机制

### 派生中断自动恢复

启动写命令时，会自动检查是否有未完成的派生（标记超过 10 分钟）。恢复的动作是
**从该日起后缀重放**，不是只重算那一天——单日重放会把 `current` 覆盖成当天的值，
等于把当前态整体回退（见 `pipeline/recovery.py` 与 GLOSSARY #28、#78）。

**观察**：
```
⚠️  检测到 1 个未完成的派生，已自动恢复：[date(2026, 11, 7)]
   提示：残留派生层已清理；有 Raw 的日期已重新派生，无 Raw 的仅清理
```

**手动检查**：
```bash
ls data/state/marker-*.marker
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

**自动**：下次写命令会自动恢复（如果标记文件还在且超过 10 分钟），恢复同样走后缀重放。

**手动**：
```bash
uv run hf-data-crawl build        # 重放近 7 天的 Raw，重建热区派生层
uv run hf-data-crawl build --full # 清空派生层并按全部 Raw 重放（逃生舱）
```

`--full` 的前提是**全部 Raw 都在 `data/raw`**。冷区日期的分块已被 `compact` 移去
`data/backup/raw/`，那些日期的 silver / observations 一旦清空就补不回来，所以只要
存在这种日期，`--full` 会直接拒绝并在文案里给出两条路：改用默认窗口 / `--stale`，
或先把 `backup/raw/dt=<日期>/` 搬回主目录再跑。确认接受这批历史丢失时才加
`--allow-cold-loss`——放行后仍会印出「冷区 N 天未参与重放」。

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
`runs/` 按 `<时间戳>-<命令>.json` 命名（不是 `collect-*.json`），结构与字段见
[监控指标](MONITORING.md) 的「台账速查」。

```bash
cat "$(ls -t data/runs/*-collect*.json | head -1)"
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

**冷区压缩**（刷新链的串行头第一步，每日 00:30 起跑；也可手动）：
```bash
uv run hf-data-crawl compact                       # 默认压缩超过 7 天的日期
uv run hf-data-crawl compact --date 2026-10-01    # 只压指定日期
```
压缩把多个日期的全量快照按 id 跨天去重，合并成 `data/cold/raw/baseline-*`
（新基线 `.json.zst`，2026-11-16 之前的 `.json.gz` 仍可照读）
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

- **每日**：检查 `hf-refresh` 单链是否跑完（串行头 `compact → collect --full-scan → build --stale`，并行段 `reconcile ‖ sizes ‖ readme ‖ preview`，串行尾 `parse-readme → export-wide → request run-due → prune → doctor`）；到 09:00 未启动的步骤留给次日，不算失败
- **每周**：核对本周每天都有 `refresh` 记录（`data/runs/` 里按命令名筛），缺的那天看 `data/logs/*-refresh-*.log` 断在哪一步；确认无误后清理 `backup/`
- **每月**：运行 `build`（默认只重放近 7 天热区）核对派生层；需要整体重算时，**先确认 `data/raw/dt=` 覆盖了全部采过的日期**（含冷区）再跑 `build --full`——冷区未复原时它会拒绝执行，硬要跑只能加 `--allow-cold-loss`，而那等于承认这批历史不要了
- **`exports/` 不在恢复范围内**：单文件宽表是可重建投影，损坏或过期时重跑 `hf-data-crawl export-wide` 即可，不需要备份，也不能作为重建其它层的依据
- **`enrich/size.parquet` 与 `enrich/files.parquet` 是 NETWORK 表**：`build` / `compact` 不会重建它们，丢了只能分别重跑 `fetch-sizes` / `fetch-files`；`files.truncated=true` 只说明当次取了一页 tree，不是全库真值，必要时用 `--max-pages` 重补
