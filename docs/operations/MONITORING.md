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

**需求交付也进台账**：`request run` 记 `request-run`、`run-due` 记 `request-run-due`，
失败同样落盘（`reason` 说明被哪道闸门拦住）。单次 `request-run` 的失败**不计入**
「近 7 天未恢复失败」——它是人当场发起、当场看见的，把「今天没有新信息」报成七天红点
只会训练人忽略告警；`request-run-due` 属没人盯着的定时路径，坏了照常报。
`--dry-run` / `--preview` 不记账（草稿）。

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
>
> 每份日志的末行都带**实耗秒数**。某一步若被墙钟掐断，末行会写
> `（墙钟 Ns 用尽、实耗 Ms，已强杀进程树）`，摘要日志里对应一行 `[超时]`，
> 退出码 **124**。看到 124 不必当成数据出错：这一步没跑完，依赖它的步骤会被跳过，
> 补跑该步即可；连它为什么卡住都查得出来——见 DECISIONS #118。

---

### 3. 无法检测变更的库数量

**采集方法**：
```bash
uv run hf-data-crawl query --sql "
WITH latest AS (
    SELECT max(filename) AS f
    FROM read_parquet('data/reports/reconcile-*.parquet', filename = true)
)
SELECT count(*) FILTER (WHERE kind = 'undetectable_change') AS undetectable,
       count(*) AS total
FROM read_parquet('data/reports/reconcile-*.parquet', filename = true), latest
WHERE filename = latest.f"
```

> 对账清单自 2026-11-18 起存 parquet（此前是 CSV）：`kind` / `id` 都是高重复的短
> 字符串，zstd 压完只剩几 MB，而首份全量清单 CSV 有 50.5 MB。取「最近一份」要按
> `filename` 虚拟列过滤，直接读会把这一个月 30 份混在一起。

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
- **采集类分块**逐日稳定（实测 11-13..11-19 的 `lean` + `top_refresh` + `card_refresh`
  落在 30~38 块），判据取「当日采集类块数 < 近 7 天中位的 70%」。
  为什么要剔除 `reconcile_createdAt`：它占全天分块数的 97%，页数由对账的
  `--max-pages` 决定（旧链 600 页分批、新链扫全站约 1130 页）——把它一起数，
  判据度量的其实是「对账批次多大」，而不是「采集停没停」。2026-11-20 就误报过一次：
  11-19 的 641 块被判「采了一半」，而当天采集类 34 块与前一日分毫不差
- `cold/raw/manifest.json` 的日期与已压缩分区一致；`build` 默认只重放热区
- 来源清单应只含 `lean_lastModified` / `top_refresh_downloads` / `enrich_downloads` / `reconcile_createdAt` / `legacy`；出现陌生来源说明新增扫描未登记
- `data/backup/raw/` 无超过 7 天的分区

**告警**（`doctor` 自动判定并写 `ALERT.txt`）：
- `data/raw` > 5 GB 或 > 20,000 文件
- 某日**采集类**分块数明显低于近期中位（当天只采了一部分；与「断日」互补，断日判据看不见这种情况）
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
- **富化滞后**：README 与规模两项合计超期 > 2,000 个在架库。「超期」指内容已经变了
  （`source_sha` 与当前 `sha` 不一致、或 `lastModified` 晚于抓取日）或从来没有记录，
  且已超过 3 天没被刷到；新库有 3 天宽限。判据复用抓取端同一套
  `stale_readme_ids` / `stale_ids`，所以体检说「该刷」的，就是下一轮真会去刷的那批。
  它补的是另外两项看不见的格子：`readme_freshness` 看「最近抓过没有」（表里最新
  快照日）、`field_freshness` 看「有行占比」，两者答的都是「最近有没有动」，
  整层落后几千个时照样全绿（2026-11-20 实测：落后 5,416 + 6,731 个，体检 30 项全 ok）
- 文档口径漂移：README / AGENTS / operations / skill 里出现已弃用的任务名或列数
  （判据是一份固定的弃用词表，行内带「已删除 / 已并入 / 历史」的豁免）

---

## 调度与依赖

只有一个任务 `hf-refresh`（每日 00:30），由 `scripts/refresh.ps1` 调
`hf-data-crawl refresh`，编排逻辑在 `ops/refresh.py`（ADR-0008）。三段式：

```
① 串行   compact → collect --full-scan → build --stale        （current 定稿）
② 并行   reconcile ‖ fetch-sizes --changed-all
                    ‖ fetch-readme --changed-all
③ 串行   parse-readme → export-wide（sizes 合并后即跑）→ report → request run-due → archive → prune → doctor
```

**`fetch-files` 与 `fetch-preview` 不在链上**（DECISIONS #149，2026-11-20 起）：两者
都是按需取用的富化资产，全量一轮要几百 GB 下载与几十小时，塞进每日窗口只会挤掉
采集。要跑就手动跑，命令见 `README.md`；它们已不在 `STEP_NAMES` 里，
`refresh --skip` / `--only` 都不接受这两个名字。

依赖与并发由**编排器**表达，不由钟点表达；前置失败时下游记 `[跳过]`，不相关的
并行分支照常跑完。**09:00 软边界**：之后不再启动新的抓取，未完成的步骤次日从
runs 台账 + staging 分片续跑；次日 00:30 若发现上一条还在跑，跳过本次触发。

**「今天已完成」是有条件的**（DECISIONS #150 / #151）：它要求前置今天也完成，且
前置的**完成时刻不晚于自己**。同一天里白天手工跑过、夜里才补采集时，`build` /
`export` / `sizes` / `readme` / `parse` 都会按新底库重开——摘要里会写「今天已完成」
以外的空备注，表示这一轮真跑了。判断依据只有一处：`runs/` 台账的 `finished_at`。

旧的六个任务（`hf-daily` / `hf-sizes` / `hf-files` / `hf-archive` / `hf-compact` /
`hf-readme`）已删除并并入本链。README 全量（约 21 小时）仍不排日程，留人工择机。

---

## 配额与限流（`/api`）

`/api/datasets` 有速率限制，响应头 `RateLimit: "api";r=<剩余>;t=<窗口剩余秒>` 是唯一
可信的来源（另有 `RateLimit-Policy: "fixed window";q=...;w=...` 说明窗口）。

口径（HF 官方文档，2025-09）：

| 身份 | API 配额 | 窗口 |
|---|---:|---|
| 匿名（按 **IP**） | 500 | 5 分钟固定窗 |
| 免费用户（按 **账号**） | **1,000** | 5 分钟固定窗 |
| PRO | 2,500 | 同上 |

**一个请求就是一个单位，与页大小无关**；`r` 是「本窗口还能发几次」。

关键一点：**限流按账号算，不按 token**。同一账号下给不同机器配的不同 token，共用
同一个 1,000 / 5 分钟的池子。所以「我这台没跑，`r` 却在掉」是正常现象，不是统计
错误——别把它读成「一页很贵」再去调阈值（2026-11-20 就这么误判过一次，见
DECISIONS #148）。

客户端据此做三件事（`acquire/hf/api.py`）：

| 场景 | 行为 |
|---|---|
| 取下一页前 | `r ≤ PAGE_COST_UNITS`（2）就等窗口重开——只守「还够不够再翻一页」 |
| 扫描途中被 429 | 睡满 `t`（上限 600 秒）再重试，**不截断** |
| 逐条探测被 429 | 仍封顶 30 秒——探测持写锁，睡满窗口会拖死整条链（详见 `MAX_RETRY_SLEEP`） |

**诊断顺序**（别一上来就怀疑网络）：

1. 全站 1,140 页 ÷ 1,000 每次窗口 ≈ 2 个窗口 ≈ 10 分钟。明显超出，先查是不是有
   第二方在共用账号——发一次请求记下 `r`，**什么都不做**等 60 秒再发一次，若 `r`
   自己掉了几十，就是别人在扣。
2. 日志里出现「重试已耗尽（HTTP 429）」才是缺陷（退避与窗口不匹配）；
   只是**变慢**（跑几十页停几分钟）属正常，别当故障查。

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
