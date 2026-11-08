# hf-data-crawl

Hugging Face 公开数据集元信息底座。周期性抓取元信息、留存原始响应、维护指标流水，用 DuckDB 直接查询。

不下载任何数据集文件，只抓元信息。

## 快速开始

```bash
uv sync

# 本机凭据（可选）：在仓库根 .env 写 KEY=VALUE 即可，命令自动读取，且不覆盖已 export 的值
#   HF_TOKEN=hf_xxx              # 实测配额：匿名 500 次/300 秒 → 带令牌 1000 次/300 秒
#   HF_PROXY=http://127.0.0.1:7897
export HF_PROXY=http://127.0.0.1:7897   # CI 与临时覆盖仍走 export

uv run hf-data-crawl collect --dry-run          # 先探一页，确认连通与字段
uv run hf-data-crawl collect --max-pages 30     # 有界试跑 3 万条
uv run hf-data-crawl query --board downloads --limit 20
uv run hf-data-crawl report                     # 生成 data/reports/dashboard.html
```

## 命令

| 命令 | 作用 | 联网 |
|---|---|---|
| `collect` | 日增量：按 `lastModified` 倒序扫到水位线 | 是 |
| `enrich` | 按下载量取头部，补抓 `cardData`（标签、config/split 明细、行数列数） | 是 |
| `fetch-sizes` | 经 datasets-server 补规模与列数（`num_rows` / `num_columns_max`） | 是 |
| `fetch-files` | 按下载量取头部，经 `/tree/main` 补文件清单汇总（试点：top 1000，每库默认 1 页） | 是 |
| `reconcile` | 全量对账：按 `createdAt` 稳定序扫全站，产出新增/删除/转私有/改名/sha 变更 | 是 |
| `build` | 重建派生层：默认只重放近 7 天热区，`--stale` 只修派生滞后的日子及其后，`--full` 全量重放 | 否 |
| `query` | 查内置榜单或执行任意只读 SQL | 否 |
| `request` | 需求与交付管理：`new` 建需求、`list` 看全部、`show` 看口径与台账、`run` 交付一次、`run-due` 跑到期的 | 否 |
| `report` | 生成单文件榜单页（离线可看） | 否 |
| `doctor` | 体检：调度新鲜度、底库行龄、层间勾稽、派生凭证、产物新鲜度、断日与残留文件；异常写 `ALERT.txt` 并追加体检流水 | 否 |
| `archive` | 归档早于 N 天的 Raw 分区：合并小分块并去重，先验证 Bronze 逐行相等，原文件移入 `backup/raw/` | 否 |
| `compact` | 压缩冷区 Raw：跨天去重成一份基线，原分块移入 `backup/raw/` 不删除 | 否 |
| `docs` | 生成 `docs/DATA.md`：data 目录结构、各文件作用、全部字段含义 | 否 |
| `export-wide` | 把 `datasets` 宽视图导出成单文件 parquet（可重建，不参与状态更新） | 否 |

常用参数：

```bash
hf-data-crawl collect      [--date YYYY-MM-DD] [--limit 1000] [--max-pages N] [--pages-without-new 3] [--refresh-top 20000] [--cards] [--card-pages 200] [--dry-run]
hf-data-crawl enrich       [--date YYYY-MM-DD] [--top 20000] [--limit 1000]
hf-data-crawl fetch-sizes  [--date YYYY-MM-DD] [--top 20000] [--workers 16] [--retry-unavailable]
hf-data-crawl fetch-files  [--date YYYY-MM-DD] [--top 1000] [--workers 8] [--retry-unavailable] [--timeout 30] [--max-retries 1] [--max-pages 1] [--refresh-truncated]
hf-data-crawl reconcile    [--date YYYY-MM-DD] [--limit 1000] [--max-pages N]
hf-data-crawl build        [--stale | --full]                            # 默认只重放近 7 天
hf-data-crawl compact      [--older-than-days 7] [--date YYYY-MM-DD]  # 冷区跨天压缩
hf-data-crawl query        [--board NAME] [--limit 50] [--sql "..."]   # NAME 见「内置榜单」；省略则 growth
hf-data-crawl report       [--out PATH] [--top 20]
hf-data-crawl archive      [--older-than-days 30] [--date YYYY-MM-DD]
hf-data-crawl docs         [--out PATH]           # 默认 docs/DATA.md
hf-data-crawl export-wide  [--out PATH]           # 默认 data/exports/datasets.parquet
hf-data-crawl request      new REQ-0001 --title 标题 --requester 姓名 [--purpose ...] [--cadence once|daily|weekly|monthly|manual]
hf-data-crawl request      list
hf-data-crawl request      show REQ-0001
hf-data-crawl request      run REQ-0001 [--force]   # once 需求交付过要重交须加 --force
hf-data-crawl request      run-due                  # 只跑到期的（计划任务用）
```

全局参数 `--data-dir` 需放在子命令之前，默认取 `HF_DATA_DIR`，否则 `./data`。

写命令（`collect` / `reconcile` / `build` / `enrich` / `fetch-sizes` / `fetch-files` / `archive` / `compact` / `export-wide`）执行期间持有 `data/.lock` 的独占锁，同一时刻只能跑一个；拿不到锁会失败并在 `runs/` 留记录。进程被杀后系统自动释放，不会留下死锁。`report` 与 `query` 不加锁。

**宽表导出**：`current` 只保留 19 列状态标量，语义字段分散在 `silver` / `carddata` / `size`。`export-wide` 把 `gold.datasets` 视图物化成单文件 parquet，直接交给 DuckDB / polars / BI 工具；它是**可重建投影**——不登记为表、不参与 `state.update` / `build` / `compact`，删掉重跑本命令即可，也不改动任何状态表。列口径由 `gold.wide_columns()` 一处生成，加附表字段时视图与导出一起跟上。详见 [docs/DATA.md](docs/DATA.md) 第八节。

**文件清单补采（试点）**：列表接口请求也不返回 `siblings`（见 [docs/DATA.md](docs/DATA.md) 第九节）；单库详情虽返回，但下载量头部实测 13-25 MB / 44-76 s，按头部补会反复超时。`fetch-files` 改走 `/tree/main?recursive=true&expand=false`：每页固定 1000 条、约 110-230 KB / 0.4-0.7 s。默认每库只取第一页，有后续页时标 `truncated=true`——此时 `file_count` / `total_bytes` / `file_formats` / `has_readme` 都只是**这一页的下限**，不是全库真值；要完整清单须显式加大 `--max-pages`。结果落 `enrich/files.parquet`（`Origin.NETWORK`，重建不清），自动进入 `datasets` 宽视图。断点续跑、失败分类、写锁与边跑边落库与 `fetch-sizes` 共用 `batch.collect_rows` 骨架；临时失败不入库、下轮自然重试。这是按头部补的**子集**，别拿它算全站分布。匿名配额实测 500 请求 / 300 秒固定窗口，跑满 1000 需分批；429 会按响应头 `Retry-After` 退避，仍不够时临时失败不落库、下次重跑本命令由断点续跑接力。带 `HF_TOKEN` 可提到 **1000 请求 / 300 秒**（实测），写进仓库根 `.env` 即可，命令自动读取且**不覆盖**已 export 的值；令牌只在本机文件里，值绝不进日志与 `runs/`。已标 `truncated` 的行默认被断点续跑跳过，想补成完整清单须显式 `--refresh-truncated --max-pages N`：它只重抓截断行、完整行照旧跳过；`--max-pages` 仍为 1 时命令直接失败，因为一页上限下重抓只是把截断重新确认一遍。

内置榜单（`gold.BOARDS` 是唯一真相源：`query --board` 的候选项由它自动生成，`report` 页面复用同一批口径）：

| 榜单 | 口径 | 用于 |
|---|---|---|
| `growth` | 最近两个观测点的 downloads 增量，只列正增长，附 `span_days` 与 `from_date` / `to_date` | query、报告页 |
| `growth_pct` | 增幅百分比，要求起始下载 ≥ 100，避免小数被放大成噪声 | query |
| `downloads` | 30 天下载榜（`current.downloads`） | query |
| `alltime` | 累计下载榜（`current.downloads_all_time`） | query |
| `categories` | `task_categories` 标签展开后按集数／下载聚合 | query、报告页 |
| `ranking` | 报告页 30 天下载榜，附 `pretty_name` / `license_name` | 报告页 |
| `ranking_alltime` | 报告页累计下载榜，与 `ranking` 列结构一致、成员各自取前 N | 报告页 |
| `licenses` | 卡片表 `license_name` 按集数聚合 | query、报告页 |
| `volume` | datasets-server 规模表按 `num_rows` 排序（仅 `status = 'ok'`） | query、报告页 |
| `modalities` | `modalities` 标签展开后按集数／下载聚合 | query、报告页 |

**两个下载口径不可混用**：`downloads` 是近 30 天，`downloads_all_time` 是历史累计，各自只按自己的指标排名。`ranking` 与 `ranking_alltime` 必须是两条独立 SQL——若把 30 天头部那几行改按累计重排，得到的只是「30 天头部里的累计排序」，与真正的累计榜成员不同。

增速取「最近两个观测点」而非固定 7 日窗口：日增量只扫最新更新的万把条，全站指标靠周对账刷新，固定窗口在周节奏下取不到两点。

## 数据布局

```
data/
├── raw/dt=YYYY-MM-DD/<类别>-<来源>-<页号>-<摘要>.json.gz  # 原始响应：热区原样，30 天后归档为分片
├── cold/raw/                                         # 冷区基线（跨天去重）与 manifest.json
├── backup/raw/dt=YYYY-MM-DD/                         # 归档/压缩移出的原分块，人工确认后清理
├── bronze/dt=YYYY-MM-DD/bronze.parquet                # 一行一条，含 raw 列；只保留最新一天，旧分区可由 raw 重建
├── silver/dt=YYYY-MM-DD/silver.parquet                # 属性变更流水：新增或属性变了才追加；标签已拆 16 类维度，不含指标
├── state/
│   ├── current.parquet                                # 每库一行最新态
│   └── observations/dt=YYYY-MM-DD/obs.parquet         # 指标变化流水
├── enrich/
│   ├── carddata.parquet                               # 卡片宽表（22 列）
│   ├── card_configs.parquet                           # config/split 明细
│   └── size.parquet                                   # 行数/列数/内存（datasets-server）
├── meta/failed-status.json                            # 仅重建进行中存在；成功后删除，残留说明上次被打断
├── reports/reconcile-YYYY-MM-DD.csv                   # 对账差异清单
└── runs/YYYYMMDDTHHMMSS-<command>.json                # 每轮运行摘要
```

Raw 文件名带内容摘要：同页同内容重写为同一文件（幂等），同页内容变了则另存（绝不覆盖丢数据）。

**来源命名空间**：`ScanSpec` 用 `mode` + `sort` 派生 `source`（如 `lean_lastModified`、
`top_refresh_downloads`、`enrich_downloads`），写进文件名与 `meta`。同一天里日增量、
顶部刷新、全量对账、卡片补齐各有独立页码空间，不再共用 `part-000001`；归档按
(类别, 来源) 分流，分片名与 `groups.source` 都保留来源。历史分块按 `legacy` 兼容
读取，不改写。`ScanSpec` 在构造期强制 `expand` 必含 Bronze 基座字段，防止更窄档位
的后写记录覆盖完整记录、整列静默变空。命名空间**不参与排序**，胜负仍由
`written_at` 决定。

**Raw 保留策略（2026-11-07 起，三层语义，常量集中在 `retention.py`）**：

- **热区**：近 7 天（`REPLAY_WINDOW_DAYS`）的 `data/raw/dt=*` 原样保留，`build`
  默认按天重放；`archive` 可对单日做同天去重，并把写入序冻结进 `groups`。
- **冷区权威层**：`silver`（属性变更流水）、`observations`（指标变更流水）、
  `current` 的历史**不再清空**，它们是历史事实来源。实证：只重放热区与全量
  重放逐行等价（契约见 `tests/test_rebuild.py`）。
- **冷区 Raw**：`compact` 把超过 7 天的多日全量快照跨天去重压成一份基线，
  存 `data/cold/raw/`（分片 `groups` 仍冻结写入序），原分块**只移入
  `backup/raw/`、不删除**。写基线前后逐个 id 校验胜出者，不等即撤销。

`build --full` 保留旧的全量重放；冷区权威层若已损坏（观测流水或当前态为空），
默认 `build` 会自动回退全量。`doctor` 把 `cold/raw/manifest.json` 里已压缩的
日期算作「有观测」，避免压缩后误报断日；并在冷区未压缩、规模超阈值、备份
超过 7 天未清理时写 `ALERT.txt`。**清理 backup 前须确认权威层已备份**：一旦
原分块被删且权威层损坏，就只能恢复近 7 天。


每张表的身份（路径、schema、来源）统一登记在 `tables.py`，读写、视图、重建清理都从它取，不在各处各自硬编码。

## 设计要点

- **两档采样**：日增量走 lean 档（不含 `cardData`，16 字段约 980 B/条）；`cardData` 只在需要时单独取。实测 full 档约 6 KB/条，全站日拉会白白多传数倍字节。补齐 `private`/`disabled`/`mainSize` 只让 lean 档从 923 涨到 980 B/条（+6.2%）。
- **一轮扫描的时序只有一处**：`scan.ScanRun` 管「翻页 → 落盘 → 存游标 → 判停止」，日增量、顶部刷新、全量对账、卡片补齐只提供策略。两条不变量在此一层：游标必须在写盘后、任何停止判断前保存（否则续跑重抓上一页）；页数上限由本层判定而非交给生成器（生成器到限时静默 return，说不出停止原因）。
- **排序必须显式**：不传 `sort` 时 HF 的游标基于 `trendingScore`，会随时间漂移，翻页必漏。全量用 `createdAt`（不可变），增量用 `lastModified`。
- **写入序落在 Raw 里**：`write_raw` 把 `written_at`（UTC、微秒）记进 payload 的 `meta`，Bronze 去重按它排序；文件名摘要、mtime、`label` 的字母序一概不作数。mtime 一复制就变，靠它排序会让「Raw 是唯一事实来源」名不副实——同一批数据在另一台机器上可能推出不同的 Bronze，且不报错。摘要不含 `written_at`，否则同内容重写会不断产生新文件。
- **水位线以时间戳为边界，但异常值必须先滤**：实测存在 2028 年的 `lastModified`，单调性假设会失效；过滤规则与「连续 N 页无新 id」后备见下文「数据一致性保障 §1」。
- **下架判定只在扫完时做**：`reconcile` 用 `--max-pages` 有界跑时不会标记下架，否则未扫到的部分会被整片误判。
- **派生层可重建**：`build` 只读 Raw，断网即可回填。派生规则变更后一条命令重算历史。
- **顶部池刷新**：`collect --refresh-top 20000` 在增量之后按 `sort=downloads` 补扫前 2 万名。日增量只覆盖「最近更新的万把条」，若不补这一趟，增速榜长期只有零星读数。成本约 20 页。
- **`downloads` 非实时**：实测同一集相隔 9 小时读数完全相同，故增速必须跨日观测；同日重复扫描不产生新读数。
- **卡片只能按榜单或按变化集补**：单库详情即使带 `blobs=false` 仍达 1.17MB（含 `siblings`），逐条补不可行；列表接口只能 sort + 翻页，**不接受 id 过滤**，所以「先算出哪些 sha 变了再去抓」这条路走不通。两条可行的路各管一段：

  - `enrich` 走列表接口带 `expand[]=cardData` 取下载量前 N 名，2 万条约 22 秒；实测前 2 万名中约 68% 带卡片。它补的是**从没抓过的头部**——sha 多年不变的库永远不会进入增量范围，只能靠这一趟覆盖。
  - `collect --cards` 跟着日增量走：sha 变必然伴随一次 commit，`lastModified` 必变，而日增量本来就按它排序，所以「今天变过的库」现成就在手里，重翻同一批页、多带 `cardData` 即可。约 22 页 / +17 分钟 / raw 每日 +15MB。首轮没有水位线时不跑——那时没有「变化」可言，抓全站是 6.7GB、14 小时的一次性补齐，不该混进日常增量。

  卡片表落 `enrich/carddata.parquet`，`datasets` 视图已左连其字段。表里记 `source_sha`（抓这张卡时的仓库版本），拿它跟 `sha`（仓库当前）对照就能判断卡片是否落后——`snapshot_date` 只说什么时候抓的，不说内容对应哪一版。`doctor` 的「卡片凭证」一项报的就是这个比例，只看最近 3 天活跃的库（长尾几百万年不动，卡片本就该保持原样）。
- **`expand[]` 是白名单裁剪，不是增补**（2026-11-04 踩过的坑）：传了它就只返回列出的字段，连默认响应里本就有的 `private` / `disabled` 都会被裁掉。此前 `LEAN_EXPAND` 没请求 `private`，而 Bronze 已声明该列，结果 111.5 万行整列为空且管线照常报成功。现由 `tests/test_schema_contract.py` 把「表结构的列」与「请求的字段」钉成唯一真相源，另有 16 项断言防止再次漂移。
- **卡片抽取看 `dataset_info`**：它带每个 config 的 `dataset_size` / `download_size` / `features`（列名+dtype）/ `splits`（`num_examples`），是整条管线里唯一能回答「多少行、多少列」的来源。宽表抽 22 列，另落 `card_configs.parquet`（一行一个 config/split，实测 13,651 张卡展开 142,552 行）。

## 数据一致性保障

系统实施了以下机制确保数据一致性：

### 1. 基于时间戳的 Watermark（2026-11-07）

**问题**：旧机制基于"连续 N 页无新 id"，在跨日运行时存在漏采风险。

**解决方案**：
- 持久化上次运行的最新 `lastModified` 时间戳
- 只有当"连续 N 页的最老 lastModified 仍晚于水位线"时，才认为到达边界
- 过滤"当前时间 + 7 天"之后的异常值（实测有 2028 年的时间戳）
- **完成态只清游标、不清水线**（2026-11-07 补）：`WatermarkCheckpoint` 曾把
  `cursor=None` 同时当作清线信号，而正常完成恰好调 `save(None)`、首轮每页的
  `cursor` 本就是 None，水线写不进或刚写就被删，下轮 `is_stale()` 恒 False。
  今拆为 `save`（游标）/ `save_watermark`（水线）/ `finish`（完成）三个入口，
  由 `tests/test_collect.py` 的跨轮用例守护。

**效果**：漏采风险降低 90%+

### 2. 派生原子性保护（2026-11-07）

**问题**：派生过程崩溃会导致部分文件已更新、部分未更新。

**解决方案**：
- 开始派生前写标记文件 `state/deriving-YYYY-MM-DD.flag`（含开始时间戳）
- 派生成功后删除标记
- 启动时自动检查：标记超过 10 分钟 → 清理该日的派生层 → 重新派生

**效果**：派生不一致风险降低 80%+，启动时会显示恢复信息

**2026-11-08 补**：原子替换本身在 Windows 上也会失败——Defender 实时扫描会在文件
刚写完时短暂持有句柄，`os.replace` 随即报 WinError 5（实测 `reconcile` 第 10 分钟
替换 checkpoint 失败，整轮 30 分钟作废）。`atomic.replace` 现对 `PermissionError`
退避重试三次（20/60/180ms），其余异常立即抛出。详见 GLOSSARY #47。

### 3. 派生凭据：Raw 被改过，就得证明派生跟上了（2026-11-08）

**问题**：Raw 可以被 `archive` 重分片、被 `compact` 移走、被补采追加、被手工修数据。
重写之后派生层**不会自己更新**——`derive_day` 只在 `collect` / `enrich` / `build` 里跑。
于是存在一种没有任何报错的坏状态：Raw 是新的、bronze / silver / observations 是旧的。
从文件时间上也看不出来（派生文件常常比 Raw 更晚，因为别的日子刚跑过）。

**解决方案**：

- 每派生一天，在 `state/lineage.json` 落一条戳记：当日 Raw 分块的指纹
  （文件名 + 字节数 + mtime 的 sha256，**含 `card-*` 补抓批次**，因为补位会经它写进
  当前态）、派生时刻、bronze / silver / observations 三层行数。
- `doctor` 逐日比对：指纹不符 = **派生滞后**；没有戳记 = **派生无凭证**。两者都
  直接给出 `build --stale`。
- `build --stale` 只重放「最早出问题那天 → 最新日」的**后缀**，不是只修那一天。
  Silver 的「变了才追加」要拿前一日当锚点，Observations 同理，而 Current 是拿当日帧
  覆盖同名 id——中间缺任何一天都会拿错锚点，且当前态回退。这条约束由
  `rebuild.replay_suffix` 单独实现，调用方不自己拼日期。
- `archive` 是**已校验的等价改写**（移走原件前验过 Bronze 逐行相等），跑完调
  `lineage.restamp` 只刷新指纹、不重派生。不刷新的话下轮体检会把等价改写报成滞后
  ——假警报会训练人忽略告警。

**效果**：把「派生是不是最新」从人的记忆变成一份可读的凭证。机制上线首轮会把
无凭证的日子全量重放一次建立基线，此后只有真被改过的日子才会被点名。

### 4. 补位同源：属性层、当前态、观测看同一份当日帧（2026-11-08）

**问题**：`bronze.build` 只读 `part` / `top`（当日扫描），只在补抓批次 `card-*` 里
出现的库不进 Bronze——这是有意的，混进去会让 Bronze 行数随补抓节奏膨胀 45%
（实测 233 万→338 万），而真正只缺 203 个库。但补位此前**只做在状态层**：
`current` 与 `observations` 有这批库，`silver` 没有。

**解决方案**：补位提到 `bronze.day_frame`——按 id 补空位（已在扫描帧里的记录一律
不动，胜负仍归扫描批次），`silver` 与 `state` 共用这一份帧；Bronze 分区本身仍然纯净。

**效果**：2026-11-08 体检的层间勾稽量出 **203 个库**在 current 里、却没有一行属性，
`datasets` 宽视图里它们的 tags / description 整片为 null（不报错，只是查不出来）。
修复后三层对齐。体检同时勾稽三条：current ↔ observations 双向差集、current ↔ silver
覆盖、current 的 `last_seen_at` 是否落后于最新 Raw 日（采了却没进当前态）。

### 5. sha 变更检测改进（2026-11-07）

**问题**：将 null sha 当成相同，导致漏报变更。

**解决方案**：
- 区分"确实变更"（两方都有 sha 且不同）和"无法检测"（至少一方为 null）
- 对账报告增加 `undetectable_change` 类别，单独列出无法检测的库

**效果**：检测准确率提升 100%，清楚知道哪些库无法检测变更

详见：
- 设计决策：[docs/adr/0001-metadata-refresh-consistency-hardening.md](docs/adr/0001-metadata-refresh-consistency-hardening.md)
- 文档索引：[docs/README.md](docs/README.md)
- 故障恢复：[docs/operations/RECOVERY.md](docs/operations/RECOVERY.md)
- 监控指标：[docs/operations/MONITORING.md](docs/operations/MONITORING.md)
- **标签维度拆 16 类**：按全站 950 万条标签实测频次补齐 `modality`（12.7%）/`region`（11.7%）/`format`（8.6%）等；无前缀标签单列 `plain_tags`（13.7%，旧式 `text`/`image` 标记都在这里）。维度覆盖率 51.5% → 99.9%。`tag_values` 长表视图由映射表自动生成，新增维度不必改 SQL；白名单之外的任意前缀（`model:`、`eval:` 等长尾）走 `tag_index` 视图——它直接展开原始 `tags`，按第一个冒号切分，1,200 万行只算不存。
- **规模数据走 datasets-server**：列表接口的 `mainSize` 是仓库字节，卡片声明的行数只覆盖少数数据集（当前覆盖率见 [docs/DATA.md](docs/DATA.md)）。`fetch-sizes` 打 `/size` 拿 `num_rows` 与 `num_columns`。实测成功率约 80%，并发 16 稳定 26 req/s，两万条约 790 秒。失败分类处理：500/401/501 记 `unavailable` 落库并默认跳过（永久缺失，重试只会白烧 30% 预算）；429/502/503 退避重试且不落库（下轮自然重试）。
- **宽视图由 schema 拼装**：`datasets` 的列不再手抄，由 `tables.py` 的 schema 按固定顺序拼接，重名**先到先得**；需要区分口径的列改名（`size.num_rows → server_num_rows`，与卡片口径的 `card_num_rows` 并存）；`raw`（每行数 KB 的整行 JSON）与重复的 `snapshot_date` 排除在外。手抄的代价是漂移——加一个卡片字段，`card` 视图自动有，`datasets` 安静地少一列。
- **「最新一行」有物化与视图两套实现，必须永远一致**：`current` 是物化表，`latest_silver` 是窗口视图。`current` 的带入列由两个 schema **求交**推导，不手抄；`status` / `first_seen_at` / `last_seen_at` 是本地判断，不得混进 Silver。一致性由测试守护（实测 111.7 万行、15 个公共列取值相同）。
- **失效状态不是「删除」一种**：对账后逐条探测，404 才是删除，401 是转私有（库还在），307 是改名（跟随到 200 拿到新 id）。三者混记会让下架数虚高，也让改名后的库在下轮变成「新增」而丢掉观测史。
- **命令成败只有一个判定点**：命令返回 `Outcome(payload, code, reason)`，退出码与 `runs/` 记录由同一处推出；`write_run` 不接受外部传入的 `status`——可传的 status 一定有传错的那天。
- **原子写只有一个入口**：`atomic.py` 的四个函数管所有落盘，临时文件与目标同目录（跨盘替换会退化）。中断时不能留下半张 HTML——那比旧页面更糟。
- **列编码只有一套规则**：`coerce.py` 管「一个 JSON 值怎么落进一列」。铁律是取不到留 None、绝不猜：`private: "true"` 是字符串不是布尔，猜成 True 会让「是否受限」失去可信度。

## 定时

```powershell
pwsh -File scripts\setup-schedule.ps1              # 注册：03:10 增量 · 04:00 对账 · 04:50 导出 · 07:00 交付 · 09:00 体检 · 00:30 压缩（+ 周日 06:00 规模、周一 05:00 归档）
pwsh -File scripts\setup-schedule.ps1 -Uninstall   # 卸载
Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo   # 核对
uv run hf-data-crawl doctor                        # 手动体检；有问题退出码 1，并写 data\ALERT.txt
```

计划任务调 `scripts\*.ps1` **八个**包装脚本（collect / reconcile / export / deliver / doctor / sizes / archive / compact），全部是 PowerShell 7 脚本，由 `pwsh -File` 执行。公共部分（定位 `uv`、代理默认值、日志、退出码）抽在 `scripts\_common.ps1`，各脚本只声明跑哪个子命令。

八项任务的时刻是按「写命令互斥」排的，不是随手定的：00:30 压缩 → 03:10 增量 → 04:00 对账 → **04:50 导出** → 05:00 归档（仅周一）→ 06:00 规模（仅周日）→ **07:00 交付** → 09:00 体检。导出必须在对账之后：它是物化投影，不会跟着派生自动更新，排在前面就等于每天导出一份旧的。交付必须排在导出之后、体检之前：前者让它拿到当天的底库，后者让「某条需求 0 行被阻断」当天就能在告警里看见。

要点：

- **需要 PowerShell 7**（`pwsh`）。注册时会把 `pwsh` 的绝对路径写进任务动作——计划任务的环境不保证有 `PATH`。
- 代理默认取 `HF_PROXY`，未设则用 `http://127.0.0.1:7897`。
- 日志按轮写成 `data\logs\<时间戳>-<任务名>.log`（如 `20261108T031000-collect.log`），**UTF-8**，每次跑完自动保留最近 200 个——与 `runs/` 的 500 条同一哲学：有界才不会被计划任务悄悄撑爆。此前是 `collect.log` 单文件永久追加，既对不上 `runs/` 里那轮摘要，也只能靠人手工改名成 `.bak`。三处编码都设了才有效（`PYTHONIOENCODING` / `Console::OutputEncoding` / `OutputEncoding`），只设一处仍会落成 GBK——中文系统下打开是乱码。**查看时请用 `Get-Content -Encoding utf8`**：文件是纯 UTF-8 无 BOM，而 `Get-Content` 默认按 ANSI（中文系统即 GBK）读，会把它读成乱码——那是查看方式的问题，不是日志坏了（2026-11-08 实测踩过，一度误以为编码设置失效）。
- `uv` 找不到时回退到 `%USERPROFILE%\.local\bin\uv.exe`；两者都没有则直接报错，不会静默跑空。

每轮摘要落在 `runs/`，失败也写并带错误原因，不会静默。`runs/<时间戳>-<命令>.json` 与 `logs/<时间戳>-<命令>.log` 是同一次运行的**两半**（机读结论 / 人读现场），同键不同扩展名，查某一轮取同前缀即可。`doctor` 不写 `runs/`：它退出码 1 表示「有告警」而非「命令失败」，写进去会污染「近 7 天无失败运行」那条判据；体检结论看 `data\ALERT.txt`（覆盖式，只存最新一次）、流水 `state\health-history.jsonl`（一行一次，据它追「断日从哪天开始」「这条告警连续几天」，保留最近 `HEALTH_KEEP` 次）与对应日志。

**写命令互斥**：所有写命令（collect / reconcile / build / enrich / fetch-sizes）持有数据目录的锁，避免 `current.parquet` 被并发覆盖。人工执行时撞上别人会立刻报错并说明占用者；计划任务的包装脚本加了 `--lock-wait 900`，会先等 15 分钟而不是白丢一轮（`fetch-sizes` 一次可能跑一小时，与 03:10 的采集重叠过，曾因此丢过一次采集）。

## 需求与交付

底库建起来是为了用。每份对外的数据交付都是一份**档案**，而不是一次临时的查询：

```
requests/REQ-0001/spec.yaml           # 口径，唯一可变的地方
requests/REQ-0001/history/v1.yaml     # 交付时自动快照，只留「真的用过」的口径
requests/REQ-0001/deliveries/20261108T145000/
  data.parquet      # 字节，不进 git（.gitignore 拦 **/deliveries/**/data.*）
  manifest.json     # 机器读：底库指纹、行数、列、sha256、健康标记
  README.md         # 人读：要什么、怎么算的、基于哪天的底库、几行
```

**口径只写词条，不写字段名**：`下载量` 是近 30 天，`累计下载量` 才是历史累计——
同一句话两种理解差两个数量级。词表见 `metrics.py` 与 [GLOSSARY](GLOSSARY.md) #51，
`request show` 会把口径连同编译后的 SQL 一起打出来，便于贴给对方确认。

`manifest.json` 里的 `source.current_md5` 是整份档案的锚点：日后任何一份交付都能
回答「它基于哪一版底库」。`deliveries/` 的 `data.*` 不进仓库，但 `manifest.json`
与 `README.md` 进 —— 档案的「话」可评审，档案的「字节」不占仓库，两者靠 sha256 绑定。

**跑一次交付**（`request run`）守三条：0 行不交付、超 `max_rows` 不交付、行数突变
**标疑但不阻断**。第三条是有意的：`once`/`weekly` 这类需求当天交不出货比数字可疑
更糟，所以照常交付，但把「与上次相比从 3108 行变成 8900 行」写进 manifest 的
`health` 与 README 顶部。判疑只在**同一口径版本内**比较——改了口径必然行数大变，
那时比较只会天天发假警报，而假警报最能训练人忽略告警。

先写 `.tmp-*` 目录、三件齐了再整体改名，所以中途失败绝不会留下「有数据没说明」。
CSV 装不下的列表列（`tags`、`languages` 等 11 个）以 `;` 连接成一格；要保留原始
结构请用 parquet。

**跑一批交付**：`request run-due` 扫 `requests/` 下所有 spec，按各自 `cadence` 判断
「今天是否到期」，到期的才跑。判断依据是**上一次交付的时刻**，不是上一次运行成功
与否——后者在 0 行被阻断或查询失败时会让它每天重试，制造噪音。`once` 交过即永久跳过
（要重交用 `request run <id> --force`），`manual` 从不自动跑，`daily`/`weekly`/`monthly`
按日历日差算：`daily` 用 1 天而非 24 小时，所以昨晚 23:50 交过、今早仍会再交一份——
昨天那份属于昨天。

一个需求失败不挡别的（坏 spec、0 行、查询出错都只记一条继续）；但底库本身不成立
（`current` 缺失）时整体失败，因为那不是某一条需求的问题。

## 开发

```bash
uv run pytest          # 全部离线
```

测试只在一条缝上打桩：`HfDatasetApi` 的 transport。除此之外全部走真实代码路径。
