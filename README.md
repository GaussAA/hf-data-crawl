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
| `enrich` | 按下载量取头部补抓 `cardData`（标签、config/split 明细、行数列数）；`--ids a,b,c` 按需补（上限 200） | 是 |
| `fetch-sizes` | 经 datasets-server 补规模与列数（`num_rows` / `num_columns_max`）；`--changed N` 只重取内容已变的库，`--top 0` 表示全量（按 `current` 实际行数，不写死常量），`--ids a,b,c` 只补点名的库（上限 200，忽略 `--top`） | 是 |
| `fetch-files` | 按下载量取头部，经 `/tree/main` 补文件清单汇总（试点：top 1000，每库默认 1 页）；`--ids a,b,c` 只补点名的库（上限 200，忽略 `--top`） | 是 |
| `fetch-readme` | 逐库抓 README 全文（`description` 只有约 600 字符摘要）；`--ids` 按需、`--top N` 取头部、`--top 0` 全量、`--changed N` 只重取 commit 已变的、`--rate 15` 匀速（配额 5000 次/5 分钟）；独立表，**不进默认 31 列导出** | 是 |
| `reconcile` | 全量对账：按 `createdAt` 稳定序扫全站，产出新增/删除/转私有/改名/sha 变更 | 是 |
| `build` | 重建派生层：默认只重放近 7 天热区，`--stale` 只修派生滞后的日子及其后，`--full` 全量重放 | 否 |
| `query` | 查内置榜单或执行任意只读 SQL | 否 |
| `bronze` | 按日期从 Raw 现算 Bronze 并打印／导出（只读、不落盘、不写台账）。Bronze 只保留最新一天，历史那几天只能这样回看——**仅限 Raw 里还在的日期**（`archive` 前的 30 天窗口）；已压入冷区的日期回不来，cold 基线按 id 跨天去重、不是某天的快照 | 否 |
| `request` | 需求与交付管理：`new` 建需求、`list` 看全部、`show` 看口径与台账、`method` 校验过程记录并重算脚本索引、`input` 收新版输入表、`diff` 比两版口径、`compare` 比两次交付、`run` 交付一次（`--dry-run` 只算不写、`--preview` 落草稿区可打开看）、`run-due` 跑到期的、`index` 重写交付索引、`usage` 反查词条影响面、`prune` 清旧产物字节 | 否 |
| `report` | 生成单文件榜单页（离线可看） | 否 |
| `doctor` | 体检：调度新鲜度、底库行龄、层间勾稽、派生凭证、产物新鲜度、断日与残留文件；异常写 `ALERT.txt` 并追加体检流水 | 否 |
| `archive` | 归档早于 N 天的 Raw 分区：合并小分块并去重，先验证 Bronze 逐行相等，原文件移入 `backup/raw/` | 否 |
| `compact` | 压缩冷区 Raw：跨天去重成一份基线，原分块移入 `backup/raw/` 不删除 | 否 |
| `prune` | 清理备份：把 `backup/` 里超过 7 天的分片真正删掉，释放磁盘（只动 `backup/`） | 否 |
| `docs` | 生成 `docs/DATA.md`：data 目录结构、各文件作用、全部字段含义 | 否 |
| `export-wide` | 把 `datasets` 宽视图导出成单文件 parquet（zstd 压缩；可重建，不参与状态更新） | 否 |

常用参数：

```bash
hf-data-crawl collect      [--date YYYY-MM-DD] [--limit 1000] [--max-pages N] [--pages-without-new 3] [--preset NAME] [--refresh-top 20000] [--cards] [--card-pages 200] [--skip-if-done] [--dry-run]
hf-data-crawl enrich       [--date YYYY-MM-DD] [--top 20000] [--ids a,b,c] [--limit 1000]
hf-data-crawl fetch-sizes  [--date YYYY-MM-DD] [--top 20000] [--changed N] [--ids a,b,c] [--workers 16] [--retry-unavailable]
hf-data-crawl fetch-files  [--date YYYY-MM-DD] [--top 1000] [--ids a,b,c] [--workers 8] [--retry-unavailable] [--timeout 30] [--max-retries 1] [--max-pages 1] [--refresh-truncated]
hf-data-crawl fetch-readme [--date YYYY-MM-DD] [--top 20000|0] [--ids a,b,c] [--changed N] [--workers 12] [--timeout 30] [--max-retries 2] [--retry-denied]
hf-data-crawl reconcile    [--date YYYY-MM-DD] [--limit 1000] [--max-pages N] [--probe-limit 2000]
hf-data-crawl build        [--stale | --full]                            # 默认只重放近 7 天
hf-data-crawl compact      [--older-than-days 7] [--date YYYY-MM-DD]  # 冷区跨天压缩
hf-data-crawl prune        [--dry-run]                              # 清理备份（只动 backup/）
hf-data-crawl query        [--board NAME] [--limit 50] [--sql "..."]   # NAME 见「内置榜单」；省略则 growth
hf-data-crawl bronze       [--date YYYY-MM-DD] [--id 作者/名称] [--limit 5] [--out PATH]  # 回看某天的原始标量
hf-data-crawl report       [--out PATH] [--top 20]
hf-data-crawl archive      [--older-than-days 30] [--date YYYY-MM-DD]
hf-data-crawl docs         [--out PATH]           # 默认 docs/DATA.md
hf-data-crawl export-wide  [--out PATH]           # 默认 data/exports/datasets.parquet
hf-data-crawl request      new REQ-0001 --title 标题 --requester 姓名 [--purpose ...] [--cadence once|daily|weekly|monthly|manual] [--kind query|backfill] [--source 输入表.xlsx]
hf-data-crawl request      list
hf-data-crawl request      show REQ-0001
hf-data-crawl request      method REQ-0001 [--check]   # 校验 method/ 并按目录实际内容重算脚本索引
hf-data-crawl request      input REQ-0001 --add 新表.xlsx   # 收进 input/v<n>/，报字段差异，不改 spec
hf-data-crawl request      diff REQ-0001 [v1 v2]     # 比两版口径；默认最近两版
hf-data-crawl request      compare REQ-0001 [A B]    # 比两次交付的逐列差异；默认最近两次
hf-data-crawl request      run REQ-0001 [--force] [--dry-run] [--preview]   # --dry-run 只算不写；--preview 落 scratch/preview/ 可打开看
hf-data-crawl request      run-due                  # 只跑到期的（计划任务用）
hf-data-crawl request      index [REQ-0001]         # 重写 deliveries/INDEX.md；不写则全部重写
hf-data-crawl request      usage 下载量               # 反查：这个词条/库列被哪些需求用到、最近交的哪版
hf-data-crawl request      prune [REQ-0001] [--keep 1] [--dry-run]   # 清旧交付的产物字节，档案与 sha256 留着
```

`--kind backfill` 时 `--source` 必填：该文件会被复制进 `<需求目录>/input/`，原件不动，
此后需求不再依赖外部路径。目录名按 `REQ-xxxx-<kind>-<标题短名>` 自动生成，命令仍只用编号。

全局参数 `--data-dir` / `--requests-dir` / `--lock-wait` 需放在子命令之前；前两者默认取
`HF_DATA_DIR` / `HF_REQUESTS_DIR`，否则 `./data` 与 `./requests`。

锁分三档（见 `src/hf_data_crawl/ops/cli.py`）：**写命令**（`collect` / `reconcile` / `build` / `enrich` / `fetch-sizes` / `fetch-files` / `fetch-readme` / `archive` / `compact` / `prune` / `export-wide`）执行期间持有 `data/.lock` 的独占锁，同一时刻只能跑一个；拿不到锁会失败并在 `runs/` 留记录，进程被杀后系统自动释放，不会留下死锁。**`report` / `docs` 不改数据，但要同时读多张表**，故与写命令共用同一把独占锁以保证一致快照（默认等锁 60 秒）。**`fetch-sizes` / `fetch-readme` 例外**：全量抓取耗时可达数小时到十几小时，只在「并入正式表」时自排短锁，否则会饿死当天的 `collect`。`doctor` 等锁 2 秒，拿不到就降级为非快照；只有 `query` 完全不加锁。

**默认交付底库**：`current` 只留状态标量，语义字段分散在 `silver` / `carddata` / `size`。
`export-wide` 按 `delivery/fields.py` 的**默认交付字段契约**把 `gold.datasets` 视图裁成
默认列集（当前 31 列），写成 `data/exports/datasets.parquet`——只留与数据集有关、有用且
必要的画像字段（标识、描述、热度、许可、语言、模态、任务、数据类型、标签、补充画像、
学术关联、规模、大小），把水印、抓取状态、卡片凭证与九成空的补抓列排除在外。
多值列就是 LIST 原值（与视图同构，可直接 `list_contains` 精确筛）；只有 CSV 交付这一层
才展平并把机器码换成页面文字。契约的增删与 v3 压平、v5 撤销的来龙去脉见
GLOSSARY #62–#66，版本号在 `delivery/fields.py` 的 `CONTRACT_VERSION`。

`regions` 实测 100% 有值但 **99.94% 是 HF 自动推断的 `us`**，故不进默认面；它与 DOI、引用、项目主页等扩展字段一起登记在 `EXTENDED_FIELDS`，需要时写 `output.columns: [id, regions, ...]` 点名即可投列——**默认面管「不指定时给什么」，不是「总共有什么」**。完整清单见 [docs/DATA.md](docs/DATA.md) 第八节。这份文件也是**数据集筛选需求未指定 `output.columns` 时的默认投影**；不登记为表、不参与状态更新，视图本身仍保留全部列。四处共用一份定义，详见 [GLOSSARY](GLOSSARY.md) #62–#66。

**文件清单补采（试点）**：列表接口不返回 `siblings`（见 [docs/DATA.md](docs/DATA.md) 第九节），
单库详情按头部补又会反复超时，故 `fetch-files` 改走 `/tree/main?recursive=true&expand=false`：
每页固定 1000 条、约 110–230 KB / 0.4–0.7 s。`hf-files` 每周日 07:30 补下载量前 1000 名，
体检给 10 天年龄红线——漏跑一次不喊，连丢一周才说明停了。默认每库只取第一页，有后续页时
标 `truncated=true`：此时 `file_count` / `total_bytes` / `file_formats` / `has_readme`
都只是**这一页的下限**，要完整清单须显式加大 `--max-pages`；`--refresh-truncated` 只重抓
截断行，且 `--max-pages` 仍为 1 时会直接失败。结果落 `enrich/files.parquet`
（`Origin.NETWORK`，重建不清），自动进入宽视图。这是按头部补的**子集**，别拿它算全站分布。
凭据、配额与断点续跑的细节见 GLOSSARY #34–#36。

内置榜单（`query.gold.BOARDS` 是唯一真相源：`query --board` 的候选项由它自动生成，`report` 页面复用同一批口径）：

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

**报告页会标出非在架行**：`disappeared` / `restricted` / `renamed` 的指标停在 `last_seen_at` 那天，榜里的这些行带「非在架」角标，首屏「30 天下载合计」也会注明其中含多少行非在架。数值一个不改——那是既有统计口径；只是不让冻结值冒充今日读数。

## 数据布局

```
data/
├── raw/dt=YYYY-MM-DD/<类别>-<来源>-<页号>-<摘要>.json.gz  # 原始响应：热区原样；满 7 天由 compact 压入 cold
├── cold/raw/                                         # 冷区基线（跨天去重）与 manifest.json
├── backup/raw/dt=YYYY-MM-DD/                         # 归档/压缩移出的原分块，确认后由 prune 清理
├── bronze/dt=YYYY-MM-DD/bronze.parquet                # 一行一条，含 raw 列；只保留最新一天，旧分区可由 raw 重建
├── silver/dt=YYYY-MM-DD/silver.parquet                # 属性变更流水：新增或属性变了才追加；标签已拆 16 类维度，不含指标
├── state/
│   ├── current.parquet                                # 每库一行最新态
│   └── observations/dt=YYYY-MM-DD/obs.parquet         # 指标变化流水
├── enrich/
│   ├── carddata.parquet                               # 卡片宽表
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

**Raw 保留策略（2026-11-07 起，三层语义，常量集中在 `platform/retention.py`）**：

- **热区**：近 7 天（`REPLAY_WINDOW_DAYS`）的 `data/raw/dt=*` 原样保留，`build`
  默认按天重放；`archive` 可对单日做同天去重，并把写入序冻结进 `groups`。
- **冷区权威层**：`silver`（属性变更流水）、`observations`（指标变更流水）、
  `current` 的历史**不再清空**，它们是历史事实来源。实证：只重放热区与全量
  重放逐行等价（契约见 `tests/pipeline/test_rebuild.py`）。
- **冷区 Raw**：`compact` 把超过 7 天的多日全量快照跨天去重压成一份基线，
  存 `data/cold/raw/`（分片 `groups` 仍冻结写入序），原分块**只移入
  `backup/raw/`、不删除**。写基线前后逐个 id 校验胜出者，不等即撤销。

`build --full` 保留旧的全量重放；冷区权威层若已损坏（观测流水或当前态为空），
默认 `build` 会自动回退全量。**`--full` 的前提是全部 Raw 都在 `data/raw`**：冷区
日期的分块早已被 `compact` 移去 `backup/raw/`，那些日期的历史清空后补不回来，故
检测到这类日期时 `--full` 直接拒绝（`--allow-cold-loss` 可显式放行，recovery 回退
则只告警不拒）。`doctor` 把 `cold/raw/manifest.json` 里已压缩的
日期算作「有观测」，避免压缩后误报断日；并在冷区未压缩、规模超阈值、备份
超过 7 天未清理时写 `ALERT.txt`。

**销毁单独成一步（`prune`，2026-11-09 起）**：搬运与销毁分开——`compact` /
`archive` 只移不删，`prune` 才删，且只动 `data/backup/`。理由很直白：
「永不删除」等于把磁盘交给时间，每天约 330 MB 原始分块进 backup，一年上百 GB，
而它们早已被冷区基线（按 id 去重、体量恒定）取代。阈值与 `compact` 共用
`platform.retention.BACKUP_PRUNE_AFTER_DAYS`（7 天），按**目录名**判龄而不是 mtime
（同步盘会整体改掉 mtime）。`prune --dry-run` 只算不删；`doctor` 的「存储」一项
把 data 总量摊成 raw / cold / backup / reports / 其余，谁在长一眼可见。


每张表的身份（路径、schema、来源）统一登记在 `platform/tables.py`，读写、视图、重建清理都从它取，不在各处各自硬编码。

## 关键机制

每项机制只在一个地方实现；改动前先看对应的决策记录，那里写着为什么是它、以及被否掉的方案。

| 机制 | 它保证什么 | 代码 | 决策 |
|---|---|---|---|
| 时间戳水位线 | 跨日增量不漏采；未来 7 天外的异常 `lastModified` 先过滤 | `acquire/hf/watermark.py` | GLOSSARY #6、#27 |
| 一轮扫描的时序 | 翻页 → 落盘 → 页钩子 → 存游标 → 判停止，只实现一次 | `acquire/hf/scan.py` | GLOSSARY #20 |
| 写入序 | `written_at` 记进 Raw，Bronze 去重按它排序；mtime 与文件名不作数 | `pipeline/raw.py` | GLOSSARY #17 |
| 派生原子性 | 标记文件 + 启动恢复；恢复是**从该日起后缀重放**，不是只重算那一天 | `pipeline/marker.py`、`pipeline/recovery.py` | GLOSSARY #28、#78 |
| 派生凭据 | 每日 Raw 指纹落 `state/lineage.json`；指纹不符即 `build --stale` | `pipeline/lineage.py` | GLOSSARY #44 |
| 同日帧 | 卡片补抓批次经 `day_frame` 补位，Bronze 分区仍纯净 | `pipeline/bronze.py` | GLOSSARY #45 |
| 层间勾稽 | `current` ↔ `observations` ↔ `silver`、导出与 current 逐 id 对齐 | `ops/doctor.py` | GLOSSARY #48、#76 |
| 失效状态 | 404 删除 / 401 转私有 / 307 改名分开记；探测被限流时轮转 | `acquire/reconcile.py` | GLOSSARY #15、#72–#73 |
| 默认交付字段 | 31 列契约只由 `delivery/fields.py` 定义，导出与查询都从它投影 | `delivery/fields.py` | GLOSSARY #62–#66 |

**恢复不是「只重算那一天」**：单日重放会把 `current` 覆盖成那天的值，等于把当前态整体回退；
`pipeline.recovery` 一律走 `replay_suffix`。手工排障步骤见
[RECOVERY.md](docs/operations/RECOVERY.md)。

**派生的边界**：默认只重放近 7 天；冷区靠 `silver` / `observations` / `current` 的权威层，
Raw 原块由 `compact` 移入 `backup/`（只移不删）。`build --full` 的前提是所有 Raw 都还在
`data/raw`，冷区被搬走时它会直接拒绝——细节见 GLOSSARY #31、#78。

**刷新的节奏**：日增量扫最近更新的库，`collect --refresh-top N` 补头部下载量池；
采集走 lean 档（16 字段约 980 B/条），`cardData` 只在变化集（`collect --cards`）或按需
（`enrich --ids`，上限 200）时取——full 档约 6 KB/条，全站日拉会多传数倍字节。
`downloads` 不是实时值，增速必须跨日观测（GLOSSARY #9）；卡片**不做**全站遍历（GLOSSARY #68）。

## 定时

```powershell
pwsh -File scripts\setup-schedule.ps1              # 注册：hf-daily（每日 03:10 + 登录后 3 分钟，按 daily.ps1 的顺序跑完当日例行）· 每日 00:30 压缩 · 周日 06:00 规模 · 周日 07:30 文件清单 · 周一 05:00 归档
pwsh -File scripts\setup-schedule.ps1 -Uninstall   # 卸载
Get-ScheduledTask -TaskName hf-* | Get-ScheduledTaskInfo   # 核对
uv run hf-data-crawl doctor                        # 手动体检；有问题退出码 1，并写 data\ALERT.txt
```

**例行是一条链，不是一个钟点表**。`scripts\daily.ps1` 按序跑八步（采集 → 派生 → 对账 → 规模随变化刷新 → 导出 → 交付 → 清理备份 → 体检），每步直接调一条 `hf-data-crawl` 子命令，`Invoke-HfTask` 负责日志与退出码；`scripts\*.ps1` 仍是可单独手动跑的入口（collect / reconcile / export / deliver / doctor / sizes / files / archive / compact / prune）。公共部分（定位 `uv`、代理默认值、日志、退出码）抽在 `scripts\_common.ps1`。

时钟只负责「什么时候开始」，**先后由脚本里的顺序保证**。采集撞上瞬时断连会隔 2 分钟重试一次（2026-11-15 03:10 实测：TLS `UNEXPECTED_EOF`，一分钟后对账却正常）；仍失败就照旧记失败，重试只治抖动、不掩盖故障。导出与交付**依赖 `build` 与 `reconcile` 成功**（派生层没跟上就发布，等于把旧值当新值交出去；对账没完就导出，等于发布一份还没对齐的全站快照），前置失败即跳过并在 `-daily.log` 里写明原因；对账是独立的全站扫描，采集失败也照样跑；体检永远跑，且它的非零只记「告警」不算链条失败——告警与故障挤进同一个码，铃声很快就没人听。

这条设计来自 2026-11-09 的一次实测：机器 00:30 之后关机、19:03 开机，`-StartWhenAvailable` 把错过的五个任务**在同一分钟全部补跑**，导出抢在对账前完成，一出来就落后 2,466 行，体检随即误报并以退出码 1 收场。故补跑只留给 `hf-daily`（补的是整条链），compact / sizes / files / archive 是互不相干的纯周期维护，错过就等下一轮——它们补跑只会跟链抢写锁。

要点：

- **需要 PowerShell 7**（`pwsh`）。注册时会把 `pwsh` 的绝对路径写进任务动作——计划任务的环境不保证有 `PATH`。
- 代理默认取 `HF_PROXY`，未设则用 `http://127.0.0.1:7897`。
- 日志按轮写成 `data\logs\<时间戳>-<任务名>.log`（如 `20261108T031000-collect.log`），**UTF-8**，每次跑完自动保留最近 200 个——与 `runs/` 的 500 条同一哲学：有界才不会被计划任务悄悄撑爆。此前是 `collect.log` 单文件永久追加，既对不上 `runs/` 里那轮摘要，也只能靠人手工改名成 `.bak`。三处编码都设了才有效（`PYTHONIOENCODING` / `Console::OutputEncoding` / `OutputEncoding`），只设一处仍会落成 GBK——中文系统下打开是乱码。**查看时请用 `Get-Content -Encoding utf8`**：文件是纯 UTF-8 无 BOM，而 `Get-Content` 默认按 ANSI（中文系统即 GBK）读，会把它读成乱码——那是查看方式的问题，不是日志坏了（2026-11-08 实测踩过，一度误以为编码设置失效）。
- `uv` 找不到时回退到 `%USERPROFILE%\.local\bin\uv.exe`；两者都没有则直接报错，不会静默跑空。

每轮摘要落在 `runs/`，失败也写并带错误原因，不会静默。`runs/<时间戳>-<命令>.json` 与 `logs/<时间戳>-<命令>.log` 是同一次运行的**两半**（机读结论 / 人读现场），同键不同扩展名，查某一轮取同前缀即可。`doctor` 不写 `runs/`：它退出码 1 表示「有告警」而非「命令失败」，写进去会污染「近 7 天无失败运行」那条判据；体检结论看 `data\ALERT.txt`（覆盖式，只存最新一次）、流水 `state\health-history.jsonl`（一行一次，据它追「断日从哪天开始」「这条告警连续几天」，保留最近 `HEALTH_KEEP` 次）与对应日志。

**写命令互斥**：所有写命令（collect / enrich / fetch-sizes / fetch-files / fetch-readme / reconcile / build / archive / compact / prune / export-wide）持有数据目录的锁，避免 `current.parquet` 被并发覆盖。人工执行时撞上别人会立刻报错并说明占用者；计划任务的包装脚本加了 `--lock-wait 900`，会先等 15 分钟而不是白丢一轮（`fetch-sizes` 一次可能跑一小时、`fetch-readme` 全量要十几小时，与 03:10 的采集重叠过，曾因此丢过一次采集）。

## 需求与交付

底库建起来是为了用。每份对外的数据交付都是一份**档案**，而不是一次临时的查询。

**一套生命周期，变只在 `kind` 与执行体**：出一份清单、按 URL 回填一张表、日后的对账
报表，本质都是「有人提了需求 → 按节奏跑 → 交得出去 → 说得清来源」。所以它们共用
同一个 `requests/` 目录、同一套 `REQ-xxxx` 编号、同一条 `request run / run-due` 命令、
同一份交付档案。按产出形态另开目录或命令，等于把「不变」的部分复制若干份，而形态
是列不完的。

```
requests/REQ-0001-query-高热度数据集清单/
  spec.yaml                  # 公共头 + kind + 该形态的口径体
  history/v1.yaml            # 交付时自动快照，只留「真的用过」的口径
  method/METHOD.md           # 过程记录：步骤、判据、哪步机器哪步人判
  method/method.json         # 过程记录的机器读版本：逐脚本 用途/怎么跑/sha256
  method/scripts/            # 实际跑过的脚本原件（指纹不匹配就不许交付）
  input/清单.xlsx             # 回填型：受理时复制进来的输入快照（query 型没有）
  overrides.json             # 回填型：逐库人工判读结果
  deliveries/20261108T145000/
    data.parquet / 已补全.xlsx    # 产物字节，不进 git（白名单只放行两份说明）
    manifest.json            # 机器读：kind、底库指纹、输入指纹、方法指纹、行数、sha256、健康
    README.md                # 人读：要什么、怎么算的、基于哪天的底库、几行
  deliveries/INDEX.md        # 引擎生成的台账索引：每条的性质、可复现性、当前有效
  deliveries/AUDIT.md        # 唯一的人工文件：引擎事后核不出的历史事实
  scratch/preview/           # 预览产物（--preview），草稿、不进台账、交付后删除
```

**交付目录是不可变台账，不是货架**：每条只增不删，被取代的留在原地；「哪条算数」看
`deliveries/INDEX.md` 的最后一行。想先看产物长什么样，用 `request run <id> --preview`
把它落进 `scratch/preview/`——「想看一眼」不该以多一条交付为代价（ADR-0003）。

**出口两道闸门**：代码脏（`src/`、`pyproject.toml`、`uv.lock` 有未提交改动）不交付——
档案记的 commit 必须真能跑出这份产物；底库与产物字节都没变也不交付——除了给版本号留个
摆设，那条账没有新信息，`run-due` 会把它记成「跳过」而不是「失败」。

**失败也有下文**：被拦下的交付**不占版本号**（`history/vN.yaml` 只在换名成功后才留下），
但每次正式交付都会写一条 `runs/<时刻>-request-run.json`，失败也写——「这轮到底跑过没有、
被什么拦住」不再只活在终端里。体检的 `delivery_anchors` 逐份对账输入快照与历史版本，
删掉或改过都会被点名。取舍见 [ADR-0004](docs/adr/0004-failure-semantics.md)。

**产物可清、台账不可清**：`request prune` 只删旧交付的产物字节，manifest 与 README
一份不动，删掉的每一样都写进 `deliveries/PRUNED.json`（连同 sha256），`INDEX` 标「已清理」。
当前有效那条永远留、`--keep` 至少 1。想清之前先 `--dry-run` 看会掉多少。
体检的 `delivery_products` 盯的是**账实相符**（说清理了却还在 / 记录与 sha256 不符），
产物没留存只记个数——它是可重算的副本，不是台账。取舍见
[ADR-0005](docs/adr/0005-delivery-retention.md)。

**改词条前先查影响面**：`request usage 下载量` 列出所有引用它的需求（条件、排序、输出列、
回填列），以及各自最近交的版本。口径散在各需求目录里，`grep` 只会把注释里提过一句的也算上。

**目录名 = `REQ-xxxx-<kind>-<标题短名>`**：编号保唯一与排序，`kind` 一眼看出执行路径，
标题短名不用打开文件就知道这单是什么。命令始终用编号（`request run REQ-0002`），
目录改动不必跟着改命令；改标题也不搬目录，正式名字以 `spec.yaml` 为准。

**需求目录自包含**：对方给的文件路径只在受理时用一次——`request new --source` 把输入表
复制进 `input/`，spec 只写相对路径；产物只进 `deliveries/<时刻>/`。绝对路径、`..` 越界、
`copy_to` 外写一律在读入时拒绝。整个目录拷走或 clone 下来仍读得懂、跑得动，外部原件
之后被删被改都影响不到它。每次交付的 `manifest.json` 还会记下输入快照的相对路径与
sha256，于是「基于哪一版清单」和「基于哪一版底库」一样能自证。

**过程也要自包含**：输入、口径、产物三件齐了，仍回答不了「怎么算出来的」——
`spec.yaml` 能写「取 `downloads` 列」，写不了「这 24 个库的训练阶段是逐个读 README
判的，判据是明写 pretrain / fine-tune / post-training 才算」。所以每份需求还有
`method/`：`METHOD.md` 记步骤与判据，`method/scripts/` 存实际跑过的脚本原件，
`method.json` 逐份钉 sha256。**方法不合规就不交付**——交付档案里会钉住方法指纹，
但钉不住已经忘了的判据；数据交出去就永久补不回来历，宁可不交。

分工是明确的：机器算得出 sha256，算不出这个脚本为什么存在。所以 `request method`
只负责重算指纹与增删条目，脚本的用途、怎么跑、读写了什么，一律留给人写；新冒出来的
脚本给一条「待补」占位，而不是替它编一句。方法文档里那行 `<!-- 待填：` 没删，
就视同「还没有方法」，照样拦。

环境这一层**不复制、只钉住**：脚本 import 的底库代码在 `src/`，往每个需求目录塞一份
只会得到 N 份迟早各不相同的副本。`manifest.method.env` 记下包版本、契约版本与提交号，
变了就对不上账——与用 `current_md5` 钉底库是同一手法。

`spec.yaml` 里 `kind: query` 带 `criteria` / `order_by` / `output`，`kind: backfill`
带 `fill:`（输入表、逐列取值方式）。`delivery/requirement.py` 负责读与校验，`delivery/delivery.py`
只认统一的 `Payload`，按 `EXECUTORS` 把活分派给执行体——将来多一种形态，写一个
执行体、注册一行即可，不必新开目录、命令或台账。

**口径会变，版本号不能复用**：需求本来就很少一次说定。改了条件、换了输入表、调了
逐列规则，都要把 `version` 加一——`history/vN.yaml` 是「那一版真的用过」的忠实快照，
交付档案里的 `spec_version` 指的就是它。若 vN 已存在、内容却被就地改过，交付会**被
拦下**（不是警告）：档案指着一份「当时说过、现在已经不是」的定义，这种错不会报错，
只会让人拿着错的前提核对三个月。

判定用**语义指纹**（`spec_digest`）而非文件字节：只比「决定产物长什么样」的那部分，
所以注释、键序、显式写出的默认值不算变更，`下载量 > 1000` 改成 `> 800` 一定算。
`manifest.spec_sha256` 记下每份交付对应的口径指纹，`request show` 并列显示；
体检对「已交付但版本号与内容对不上」的需求判红。完整取舍见 GLOSSARY #70。

变更这件事本身有三件配套工具：

- **换输入表**：`request input REQ-0002 --add 新表.xlsx` 把新表收进 `input/v<n>/`，
  逐项报出与当前表头的差异（新增 / 少了 / 共同列挪位），并点出 spec 声明了、新表里
  却没有的列。旧快照原地保留——覆盖会毁掉旧档案的自证链。命令**不代劳改 spec**：
  改 `fill.source.path` 等于换口径，得升版本、得写清为什么换。
- **看口径差**：`request diff REQ-0002 [v1 v2]` 默认比最近两版，按 `metric` 与列名
  对齐（插一条条件不会让后面每条都显示「变了」），只比决定产物的部分——改标题会
  单独列一句「不影响产物」。
- **看交付差**：`request compare REQ-0002 [A B]` 逐列比对两份产物：行数、增删行、
  共同列的改值格数并抽样。查询型按 `id` 对齐，回填型按标识列对齐且先把链接归一成
  `作者/名称`——不归一，同一个库会被当成两个。

**表头指纹**同时进了档案：`manifest.fill.input.header` 记列名与顺序的 sha256 及
全量列名。交付时与上一份比，变了就在 README 顶部写一行「输入表头与上一份交付不同：
新增 X；少了 Y」——不阻断，但「字段悄悄换了」再也不会无声无息。

**query 形态的口径只写词条，不写字段名**：`下载量` 是近 30 天，`累计下载量` 才是
历史累计——同一句话两种理解差两个数量级。词表见 `delivery/metrics.py` 与
[GLOSSARY](GLOSSARY.md) #51，`request show` 会把口径连同编译后的 SQL 一起打出来。

**不写 `output.columns` 就按默认底库交付**：契约在 `delivery/fields.py`（逐列中文定义、
分组、页面标签对照、`EXTENDED_FIELDS`、`CONTRACT_VERSION`），`data/exports/datasets.parquet`
是它的物化，列序完全一致。交付 README 会附逐列定义、「详情页标签 ↔ 库列」对照与扩展
字段清单——页面叫 `Tasks` / `Size` / `Tags`，库里叫 `task_categories` / `size_categories` /
`plain_tags`，值也多为机器码，不对照就会以为「库里没内容」。需要默认面之外的字段时在
spec 写 `output.columns: [id, regions, arxiv_ids]`；`doctor` 会勾稽导出列集与契约。

`manifest.json` 里的 `source.current_md5` 是整份档案的锚点：日后任何一份交付都能
回答「它基于哪一版底库」。`.gitignore` 对 `deliveries/` 用的是**白名单**——只放行
`manifest.json` 与 `README.md`，产物字节一律留在本机，靠 sha256 绑定。按扩展名逐个
排除永远列不完，因为产物形态会随 `kind` 增加。

**跑一次交付**（`request run`）守三条：0 行不交付、超 `max_rows` 不交付、行数突变
**标疑但不阻断**。第三条是有意的：`once`/`weekly` 这类需求当天交不出货比数字可疑
更糟，所以照常交付，但把「与上次相比从 3108 行变成 8900 行」写进 manifest 的
`health` 与 README 顶部。判疑只在**同一口径版本内**比较——改了口径必然行数大变，
那时比较只会天天发假警报，而假警报最能训练人忽略告警。

先写 `.tmp-*` 目录、三件齐了再整体改名，所以中途失败绝不会留下「有数据没说明」。
CSV 交付时才把多值列展平成 `;` 文本，并把语言 / 任务 / 规模的机器码换成页面形式
（`ko` → `Korean`）；parquet 保留 LIST 与机器码。spec 若显式投了 LIST 列，写 CSV 前
同样先拍平——否则 Polars 会拒绝嵌套类型。

**跑一批交付**：`request run-due` 扫 `requests/` 下所有 spec，按各自 `cadence` 判断
「今天是否到期」，到期的才跑。判断依据是**上一次交付的时刻**，不是上一次运行成功
与否——后者在 0 行被阻断或查询失败时会让它每天重试，制造噪音。`once` 交过即永久跳过
（要重交用 `request run <id> --force`），`manual` 从不自动跑，`daily`/`weekly`/`monthly`
按日历日差算：`daily` 用 1 天而非 24 小时，所以昨晚 23:50 交过、今早仍会再交一份——
昨天那份属于昨天。

一个需求失败不挡别的（坏 spec、0 行、查询出错都只记一条继续）；但底库本身不成立
（`current` 缺失）时整体失败，因为那不是某一条需求的问题。

**这套东西最终是给 Agent 用的**：人只说「要什么」，判定形态、写口径、写方法、预演、
交付、变更与比对都由 Agent 走。规程写在 `.agents/skills/hf-request/SKILL.md`——
接收怎么判定、响应要落下哪几件、被拦下时按什么改、哪些做法一见就停。命令负责把关，
skill 负责别把工夫用错地方；两者分开，是因为「规则」要能被机器执行，「做法」要能被
下一个接手的 Agent 读懂。

## 回填形态：`kind: backfill`

对方常给一张只有 URL 一列的表，其余列要按约定口径填上。这类活做一次可以，做到第五次
必然出错——尤其「下载量取 30 天还是累计」换个人就重新裁一遍。**它不是另一套体系**：
`REQ-0002` 与 `REQ-0001` 的公共头、节奏、护栏、交付档案逐字同形，只是 `kind: backfill`
并把口径写进 `fill:` 段；跑它的命令也是同一条。

```bash
hf-data-crawl request new REQ-0002 --title 清单补全 --requester 姓名 --kind backfill --source "C:/.../清单.xlsx"
hf-data-crawl request run REQ-0002 --dry-run   # 只算不写：逐列能填多少、哪些留空
hf-data-crawl request run REQ-0002             # 出正式交付档案
```

```
requests/REQ-0002-backfill-具身感知数据集信息补充/
  spec.yaml                       # 公共头 + kind + fill: 口径体
  method/METHOD.md                # 判读规程：哪 8 列人判、每列判据、哪步机器哪步人
  method/scripts/verify_overrides.py  # 判读表自检（URL 在不在清单、列名声明过没有）
  input/清单.xlsx                  # --source 指定的表，受理时复制进来；原件不动
  overrides.json                  # 逐库人工判读结果（引擎只合并、不生成）
  history/v1.yaml                 # 首次交付时留存的口径快照
  deliveries/<时刻>/
    清单-已补全.xlsx / manifest.json / README.md
```

`--source` 指向的外部路径**只在受理时用一次**；spec 里写的是
`fill.source.path: input/清单.xlsx`，`output` 只给文件名。产物不会再落到 Downloads 或
原件同目录，台账就是这份 manifest，不再另记一份 `runs/*.json`。

**机械的归引擎，判断的归人**：一列只能用一种取值方式。

| 方式 | 含义 | 例 |
|---|---|---|
| `field` | 取底库某列 | `{field: main_size, format: gb}` |
| `template` | 按底库列拼串，占位符缺值整格留空 | `{template: "https://arxiv.org/abs/{arxiv_ids}"}` |
| `const` | 常量 | `{const: HuggingFace}` |
| `manual` | 引擎不填，值留空、原因进说明页 | `{manual: README 明写训练阶段才算}` |

**五条只写不说的规矩**：

1. **绝不改原件，也绝不外写产物**：输入只读；产物只进 `deliveries/<时刻>/`。`copy_to` 与带路径的 `output` 在读入时直接拒绝。
2. **输入必须自包含**：`fill.source.path` 只能写需求目录内的相对路径，绝对路径与 `..` 越界直接拒绝。
3. **默认不覆盖**：只填空格。人家手填的可能比底库新，也可能有底库没有的判断；要覆盖须显式 `overwrite: true`。
4. **不猜**：判读不出来的列写 `manual`，留空并在「补全说明」页点名。一份看着齐整、实则编造的表，比诚实地空着几格危险得多——空着会有人来问，编造不会。
5. **未声明的列一律不碰**，并在报告里列出来，让人看得见还剩多少活。

产物是**新文件**，附一页「补全说明」（口径、留空原因、未命中清单）。同输入同底库连跑两次
**逐字节相同**：xlsx 里的条目时间戳与 `dcterms:modified` 都锚到底库快照日，否则内容全同、
md5 却每次不同，「这次和上次一样吗」只能靠人肉比对。

判读层（README 里「训练阶段是微调还是后训练」「构造方式是仿真还是真机」）写进
`overrides.json`，与机械层分开：引擎自己不读 README、不替人下结论，但会把「哪些格子是
人工判读来的」记进 `manifest.json` 的 `fill.tallies` 与 README 的逐列规则表。**判读规程**
（哪几列必须人判、每列的判据是什么、README 与 splits 怎么核）写在 `method/METHOD.md`，
判读表本身则由 `method/scripts/` 里的自检脚本复验——结论和得出它的规则放在同一个目录里，
下次换人或换模型接手，照着做就行。判读表也可以放进子目录（`fill.overrides: judge/ov.json`），
档案记的是 spec 里写的**相对路径**，`method.inputs` 按同一路径钉 sha256：记 basename 会让
`judge/ov.json` 与 `ov.json` 在档案里长得一模一样，挪一次文件等于换了输入却看不出来。

输入快照连同 spec、overrides、history 与历次交付一起构成封闭单元；`manifest.json` 的
`fill.input` 记着它的相对路径、字节数与 sha256。要换一张新表，把文件放进 `input/`、
改 `fill.source.path` 并把 `version` 加一即可，全程不必碰外部路径。

## 代码结构

`src/hf_data_crawl/` 按垂直切片组织，依赖只能单向下行：

```
platform < pipeline < {acquire, query} < delivery < ops
```

| 切片 | 职责 |
| --- | --- |
| `platform/` | 锁、原子写、存储、词表、运行台账——最底层，不依赖任何切片 |
| `pipeline/` | bronze → silver → observations → current、marker、lineage、recovery |
| `acquire/` | 采集、对账、卡片、规模与文件清单；`hf/` 为其内部子包（API、传输、水位线） |
| `query/` | Gold 视图与榜单 |
| `delivery/` | 需求管理与交付：字段契约、回填、宽表导出、口径与产物比对。执行体各归其位（`query_exec` / `fill_exec`），档案层只认 `contract.Payload` |
| `ops/` | 体检、归档、压缩、清理、文档生成，以及命令行入口 `ops/cli.py` |

`acquire` 与 `query` 同档，互不依赖；`ops/cli.py` 是唯一的组合根，只调各切片公开的
`run_*` 入口，不碰下划线开头的实现。这六条规则由 `tests/test_architecture.py` 强制：
切片归位、只许下行、`platform` 不依赖他人、模块图无环（含延迟导入与 `TYPE_CHECKING`）、
跨切片禁用私有名、档案层与契约都不得认识具体的交付形态。

## 开发

```bash
uv run pytest          # 全部离线
```

测试只在一条缝上打桩：`HfDatasetApi` 的 transport。除此之外全部走真实代码路径。
