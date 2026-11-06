# hf-data-crawl

Hugging Face 公开数据集元信息底座。周期性抓取元信息、留存原始响应、维护指标流水，用 DuckDB 直接查询。

不下载任何数据集文件，只抓元信息。

## 快速开始

```bash
uv sync

# 本机需经代理访问 HF 时（可选）
export HF_PROXY=http://127.0.0.1:7897
export HF_TOKEN=hf_xxx          # 可选；匿名额度为 5 分钟 500 次

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
| `reconcile` | 全量对账：按 `createdAt` 稳定序扫全站，产出新增/删除/转私有/改名/sha 变更 | 是 |
| `build` | 从 Raw 重建全部派生层（含卡片与明细） | 否 |
| `query` | 查内置榜单或执行任意只读 SQL | 否 |
| `report` | 生成单文件榜单页（离线可看） | 否 |

常用参数：

```bash
hf-data-crawl collect      [--date YYYY-MM-DD] [--limit 1000] [--max-pages N] [--pages-without-new 3] [--refresh-top 20000] [--dry-run]
hf-data-crawl enrich       [--date YYYY-MM-DD] [--top 20000] [--limit 1000]
hf-data-crawl fetch-sizes  [--date YYYY-MM-DD] [--top 20000] [--workers 16] [--retry-unavailable]
hf-data-crawl reconcile    [--date YYYY-MM-DD] [--limit 1000] [--max-pages N]
hf-data-crawl query        [--board growth|growth_pct|downloads|alltime|categories] [--limit 50] [--sql "..."]
hf-data-crawl report       [--out PATH] [--top 20]
```

全局参数 `--data-dir` 需放在子命令之前，默认取 `HF_DATA_DIR`，否则 `./data`。

内置榜单：

- `growth` 最近两个观测点的 downloads 增量（只列正增长，附 `span_days` 说明跨度）
- `growth_pct` 增幅百分比（要求起始下载 ≥ 100，避免小数被放大成噪声）
- `downloads` 30 天下载榜
- `alltime` 累计下载榜
- `categories` 任务类别聚合

增速取「最近两个观测点」而非固定 7 日窗口：日增量只扫最新更新的万把条，全站指标靠周对账刷新，固定窗口在周节奏下取不到两点。

## 数据布局

```
data/
├── raw/dt=YYYY-MM-DD/part-<页号>-<内容摘要>.json.gz   # 原始响应，永久保留
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

每张表的身份（路径、schema、来源）统一登记在 `tables.py`，读写、视图、重建清理都从它取，不在各处各自硬编码。

## 设计要点

- **两档采样**：日增量走 lean 档（不含 `cardData`，16 字段约 980 B/条）；`cardData` 只在需要时单独取。实测 full 档约 6 KB/条，全站日拉会白白多传数倍字节。补齐 `private`/`disabled`/`mainSize` 只让 lean 档从 923 涨到 980 B/条（+6.2%）。
- **一轮扫描的时序只有一处**：`scan.ScanRun` 管「翻页 → 落盘 → 存游标 → 判停止」，日增量、顶部刷新、全量对账、卡片补齐只提供策略。两条不变量在此一层：游标必须在写盘后、任何停止判断前保存（否则续跑重抓上一页）；页数上限由本层判定而非交给生成器（生成器到限时静默 return，说不出停止原因）。
- **排序必须显式**：不传 `sort` 时 HF 的游标基于 `trendingScore`，会随时间漂移，翻页必漏。全量用 `createdAt`（不可变），增量用 `lastModified`。
- **写入序落在 Raw 里**：`write_raw` 把 `written_at`（UTC、微秒）记进 payload 的 `meta`，Bronze 去重按它排序；文件名摘要、mtime、`label` 的字母序一概不作数。mtime 一复制就变，靠它排序会让「Raw 是唯一事实来源」名不副实——同一批数据在另一台机器上可能推出不同的 Bronze，且不报错。摘要不含 `written_at`，否则同内容重写会不断产生新文件。
- **水位线不用 `max(lastModified)`**：实测存在 2028 年的异常时间戳，任何单调性假设都会失效。改为「连续 N 页无新记录」。
- **下架判定只在扫完时做**：`reconcile` 用 `--max-pages` 有界跑时不会标记下架，否则未扫到的部分会被整片误判。
- **派生层可重建**：`build` 只读 Raw，断网即可回填。派生规则变更后一条命令重算历史。
- **顶部池刷新**：`collect --refresh-top 20000` 在增量之后按 `sort=downloads` 补扫前 2 万名。日增量只覆盖「最近更新的万把条」，若不补这一趟，增速榜长期只有零星读数。成本约 20 页。
- **`downloads` 非实时**：实测同一集相隔 9 小时读数完全相同，故增速必须跨日观测；同日重复扫描不产生新读数。
- **卡片只能按榜单补**：单库详情即使带 `blobs=false` 仍达 1.17MB（含 `siblings`），逐条补不可行。`enrich` 走列表接口带 `expand[]=cardData` 取前 N 名，2 万条约 22 秒；实测前 2 万名中约 68% 带卡片。卡片表落 `enrich/carddata.parquet`，`datasets` 视图已左连其字段。
- **`expand[]` 是白名单裁剪，不是增补**（2026-11-04 踩过的坑）：传了它就只返回列出的字段，连默认响应里本就有的 `private` / `disabled` 都会被裁掉。此前 `LEAN_EXPAND` 没请求 `private`，而 Bronze 已声明该列，结果 111.5 万行整列为空且管线照常报成功。现由 `tests/test_schema_contract.py` 把「表结构的列」与「请求的字段」钉成唯一真相源，另有 16 项断言防止再次漂移。
- **卡片抽取看 `dataset_info`**：它带每个 config 的 `dataset_size` / `download_size` / `features`（列名+dtype）/ `splits`（`num_examples`），是整条管线里唯一能回答「多少行、多少列」的来源。宽表抽 22 列，另落 `card_configs.parquet`（一行一个 config/split，实测 13,651 张卡展开 142,552 行）。
- **标签维度拆 16 类**：按全站 950 万条标签实测频次补齐 `modality`（12.7%）/`region`（11.7%）/`format`（8.6%）等；无前缀标签单列 `plain_tags`（13.7%，旧式 `text`/`image` 标记都在这里）。维度覆盖率 51.5% → 99.9%。`tag_values` 长表视图由映射表自动生成，新增维度不必改 SQL。
- **规模数据走 datasets-server**：列表接口的 `mainSize` 是仓库字节，卡片行数只覆盖 7.3%。`fetch-sizes` 打 `/size` 拿 `num_rows` 与 `num_columns`。实测成功率约 80%，并发 16 稳定 26 req/s，两万条约 790 秒。失败分类处理：500/401/501 记 `unavailable` 落库并默认跳过（永久缺失，重试只会白烧 30% 预算）；429/502/503 退避重试且不落库（下轮自然重试）。
- **宽视图由 schema 拼装**：`datasets` 的列不再手抄，由 `tables.py` 的 schema 按固定顺序拼接，重名**先到先得**；需要区分口径的列改名（`size.num_rows → server_num_rows`，与卡片口径的 `card_num_rows` 并存）；`raw`（每行数 KB 的整行 JSON）与重复的 `snapshot_date` 排除在外。手抄的代价是漂移——加一个卡片字段，`card` 视图自动有，`datasets` 安静地少一列。
- **「最新一行」有物化与视图两套实现，必须永远一致**：`current` 是物化表，`latest_silver` 是窗口视图。`current` 的带入列由两个 schema **求交**推导，不手抄；`status` / `first_seen_at` / `last_seen_at` 是本地判断，不得混进 Silver。一致性由测试守护（实测 111.7 万行、15 个公共列取值相同）。
- **失效状态不是「删除」一种**：对账后逐条探测，404 才是删除，401 是转私有（库还在），307 是改名（跟随到 200 拿到新 id）。三者混记会让下架数虚高，也让改名后的库在下轮变成「新增」而丢掉观测史。
- **命令成败只有一个判定点**：命令返回 `Outcome(payload, code, reason)`，退出码与 `runs/` 记录由同一处推出；`write_run` 不接受外部传入的 `status`——可传的 status 一定有传错的那天。
- **原子写只有一个入口**：`atomic.py` 的四个函数管所有落盘，临时文件与目标同目录（跨盘替换会退化）。中断时不能留下半张 HTML——那比旧页面更糟。
- **列编码只有一套规则**：`coerce.py` 管「一个 JSON 值怎么落进一列」。铁律是取不到留 None、绝不猜：`private: "true"` 是字符串不是布尔，猜成 True 会让「是否受限」失去可信度。

## 定时

```powershell
pwsh -File scripts\setup-schedule.ps1              # 注册：每日 03:10 增量、每周日 04:00 对账
pwsh -File scripts\setup-schedule.ps1 -Uninstall   # 卸载
schtasks /Query /TN hf-collect /V /FO LIST         # 核对
```

计划任务调 `scripts/collect.cmd` 与 `scripts/reconcile.cmd` 两个包装脚本：代理默认取 `HF_PROXY`，日志追加到 `data\logs\`，`uv` 找不到时回退到 `%USERPROFILE%\.local\bin\uv.exe`。

每轮摘要落在 `runs/`，失败也写并带错误原因，不会静默。

## 开发

```bash
uv run pytest          # 250 项，全部离线
```

测试只在一条缝上打桩：`HfDatasetApi` 的 transport。除此之外全部走真实代码路径。
