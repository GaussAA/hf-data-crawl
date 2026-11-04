# CONTEXT — hf-data-crawl 领域词表

> 本项目所有代码、测试名、issue 措辞一律使用本表词汇。新词先入表，再用。

## 核心名词

| 词 | 定义 | 不是什么 |
|---|---|---|
| **Dataset** | HF 上一个公开数据集的元信息快照，以 `id`（`author/name`）唯一标识 | 不是数据文件本身；本项目从不下载仓库内容 |
| **Record** | HF 列表 API 返回的单条 JSON 对象 | 不是我们落盘的行——落盘的叫 Row |
| **Expand 白名单** | HF 列表接口 `expand[]` 允许的字段集合，取错值直接 400 并回显全表 | 不是 `full=true` 的同义词；更**不是「在原响应上增补」——它是裁剪，传了它就只返回列出的字段** |
| **Lean scan** | 只取身份与指标字段（不含 `cardData`）的列表遍历，16 字段约 980 B/条 | 不是采样，覆盖仍是全站 |
| **Full fetch** | 取含 `cardData` 的完整记录，约 6 KB/条 | 不是单库详情接口（那条路会带上 `siblings`，单库可达 7 MB） |
| **Card Config** | 卡片里一个配置（子集）及其 split 明细，带行数、字节、列结构 | 不是数据集本身；一个 Dataset 可有数十个 Config |
| **Dataset Size** | datasets-server 给出的转换后规模（`num_rows` / `num_columns` / 内存字节） | 不是仓库字节（`mainSize`），也不是原始文件大小 |
| **Cursor page** | 一次列表请求的结果，下一页地址在响应头 `Link: rel="next"` 的 `cursor` 参数里 | 不是 offset 分页，HF 已无 `page` 参数 |
| **Watermark** | 增量扫描的停止条件，表示「早于此刻的记录本轮不必再看」 | 不是 `max(lastModified)`——实测存在 2028 年的异常值 |
| **Reconcile** | 全量核对：确认哪些 Dataset 仍在、哪些已下架、哪些字段被外部改动 | 不是重新抓一遍就完事，核心产出是差异 |
| **Disappeared** | 曾入库、本轮全量核对未再出现的 Dataset | 不是删除——本地观测史必须保留 |
| **Observation** | 某一轮对某 Dataset 指标的一次读数，带 `snapshot_date` | 不是当前态；当前态只有一行 |

## 分层

| 层 | 内容 | 形态 |
|---|---|---|
| **Raw** | API 原始响应，逐页原样落盘 | `raw/dt=YYYY-MM-DD/*.json.gz` |
| **Bronze** | 一行一条 Record，保留原始 JSON 列 + 少数可筛字段 | parquet |
| **Silver** | 展开后的结构化 Dataset（标签拆 16 类维度、许可证归一） | parquet |
| **Enrich** | 按需补齐的卡片与规模数据：宽表、config 明细、规模表 | parquet，按 id 覆盖 |
| **Current** | 每个 Dataset 最新一行状态 | parquet，整表重写 |
| **Observations** | 指标变化流水，仅变化时追加 | parquet，追加 |
| **Gold** | 榜单视图（增速、总量、分类榜），DuckDB 视图而非实体表 | 视图 |

## 外部接口事实（2026-11-03 实测，非推测）

1. `GET /api/datasets` 分页为 **cursor 制**，`limit` 上限 1000。
2. `expand[]` 合法值仅此白名单：`author`、`cardData`、`citation`、`createdAt`、`description`、`disabled`、`downloads`、`downloadsAllTime`、`gated`、`lastModified`、`likes`、`mainSize`、`paperswithcode_id`、`private`、`siblings`、`sha`、`tags`、`trendingScore`、`resourceGroup`、`xetEnabled`。传错值返回 400 并回显全表。
3. **`downloadsAllTime` 可通过 `expand[]` 批量取得**，不必逐库查详情（早前「累计量只能单库查」的假设是错的）。
4. 匿名限流：`RateLimit-Policy: "fixed window";"api";q=500;w=300`，即 5 分钟内 500 次；响应头 `RateLimit: "api";r=<剩余>;t=<重置秒>`。
5. **默认排序不是稳定序**：不传 `sort` 时游标实为 `trendingScore` 游标（解码可见），会随时间漂移。全量遍历必须显式 `sort=createdAt`（不变序）；增量遍历用 `sort=lastModified&direction=-1`。
6. `lastModified` 存在未来年份异常值（实测见 `2028-12-10`），**不可用于 watermark 单调性假设**。
7. 单库详情接口默认携带 `siblings`，体量可达 MB 级，不用于批量。
8. 经本机代理（`127.0.0.1:7897`）实测吞吐：1000 条 full 档 5.9 MB / 35–64 s。瓶颈是带宽（约 90–170 KB/s），不是限流。
9. **`downloads` 不是实时值**：`huggingface/documentation-images` 相隔约 9 小时两次读取均为 5,839,203（`likes` 同为 253）。同日内重复扫描不会产生新读数，故「增速」必须以跨日观测为前提。
10. **`expand[]` 是白名单裁剪，不是增补**（2026-11-04 实测）：只传 `expand[]=downloads` 时，返回体仅剩 `_id` / `downloads` / `id`；默认响应里本就有的 `private`、`disabled`、`key` 会一并消失。任何未列进 expand 的字段都拿不到，且不报错。
11. **`siblings` 在列表接口恒为空数组**：即使 `expand[]=siblings` 也返回 `[]`，文件清单只能走单库详情（`blobs=false` 约 1.8 KB/条）。`usedStorage`（含历史版本，alpaca 318 MB）亦只在详情接口，与 `mainSize`（主分支，alpaca 24 MB）相差一个量级。
12. **`datasets-server.huggingface.co`** 提供 `/size`（`num_rows`、`num_columns`、`num_bytes_memory`）、`/splits`、`/statistics`、`/is-valid`。`/size` 实测成功率约 80%，未转 parquet 返回 500、受保护返回 401、不支持返回 501——**均属永久缺失，不该重试**；并发 16 稳定 26 req/s，未见 429。
13. **HF 没有下载量时序接口**：`/api/datasets/{id}/downloads` 返回 404。增速只能靠自建观测流水累积，这是本项目保留 `observations` 分区的根本原因。
14. **`trendingScore` 是整数且可为 null**，`gated` 取值是 `false` / `"manual"` / `"auto"` 三种，`mainSize` 最大观测到 489 GB。
15. **列表里「没扫到」≠ 已删除**（2026-11-04 抽查 3 条实测）：对失效 id 请求单库详情，走 `follow_redirects` 后可分辨三种情况——**404 真删除、401 转私有（库还在）、307 改名（跟随到 200，返回新 id）**。三者对历史的意义完全不同，故 `current.status` 取 `active` / `disappeared` / `restricted` / `renamed` 四值。
16. **单库详情不能用于批量补 `siblings` / `usedStorage`**（2026-11-04 实测 12 条抽样，已放弃）：
   - 详情接口的 `expand[]` 是**增补**语义，与列表接口相反——传 `expand[]=usedStorage` 仍会带回 `siblings`，响应 12,488,000 B，无法裁剪；
   - 体量分布：中位 **346 KB**、平均 **6.7 MB**、最大 **28.4 MB**（`siblings` 在 10 万个文件处截断，大库清单本就不完整）；
   - 据此推算两万条约 **133 GB**，按实测 130 KB/s 需 **280 小时**；
   - `/tree/main` 顶层只要 589 B，但只给顶层，拿不到分片文件的扩展名；
   - 卡片的 `data_files` 仅 4,797 个库有值（占 13,651 张卡的 35%），覆盖太窄；
   - **格式信息已有更优来源**：`format:` 标签覆盖 parquet 53.3 万、json 10.0 万、optimized-parquet 6.8 万、csv 5.2 万、imagefolder 3.3 万，量级远超两万条 siblings；
   - 体积信息已由 `mainSize`（100% 覆盖）与 datasets-server 的 `num_bytes_original` 双重覆盖，`usedStorage` 的差值仅是 git 历史开销。
