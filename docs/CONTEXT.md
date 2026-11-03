# CONTEXT — hf-data-crawl 领域词表

> 本项目所有代码、测试名、issue 措辞一律使用本表词汇。新词先入表，再用。

## 核心名词

| 词 | 定义 | 不是什么 |
|---|---|---|
| **Dataset** | HF 上一个公开数据集的元信息快照，以 `id`（`author/name`）唯一标识 | 不是数据文件本身；本项目从不下载仓库内容 |
| **Record** | HF 列表 API 返回的单条 JSON 对象 | 不是我们落盘的行——落盘的叫 Row |
| **Expand 白名单** | HF 列表接口 `expand[]` 允许的字段集合，取错值直接 400 并回显全表 | 不是 `full=true` 的同义词 |
| **Lean scan** | 只取身份与指标字段（不含 `cardData`）的列表遍历，约 1.6 KB/条 | 不是采样，覆盖仍是全站 |
| **Full fetch** | 取含 `cardData` 的完整记录，约 6 KB/条 | 不是单库详情接口（那条路会带上 `siblings`，单库可达 7 MB） |
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
| **Silver** | 展开后的结构化 Dataset（标签拆维度、许可证归一） | parquet |
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
