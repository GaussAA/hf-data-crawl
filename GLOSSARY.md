# GLOSSARY — hf-data-crawl 领域词表

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
| **Status** | `current.status` 的取值，四值：`active` / `disappeared` / `restricted` / `renamed`，词表即 `status.Status`，写入时校验。探测结论到状态的映射也在 `status.py`：`deleted→disappeared`、`restricted→restricted`、`renamed→renamed`、`present→active` | 不是 `Disappeared` 的同义词；「没扫到」不等于 `disappeared` |
| **Reconcile session** | 一次（可能分段的）全量核对的游标、已见集合与基线，同键、同生命周期，由 `session.ReconcileSession` 拥有；基线固定在会话开始时，分段跑与单次跑的差异计数必须一致 | 不是 `Checkpoint`——后者只记游标一件事 |
| **Disappeared** | 全量核对没扫到、且单库探测返回 404 的 Dataset（Status 的一个取值） | 不是删除——本地观测史必须保留；转私有是 `restricted`，改名是 `renamed` |
| **Observation** | 某一轮对某 Dataset 指标的一次读数，带 `snapshot_date` | 不是当前态；当前态只有一行 |

## 分层

| 层 | 内容 | 形态 |
|---|---|---|
| **Raw** | API 原始响应，逐页原样落盘 | `raw/dt=YYYY-MM-DD/*.json.gz` |
| **Bronze** | 一行一条 Record，保留原始 JSON 列 + 少数可筛字段；只保留最新一天，是 Raw 的可重建展开 | parquet |
| **Silver** | 属性变更流水：一个 id 在首次出现或属性变了才追加一行（标签拆 16 类维度），不含指标与原始 JSON；指标历史归 observations | parquet |
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
17. **写入序是 Raw 的不变量**（2026-11-05）：`write_raw` 把 `written_at`（UTC、微秒）写进 payload 的 `meta`，Bronze 去重按它排序。**文件名摘要、mtime、`label` 的字母序都不参与排序**——mtime 在复制目录、解压归档、换机迁移后都会变，届时同一批 Raw 会推出不同 Bronze，与 `rebuild` 的承诺直接冲突。摘要**不含** `written_at`，否则同内容重写会不断产生新文件、幂等失效。旧分块无此字段时按文件名确定性定序，**不再读 mtime**（2026-11-07）；历史分区应先跑 `archive --date`，把真实写入序冻结进归档分片的 `groups`，此后 Raw 自包含、迁移不改结果。键首位 0/1 保证新分块永远排在其后。
18. **`rebuild` 有两处不来自 Raw 的例外**，重建时须特殊对待：① `enrich/size.parquet` 来自 datasets-server，删了无法重算，故只删卡片表；② `current.status` 的失效值来自单库探测，重建前留存、重建后写回，否则一次 build 就把全部 restricted / renamed 清零（2026-11-05 实测 463 条归零）。
19. **表身份登记在 `tables.py`**（2026-11-05）：每个 `TableSpec` 记录名字、路径模板（`{dt}` 表示按日分区）、schema 与 `Origin`（`RAW` / `NETWORK` / `STATE`）。读写、Gold 视图、重建清理一律从它派生。
    **`Origin` 决定重建时该不该删**——`NETWORK` 表（目前只有 `size`）删了就得重抓，必须保留；`RAW` 与 `STATE` 可重算，重建时清除。
    此前这门知识散在五处（store 的文件名常量、各模块的 schema、gold 的硬编码路径、rebuild 的删除清单、README 的目录树），任一处改名都不报错：gold 的 glob 落空会静默退化成空视图，rebuild 曾因此误删两万条规模数据。
20. **一轮扫描的时序只有一处实现**（`scan.ScanRun`，2026-11-05）：翻页 → 落盘 → 页钩子 → 存游标 → 判停止。四个调用方（日增量、顶部刷新、全量对账、卡片补齐）只提供 `ScanSpec` 策略。
    两条不变量写在这个 module 里而不交给调用方：
    ① **游标必须在写盘之后、任何 break 判断之前保存**——调用方随时可能停在下一页之前，而 `api.scan` 的生成器此刻已暂停，它内部那句 `save` 不会执行，续跑会重抓上一页。**页钩子排在存游标之前**：钩子带副作用（对账追加已见集合），若游标先存，两者之间被杀，续跑会跳过这一页的副作用；反过来最坏只是重做一页，所以钩子必须幂等；
    ② **页数上限由本层判定而非传给生成器**——生成器到限时是静默 `return`，只有本层判才能说出准确的停止原因（`max_pages` 与 `end` 必须能区分，前者要保住游标）。
21. **命令的成败只有一个判定点**（`runs.Outcome`，2026-11-05）：命令返回 `Outcome(payload, code, reason)`，`_execute` 据 `failed` 写唯一一份运行记录，`write_run` **不接受外部传入的 status**——可传的 status 一定有传错的那天。
    此前异常路径写 `failed`、正常返回一律写 `success`，于是 `report` 在无数据时退出码 1、`runs/` 里却是成功记录：同一条命令的结局被劈成两半。
22. **宽视图 `datasets` 由 schema 拼装**（2026-11-05）：列不再手抄，按 `_VIEW_SOURCES` 顺序拼接，**重名先到先得**；需区分口径的列在 `_VIEW_RENAMES` 改名（`size.num_rows → server_num_rows`，与卡片口径的 `card_num_rows` 并存）；`raw`（每行数 KB 的整行 JSON）与重复的 `snapshot_date` 在 `_VIEW_EXCLUDED` 排除。
    此前手抄 42 列，加一个卡片字段时 `card` 视图由 schema 生成、自动就有，`datasets` 却安静地少一列——查询照常跑，只是那一列不存在。
23. **「最新一行」有两套实现，必须永远一致**（2026-11-05）：`current` 是物化（18 列），`latest_silver` 是窗口视图（36 列，带全部维度）。
    - `state.CARRIED_COLUMNS` 由 `CURRENT_SCHEMA` 与 `BRONZE_SCHEMA`（当天完整标量状态，含指标）**求交**而来，不再手抄——手抄会漏，而漏掉的表现是「视图里查得到、物化表里是 null」。
    - `state.COMPUTED_COLUMNS`（`first_seen_at` / `last_seen_at` / `status`）是本地判断，**不得出现在 Silver 里**，否则会出现两个 status 各说各话。
    - 两者的一致性由测试守护：实测 111.7 万行、15 个公共列取值完全相同。
24. **原子写只有一个入口**（`atomic.py`）：`write_bytes` / `write_text` / `write_parquet` / `write_csv`。临时文件必须与目标**同目录**——`os.replace` 的原子性依赖同一文件系统，跨盘会退化成「复制 + 删除」。六个写点曾只做三处，遗漏的恰是面向用户的三个（榜单页、运行记录、对账清单）。
25. **列编码规则只有一套**（`coerce.py`）：`as_int` / `as_float` / `as_bool` / `as_text` / `as_list` / `as_json`。
    铁律是**取不到留 None，绝不猜**：`private: "true"` 是字符串不是布尔，猜成 True 会让「是否受限」失去可信度。此前 `_as_int` 在三处逐字相同、`_as_text` 同名而异义（一处 `str()`、一处逗号拼接）。
26. **`rebuild` 不是事务性的**（2026-11-05 实测）：它在「清除 state」与「写回失效状态」之间被打断时，那些不来自 raw 的判断会**永久丢失**——一次 build 被 SIGTERM 杀掉，456 条 `restricted` / `renamed` 全部归零。
    故状态在清理**之前**写入 `meta/failed-status.json`（不属于任何登记表，清理够不到），下次 build 从快照接上。
    **快照只表示「重建进行中」**（`status.retain` / `status.restore`）：写回成功后立刻删除。留着的话，下一次重建会把它当成「上次被打断」，把期间已恢复的库改回旧状态。
    已接受的缝隙：写回完成、快照尚未删除时被杀，且之后又有对账恢复了某个库，下次重建会被旧快照改回——窗口极窄，不做合并逻辑。
27. **Watermark 基于时间戳而非"连续 N 页无新 id"**（2026-11-07 修复）：旧机制在跨日运行时存在漏采风险——假设 00:01 启动 collect，HF 上有 10 个今天新上架的库排在列表最前面，连续 3 页都没有"昨天晚上更新的库"，watermark 提前触发，但实际上昨天 23:58 更新的库还没扫到。
    新机制：只有当"连续 N 页的最老 lastModified 仍晚于上次运行记录的最新 lastModified"时，才认为到达水位线。
    **异常值过滤**：记录和判定时都过滤掉"当前时间 + 7 天"之后的异常值（GLOSSARY 43 条记录的 2028 年异常值）。如果一页内全是异常值，视为该页无有效记录、不更新水位线。
    折衷：如果 HF 修复了异常时间戳，这些记录的 lastModified 会突然变旧，可能再次漏采——但这比"永久卡住"或"永久漏采正常记录"要好。
28. **派生原子性通过标记文件保护**（2026-11-07 实施）：`derive_day()` 依次更新 bronze → silver → observations → current 四个文件。如果中途崩溃（如 observations 写完、current 写入前），会导致部分文件已更新、部分未更新。
    保护机制：开始派生前写 `state/deriving-YYYY-MM-DD.flag`（含开始时间戳），派生成功后删除。启动时检查所有标记文件，如果超过 10 分钟，说明上次派生可能未完成，删除该日的**所有派生层分区**（bronze/silver/observations），然后重新派生。
    不覆盖的场景：派生完成、标记删除前崩溃，下次启动看不到标记文件、不会清理。但这个窗口极窄（只有"删除文件"这一步），且影响有限（派生已完成）。
29. **sha 为 null 的库无法检测变更**（2026-11-07 明确）：对账通过比对基线与已见集合的 `sha` 来检测变更。如果基线或已见至少一方 sha 为 null，无法判断是否变更，归入报告的 `undetectable_change` 类别，单独列出"无法检测变更的库清单"。
    只有两方都有非 null sha 且不同时，才算 `sha_changed`。此前 null 值会被当成相同（`None == None` 是 True），导致漏报变更。
    折衷：不强制调用详情接口（会让对账从数小时变成可能一整天），接受"部分库无法检测变更"的现实，通过报告提升可观测性。
30. **Raw 文件名带来源命名空间**（2026-11-07）：文件名是
    `{类别}-{source}-{页码:06d}-{摘要}.json.gz`，`source` 由 `ScanSpec` 用 mode + sort
    派生（`lean_lastModified` / `top_refresh_downloads` / `enrich_downloads` …），
    同时写进 `meta` 与归档分片的 `groups`。此前四种扫描共用 `part-000001` 这一模糊
    身份，归档后来源彻底丢失；如今同一天里不同目的的全站快照各有独立页码空间，
    `archive` 按 (类别, 来源) 分流，分片名与 `groups.source` 都保留来源。旧格式与
    旧归档分片一律归为 `legacy` 兼容读取，**不改写任何历史文件**。
    配套不变式：`ScanSpec.__post_init__` 强制 `expand` 必含 `api.IDENTITY_EXPAND`——
    不同来源共享同一去重池，后写的更窄记录会覆盖完整记录、让整列静默变空；
    `archive.DEDUPE_LABELS` 引用 `raw.BRONZE_LABELS`，去重类别不再有第二份字面量。
    **命名空间不参与排序**：胜负仍由 `written_at` 决定（见 #17）。
31. **Raw 不再是唯一事实来源**（2026-11-07，大帅钦点「移入 backup 不删、窗口 7 天」）：
    `rebuild` 旧契约是清空全部派生层、按天重放所有历史 Raw，于是跨天全量快照
    必须永久留着、主目录线性膨胀。新契约分三层，常量集中在 `retention.py`：
    ① 冷区权威层——`silver` / `observations` / `current` 的历史不再清空，它们
    本就是「变化才追加」的压缩表示；② 热区——`build` 默认只重放近 7 天；
    ③ 冷区 Raw——`compact` 跨天去重压成一份基线（`groups` 仍冻结写入序），
    原分块移入 `backup/raw/` 不删。**实证**：只重放热区与全量重放、逐日正常跑
    三向逐行等价（见 `tests/test_rebuild.py`）。`build --full` 保留旧逃生舱；
    权威层缺失时自动回退全量。`doctor` 把 `cold/raw/manifest.json` 的日期算作
    「有观测」，避免部分压缩时残留分区拉长窗口造成断日误判。    **代价**：清理 backup 后若权威层
    损坏，只能恢复近 7 天——故原分块默认绝不自动删除。
32. **宽表导出是可重建投影**（2026-11-07）：`current` 的 19 列不是「字段太少」——
    语义字段早已全库采到，分别在 `silver`（18 列）/ `carddata`（20 列）/ `size`（8 列），
    由 `gold.datasets` 宽视图 LEFT JOIN 成 65 列；`latest_silver` 与 `dataset_size`
    各覆盖 112 万 id。新增 `export-wide` 把该视图流式 COPY 成
    `data/exports/datasets.parquet`，供需要单文件宽表的分析工具使用。
    **它不是表**：不登记进 `TABLES`、不参与状态更新与重建，随时可删可重建；
    事实源仍是 `raw/`。**为什么不扩 `CURRENT_SCHEMA`**：每轮只扫约 22 万行，
    其余约 90 万行由 `_rebuild_current` 沿用 previous，而 `_align_frame` 对旧表
    缺列一律补 null——加列后约九成行的新字段会长期为空（与 2026-11-04
    `private` 整列静默变空同源）；且 `silver` 已是「同 id 最新即当前属性」，
    再存一份会形成双真相源。列清单由 `gold.wide_columns()` 单一生成：
    视图 SQL、导出校验、数据说明第八节共用，杜绝第二份手抄清单。
33. **白名单合法 ≠ 列表接口会返回**（2026-11-07 实测）：383 万条 Raw 全量普查并
    逐项探接口后确认，`siblings` / `resourceGroup` 在 `expand[]` 白名单里合法，
    但列表接口收到请求也不返回（带 `full=true` 同样没有），全库出现 0 次。
    `siblings` 只在单库详情接口（一次约 1.2 MB，含 rfilename / blobId / size），
    `/tree/main?recursive=true` 约 200 KB 且硬上限 1000 条，逐库补 112 万不现实。
    故 `api.LIST_UNRETURNED_FIELDS` 登记二者，`ALLOWED_PLACEHOLDERS` 只放行
    `resourceGroup`（历史占位列、整列为空），契约测试禁止 `siblings` 进任何扫描请求。
    **顺带纠正一次误判**：`citation`（8,486 次）/ `paperswithcode_id`（8,452 次）/
    `cardData`（96.7 万次）并非「从未采」——早先只取每个分块首条记录看 key，
    稀疏字段因此被漏看；`cardData` 覆盖率受 `enrich --top` 限制。
    前二者现纳入 `IDENTITY_EXPAND`（实测只多约 0.2% 带宽），`ENRICH_EXPAND` 去掉
    重复项，`api.scan` 对 expand 去重保序；它们不对应 Bronze 列、不改宽表，
    仅让各来源 Raw 留下可回溯副本。文件级元数据若要做，只能按榜单头部按需补（B 案）。
34. **文件清单走 tree、按头部补**（2026-11-07，B 案试点）：列表接口请求也不返回
    `siblings`；单库详情虽返回，但下载量头部实测 13-25 MB / 44-76 s（首次
    `--top 20` 跑满 240 s 被杀），故详情方案实测被推翻。改用
    `/tree/main?recursive=true&expand=false`，每页固定 1000 条、约 110-230 KB /
    0.4-0.7 s，`Link: rel="next"` 分页。默认每库只取一页，未取完标 `truncated`，
    此时 file_count / total_bytes / formats / has_readme 全是**下限**；`--max-pages`
    可显式翻页。此前 #16 否定的「589 B 顶层树」是**非递归**树，拿不到子目录文件，
    与本次递归树并不矛盾。落 `files` 表（`Origin.NETWORK`，`rebuild` 不清），
    tree 拿不到 `usedStorage`，故不留 `storage_bytes` 空壳列；并发调度、flush 与进度
    抽到 `batch.collect_rows`，`size` 与 `files` 共用骨架；失败仍分「永久缺失落库」
    与「临时失败不落库」两类。**宽视图改名判重同步修正**：旧逻辑先拿原列名查 used，
    `files.status` 因与 `current.status` 撞名被跳过、改名 `files_status` 没机会生效，
    `files.formats` 也与 `silver.formats` 撞名；现按改名后的目标名判重，宽视图 65 → 71
    列，契约测试与反向验证钉死。此表是头部子集，不能用于全站分布统计。
    **限流**：匿名 `ratelimit-policy` 实测 `q=500;w=300` 固定窗口，首轮 1000 里
    321 个撞 429；根因是固定 2 秒退避早于窗口重置，改为尊重 `Retry-After`
    （非有限值 / 负数回退指数、合法值夹 60 秒）后，续跑 321/321 全成功、0 失败。
35. **本机凭据放 `.env`，只在联网那一刻读**（2026-11-07）：`HF_TOKEN` / `HF_PROXY`
    由 `env.load_env_file` 在 `_open_transport` / `_files_transport` 里加载，**不覆盖**
    已存在的环境变量（命令行与 CI 显式给的值优先）；文件缺失、坏编码一律静默跳过，
    返回值只含键名——值一旦被打进日志或 `runs/`，`.gitignore` 就白设了。
    不在 `main` 开头统一读盘：测试注入假传输时便不碰磁盘，本机令牌不污染用例。
    计划任务由 `_common.ps1` 先 `Push-Location` 到仓库根，故读到同一份 `.env`。
    实测配额：匿名 500 次 / 300 秒，带令牌 **1000 次 / 300 秒**（同为固定窗口）。
36. **刷新截断要显式开口**（2026-11-07）：`fetch-files` 的断点续跑原本把「已有行」
    一律跳过，于是 `truncated=True` 的行永远是下限、永远补不成完整清单。新增
    `run(refresh_truncated=...)` 与 `--refresh-truncated`：只把截断行从跳过集合里
    摘出来重抓，完整行照旧跳过（不偷偷重烧流量），摘要新增 `refreshed` 计数。
    `--max-pages<=1` 时命令直接失败并留痕——一页上限下重抓只是把截断重新确认一遍。
    旧表没有 `truncated` 列时视为未截断，不擅自全量重抓。此开关是**一次性补全**
    用的，别放进每日计划任务：仍取不完的超大仓库每天都会被重抓一遍。
37. **目录排在文件前，0 不是空仓库**（2026-11-07，实跑暴露）：`recursive` 按路径排序，
    目录在文件之前，嵌套深的仓库头几页可能一个文件都没有。首轮 `--max-pages 1` 因此
    把 190 个库记成 `file_count=0`、`has_readme=false`、`formats={}`——**这三样都是
    没验证过的断言**。两处修正：① 尚无文件时额外允许翻 `EMPTY_PAGE_GRACE=3` 页
    （一旦见到文件就严格按 `max_pages` 收工，硬上限 = max_pages+3，防病态仓库无限翻）；
    ② 仍一个文件都没见到且明确未完时，`file_count` / `total_bytes` / `formats` /
    `has_readme` 一律留空——「没看到」与「没有」是两件事，与 `total_bytes` 不拿 0
    冒充是同一原则。一次补全后：465 行完整、388 行截断但有真实下限、147 行如实留空，
    0 文件误报归零。
38. **无前缀模态词并入模态、语言码折成两字码**（2026-11-08，A 案）：15 个高频前缀早已
    拆成维度列（覆盖率见 #33），但两处口径缺口在实跑里暴露：① 无前缀标签占 18.9%，
    其中 `multimodal`（2,516 库）、`text`/`image`/`audio` 等模态词只进 `plain_tags`，
    于是 `modalities` 里**没有 `multimodal` 取值**，「多模态有多少」查不出来；②
    `language:` 两套 ISO 码混用（`eng` 与 `en` 并存），按语言聚合会把同一语言劈成两行。
    修法：无前缀模态词**额外**投一份进 `modalities`（原词仍留 `plain_tags`，不丢原样），
    只收语义明确的 11 个别名，`lidar`/`rgb`/`mesh` 刻意不归一（归错比不归更坏）；
    语言只做 **3→2 单向折叠**（`langs.ISO3_TO_ISO1` 184 条，pycountry 一次性导出后
    固化，运行时不依赖它），无 639-1 码的 `yue`/`ceb`/`cmn` 原样保留。`multimodal`
    是本仓库**扩展取值**，HF 官方 9 种 modality 里没有。实跑（全量重放后）：全库
    3,358 个库可查 `multimodal`，`eng` 残留归零、`en` 合并至 141,285 次，
    `modalities` 今日批次覆盖 73.9% → 74.3%。
39. **全量重放暴露 27 行不可复现**（2026-11-08，待大帅定夺）：`build --full` 后
    `current` 从 1,121,641 降到 1,121,614。逐条追查：27 条全部 `first_seen=11-07`，
    raw 里**只存在于 `card-*` 分块**，而 `bronze.build` 按设计只读 `part`/`top`
    （见 `bronze.py` 注释：卡片批次不属当日标量快照），故重放时它们进不了
    bronze → silver → current。**与 #38 的 silver 改动无关**（silver 不参与 current），
    是首次全量重放才暴露的既有不可复现性。已从
    `data/backup/state-before-silver-20261108-0015/` 还原该 27 行，id 集合与备份
    完全一致（两向差集皆 0）；但**下次 `build --full` 仍会丢**。两条路待选：
    ① 让 bronze 也接收「card 分块里 part/top 未见的 id」，重放才真正等价于增量；
    ② 接受 0.002% 不可复现，在文档写明并靠 backup 兜底。臣未擅自改 bronze 口径。
40. **补抓批次在状态层补位，不进 Bronze**（2026-11-08，接 #39）：解 #39 的那 27 行时，
    第一版把 `card-*` 并进 `bronze.build`，实测 Bronze 从 **233 万涨到 338 万（+45%）**，
    而真正只缺 203 个库——正是 `bronze.build` 注释里警告的「行数随补抓节奏膨胀」。
    改为在**状态层**补空位：`state._with_supplements` 读补抓批次，只把当日帧未见的 id
    并进来（已在的一律不动，胜负仍归扫描批次），Bronze 保持纯净。台账：`current`
    由 1,121,641 变为 **1,121,817**——多出的 176 个库是 enrich 抓过、却从未进过任何
    一天扫描的真实数据集，现在既进底库也能重放出来。连续两次 `build --full` 的
    `current.parquet` **md5 完全一致**（可复现这条承诺才立得住）。
    **连带影响须记明**：观测台账同样被补位，11-07 由 205,545 涨到 782,026，前三天
    分毫未变。这不是虚增——11-07 的 `card-*` 批次含 1,119,899 个去重 id（补抓覆盖
    近全库），而当日扫描只有 219,869 个，故原先那 20.5 万才是异常低值。代价是
    **「观测 ⊄ bronze」**这条不变量被打破：补位行由状态层合成，只能靠 `state` 逻辑
    复现，不能只拿 bronze 反推。口径已写入 `docs/DATA.md` 第五节第 12 条。
41. **任意前缀走 `tag_index` 视图，不物化长表**（2026-11-08，B 案的轻量替代）：
    `tag_values` 只覆盖已拆的 15 维 + `plain_tags`；白名单外的长尾前缀（`model:` /
    `eval:` / `mode:`，合计占标签 0.06%）原先是盲区。原计划新建 `tags_long` 表，
    但量级是 112 万 × 平均 11 条 ≈ **1,200 万行/快照**，与 `tags` 列重复存一份。
    改为视图：`tag_index(id, prefix, value)` 直接 `unnest(latest_silver.tags)` 并按
    第一个冒号原样切分，任意前缀都能查，**零存储**。代价是每次查询现算（12M 行级），
    可接受；真要提速再物化不迟。
42. **`runs/` 与 `logs/` 是同一次运行的两半，不合并**（2026-11-08）：`runs/` 是
    机读台账——Python 写、JSON、一轮一条、doctor 的新鲜度与失败判定都读它、上限
    500 条；`logs/` 是人读现场——PowerShell 的 `*>>` 重定向写、纯文本、含进度与
    逐条 `[ok]`/`[!]`。写入者、格式、读取者、清理策略四样全不同，合并只会让
    `runs/` 失去「整目录即台账」这层语义，且两套命名与清理规则挤在一处更难守。
    **但原先确有两处病**：① logs 按任务名永久追加、从不清理，只能靠人手工改成
    `.bak`（当时 collect / reconcile / sizes 三个命令就只剩 `.bak`，`.log` 已消失）；
    ② 两边对不上——`runs` 的键是「时间戳+命令」，`logs` 只有命令名。现已统一为
    `时间戳-任务名.log`，与 `runs` 同键不同扩展名，写完修剪到最近 `LogKeep=200` 个；
    修剪按文件名排序而非 mtime（文件名前缀即时间戳，mtime 会被同步/备份整体改掉）。
    **`doctor` 刻意不写 `runs/`**：它退出码 1 是「有告警」而非「命令失败」，记成
    failed 会污染「近 7 天无失败运行」；代价是体检结论只在 `ALERT.txt`（覆盖式，
    只存最新一次）与日志里，**没有历史**——「断日从哪天开始」这类问题仍无处可查，
    属已知缺口，未在本轮处理。
43. **全量对账与压缩改按日，底库行龄进体检**（2026-11-08，钦点）：实测日增量按
    `lastModified` 走，一天只覆盖约 **6%**（11-07：part 47,759 + top 20,034 ≈ 6.8 万
    / 112 万）。一个两年没更新的库永远不会进入增量范围，可它的 downloads 每天都在
    变——长尾全靠全量对账刷新。原先是「周日 04:00 一次」，属**单点**：错过或失败
    （11-03 就因 SSL 失败过）全库就跳两周。改**每日 04:00**：1114 页 / 37 分钟 /
    约 30 请求每分钟，远低于配额，错过由 `-StartWhenAvailable` 补跑。
    `RECONCILE_MAX_AGE` 随之由 9 天收到 **36 小时**；`REPORT_KEEP` 由 8 份提到
    **30 份**（每日一份，8 份只够八天）。`compact` 由「周一 06:00」改**每日 00:30**：
    门槛仍是 7 天，实际每天最多压掉一个刚满期的分区，把「周一才清一次、平日热区
    一路涨」摊平；放 00:30 是避开 03:10 采集、04:00 对账与 06:00 规模补齐
    （同一天 06:00 会与 sizes 抢写锁）。**体检新增「底库新鲜度」**：按 `last_seen_at`
    算行龄（中位 / P95 / 最旧，P95 超过 7 天告警）。为什么要它——`doctor` 原先只说
    「对账几小时前跑成功了」，不说明库里每行是哪天的值；**任务成功 ≠ 底库新鲜**。
    实测当前中位 1 天、P95 1 天、最旧 5 天。
