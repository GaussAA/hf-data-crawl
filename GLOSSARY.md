# GLOSSARY — hf-data-crawl 领域词表

> 本项目所有代码、测试名、issue 措辞一律使用本表词汇。新词先入表，再用。
>
> 本表**只放词与稳定事实**：名词定义、分层、以及「这个系统由哪些层组成」。
> 带编号的设计决策与取舍在 [DECISIONS.md](DECISIONS.md)——那里的编号是跨文档
> 引用键（`DECISIONS #74`），只增不改、不复用。查「为什么是这样」去那边；
> 查「这个词是什么意思」留在这里。
>
> **代码引用给到子模块**：`delivery.delivery.read_manifest` 这样写，别写成
> `delivery.read_manifest`——后者在包上取不到，按图索骥会 `AttributeError`。
> 这 4 处是 2026-11-19 核对时才发现（`audit_glossary_symbols` 的真实产出）。
>
> 接口的行为事实（分页 cursor、`expand[]` 白名单、匿名限流、单库详情体量、
> 代理吞吐、`lastModified` 的异常值）逐条记在 [DECISIONS.md](DECISIONS.md) 的
> #1–#16——它们是实测结论，写在那边是因为带编号、会被引用。

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
| **状态分区**（`StatusPolicy`） | 一张端点自己的「状态码怎么读」：`retryable`（可重试）与 `permanent`（终态 → 落库的 `status`）。两者互斥，由 `acquire.hf.retry` 校验；同一个 500 在 `/size` 是终态、在其余端点是瞬时故障 | 不是通用重试策略——扫描/探测那条路读的是 `RateLimit` 的 `t=`，另有一套 |
| **未完成**（blocked） | 刷新链里**不是故障、但这一天没做完**的步骤：被上游挡下、超启动窗口、超时间预算。进 `payload.blocked`，链条以退出码 1 收场。与「今天已完成」「不在 `--only` 里」「`--skip`」区分——那三种是设计内的，只记一行 | 不是失败（`failed` 只装跑过且退出非零的），也不是正常跳过：当日该做的确实没做 |
| **预设**（preset） | `collect` 的一组参数（页数上限 / 头部补扫量 / 水位线页数）。**不传 `--preset` 就是 `default`**，由 `platform.presets.DEFAULT_PRESET_NAME` 一处定义，命令层与命令行帮助都从它取数 | 不是「不传就用一套更宽松的隐含值」——那正是 3 与 5 两个答案的由来。`--full-scan` 例外：它说的就是扫到末页，不受预设的页数上限约束 |
| **富化**（Enrich） | 底库之上按需补齐的资产：规模（`size`）、README 全文（`readme`）、卡片、文件清单。一律按 `id` 覆盖、都是 `Origin.NETWORK`——删掉就得重抓 | 不是采集的一部分；采集只读列表接口，富化逐库补 |
| **陈旧**（stale） | 富化行还在，但它记的版本已不是当前版本：README 的 `source_sha` 落后于 `current.sha`，规模的抓取时刻早于 `last_modified`。比什么由 `acquire.candidates.EnrichPolicy.evidence` 声明；`files` / `preview` 没有版本凭据（`evidence=NONE`），只判缺口 | 不是「抓得早」——抓得早但内容没变不算陈旧，重抓等于白烧配额 |
| **缺口**（gap） | 没有可用结论：从没抓过，或上次是 `error`。`denied`（门禁）与 `unavailable`（上游没做 parquet 转换）是结论，不算缺口 | 不是「表里没有行」——`error` 行占了一行，但它不是结论 |
| **候选集**（Candidates） | 一轮该重取的对象：点名重试 ∪ 陈旧 ∪ 缺口，按下载量降序，再按名额截断。产出是 `acquire.candidates.Candidates`（`picked` / `retried` / `stale` / `gap` / `deferred`） | 不是「表里过期的行」——还要并上缺口；更不是抓取端与体检各算一遍 |
| **名额**（budget） | 一轮最多发多少个请求。`--changed N` / `fetch-files --top N` 的 N 就是它：点名重试先吃，其次陈旧，余额给缺口；`--changed-all` 不设名额（陈旧全取），缺口另受 `GAP_BUDGET` 限制 | 不是「最多补多少陈旧」——缺口曾在名额之外再偷偷加两万个请求 |
| **部分清单**（`ok_partial`） | 文件清单只取了前几页，`file_count` 等四项都是下限。它算结论（不该天天重抓），又不算完整，故单列一个状态；补全要 `--refresh-truncated --max-pages>1` | 不是缺口——它会出现在候选里只因用户点名；也不是「完整清单」 |
| **Cursor page** | 一次列表请求的结果，下一页地址在响应头 `Link: rel="next"` 的 `cursor` 参数里 | 不是 offset 分页，HF 已无 `page` 参数 |
| **Watermark** | 增量扫描的停止条件，表示「早于此刻的记录本轮不必再看」 | 不是 `max(lastModified)`——实测存在 2028 年的异常值 |
| **Reconcile** | 全量核对：确认哪些 Dataset 仍在、哪些已下架、哪些字段被外部改动 | 不是重新抓一遍就完事，核心产出是差异 |
| **Status** | `current.status` 的取值，四值：`active` / `disappeared` / `restricted` / `renamed`，词表即 `platform.vocab.Status`，写入时校验。探测结论到状态的映射在 `acquire.reconcile.STATUS_BY_VERDICT`：`deleted→disappeared`、`restricted→restricted`、`renamed→renamed`、`present→active` | 不是 `Disappeared` 的同义词；「没扫到」不等于 `disappeared` |
| **扫到末页**（`FULL_SCAN`） | `stopped_reason == "end"`：这一轮确实扫完了。刷新链的 `collect`、对账的完整会话都认它。定义在 `platform.runs`，一处 | 不是「今天采过了」——增量模式停在水位线也算采过（`COLLECT_COMPLETE_REASONS`），但没扫完。两者合并过一次，代价是半轮顶替全量轮 |
| **Reconcile session** | 一次（可能分段的）全量核对的游标、已见集合与基线，同键、同生命周期，由 `acquire.session.ReconcileSession` 拥有；基线固定在会话开始时，分段跑与单次跑的差异计数必须一致 | 不是 `Checkpoint`——后者只记游标一件事 |
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
