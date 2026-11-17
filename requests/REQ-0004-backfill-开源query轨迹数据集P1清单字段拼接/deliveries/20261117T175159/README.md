# REQ-0004 开源query轨迹数据集P1清单字段拼接

- 提出人：大帅
- 用途：把 28 条候选数据集能拿到的元信息拼进对方原表，供 P1 立项评估
- 交付时刻：2026-11-17 17:51:59
- 需求形态：`backfill`
- 需求版本：v3（口径指纹 `78068ec622c0c58f…`）
- 交付节奏：manual

## 口径

- 形态：按 URL 清单回填表格（`kind: backfill`）
- 输入快照：`input/开源query轨迹数据集P1紧急需求清单-28条-1113.xlsx`（需求目录内，10,303 字节，sha256 `f522886a66ed4dce…`）
- 表结构：sheet Sheet1，表头第 1 行，标识列 `URL`
- 产物：`deliveries/<时刻>/开源query轨迹数据集P1清单-28条-已拼接字段.xlsx`，只写在需求目录内；外部文件一律不动
- 取值：只填空格；未命中底库的行整行留空；需要判读的列由 `overrides.json` 提供，引擎不猜
- 声明 39 列，本轮写入 901 格，原有内容保留 0 格，空 191 格
- 追加列 39 列（原表没有，写在最右侧，原表原有列一字未动）：`作者`、`展示名`、`发布时间`、`更新时间`、`最新提交(sha)`、`访问门槛`、`许可证`、`许可证名称`、`许可证链接`、`是否有README`、`数据大小(GB)`、`parquet大小(GB)`、`内存占用(GB)`、`实测行数`、`实测列数`、`配置数`、`作者声明配置数`、`文件数`、`文件格式分布`、`规模档`、`作者声明规模档`、`近30天下载量`、`累计下载量`、`点赞量`、`趋势分`、`简介`、`任务类别`、`作者声明任务类别`、`数据模态`、`语种`、`作者声明语种`、`文件格式`、`适配库`、`标签`、`作者声明标签`、`自定义标签`、`地区`、`arXiv编号`、`论文URL`

### 逐列规则

| 列名 | 来源 | 写入 | 人工判读 | 保留 | 空 |
| --- | --- | --- | --- | --- | --- |
| 作者（新增） | `author` | 28 | 0 | 0 | 0 |
| 展示名（新增） | `pretty_name` | 7 | 0 | 0 | 21 |
| 发布时间（新增） | `created_at` · date | 28 | 0 | 0 | 0 |
| 更新时间（新增） | `last_modified` · date | 28 | 0 | 0 | 0 |
| 最新提交(sha)（新增） | `sha` | 28 | 0 | 0 | 0 |
| 访问门槛（新增） | `gated` | 28 | 0 | 0 | 0 |
| 许可证（新增） | `licenses` · 拼接 `、` | 25 | 0 | 0 | 3 |
| 许可证名称（新增） | `license_name` | 25 | 0 | 0 | 3 |
| 许可证链接（新增） | `license_link` | 2 | 0 | 0 | 26 |
| 是否有README（新增） | `has_readme` | 28 | 0 | 0 | 0 |
| 数据大小(GB)（新增） | `main_size` · gb | 28 | 0 | 0 | 0 |
| parquet大小(GB)（新增） | `server_num_bytes_parquet` · gb | 27 | 0 | 0 | 1 |
| 内存占用(GB)（新增） | `server_num_bytes_memory` · gb | 27 | 0 | 0 | 1 |
| 实测行数（新增） | `server_num_rows` | 27 | 0 | 0 | 1 |
| 实测列数（新增） | `server_num_columns` | 22 | 0 | 0 | 6 |
| 配置数（新增） | `server_config_count` | 22 | 0 | 0 | 6 |
| 作者声明配置数（新增） | `card_config_count` | 19 | 0 | 0 | 9 |
| 文件数（新增） | `file_count` | 28 | 0 | 0 | 0 |
| 文件格式分布（新增） | `file_formats` | 28 | 0 | 0 | 0 |
| 规模档（新增） | `size_categories` · 拼接 `、` | 26 | 0 | 0 | 2 |
| 作者声明规模档（新增） | `card_size_categories` · 拼接 `、` | 13 | 0 | 0 | 15 |
| 近30天下载量（新增） | `downloads` | 28 | 0 | 0 | 0 |
| 累计下载量（新增） | `downloads_all_time` | 28 | 0 | 0 | 0 |
| 点赞量（新增） | `likes` | 28 | 0 | 0 | 0 |
| 趋势分（新增） | `trending_score` | 28 | 0 | 0 | 0 |
| 简介（新增） | `description` · prose | 28 | 0 | 0 | 0 |
| 任务类别（新增） | `task_categories` · 拼接 `、` | 24 | 0 | 0 | 4 |
| 作者声明任务类别（新增） | `card_task_categories` · 拼接 `、` | 24 | 0 | 0 | 4 |
| 数据模态（新增） | `modalities` · 拼接 `、` | 25 | 0 | 0 | 3 |
| 语种（新增） | `languages` · 拼接 `、` | 13 | 0 | 0 | 15 |
| 作者声明语种（新增） | `card_languages` · 拼接 `、` | 13 | 0 | 0 | 15 |
| 文件格式（新增） | `formats` · 拼接 `、` | 20 | 0 | 0 | 8 |
| 适配库（新增） | `libraries` · 拼接 `、` | 20 | 0 | 0 | 8 |
| 标签（新增） | `tags` · 拼接 `、` | 28 | 0 | 0 | 0 |
| 作者声明标签（新增） | `card_tags` · 拼接 `、` | 25 | 0 | 0 | 3 |
| 自定义标签（新增） | `plain_tags` · 拼接 `、` | 25 | 0 | 0 | 3 |
| 地区（新增） | `regions` · 拼接 `、` | 28 | 0 | 0 | 0 |
| arXiv编号（新增） | `arxiv_ids` · 拼接 `、` | 11 | 0 | 0 | 17 |
| 论文URL（新增） | 模板 `https://arxiv.org/abs/{arxiv_ids}` | 11 | 0 | 0 | 17 |

### 未声明的列（不填、原样保留）

序号、名称、URL

### 口径补充说明

- **收录规则**：底库 `datasets` 视图共 74 列，本单拼进来 39 列（38 列取自底库 + 1 列由 arXiv 号拼出的论文URL）。不拼的四类：①原表已有的 id、url；②平台与管道记账（first_seen_at/last_seen_at/status/private/disabled/xet_enabled/resource_group/fetched_at/size_status/files_status/truncated/card_source_sha），它们描述的是「我们怎么采的」，不是数据集本身；③这 28 个库整列为空的数据（dois/task_ids/multilinguality、卡片未声明的行数与字节等），拼进来只多一片空格；④三列有值但刻意不拼：server_configs 是逐 config 的明细 JSON（一格塞不下）、server_num_bytes_original 与本批 parquet 字节基本同值（20/21 行完全相同）、total_bytes 与 数据大小(GB) 基本相同（27/28 行，差的正是文件树截断那个库）。「同值」这两句由 method/scripts/columns.py 现算复核，不靠手抄。
- **上游补采**：这些库原本不在任何「按下载量取头部」的采集批次里，card_* / server_* / files_* 全为空。本单按名单单独补采了一次（enrich --ids / fetch-sizes --ids / fetch-files --ids / fetch-readme --ids），所以「实测行数」「文件数」「是否有README」这类字段这次才有值；补的是底库，交付仍只读底库。
- **下载量口径**：「近30天下载量」是 downloads（HF 数据集页显示的就是这个数）；「累计下载量」是 downloads_all_time。两列并排是为了让人一眼看出差异，不混成一个数。
- **规模口径**：大小有三列，三个不同口径，别混着用：「数据大小(GB)」是仓库字节（main_size，HF 仓库页那个数）；「parquet大小(GB)」是 datasets-server 转换后的下载体积；「内存占用(GB)」是载入内存的估算。「实测行数/列数/配置数」来自 datasets-server 对数据集的实际统计，与作者在卡片里声明的数字是两个口径（后者本批多数没写）。
- **行数口径**：「实测行数」= datasets-server 的 num_rows，即 HF 数据集页「Number of rows」那个数；一个数据集有多个 config 时是各 config 之和。27/28 有值，缺的那一个上游返回「不支持」。
- **文件口径**：「文件数」「文件格式分布」来自仓库 tree 接口（第一页递归清单）；「文件格式分布」是扩展名计数的 JSON 原文，`(none)` 表示无扩展名。「是否有README」= tree 里有没有以 README 开头的文件。
- **趋势分口径**：HF 计算的 trending_score，进趋势榜为正、未进榜为 0（0 不是空值）。该值随时间漂移，只能当天的快照看，不能用来排序翻页。
- **简介口径**：description 是 HF 抓的卡片正文摘要（会截断并附「见数据集页」），不是作者填的一句话简介；prose 只压平空白与 HTML 残片，不改写措辞。
- **标签口径**：「标签」是 tags 原样全量（带 license:、language:、task_categories: 这类前缀），「自定义标签」是拆掉前缀后的 plain_tags：两者都是平台规范化后的值。「作者声明…」四个字打头的列相反，是卡片 YAML 里作者自己写的，常与平台归一值不一致——不一致时以哪一列为准，取决于你要「作者本意」还是「平台口径」。
- **原表三列**：序号 / 名称 / URL 属对方已填内容，本份 spec 不声明、引擎不碰；名称与 URL 是否一致由 method/scripts/verify_table.py 事后核对。
- **内部流程列**：对方表里将来若加「处理人 / 质检结果 / 存储路径」这类列，同样无权填，保持空白。

> 数据来源说明：取自本地 HuggingFace 底库（对账后快照，见交付 manifest 的 source.current_md5）；这 28 个库的卡片、规模、文件清单与 README 是本单按名单从上游补采进底库的，交付本身只读底库。未凭记忆或现场浏览填写，底库没有的一律留空。


## 基于哪一版底库

- `current.parquet` md5：`4d1dbaf321d0a94f0e4b22f2a15d12f7`
- 底库总行数：1,133,802
- 最新观测日：2026-11-17

底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，
跑 `hf-data-crawl request show REQ-0004` 看交付台账。

## 结果

- 行数：28
- 列数：39
- `开源query轨迹数据集P1清单-28条-已拼接字段.xlsx`：28,193 字节，sha256 `08a1eae69da6bce7…`

## 复现

- 做法：`method/METHOD.md`（步骤、判据、哪步机器哪步人判）
- 脚本：`method/scripts/`，逐份 sha256 与所依赖的代码版本
  见 `manifest.json` 的 `method` 段

```bash
hf-data-crawl request run REQ-0004 --force
```

同一版底库、同一份输入表下重跑，sha256 应当与上面一致。若不一致，
说明底库或输入表已经变过。
