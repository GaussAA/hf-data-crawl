# REQ-0001 高热度数据集清单

- 提出人：张三
- 用途：选型参考：同时有下载量与点赞量的成熟数据集
- 交付时刻：2026-11-15 18:46:01
- 需求形态：`query`
- 需求版本：v2（口径指纹 `a618951306d6b173…`）
- 交付节奏：manual

## 口径

| 词条 | 落成字段 | 条件 | 定义 |
| --- | --- | --- | --- |
| 下载量 | `downloads` | > 1000 | 近 30 天下载量，不是累计值。想用累计值请写「累计下载量」 |
| 点赞数 | `likes` | > 1000 | 点赞总数 |


### 默认交付字段（契约 v5，共 31 列）

未指定 `output.columns`，按 `data/exports/datasets.parquet` 的字段契约投影；
该文件即默认参考底库，列序与此处完全一致。

| 分组 | 字段 | 说明 |
| --- | --- | --- |
| 标识与来源 | `id` | 数据集唯一标识（作者/名称），也是交付结果的排序兜底键 |
|  | `url` | Hugging Face 数据集页面地址，可直接点开 |
|  | `author` | 仓库所属用户名或组织 |
|  | `created_at` | 仓库创建时刻（UTC） |
|  | `last_modified` | 最后一次提交的时刻（UTC） |
| 内容描述 | `description` | 数据集简介，取自卡片原文；未写卡片则为空 |
| 热度 | `downloads` | 近 30 天下载量，不是累计值；累计值见 downloads_all_time |
|  | `downloads_all_time` | 历史累计下载量，只增不减；与近 30 天下载量是两个口径 |
|  | `likes` | 点赞总数 |
| 可用性与许可 | `status` | active 在架 / disappeared 已删除 / restricted 转私有 / renamed 已改名 |
|  | `gated` | 门禁：false 公开 / manual 需人工审批 / auto 自动授权 |
|  | `license_name` | 卡片声明的许可证名称（页面 License）；未声明则为空，不代表无许可 |
|  | `licenses` | 许可证标签列表（HF 结构化枚举），与 license_name 可能口径不同 |
| 语言与模态 | `languages` | 数据集语言标签列表（页面 Languages）；存 ISO 码如 ko，页面显示 Korean；未声明为空 |
|  | `modalities` | 数据模态标签（页面 Modalities，图像/文本/音频/视频等）；未声明则为空 |
| 任务与数据类型 | `task_categories` | 任务类别标签（页面 Tasks，即目标用途）；存 text-generation，页面显示 Text Generation |
|  | `formats` | 文件格式标签列表（页面 Formats，parquet/json/csv 等） |
|  | `libraries` | 支持库标签列表（页面 Libraries，datasets、pandas 等） |
| 标签 | `tags` | API 返回的原样标签全量（含 task_categories: 之类前缀），可追溯、可按任意前缀再拆 |
|  | `plain_tags` | 无前缀的自定义标签（页面 Tags，如 keyword / reranking）；本仓库拆分所得 |
| 补充画像 | `pretty_name` | 卡片里的展示名（可能与作者/名称完全不同）；未声明则为空 |
|  | `multilinguality` | 多语言性标记（单语 / 多语 / 翻译等）；未声明则为空 |
| 学术关联与用途 | `arxiv_ids` | 关联的 arXiv 论文编号，如 2507.02259；仅少数库在卡片里声明 |
|  | `source_datasets` | 该数据集派生自哪些上游数据集；取值由作者填写，未必是 HF 数据集 id |
|  | `task_ids` | 具体任务标识，比 task_categories 大类更细；未声明则为空 |
| 规模 | `size_categories` | 规模档位标签（页面 Size）；存 100K<n<1M，页面显示 100K - 1M |
|  | `server_num_rows` | datasets-server 实测行数；与卡片声明行数是两个口径 |
|  | `server_num_columns` | 各 config 中列数的最大值 |
|  | `server_config_count` | datasets-server 返回的 config 数量 |
| 大小 | `main_size` | 仓库主分支字节数（页面 Total file size 的机器值），十进制换算 MB 请除以 1e6 |
|  | `server_num_bytes_parquet` | datasets-server 转成 parquet 后的字节数，是数据文件体积而非仓库体积 |

### 详情页标签 ↔ 库列

页面标签行与库里列名不同、值也多为机器码，按此表对照；
空值表示作者未在卡片里声明，不代表该数据集没有这一属性。

| 页面 | 库列 | 值的显示形式 |
| --- | --- | --- |
| Tasks | `task_categories` | Text Generation ↔ `text-generation` |
| Modalities | `modalities` | Tabular, Text ↔ `tabular` / `text` |
| Formats | `formats` | json ↔ `json` |
| Languages | `languages` | Korean ↔ `ko`（ISO 码） |
| Size | `size_categories` | 100K - 1M ↔ `100K<n<1M`（档位，非精确行数） |
| Tags | `plain_tags` / `tags` | keyword / reranking … ↔ `plain_tags`；`tags` 另存含前缀全量 |
| Libraries | `libraries` | Datasets, pandas, Polars, +1 ↔ 列表四项，`+1` 是末项折叠显示 |
| License | `license_name` / `licenses` | cc-by-nc-4.0 ↔ 标量 + 标签列表 |
| Downloads last month | `downloads` | 12 ↔ 快照时点的近 30 天下载量，会随时间变化 |
| Total file size | `main_size` | 729 MB ↔ 字节数；页面按文件大小统计，口径可能略有出入 |

### 扩展字段（不进默认面，按需取用）

以下字段一直都在 `gold.datasets` 视图里，只是**不进默认交付面**：低覆盖列塞回默认面会让九成行重新变空。需要时在 spec 里写 `output.columns: [id, <列名>]` 即可投列（列名既认词条也认视图列名）。空值表示作者未声明，不代表没有。覆盖档基于 2026-11-08 全库 112 万行实测，精确值以当期 `doctor` 为准。

| 字段 | 定义 | 覆盖档 |
| --- | --- | --- |
| `regions` | 数据覆盖地区标签 | 全库有值但 99.94% 是 HF 自动推断的 us，无区分度 |
| `dois` | 关联的 DOI 编号 | 低（<1%） |
| `paperswithcode_id` | Papers with Code 条目 id | 极低（<0.2%） |
| `arxiv` | 卡片里声明的 arXiv 编号 | 极低（<0.1%） |
| `citation` | 作者给出的引用格式（BibTeX / 纯文本） | 极低（<0.5%，长文本） |
| `license_link` | 许可证全文链接 | 低（<1%） |
| `card_homepage` | 项目主页 | 极低（<0.1%） |
| `card_description` | 卡片正文的开头摘要，与 description 不是一回事 | 极低（<0.1%） |
| `annotations_creators` | 标注者类型（专家 / 众包 / 机器生成） | 低（约 1%） |
| `language_creators` | 语料来源（原始采集 / 翻译 / 机器生成） | 低（约 1%） |
| `card_multilinguality` | 卡片声明的多语言性；与 multilinguality 近重复 | 低（约 4%） |
| `card_source_datasets` | 卡片声明的上游数据集；与 source_datasets 近重复 | 低（约 1%） |
| `card_num_rows` | 卡片声明的行数；与 datasets-server 实测行数是两个口径 | 低（约 1.6%） |
| `card_dataset_bytes` | 卡片声明的数据体积 | 低（约 1.4%） |
| `card_config_count` | 卡片里列出的 config 个数 | 中（约 43%） |
| `server_num_bytes_original` | datasets-server 统计的原始文件体积 | 中（约 66%） |
| `server_configs` | datasets-server 返回的 config 明细（JSON 文本） | 中（约 67%） |
| `file_formats` | 文件清单试点得到的扩展名计数（JSON 文本） | 极低（约 0.08%，头部子集） |
| `total_bytes` | 文件清单试点得到的总字节数（下限） | 极低（约 0.08%，头部子集） |


## 基于哪一版底库

- `current.parquet` md5：`894eada4e8b6294b475b702c57e9e230`
- 底库总行数：1,139,517
- 最新观测日：2026-11-15

底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，
跑 `hf-data-crawl request show REQ-0001` 看交付台账。

## 结果

- 行数：24
- 列数：31
- `data.parquet`：26,537 字节，sha256 `b579da1f55f39370…`

## 复现

- 做法：`method/METHOD.md`（步骤、判据、哪步机器哪步人判）
- 脚本：`method/scripts/`，逐份 sha256 与所依赖的代码版本
  见 `manifest.json` 的 `method` 段

```bash
hf-data-crawl request run REQ-0001 --force
```

同一版底库、同一份输入表下重跑，sha256 应当与上面一致。若不一致，
说明底库或输入表已经变过。
