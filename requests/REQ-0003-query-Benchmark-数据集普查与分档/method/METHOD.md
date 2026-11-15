# 过程记录：REQ-0003 Benchmark 数据集普查与分档

## 目标

产出一份**全库普查表**（1,139,517 行 × 15 列），每行给定一个证据档位
`判定档 ∈ {L1, L2, L3, L4, U}`，并附「凭什么这么判」的一句话凭据与置信度。

判「做完了」的三条：

1. 行数 = `current` 表行数（普查，不预筛下载量/语言）；
2. 12 个公认榜单全部落在 L1 或 L2（`method/scripts/recall_check.py` 自检）；
3. 各档行数在口径记载的量级内：L1 ≈ 9.8k、L2 ≈ 24.6k、L3 ≈ 37.3k、L4 ≈ 3.8k、U ≈ 106.4k（万位）。

这份表回答的是「这个数据集凭什么被怀疑是榜单」，**不是**「它是否该从训练集里剔除」
——后者由治理方按档位自行裁量（见「机器算的 / 人判的」）。

## 步骤

1. **确认底库在位**。读 `data/state/current.parquet` 的行数，与 `data/silver/dt=<最新>/`
   是否同源；口径依赖 `card_config`（划分）与 `carddata`（pretty_name），两者缺失会
   让 L4 档整档归零，而不是报错——所以先看一眼各档行数是否为零。
2. **跑预演**：`hf-data-crawl request run REQ-0003 --dry-run`。核对行数（应等于底库行数）
   与逐列覆盖：`判定档`/`证据` 不该有空值，`splits` 允许为空（无划分信息）。
3. **跑召回自检**：`uv run python requests/REQ-0003-query-Benchmark-数据集普查与分档/method/scripts/recall_check.py`。
   脚本取 spec 里的 SQL 执行，检查 12 个标杆的档位；任何一行落进 U 即退出码 1。
   这一步不能省：口径改动（比如误删名录里的 `mmlu`）不会让查询报错，只会让 MMLU 静默变成 U。
4. **正式交付**：`hf-data-crawl request run REQ-0003`。
5. **输出列**：`id / url / 判定档 / 证据类型 / 证据 / 置信度 / 任务分类 / 语言 / 实测行数 / 划分 / 含训练划分 / 下载量30天 / 累计下载量 / 作者 / 状态`。
   档位判定只看前三列——`判定档`、`证据类型`、`证据`；后十列是给人复核用的上下文。
6. **读交付档案**：`deliveries/<时刻>/README.md`，核对口径、`source.current_md5` 是哪版底库、
   `method` 段是否钉住了本文件与脚本指纹。

## 机器算的 / 人判的

| 环节 | 谁做 | 判据 |
| --- | --- | --- |
| L1 标签自证 | 引擎 | `tags` 含字面 `benchmark`，或含 HF 官方命名空间 `benchmark:*`（`official` / `eval-yaml` / `odcv`） |
| L2 名录命中 | 引擎 | `id` 或 `pretty_name` 按词边界（`/ _ . - 空格`）命中名录词，如 `mmlu`、`swe-bench` |
| L3 文本自述 | 引擎 | 描述含 `benchmark` / `leaderboard`，或标题含 `bench` |
| L4 形态相似 | 引擎 | 有 `test` 划分，且 datasets-server 实测行数 < 50 万 |
| **名录本身** | **人** | 只收「广泛用于评测」的榜单（lm-evaluation-harness / HELM / Open LLM Leaderboard 常见任务名）。新榜单须人工加入并升版；名录是判定尺子，不能由机器自行扩张 |
| **L3 / L4 采不采信** | **人** | 引擎只给档位与凭据；剔不剔由治理方定（多数团队取 L1+L2 硬剔、L3 抽查、L4 只看） |
| **U 档复核** | **人** | 抽 U 里累计下载量高的库（脚本不自动判）：若真漏，补进名录或补标签，升版重交 |

## 脚本

`method/scripts/recall_check.py` —— 12 标杆召回自检。读 `spec.yaml` 的 `sql` 字段，
在只读 `GoldCatalog` 上执行，逐条打印档位；任一标杆落 U 则退出码 1。

改过脚本要跑 `hf-data-crawl request method REQ-0003` 重算指纹。改动记录：
- v1（首次）：建立自查；选定 12 个标杆时优先取「跨文本/多选/代码/数学/Agent」五类各有代表，
  避免只验文本选择题而漏掉 SWE-bench 这种无 test 划分的形态。
- v1 修订：交付列名改中文（手写 SQL 模式下交付档案不附列定义，表头即定义），
  自检随之从 `tier` 改读 `判定档`。判定逻辑未变。

## 环境

- 底库：`data/state/current.parquet`（本次 1,139,517 行）；基于哪一版见交付档案 `manifest.source.current_md5`
- 代码：`hf-data-crawl` 版本与提交号见 `manifest.method.env`
- SQL 依赖视图：`datasets`（= current + latest_silver + card + size + files）、`card_config`

## 变更记录

- **v2（2026-11-15）**：交付节奏 `once` → `manual`。只换节奏字段：本仓需求一律由人
  点名触发，不设固定周期；`manual` 不设「已交付即跳过」的门槛，想跑几次跑几次。
  分层口径、名录、阈值、SQL 一字未动，重交产物应与 v1 逐字节相同
  （同一版底库下 sha 一致，即为「没动口径」的自证）。

## 已知边界

- **U ≠ 不是榜单**。106.4 万行查不出证据，其中 68.1 万行连描述都没有（HF 未提供 `description`）。
  这是底库的观测缺口，不是判定结论。
- **名录是手写的 78 条**，覆盖 2026 年前的主流榜单；新出榜单（领域专用、厂商私有 eval）
  不会自动进 L2，只能靠 L3/L4 兜底。
- **副本与派生一并命中**：`hails/mmlu_no_train`、`lighteval/mmlu`、`princeton-nlp/SWE-bench_Verified`
  都算命中——治理时这是对的（副本同样污染），但会让 L2 的行数里「同一榜单的克隆」占大头。
- **L3 有模板句噪声**：不少卡片写「Supported Tasks and Leaderboards」小节标题，会被
  `leaderboard` 命中。这类行多为真榜单，但不足以单独定案，故置信度定 medium。
- **双重用途不裁决**：`glue`、`imdb`、`ceval-exam` 既可作训练语料又是常用评测集。
  表里给了 `has_train` 列，剔不剔交给治理方。
- **L4 的 50 万行阈值**是经验值：最初设 10 万行时把 MMLU（23.1 万行）挡在门外，遂放宽。
  再往上放宽收益递减（大语料也多带 test 划分）。
- **缺规模数据就进不了 L4**。`实测行数` 有 295,921 行为空（datasets-server 未收录），
  本单有 592 行明明带 test 划分、却因行数未知而落在 U。这是 L4 判据依赖 `size` 表的
  代价；等规模表覆盖上来，重跑一次这些行会自动进档（口径不必改）。
