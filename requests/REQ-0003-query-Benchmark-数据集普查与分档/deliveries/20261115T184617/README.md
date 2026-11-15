# REQ-0003 Benchmark 数据集普查与分档

- 提出人：大帅
- 用途：训练数据污染治理：查出全库里哪些数据集属评测榜单，召回优先，按证据分档供人工复核
- 交付时刻：2026-11-15 18:46:17
- 需求形态：`query`
- 需求版本：v2（口径指纹 `ddf9e9d47f4882d8…`）
- 交付节奏：manual

## 口径

手写 SQL：

```sql
WITH split_span AS (
    SELECT id,
           string_agg(DISTINCT lower(split), ', ' ORDER BY lower(split)) AS splits
    FROM card_config
    WHERE split IS NOT NULL
    GROUP BY id
),
base AS (
    SELECT d.id,
           d.url,
           d.author,
           d.status,
           d.downloads,
           d.downloads_all_time,
           d.task_categories,
           d.languages,
           d.server_num_rows,
           regexp_replace(coalesce(d.pretty_name, ''), '\s+', ' ', 'g') AS pretty,
           regexp_replace(coalesce(d.description, ''), '\s+', ' ', 'g') AS descr,
           coalesce(sp.splits, '') AS splits,
           list_filter(d.tags, lambda t: lower(t) = 'benchmark') AS t_plain,
           list_filter(d.tags, lambda t: starts_with(lower(t), 'benchmark:')) AS t_ns
    FROM datasets AS d
    LEFT JOIN split_span AS sp ON sp.id = d.id
),
marked AS (
    SELECT *,
           list_contains(string_split(splits, ', '), 'test') AS has_test,
           list_contains(string_split(splits, ', '), 'train') AS has_train,
           regexp_extract(
               lower(id) || ' ' || lower(pretty),
               '(^|[/_.\- ])(arc-challenge|arc-easy|lambada-openai|super-glue|natural-questions|livecodebench|bigcodebench|arena-hard|alpacaeval|seed-bench|video-mme|tau-bench|math-500|mathvista|commonsenseqa|narrativeqa|truthfulqa|winogrande|openbookqa|swe-bench|mt-bench|longbench|osworld|mmbench|hotpotqa|humaneval|hellaswag|agieval|c-mteb|super_glue|tabfact|agentbench|webarena|hendrycks|docvqa|textvqa|cmmlu|c-eval|ceval|chartqa|gpqa|mmlu-pro|mmmu|lambada|mteb|beir|gpqa|musr|qasper|musique|spider|coqa|glue|squad_v2|squad|bbh|mbpp|ifeval|asdiv|svamp|ruler|gaia|piqa|siqa|anli|drop|rte|gsm8k|aime|mmlu|arc)([/_.\- ]|$)',
               2
           ) AS roster_hit,
           (lower(pretty) LIKE '%bench%'
            OR lower(descr) LIKE '%benchmark%'
            OR lower(descr) LIKE '%leaderboard%') AS text_hit
    FROM base
),
tiered AS (
    SELECT *,
           CASE
               WHEN len(t_plain) > 0 OR len(t_ns) > 0 THEN 'L1'
               WHEN roster_hit IS NOT NULL AND roster_hit <> '' THEN 'L2'
               WHEN text_hit THEN 'L3'
               WHEN has_test AND server_num_rows IS NOT NULL AND server_num_rows < 500000 THEN 'L4'
               ELSE 'U'
           END AS tier
    FROM marked
)
SELECT
    id,
    url,
    tier AS 判定档,
    CASE tier
        WHEN 'L1' THEN 'tag'
        WHEN 'L2' THEN 'roster'
        WHEN 'L3' THEN 'text'
        WHEN 'L4' THEN 'shape'
        ELSE 'none'
    END AS 证据类型,
    CASE tier
        WHEN 'L1' THEN CASE
            WHEN len(t_plain) > 0 AND len(t_ns) > 0
                THEN 'tag:benchmark | ' || array_to_string(t_ns, ' | ')
            WHEN len(t_plain) > 0 THEN 'tag:benchmark'
            ELSE 'tag:' || array_to_string(t_ns, ' | ')
        END
        WHEN 'L2' THEN 'roster:' || roster_hit
        WHEN 'L3' THEN CASE
            WHEN position('benchmark' IN lower(descr)) > 0
                THEN 'desc:' || substr(descr, greatest(1, position('benchmark' IN lower(descr)) - 24), 64)
            WHEN position('leaderboard' IN lower(descr)) > 0
                THEN 'desc:' || substr(descr, greatest(1, position('leaderboard' IN lower(descr)) - 24), 64)
            ELSE 'name:' || substr(pretty, 1, 64)
        END
        WHEN 'L4' THEN 'split:test rows=' || coalesce(cast(server_num_rows AS VARCHAR), '?')
        ELSE 'no-evidence'
    END AS 证据,
    CASE tier
        WHEN 'L1' THEN 'high'
        WHEN 'L2' THEN 'high'
        WHEN 'L3' THEN 'medium'
        WHEN 'L4' THEN 'low'
        ELSE 'unjudged'
    END AS 置信度,
    task_categories AS 任务分类,
    languages AS 语言,
    server_num_rows AS 实测行数,
    splits AS 划分,
    has_train AS 含训练划分,
    downloads AS 下载量30天,
    downloads_all_time AS 累计下载量,
    author AS 作者,
    status AS 状态
FROM tiered
ORDER BY tier ASC, downloads_all_time DESC NULLS LAST, id ASC
```


## 基于哪一版底库

- `current.parquet` md5：`894eada4e8b6294b475b702c57e9e230`
- 底库总行数：1,139,517
- 最新观测日：2026-11-15

底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，
跑 `hf-data-crawl request show REQ-0003` 看交付台账。

## 结果

- 行数：1,139,517
- 列数：15
- `data.parquet`：48,421,000 字节，sha256 `7e813941775623ec…`

## 复现

- 做法：`method/METHOD.md`（步骤、判据、哪步机器哪步人判）
- 脚本：`method/scripts/`，逐份 sha256 与所依赖的代码版本
  见 `manifest.json` 的 `method` 段

```bash
hf-data-crawl request run REQ-0003 --force
```

同一版底库、同一份输入表下重跑，sha256 应当与上面一致。若不一致，
说明底库或输入表已经变过。
