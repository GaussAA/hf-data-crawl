"""README 正文清洗：把 Markdown 正文压成「只留信息」的版本，物化给导出用。

为什么在派生层物化
------------------
导出宽表的 `readme` 列要对 78 万条正文做五道正则。现场做一次约 14 秒（2026-11-19
实测：清洗 + 写盘 13.8 s、结果 287 MB），而 `export-wide` 每天只跑一次——把这次
成本挪到 `parse-readme`，导出就只剩「读 287 MB + join」，43.5 s → 约 26 s。

增量
----
按 `(id, source_sha)` 判新旧：正文没变的行不重洗。日常 `fetch-readme --changed-all`
只取几千条变化的库，清洗成本随变化量走，不是每天全量 14 秒。这条很关键——全量
物化只是把成本从导出挪到派生，总时长不变；增量才真有收益。

规则
----
顺序有意义：YAML 头先去掉（它含 `---` 与大量 `:`，先处理后两道正则更便宜）；图片
与徽章必须在「链接只留文字」之前——徽章是 `[![alt](img)](url)`，先转链接会留下
`![alt](img)` 这种半截。

换行一律写成 `\\r?\\n`：上游 README 的 CRLF 与 LF 混着（2026-11-19 实测，
`farwew/subsetTemplateName` 这类整篇都是 `\\r\\n`），只认 `\\n` 会让整段正则空转。

代码块一个字不动——加载示例与字段示例是有用信息。实测（抽 2000 条）frontmatter
占 26.8%、代码块 25.9%、HTML 7.5%。
"""

from __future__ import annotations

#: 去 YAML 头。回退底稿也用它：清洗后不足 100 字符时截「去 YAML 后的原文」前 500
#: 字符——截原始正文会把刚去掉的 frontmatter 又还回去。
_STRIP = "regexp_replace({col}, '(?s)^---\\r?\\n.*?---\\r?\\n', '')"

#: 完整清洗链（DuckDB 表达式）。`{col}` 是正文列名。
_CLEAN = (
    "regexp_replace("
    "regexp_replace("
    "regexp_replace("
    "regexp_replace("
    "regexp_replace({col}, '(?s)^---\\r?\\n.*?---\\r?\\n', ''),"
    " '!\\[[^\\]]*\\]\\([^)]*\\)', '', 'g'),"
    " '<[^>]+>', '', 'g'),"
    " '\\[([^\\]]*)\\]\\([^)]*\\)', '\\1', 'g'),"
    " '(?:\\r?\\n){3,}', chr(10) || chr(10), 'g')"
)


def clean_sql(column: str = "text") -> str:
    """正文清洗表达式：清洗后不足 100 字符时回退「去 YAML 后的原文」前 500 字符。

    回退不是可有可无：正文只有 frontmatter 或只有一张图的库，清洗后会变成空串，
    读的人会以为作者什么都没写。

    占位符用 `str.replace` 而不是 `str.format`：正则里有 `{3,}`，`format` 会把它
    当成格式字段并抛 `KeyError: '3,'`（2026-11-19 实测）。
    """
    clean = _CLEAN.replace("{col}", column)
    strip = _STRIP.replace("{col}", column)
    return f"CASE WHEN char_length({clean}) < 100 THEN substr({strip}, 1, 500) ELSE {clean} END"


def incremental_sql(readme_path: str, clean_path: str | None) -> str:
    """只算新增或 `source_sha` 变化的行；`clean_path` 为空表示首次全量。

    用 `LEFT JOIN + sha 不等` 而不是先取一遍已有 id 传进来：78 万对键值字符串在
    SQL 里 join 比在 Python 侧攒集合再拼 `NOT IN` 便宜得多，也不会撑大语句。
    """
    expression = clean_sql("r.text")
    if clean_path is None:
        return f"""
            SELECT r.id, r.url, r.snapshot_date, r.source_sha, {expression} AS text_clean
            FROM read_parquet('{readme_path}') r
            WHERE r.text IS NOT NULL
        """
    return f"""
        SELECT r.id, r.url, r.snapshot_date, r.source_sha, {expression} AS text_clean
        FROM read_parquet('{readme_path}') r
        LEFT JOIN read_parquet('{clean_path}') c USING (id)
        WHERE r.text IS NOT NULL
          AND (c.source_sha IS NULL OR c.source_sha <> r.source_sha)
    """
