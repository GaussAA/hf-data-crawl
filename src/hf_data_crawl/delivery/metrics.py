"""交付口径词表：把需求里说的「词」，钉到底库里的「列」。

为什么需要它
------------
需求说「下载量大于 1000」，而底库里有两个下载量：`downloads` 是**近 30 天**、
`downloads_all_time` 是历史累计。同一句话，取前者筛出几千个库，取后者几十万个。
这不是实现细节——**是需求本身没定义**。

把这件事前移到受理时刻，用一张词表钉住：需求只允许用词条，词条自带定义与默认
字段；要偏离默认就写别名，并留下理由。代价是这张表要维护，收益是歧义在受理时
暴露，而不是交付之后由对方发现。

为什么不直接用字段名
--------------------
字段名会变（本项目的宽视图已经改名过两轮，`source_sha` 进宽表时还得改成
`card_source_sha` 以免与 `sha` 混淆）。词条是给人用的稳定名字，字段是给机器用的
实现细节，两者分开，改列名时就只需改这里一处。
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum


class MetricKind(StrEnum):
    """词条能接受哪种比较。

    运算符白名单由它决定，不由调用方决定：对文本列施加 `>` 在 DuckDB 里合法，
    但语义上是字典序比较，几乎从来不是提问者想要的。宁可在编译期拒绝。
    """

    NUMBER = "number"
    TEXT = "text"
    FLAG = "flag"
    TIME = "time"


class UnknownMetric(ValueError):
    """需求用了词表没有的词。

    单列出来而非笼统的 ValueError：调用方要能把「词不认识」与「值不合法」
    分开报——前者是需求方写错了词，后者是填错了数。
    """

    def __init__(self, term: str, known: Iterable[str]) -> None:
        self.term = term
        self.known = sorted(set(known))
        super().__init__(f"词表里没有「{term}」。已登记的词条：{'、'.join(self.known)}")


@dataclass(frozen=True)
class Metric:
    """一个口径词条。"""

    #: 需求里写的词，中文为主。
    term: str
    #: `datasets` 视图里的列名。改列名只改这里。
    field: str
    kind: MetricKind
    #: 一句话定义。会原样进交付包的 README，所以要说人话、要写明歧义在哪。
    definition: str
    #: 其它可接受的写法（英文、简称）。
    aliases: tuple[str, ...] = ()


#: 全部词条。只收「有明确口径含义」的列：`id`/`url` 这类纯标识不进词表，
#: 它们由 `output.columns` 直接点名即可，不需要一套定义。
METRICS: tuple[Metric, ...] = (
    Metric(
        term="下载量",
        field="downloads",
        kind=MetricKind.NUMBER,
        definition="近 30 天下载量，不是累计值。想用累计值请写「累计下载量」",
        aliases=("downloads", "30天下载量"),
    ),
    Metric(
        term="累计下载量",
        field="downloads_all_time",
        kind=MetricKind.NUMBER,
        definition="历史累计下载量，只增不减",
        aliases=("downloads_all_time", "总下载量"),
    ),
    Metric(
        term="点赞数",
        field="likes",
        kind=MetricKind.NUMBER,
        definition="点赞总数",
        aliases=("likes", "点赞量"),
    ),
    Metric(
        term="趋势分",
        field="trending_score",
        kind=MetricKind.NUMBER,
        definition="HF 计算的趋势分，随日漂移；只宜当筛选条件，不能靠它排序翻页",
        aliases=("trending_score",),
    ),
    Metric(
        term="卡片声明行数",
        field="card_num_rows",
        kind=MetricKind.NUMBER,
        definition="作者在卡片里声明的总行数（跨 config 累加），未必准；实测值见「实测行数」",
        aliases=("card_num_rows",),
    ),
    Metric(
        term="实测行数",
        field="server_num_rows",
        kind=MetricKind.NUMBER,
        definition="datasets-server 实测行数，与卡片声明是两个口径",
        aliases=("server_num_rows",),
    ),
    Metric(
        term="列数",
        field="server_num_columns",
        kind=MetricKind.NUMBER,
        definition="各 config 中列数的最大值",
        aliases=("server_num_columns",),
    ),
    Metric(
        term="数据体积",
        field="card_dataset_bytes",
        kind=MetricKind.NUMBER,
        definition="作者声明的数据体积，单位字节",
        aliases=("card_dataset_bytes",),
    ),
    Metric(
        term="下载体积",
        field="card_download_bytes",
        kind=MetricKind.NUMBER,
        definition="作者声明的下载体积（压缩后），单位字节",
        aliases=("card_download_bytes",),
    ),
    Metric(
        term="主分支体积",
        field="main_size",
        kind=MetricKind.NUMBER,
        definition="仓库主分支的字节数",
        aliases=("main_size",),
    ),
    Metric(
        term="创建日期",
        field="created_at",
        kind=MetricKind.TIME,
        definition="仓库创建时刻（UTC）",
        aliases=("created_at",),
    ),
    Metric(
        term="最后修改",
        field="last_modified",
        kind=MetricKind.TIME,
        definition="最后一次提交的时刻（UTC）",
        aliases=("last_modified",),
    ),
    Metric(
        term="首次进库",
        field="first_seen_at",
        kind=MetricKind.TIME,
        definition="首次出现在本底库的日期（按天）",
        aliases=("first_seen_at",),
    ),
    Metric(
        term="最后观察",
        field="last_seen_at",
        kind=MetricKind.TIME,
        definition="最后一次被扫描到的日期（按天）；停更不等于下架",
        aliases=("last_seen_at",),
    ),
    Metric(
        term="状态",
        field="status",
        kind=MetricKind.TEXT,
        definition="active 在架 / disappeared 已删除 / restricted 转私有 / renamed 已改名",
        aliases=("status",),
    ),
    Metric(
        term="作者",
        field="author",
        kind=MetricKind.TEXT,
        definition="仓库所属用户名",
        aliases=("author",),
    ),
    Metric(
        term="门禁",
        field="gated",
        kind=MetricKind.TEXT,
        definition="false 公开 / manual 需人工审批 / auto 自动授权",
        aliases=("gated",),
    ),
    Metric(
        term="许可证",
        field="license_name",
        kind=MetricKind.TEXT,
        definition="卡片声明的许可证名称，未声明则为空",
        aliases=("license_name",),
    ),
    Metric(
        term="是否私有",
        field="private",
        kind=MetricKind.FLAG,
        definition="私有库对外不可见，但公开索引里可能仍留一条记录",
        aliases=("private",),
    ),
    Metric(
        term="是否停用",
        field="disabled",
        kind=MetricKind.FLAG,
        definition="上游标记为停用的库",
        aliases=("disabled",),
    ),
)


def _build_index(items: Iterable[Metric]) -> dict[str, Metric]:
    """词条、字段名、别名三套写法都认，指向同一条定义。

    冲突在导入期就炸：两个词条共用一个写法，等于让「同一个词两种意思」悄悄上线，
    而那张交付单会一直错下去，没人看得出。

    接受参数而非直接读模块级 `METRICS`，是为了让冲突检测本身可测——用两张造假
    的词表调它，好过让这条防御永远得不到验证。
    """
    index: dict[str, Metric] = {}
    for metric in items:
        for key in (metric.term, metric.field, *metric.aliases):
            existing = index.get(key)
            if existing is not None and existing is not metric:
                raise ValueError(f"词表冲突：{key!r} 同时指向 {existing.field} 与 {metric.field}")
            index[key] = metric
    return index


_INDEX = _build_index(METRICS)


def lookup(term: str) -> Metric:
    """按词条、字段名或别名取定义；不认识就立刻报，不返回 None。"""
    try:
        return _INDEX[term]
    except KeyError:
        raise UnknownMetric(term, (metric.term for metric in METRICS)) from None


def terms() -> tuple[str, ...]:
    """全部中文词条，供报错与文档列举。"""
    return tuple(metric.term for metric in METRICS)


def mismatched_fields(columns: Iterable[str]) -> dict[str, str]:
    """词表里指向「视图没有的列」的条目 → {词条: 字段}。

    加词最容易犯的错是拼错列名，而 polars / duckdb 要到查询时才报，那时已经
    在跑交付了。体检与测试都调它，把错误提前到「改完就发现」。
    """
    available = set(columns)
    return {metric.term: metric.field for metric in METRICS if metric.field not in available}
