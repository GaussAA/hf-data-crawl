"""静态榜单页：把 Gold 查询渲染成单文件 HTML。

设计约定（2026-11-07 重构）：
- 本模块与 assets/ 下的 css/js 是源，`data/reports/dashboard.html` 只是产物；
  成品内联样式与脚本，不联网、不加载任何外部资源。
- 口径与展示必须对齐：累计榜是独立集合，不能把 30 天榜的行重排冒充。
- 概览给覆盖率而不是孤立大数；状态按 GLOSSARY 四值分开列，不笼统称「失效」。
- 增速榜给出观测区间；分布表给出占比；口径说明集中一处、默认折叠。
- 深色「数据编辑部」：暖灰墨底、单一数据色、细线表格、原生控件一并造境。
- 每个表头有 scope，每张表有 caption，空态按真实列数铺满，注入一律转义。
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl

from .atomic import write_text
from .gold import DATASET_URL, GoldCatalog

_ASSETS = Path(__file__).parent / "assets"

# 样式与脚本放在 assets/ 下作为独立文件：改样式不必在 Python 字符串里找。
# 渲染时内联进单文件 HTML，成品页仍然不依赖任何外部资源。
_CSS = (_ASSETS / "report.css").read_text(encoding="utf-8")
_JS = (_ASSETS / "report.js").read_text(encoding="utf-8")


@dataclass(frozen=True)
class ReportSummary:
    path: Path
    bytes: int
    datasets: int


@dataclass(frozen=True)
class Column:
    """一列的定义。kind=share 时 alt 是第二度量的字段名。"""

    field: str
    title: str
    kind: str = "text"
    alt: str | None = None


_RANKING_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("id", "数据集", "code"),
    Column("pretty_name", "正式名", "name"),
    Column("downloads", "30 天", "num"),
    Column("downloads_all_time", "累计", "num"),
    Column("likes", "点赞", "num"),
    Column("license_name", "许可证", "text"),
]
_GROWTH_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("id", "数据集", "code"),
    Column("downloads_start", "起点", "num"),
    Column("downloads_now", "当前", "num"),
    Column("growth", "增量", "num"),
    Column("from_date", "观测区间", "window"),
    Column("span_days", "跨度(天)", "num"),
]
_VOLUME_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("id", "数据集", "code"),
    Column("num_rows", "行数", "compact"),
    Column("num_columns_max", "列数", "num"),
    Column("mem_gb", "内存(GB)", "num"),
]
_MODALITY_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("modality", "模态", "text"),
    Column("datasets", "集数", "num"),
    Column("downloads", "下载", "num"),
    Column("datasets", "集数占比", "share", alt="downloads"),
]
_CATEGORY_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("category", "任务类别", "text"),
    Column("datasets", "集数", "num"),
    Column("downloads", "下载", "num"),
    Column("datasets", "集数占比", "share", alt="downloads"),
]
_LICENSE_COLUMNS = [
    Column("rank", "#", "rank"),
    Column("license_name", "许可证", "text"),
    Column("datasets", "集数", "num"),
    Column("datasets", "占比", "share"),
]

_STATUS_TERMS = (
    ("active", "在架", "正常公开可见"),
    ("disappeared", "已删除", "对账未扫到，且单库探测返回 404"),
    ("restricted", "转私有", "库还在，探测返回 401；不是删除"),
    ("renamed", "已改名", "跟随 307 拿到新 id，观测史仍在"),
)


def _esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _as_number(value: object) -> float:
    """把 DuckDB 可能给回的 Decimal / Int128 折成 float，只用于占比与排序键。

    展示仍走 `_num`，保留原始精度；这里只做相对大小的比较。
    """
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    return 0.0


def _num(value: object) -> str:
    """数字列统一千分位。

    `sum(downloads)` 经 DuckDB → polars 常是 Decimal，早先只认 int/float，
    于是分布表里出现 `119773807` 这种裸数字——不是缺样式，是缺类型分支。
    """
    if value is None:
        return "—"
    if isinstance(value, (int, float, Decimal)):
        return f"{value:,.0f}"
    return _esc(value)


def _compact(value: object) -> str:
    """大数字压成 T/B/M，附完整值供悬停查看。"""
    if not isinstance(value, (int, float, Decimal)):
        return "—" if value is None else _esc(value)
    exact = f"{value:,.0f}"
    number = float(value)
    for unit, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(number) >= scale:
            return f'<span title="{exact}">{number / scale:.2f}{unit}</span>'
    return exact


def _pct(value: object, total: float) -> float:
    if total <= 0:
        return 0.0
    return max(0.0, min(100.0, _as_number(value) / total * 100))


def _window(record: dict[str, Any]) -> str:
    """把 SQL 已给出的 from_date / to_date 显示出来，而不是只给跨度天数。"""
    start, end = record.get("from_date"), record.get("to_date")
    if start is None or end is None:
        return "—"
    return f"{str(start)[5:]} → {str(end)[5:]}"


def _head_cell(index: int, col: Column, measurable: bool) -> str:
    if col.kind == "rank":
        return '<th scope="col" class="rank-col"><span class="sr">序号</span>#</th>'
    if col.kind == "share":
        label0 = "集数占比" if measurable else "占比"
        label1 = "下载占比" if measurable else "占比"
        return (
            f'<th scope="col" class="share-col" data-sort="num" data-col="{index}"'
            f' aria-sort="none" data-label0="{_esc(label0)}" data-label1="{_esc(label1)}">'
            f"{_esc(label0)}</th>"
        )
    numeric = col.kind in ("num", "compact")
    cls = ' class="num"' if numeric else ""
    kind = "num" if numeric else "text"
    return (
        f'<th scope="col"{cls} data-sort="{kind}" data-col="{index}" aria-sort="none">'
        f"{_esc(col.title)}</th>"
    )


def _share_cell(record: dict[str, Any], col: Column, totals: dict[str, float]) -> str:
    alt = col.alt or col.field
    v0, v1 = _as_number(record.get(col.field)), _as_number(record.get(alt))
    p0, p1 = _pct(v0, totals.get(col.field, 0.0)), _pct(v1, totals.get(alt, 0.0))
    return (
        f'<td class="share" data-v0="{v0:.0f}" data-v1="{v1:.0f}"'
        f' data-p0="{p0:.2f}" data-p1="{p1:.2f}" style="--w:{p0:.2f}%">'
        f'<i class="bar"></i><span class="share-num">{p0:.1f}%</span></td>'
    )


def _cell(record: dict[str, Any], col: Column, value: object, totals: dict[str, float]) -> str:
    raw = "" if value is None else str(value)
    if col.kind == "rank":
        return '<td class="rank"></td>'
    if col.kind == "num":
        return f'<td class="num" data-n="{_esc(raw)}">{_num(value)}</td>'
    if col.kind == "compact":
        return f'<td class="num" data-n="{_esc(raw)}">{_compact(value)}</td>'
    if col.kind == "code":
        # 榜单是拿来「找到东西」的：SQL 已带 url 列，缺失时按同一前缀兜底。
        href = record.get("url") or f"{DATASET_URL}{value}"
        return (
            f'<td class="id"><a href="{_esc(href)}" target="_blank"'
            f' rel="noopener noreferrer">{_esc(value)}</a></td>'
        )
    if col.kind == "name":
        text = _esc(value)
        return f'<td class="name">{text if text else "<span class=dim>—</span>"}</td>'
    if col.kind == "window":
        end = _esc(str(record.get("to_date") or ""))
        return f'<td class="window" data-n="{end}">{_esc(_window(record))}</td>'
    if col.kind == "share":
        return _share_cell(record, col, totals)
    return f"<td>{_esc(value)}</td>"


def _row(record: dict[str, Any], columns: list[Column], totals: dict[str, float]) -> str:
    cells: list[str] = []
    haystack: list[str] = []
    for col in columns:
        value = record.get(col.field)
        haystack.append("" if value is None else str(value))
        if col.kind == "window":
            haystack.append(_window(record))
        cells.append(_cell(record, col, value, totals))
    search = _esc(" ".join(haystack).lower())
    return f'<tr data-search="{search}">' + "".join(cells) + "</tr>"


def _table(
    table_id: str,
    frame: pl.DataFrame,
    columns: list[Column],
    *,
    caption: str,
    cap_alt: str | None = None,
    measurable: bool = False,
    hidden: bool = False,
) -> str:
    """渲染一张可筛选、可排序的表；占比的两个度量在渲染时一次算好。"""
    records = list(frame.iter_rows(named=True))
    share_fields = {
        name for col in columns if col.kind == "share" for name in (col.field, col.alt) if name
    }
    totals = {
        name: sum(_as_number(record.get(name)) for record in records) for name in share_fields
    }
    head = "".join(_head_cell(index, col, measurable) for index, col in enumerate(columns))
    rows = "".join(_row(record, columns, totals) for record in records)
    if not rows:
        rows = (
            f'<tr data-empty><td colspan="{len(columns)}" class="dim">'
            "该榜暂无数据，等下一轮采集或重建后再看。</td></tr>"
        )
    attrs = ' data-measurable="1"' if measurable else ""
    attrs += " hidden" if hidden else ""
    cap1 = cap_alt or caption
    return (
        f'<table id="{table_id}" class="data" data-searchable="1"'
        f' data-cols="{len(columns)}" data-cap0="{_esc(caption)}" data-cap1="{_esc(cap1)}"'
        f"{attrs}><caption>{_esc(caption)}</caption>"
        f"<thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table>"
    )


def _cover_row(label: str, value: object, total: float, note: str) -> str:
    pct = _pct(value, total)
    return (
        '<div class="cover-row">'
        f'<span class="cover-label">{_esc(label)}</span>'
        f'<span class="cover-value">{_num(value)} <em>{pct:.1f}%</em></span>'
        f'<div class="cover-track"><i style="--w:{pct:.2f}%"></i></div>'
        f'<p class="cover-note">{_esc(note)}</p>'
        "</div>"
    )


def _chapter_head(number: str, title: str, note: str) -> str:
    return (
        '<div class="chapter-head">'
        f'<span class="chapter-no">{number}</span>'
        f"<h2>{_esc(title)}</h2>"
        f'<p class="chapter-note">{_esc(note)}</p>'
        "</div>"
    )


class Reporter:
    def __init__(self, catalog: GoldCatalog) -> None:
        self._catalog = catalog

    def _overview(self) -> dict[str, Any]:
        return self._catalog.run(
            """
            SELECT (SELECT count(*) FROM current) AS datasets,
                   (SELECT count(*) FROM current WHERE status <> 'active') AS gone,
                   (SELECT coalesce(sum(CASE WHEN status = 'active' THEN 1 ELSE 0 END), 0)
                      FROM current) AS active,
                   (SELECT coalesce(sum(CASE WHEN status = 'disappeared' THEN 1 ELSE 0 END), 0)
                      FROM current) AS disappeared,
                   (SELECT coalesce(sum(CASE WHEN status = 'restricted' THEN 1 ELSE 0 END), 0)
                      FROM current) AS restricted,
                   (SELECT coalesce(sum(CASE WHEN status = 'renamed' THEN 1 ELSE 0 END), 0)
                      FROM current) AS renamed,
                   (SELECT count(*) FROM card) AS cards,
                   (SELECT count(*) FROM dataset_size WHERE status = 'ok') AS sized,
                   (SELECT count(*) FROM v_growth) AS growth_ready,
                   (SELECT count(*) FROM observations
                     WHERE snapshot_date = (SELECT max(snapshot_date) FROM observations)) AS moved,
                   (SELECT max(snapshot_date) FROM observations) AS as_of,
                   (SELECT max(last_seen_at) FROM current) AS last_seen,
                   (SELECT coalesce(sum(downloads), 0) FROM current) AS total_downloads
            """
        ).row(0, named=True)

    @staticmethod
    def _state_html(overview: dict[str, Any]) -> str:
        total = _as_number(overview["datasets"])
        covers = "".join(
            _cover_row(label, overview[key], total, note)
            for label, key, note in (
                ("卡片已解析", "cards", "card 视图行数；长尾多数没有卡片"),
                ("规模已实测", "sized", "dataset_size 中 status = ok"),
                ("可测增速", "growth_ready", "具备两次跨日观测点"),
                ("在架", "active", "current.status = active"),
            )
        )
        splits = "".join(
            f"<div><dt>{_esc(label)}</dt><dd>{_num(overview[key])}</dd><p>{_esc(note)}</p></div>"
            for key, label, note in _STATUS_TERMS
        )
        moved = _num(overview["moved"])
        return (
            '<div class="state-grid">'
            '<div class="headline"><p class="eyebrow">当前态 · current</p>'
            f'<p class="headline-num">{_num(overview["datasets"])}</p>'
            '<p class="headline-note">一个数据集一行，是全库唯一基准；'
            "其余各表或是流水、或是子集，行数本就不该相等。</p>"
            f'<p class="headline-foot">最近观测日有 <b>{moved}</b> 个数据集指标发生变化'
            "（不是「今天」，是最后一个有读数的自然日）。</p></div>"
            f'<div class="coverage"><h3>覆盖率</h3>{covers}</div>'
            "</div>"
            f'<dl class="status-split"><div class="split-title"><dt>状态四值</dt>'
            "<dd>「没扫到」不等于删除，四种状态各有去留</dd></div>"
            f"{splits}</dl>"
        )

    @staticmethod
    def _growth_html(growth: pl.DataFrame, overview: dict[str, Any]) -> str:
        if growth.height:
            return _table(
                "growth",
                growth,
                _GROWTH_COLUMNS,
                caption="起点与当前取自最近两个观测点；观测区间给出具体日期，跨度以天计。",
            )
        return (
            '<p class="empty">暂无正增长可比：全库 <strong>'
            f"{int(overview['growth_ready']):,}"
            "</strong> 个数据集具备两次跨日读数。HF 的下载量为批量更新，"
            "同一天重扫不会产生新读数，需再等一个自然日。</p>"
        )

    def render(self, *, top: int = 25, overview: dict[str, Any] | None = None) -> str:
        catalog = self._catalog
        overview = overview or self._overview()

        # 每张榜都取自内置榜单，和 `query --board` 同一口径：口径只在 gold.BOARDS
        # 改一处。两个下载榜是各自独立的 LIMIT，不能由同一批行重排得到。
        ranking = catalog.board("ranking", top)
        ranking_alltime = catalog.board("ranking_alltime", top)
        categories = catalog.board("categories", top)
        licenses = catalog.board("licenses", top)
        growth = catalog.board("growth", top)
        volume = catalog.board("volume", top)
        modalities = catalog.board("modalities", top)

        ranking_html = _table(
            "ranking",
            ranking,
            _RANKING_COLUMNS,
            caption=f"按 30 天下载排序的前 {top} 名；「累计」一列只是并列的另一口径，勿相加。",
        )
        alltime_html = _table(
            "ranking-alltime",
            ranking_alltime,
            _RANKING_COLUMNS,
            caption=f"按累计下载排序的前 {top} 名；与 30 天榜成员不同，是另一条 SQL。",
            hidden=True,
        )
        modality_html = _table(
            "modalities",
            modalities,
            _MODALITY_COLUMNS,
            caption="占比以本表集数合计为分母；一个数据集可标多个模态，各占比之和可超过 100%。",
            cap_alt="占比以本表下载合计为分母；旧式无前缀标签归入 plain_tags，不计入本表。",
            measurable=True,
        )
        category_html = _table(
            "categories",
            categories,
            _CATEGORY_COLUMNS,
            caption="占比以本表集数合计为分母；取标签 task_categories 展开后聚合。",
            cap_alt="占比以本表下载合计为分母；用于判断哪些方向正在被集中使用。",
            measurable=True,
        )
        license_html = _table(
            "licenses",
            licenses,
            _LICENSE_COLUMNS,
            caption="仅统计已补齐卡片的数据集，未补齐者不计入分母；占比以本表集数合计为分母。",
        )
        volume_html = _table(
            "volume",
            volume,
            _VOLUME_COLUMNS,
            caption="行数为 datasets-server 实测的 parquet 行数，内存为载入内存估算；"
            "与仓库字节 main_size 不是一回事。",
        )
        growth_html = self._growth_html(growth, overview)
        state_html = self._state_html(overview)

        as_of = _esc(overview["as_of"])
        last_seen = _esc(overview["last_seen"])
        total_downloads = _num(overview["total_downloads"])

        return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HF 数据集底座 · 榜单</title><style>{_CSS}</style></head>
<body><main class="sheet">

<header class="masthead">
  <div class="masthead-main">
    <p class="eyebrow">HF DATASET BASE <span>/</span> 离线快照</p>
    <h1>数据集底座 · 榜单</h1>
    <p class="lede">每张表都取自 Gold 视图，与 <code>query --board</code> 同一口径。
    页面不联网、不加载外部资源，数据只到生成那一刻为止。</p>
  </div>
  <dl class="meta">
    <div><dt>观测截至</dt><dd>{as_of}</dd></div>
    <div><dt>最后扫描</dt><dd>{last_seen}</dd></div>
    <div><dt>每榜条数</dt><dd>Top {top}</dd></div>
    <div><dt>30 天下载合计</dt><dd>{total_downloads}</dd></div>
  </dl>
</header>

<nav class="jump" aria-label="章节导航">
  <a href="#state"><span>01</span> 底座状态</a>
  <a href="#heat"><span>02</span> 热度与变化</a>
  <a href="#shape"><span>03</span> 结构分布</a>
  <a href="#volume"><span>04</span> 规模上限</a>
</nav>

<div class="toolbar" role="search">
  <label class="q"><span>筛选全表</span>
    <input id="q" type="search" placeholder="数据集 id / 正式名 / 标签 / 许可证" autocomplete="off">
  </label>
  <span id="q-status" class="q-status" aria-live="polite"></span>
  <p class="hint">按 <kbd>/</kbd> 聚焦筛选 · 点表头排序 · 双击页边切换明暗</p>
</div>

<section id="state" class="chapter">
  {_chapter_head("01", "底座状态", "先看这份报告能代表多少：各张附表都是子集，覆盖率比绝对数更能说明问题。")}
  {state_html}
  <details class="glossary">
    <summary>口径说明 <span>六条，点开查看</span></summary>
    <dl>
      <div><dt>下载量</dt><dd><code>downloads</code> 是近 30 天，<code>downloads_all_time</code> 是历史累计；
        两者只增不减的口径不同，不能相加，也不能用累计值排「近 30 天」。</dd></div>
      <div><dt>观测与增速</dt><dd>HF 没有下载量时序接口，增速只能靠自建 observations 流水；
        同一天重扫不会产生新读数，所以增速榜一律取最近两个跨日观测点，并在表里给出具体日期与跨度。</dd></div>
      <div><dt>行数</dt><dd><code>card_num_rows</code> 是作者声明，<code>size.num_rows</code> 是
        datasets-server 实测（parquet 转换后）。两者留着就是为了让它们对不上时被看见。</dd></div>
      <div><dt>体积</dt><dd><code>main_size</code> 是仓库字节，含历史版本与 LFS 开销；
        规模榜的「内存」是载入内存估算，都不是原始文件大小。</dd></div>
      <div><dt>状态</dt><dd><code>active</code> / <code>disappeared</code> / <code>restricted</code> /
        <code>renamed</code> 四值各有去留；「对账没扫到」本身不构成删除结论。</dd></div>
      <div><dt>覆盖</dt><dd>卡片、规模、增速都是按下载量取头部或按需补齐的子集；
        每张榜只列 Top {top}，占比以表内可见条目为分母，不代表全站占比。</dd></div>
    </dl>
  </details>
</section>

<section id="heat" class="chapter">
  {_chapter_head("02", "热度与变化", "谁在被下载，以及两个观测点之间谁在涨。")}
  <div class="board">
    <div class="board-head">
      <div><h3>下载榜</h3>
        <p class="board-note">两个口径各取 Top {top}，成员不同；切换的是整张表，不是把同一批行重排。</p></div>
      <div class="controls">
        <div class="seg" role="group" aria-label="下载榜口径">
          <button type="button" data-group="ranking" data-target="ranking" aria-pressed="true">30 天</button>
          <button type="button" data-group="ranking" data-target="ranking-alltime" aria-pressed="false">累计</button>
        </div>
        <span class="board-count" aria-live="polite"></span>
      </div>
    </div>
    {ranking_html}
    {alltime_html}
  </div>
  <div class="board">
    <div class="board-head">
      <div><h3>增速榜</h3>
        <p class="board-note">最近两个观测点的 30 天下载增量，只列正增长。</p></div>
      <div class="controls"><span class="board-count" aria-live="polite"></span></div>
    </div>
    {growth_html}
  </div>
</section>

<section id="shape" class="chapter">
  {_chapter_head("03", "结构分布", "按标签维度看全站构成；占比可切换为按集数或按下载。")}
  <div class="board">
    <div class="board-head">
      <div><h3>分布度量</h3>
        <p class="board-note">许可证榜只有集数，没有下载口径，不随切换变化。</p></div>
      <div class="controls">
        <div class="seg" role="group" aria-label="分布度量">
          <button type="button" data-measure="0" aria-pressed="true">按集数</button>
          <button type="button" data-measure="1" aria-pressed="false">按下载</button>
        </div>
      </div>
    </div>
    <div class="dist-grid">
      <div class="dist-cell"><h4>模态分布</h4>{modality_html}</div>
      <div class="dist-cell"><h4>任务类别</h4>{category_html}</div>
      <div class="dist-cell wide"><h4>许可证分布</h4>{license_html}</div>
    </div>
  </div>
</section>

<section id="volume" class="chapter">
  {_chapter_head("04", "规模上限", "头部两万名补过 datasets-server 的规模；行数动辄万亿，用 T/B/M 压缩并保留完整值。")}
  <div class="board">
    <div class="board-head">
      <div><h3>规模榜</h3>
        <p class="board-note">按实测行数降序；列数取各 config 中的最大值。</p></div>
      <div class="controls"><span class="board-count" aria-live="polite"></span></div>
    </div>
    {volume_html}
  </div>
</section>

<footer>
  <p>由 <code>hf-data-crawl report</code> 生成 · 单文件、离线可读 ·
  口径改动只需改 Gold 视图的 SQL，不重跑采集。</p>
  <p class="views">视图 <code>current</code> / <code>datasets</code> / <code>card</code> /
  <code>card_config</code> / <code>dataset_size</code> / <code>tag_values</code> /
  <code>v_growth</code></p>
</footer>
</main><script>{_JS}</script></body></html>
"""

    def write(self, path: Path, *, top: int = 25) -> ReportSummary:
        overview = self._overview()
        content = self.render(top=top, overview=overview)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_text(path, content)
        return ReportSummary(
            path=path,
            bytes=len(content.encode("utf-8")),
            datasets=int(overview["datasets"]),  # type: ignore[arg-type]
        )
