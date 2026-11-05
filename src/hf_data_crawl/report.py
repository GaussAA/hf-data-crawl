"""静态榜单页：把 Gold 查询渲染成单文件 HTML。

设计约定（对应 2026-11-04 复盘出的反模式清单）：
- 单一强调色；不做渐变文字，不用装饰符号。
- 玻璃质感只在有纹理的底上使用，避免空转的 backdrop-filter。
- 一张表承载两个排序口径，而不是并排两张重复表。
- 不设嵌套滚动容器；表格随页面滚动。
- 每个输入框有可访问名称，每个表头有 scope，空态按列数铺满。
- 原生控件（滚动条、焦点环、清除钮）随主题造境。
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from .atomic import write_text
from .gold import GoldCatalog

_CSS = """
:root{
  color-scheme:dark;
  --bg:#0b0c10;
  --surface:rgba(255,255,255,.035);
  --surface-2:rgba(255,255,255,.06);
  --line:rgba(255,255,255,.08);
  --line-2:rgba(255,255,255,.14);
  --fg:#e8e8ef; --fg-dim:#9b9bab; --fg-mute:#71717f;
  --accent:#a78bfa; --up:#9bd6a0;
  --fs-xs:12px; --fs-sm:13px; --fs-md:15px; --fs-lg:26px;
}
*{box-sizing:border-box}
*{scrollbar-width:thin;scrollbar-color:rgba(167,139,250,.4) transparent}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(167,139,250,.34);border-radius:999px;border:2px solid transparent;background-clip:content-box}
::-webkit-scrollbar-thumb:hover{background:rgba(167,139,250,.6);background-clip:content-box}
::-webkit-scrollbar-corner{background:transparent}
::selection{background:rgba(167,139,250,.3);color:#fff}
body{
  margin:0;min-height:100vh;color:var(--fg);
  font:var(--fs-md)/1.6 system-ui,-apple-system,"Segoe UI","Noto Sans SC",sans-serif;
  caret-color:var(--accent);
  background-color:var(--bg);
  background-image:
    radial-gradient(760px 420px at 18% -6%, rgba(167,139,250,.10), transparent 62%),
    repeating-linear-gradient(0deg, rgba(255,255,255,.016) 0 1px, transparent 1px 32px),
    repeating-linear-gradient(90deg, rgba(255,255,255,.016) 0 1px, transparent 1px 32px);
  background-attachment:fixed;
}
input,select,button,textarea{font:inherit;color:inherit}
input:focus-visible,select:focus-visible,button:focus-visible{
  outline:2px solid rgba(167,139,250,.6);outline-offset:2px}
input[type=search]::-webkit-search-cancel-button{
  -webkit-appearance:none;appearance:none;height:12px;width:12px;margin-right:4px;
  color:var(--fg-mute);cursor:pointer;background:currentColor;
  -webkit-mask:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath d='M1 1l10 10M11 1L1 11' stroke='%23000' stroke-width='1.6' stroke-linecap='round'/%3E%3C/svg%3E") center/contain no-repeat;
}
input[type=search]::-webkit-search-cancel-button:hover{color:var(--accent)}
.sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}

.wrap{max-width:1120px;margin:0 auto;padding:52px 26px 72px}
header{margin-bottom:30px}
h1{margin:0 0 8px;font-size:var(--fs-lg);font-weight:620;letter-spacing:-.2px}
.lede{color:var(--fg-dim);font-size:var(--fs-sm);max-width:70ch}
.lede b{color:var(--fg);font-weight:560;font-variant-numeric:tabular-nums}

.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin:26px 0 40px}
.stat{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:16px 18px;
  backdrop-filter:blur(10px) saturate(115%);-webkit-backdrop-filter:blur(10px) saturate(115%)}
.stat dt{color:var(--fg-mute);font-size:var(--fs-xs)}
.stat dd{margin:6px 0 0;font-size:24px;font-weight:600;font-variant-numeric:tabular-nums;letter-spacing:-.3px}
.stat p{margin:4px 0 0;color:var(--fg-mute);font-size:var(--fs-xs)}

section{margin:0 0 44px}
.sec-head{display:flex;align-items:center;justify-content:space-between;gap:18px;flex-wrap:wrap;margin-bottom:12px}
h2{margin:0;font-size:var(--fs-md);font-weight:600}
.sec-note{margin:0;color:var(--fg-mute);font-size:var(--fs-xs)}
.tools{display:flex;align-items:center;gap:10px}
label.field{display:flex;align-items:center;gap:8px;color:var(--fg-dim);font-size:var(--fs-xs)}
input[type=search]{background:rgba(255,255,255,.045);border:1px solid var(--line);color:var(--fg);
  border-radius:9px;padding:6px 10px;font-size:var(--fs-sm);width:190px}
input[type=search]::placeholder{color:var(--fg-mute)}
.counter{color:var(--fg-mute);font-size:var(--fs-xs);font-variant-numeric:tabular-nums;min-width:64px;text-align:right}

.seg{display:inline-flex;border:1px solid var(--line);border-radius:9px;overflow:hidden}
.seg button{background:transparent;border:0;padding:6px 12px;font-size:var(--fs-xs);color:var(--fg-dim);cursor:pointer}
.seg button+button{border-left:1px solid var(--line)}
.seg button[aria-pressed=true]{background:var(--surface-2);color:var(--fg)}

table{width:100%;border-collapse:collapse;font-size:var(--fs-sm)}
caption{text-align:left;color:var(--fg-mute);font-size:var(--fs-xs);padding:0 0 8px}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line)}
th{color:var(--fg-mute);font-weight:520;font-size:var(--fs-xs)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tbody tr:hover{background:var(--surface)}
tr[hidden]{display:none}
.id{font-family:ui-monospace,"Cascadia Mono",Consolas,monospace;font-size:12px;color:#cfcfe2}
.name{color:var(--fg)}
.dim{color:var(--fg-mute)}
.empty{background:var(--surface);border:1px solid var(--line);border-radius:12px;
  padding:18px;color:var(--fg-dim);font-size:var(--fs-sm)}
.empty strong{color:var(--fg);font-weight:560}
footer{border-top:1px solid var(--line);padding-top:16px;color:var(--fg-mute);font-size:var(--fs-xs);line-height:1.8}
footer code{font-family:ui-monospace,Consolas,monospace;color:var(--fg-dim)}
@media (max-width:720px){
  .wrap{padding:36px 16px 56px}
  input[type=search]{width:100%}
  .tools{width:100%}
  label.field{flex:1}
  td,th{padding:8px 8px}
  .id{font-size:11px}
}
@media (prefers-reduced-motion:reduce){*{scroll-behavior:auto}}
"""

_JS = """
(function(){
  function cells(row){return row.children;}
  document.querySelectorAll('input[data-filter]').forEach(function(box){
    var table=document.getElementById(box.dataset.filter);
    if(!table) return;
    var count=document.getElementById(box.dataset.filter+'-count');
    var rows=Array.prototype.slice.call(table.tBodies[0].rows);
    var timer=null;
    function apply(){
      var q=box.value.trim().toLowerCase();
      var shown=0;
      rows.forEach(function(row){
        var hit=!q || row.textContent.toLowerCase().indexOf(q)>-1;
        row.hidden=!hit;
        if(hit) shown++;
      });
      if(count) count.textContent=shown+' / '+rows.length;
    }
    box.addEventListener('input',function(){clearTimeout(timer);timer=setTimeout(apply,120);});
    apply();
  });

  document.querySelectorAll('.seg button').forEach(function(btn){
    btn.addEventListener('click',function(){
      var table=document.getElementById(btn.dataset.target);
      if(!table) return;
      var key=btn.dataset.key;
      var body=table.tBodies[0];
      Array.prototype.slice.call(body.rows)
        .sort(function(a,b){return (parseFloat(b.dataset['k'+key])||0)-(parseFloat(a.dataset['k'+key])||0);})
        .forEach(function(row){body.appendChild(row);});
      btn.parentNode.querySelectorAll('button').forEach(function(other){
        other.setAttribute('aria-pressed', other===btn ? 'true':'false');
      });
    });
  });
})();
"""


@dataclass(frozen=True)
class ReportSummary:
    path: Path
    bytes: int
    datasets: int


def _esc(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _num(value: object) -> str:
    if value is None:
        return "—"
    if isinstance(value, (int, float)):
        return f"{value:,.0f}"
    return _esc(value)


def _compact(value: object) -> str:
    """大数字压成 T/B/M，附完整值供悬停查看。

    行数动辄万亿，写成 4,131,984,348,623 既占宽又难比大小；而只给 4.1T
    又会丢掉精度——两者都给，谁需要哪个自己取。
    """
    if not isinstance(value, (int, float)):
        return "—" if value is None else _esc(value)
    exact = f"{value:,.0f}"
    for unit, scale in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(value) >= scale:
            return f'<span title="{exact}">{value / scale:.2f}{unit}</span>'
    return exact


def _table(
    table_id: str,
    frame: pl.DataFrame,
    columns: list[tuple[str, str, str]],
    *,
    caption: str,
    sort_keys: list[str] | None = None,
) -> str:
    """columns: (字段名, 表头, 类型)，类型取 text / num / code / name。"""
    head = "".join(
        f'<th scope="col"{' class="num"' if kind == "num" else ""}>{_esc(title)}</th>'
        for _, title, kind in columns
    )
    rows: list[str] = []
    for record in frame.iter_rows(named=True):
        attrs = ""
        if sort_keys:
            attrs = " ".join(
                f'data-k{index}="{record.get(column) if isinstance(record.get(column), (int, float)) else 0}"'
                for index, column in enumerate(sort_keys)
            )
        cells: list[str] = []
        for name, _, kind in columns:
            value = record.get(name)
            if kind == "num":
                cells.append(f'<td class="num">{_num(value)}</td>')
            elif kind == "compact":
                cells.append(f'<td class="num">{_compact(value)}</td>')
            elif kind == "code":
                cells.append(f'<td class="id">{_esc(value)}</td>')
            elif kind == "name":
                text = _esc(value)
                cells.append(
                    f'<td class="name">{text if text else "<span class=dim>—</span>"}</td>'
                )
            else:
                cells.append(f"<td>{_esc(value)}</td>")
        rows.append(f"<tr {attrs}>".replace(" >", ">") + "".join(cells) + "</tr>")

    body = "".join(rows) or (f'<tr><td colspan="{len(columns)}" class="dim">无匹配记录</td></tr>')
    return (
        f'<table id="{table_id}"><caption>{_esc(caption)}</caption>'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
    )


class Reporter:
    def __init__(self, catalog: GoldCatalog) -> None:
        self._catalog = catalog

    def _overview(self) -> dict[str, object]:
        return self._catalog.run(
            """
            SELECT (SELECT count(*) FROM current) AS datasets,
                   (SELECT count(*) FROM current WHERE status <> 'active') AS gone,
                   (SELECT count(*) FROM card) AS cards,
                   (SELECT count(*) FROM dataset_size WHERE status = 'ok') AS sized,
                   (SELECT count(*) FROM v_growth) AS growth_ready,
                   (SELECT count(*) FROM observations
                     WHERE snapshot_date = (SELECT max(snapshot_date) FROM observations)) AS moved,
                   (SELECT max(snapshot_date) FROM observations) AS as_of
            """
        ).row(0, named=True)

    def render(self, *, top: int = 25, overview: dict[str, object] | None = None) -> str:
        catalog = self._catalog
        overview = overview or self._overview()

        ranking = catalog.run(
            """
            SELECT d.id, d.pretty_name, d.downloads, d.downloads_all_time, d.likes, d.license_name
            FROM datasets AS d
            ORDER BY d.downloads DESC NULLS LAST
            LIMIT ?
            """,
            [top],
        )
        categories = catalog.run(
            """
            SELECT unnest(task_categories) AS category, count(*) AS datasets, sum(downloads) AS downloads
            FROM datasets GROUP BY 1 ORDER BY downloads DESC NULLS LAST LIMIT ?
            """,
            [top],
        )
        licenses = catalog.run(
            """
            SELECT license_name, count(*) AS datasets
            FROM card WHERE license_name IS NOT NULL
            GROUP BY 1 ORDER BY datasets DESC LIMIT ?
            """,
            [top],
        )
        growth = catalog.run(
            """
            SELECT id, docs_start, docs_now, growth, span_days FROM (
              SELECT id, downloads_start AS docs_start, downloads_now AS docs_now, growth, span_days
              FROM v_growth WHERE growth > 0
            ) ORDER BY growth DESC LIMIT ?
            """,
            [top],
        )
        volume = catalog.run(
            """
            SELECT d.id, ds.num_rows, ds.num_columns_max,
                   round(ds.num_bytes_memory / 1024.0 / 1024 / 1024) AS mem_gb
            FROM dataset_size AS ds
            JOIN datasets AS d ON d.id = ds.id
            WHERE ds.status = 'ok' AND ds.num_rows IS NOT NULL
            ORDER BY ds.num_rows DESC NULLS LAST
            LIMIT ?
            """,
            [top],
        )
        modalities = catalog.run(
            """
            SELECT unnest(modalities) AS modality, count(*) AS datasets,
                   sum(downloads) AS downloads
            FROM datasets GROUP BY 1 ORDER BY datasets DESC NULLS LAST LIMIT ?
            """,
            [top],
        )

        if growth.height:
            growth_html = _table(
                "growth",
                growth,
                [
                    ("id", "数据集", "code"),
                    ("docs_start", "起点", "num"),
                    ("docs_now", "当前", "num"),
                    ("growth", "增量", "num"),
                    ("span_days", "跨度(天)", "num"),
                ],
                caption="按最近两个观测点的增量排序；跨度以天计，过短则读数可能尚未反映真实趋势。",
            )
        else:
            growth_html = (
                '<p class="empty">暂无正增长可比：全库 <strong>'
                f"{int(overview['growth_ready']):,}"
                "</strong> 个数据集具备两次跨日读数。HF 的下载量为批量更新，"
                "同一天重扫不会产生新读数，需再等一个自然日。</p>"
            )

        as_of = overview["as_of"]
        stats = [
            ("数据集", f"{int(overview['datasets']):,}", f"其中已失效 {int(overview['gone']):,}"),
            ("今日变动", f"{int(overview['moved']):,}", "指标发生变化的集数"),
            ("卡片覆盖", f"{int(overview['cards']):,}", "有正式名或标签声明"),
            ("规模覆盖", f"{int(overview['sized']):,}", "已取到行数与列数"),
            ("可测增速", f"{int(overview['growth_ready']):,}", "具备两次跨日读数"),
        ]
        stat_html = "".join(
            f'<div class="stat"><dt>{_esc(term)}</dt><dd>{_esc(value)}</dd>'
            f"<p>{_esc(hint)}</p></div>"
            for term, value, hint in stats
        )

        return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HF 数据集底座 · 榜单</title><style>{_CSS}</style></head>
<body><main class="wrap">
<header>
  <h1>Hugging Face 数据集底座</h1>
  <p class="lede">数据截至 <b>{_esc(as_of)}</b>，共 <b>{
            int(overview["datasets"]):,}</b> 个公开数据集。
  指标取自 HF 列表接口，下载量为 30 天口径，累计另列；增速按最近两个观测点计算。</p>
</header>

<dl class="stats">{stat_html}</dl>

<section>
  <div class="sec-head">
    <div>
      <h2>下载榜</h2>
      <p class="sec-note">同一张表可切换排序口径，避免并排两张重复榜单。</p>
    </div>
    <div class="tools">
      <div class="seg" role="group" aria-label="排序口径">
        <button type="button" data-target="ranking" data-key="0" aria-pressed="true">30 天</button>
        <button type="button" data-target="ranking" data-key="1" aria-pressed="false">累计</button>
      </div>
      <label class="field">筛选
        <input type="search" placeholder="id / 名称 / 许可证" data-filter="ranking">
      </label>
      <span class="counter" id="ranking-count" aria-live="polite"></span>
    </div>
  </div>
  {
            _table(
                "ranking",
                ranking,
                [
                    ("id", "数据集", "code"),
                    ("pretty_name", "正式名", "name"),
                    ("downloads", "30 天", "num"),
                    ("downloads_all_time", "累计", "num"),
                    ("likes", "点赞", "num"),
                    ("license_name", "许可证", "text"),
                ],
                caption=f"按 30 天下载排序的前 {top} 名。",
                sort_keys=["downloads", "downloads_all_time"],
            )
        }
</section>

<section>
  <div class="sec-head"><div><h2>增速榜</h2>
    <p class="sec-note">最近两个观测点的下载增量，只列正增长。</p></div></div>
  {growth_html}
</section>

<section>
  <div class="sec-head"><div><h2>规模榜</h2>
    <p class="sec-note">行数与列数取自 datasets-server，覆盖按下载量排序的头部两万名。</p></div></div>
  {
            _table(
                "volume",
                volume,
                [
                    ("id", "数据集", "code"),
                    ("num_rows", "行数", "compact"),
                    ("num_columns_max", "列数", "num"),
                    ("mem_gb", "内存(GB)", "num"),
                ],
                caption="行数与列数由 HF 转换后的 parquet 统计，与仓库原始体积不是一回事。",
            )
        }
</section>

<section>
  <div class="sec-head"><div><h2>模态分布</h2>
    <p class="sec-note">按 modality 标签展开；只列已标注该维度的数据集。</p></div></div>
  {
            _table(
                "modalities",
                modalities,
                [
                    ("modality", "模态", "text"),
                    ("datasets", "集数", "num"),
                    ("downloads", "下载", "num"),
                ],
                caption="旧式数据集用无前缀标签（text/image/audio）标注，归入 plain_tags，不计入本表。",
            )
        }
</section>

<section>
  <div class="sec-head"><div><h2>任务类别</h2>
    <p class="sec-note">按类别汇总集数与下载量，用于判断哪些方向正在被集中使用。</p></div></div>
  {
            _table(
                "categories",
                categories,
                [
                    ("category", "类别", "text"),
                    ("datasets", "集数", "num"),
                    ("downloads", "下载", "num"),
                ],
                caption="取标签 task_categories 展开后聚合。",
            )
        }
</section>

<section>
  <div class="sec-head"><div><h2>许可证分布</h2>
    <p class="sec-note">仅统计已补齐卡片的数据集，未补齐者不计入分母。</p></div></div>
  {
            _table(
                "licenses",
                licenses,
                [("license_name", "许可证", "text"), ("datasets", "集数", "num")],
                caption="卡片 license 字段归一后的分布。",
            )
        }
</section>

<footer>
  由 <code>hf-data-crawl report</code> 生成 · 视图 <code>current</code> / <code>datasets</code> /
  <code>card</code> / <code>card_config</code> / <code>dataset_size</code> /
  <code>tag_values</code> / <code>v_growth</code> · 口径改动只需改 SQL，不重跑采集。
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
