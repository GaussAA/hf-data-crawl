"""静态榜单页：把 Gold 查询渲染成单文件 HTML。

不引入模板引擎与前端构建：一个文件、无外部依赖、断网可看。
所有从数据里来的文本一律经 html.escape，避免 id 或卡片里的尖括号破坏结构。
"""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from .gold import GoldCatalog

_CSS = """
:root{
  color-scheme:dark;
  --bg:#0a0a12; --panel:rgba(255,255,255,.045); --line:rgba(255,255,255,.09);
  --fg:#e9e9f2; --dim:#9a9ab0; --violet:#a78bfa; --cyan:#67e8f9; --lime:#bef264;
}
*{box-sizing:border-box}
/* 原生控件造境：不写这一层，滚动条与输入框内建按钮会露出系统亮色，割裂整个页面 */
*{scrollbar-width:thin;scrollbar-color:rgba(167,139,250,.45) transparent}
::-webkit-scrollbar{width:10px;height:10px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:rgba(167,139,250,.38);border-radius:999px;border:2px solid transparent;background-clip:content-box}
::-webkit-scrollbar-thumb:hover{background:rgba(167,139,250,.65);background-clip:content-box}
::-webkit-scrollbar-corner{background:transparent}
::selection{background:rgba(167,139,250,.32);color:#fff}
body{caret-color:var(--violet)}
input,select,button,textarea{font:inherit;color:inherit}
input:focus-visible,select:focus-visible,button:focus-visible{outline:2px solid rgba(167,139,250,.55);outline-offset:2px}
input[type=search]::-webkit-search-cancel-button{
  -webkit-appearance:none;appearance:none;height:12px;width:12px;margin-right:4px;
  color:var(--dim);cursor:pointer;background:currentColor;
  -webkit-mask:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 12 12'%3E%3Cpath d='M1 1l10 10M11 1L1 11' stroke='%23000' stroke-width='1.6' stroke-linecap='round'/%3E%3C/svg%3E") center/contain no-repeat;
}
input[type=search]::-webkit-search-cancel-button:hover{color:var(--violet)}
body{margin:0;background:
   radial-gradient(900px 600px at 12% -8%, rgba(167,139,250,.20), transparent 60%),
   radial-gradient(760px 520px at 88% 0%, rgba(103,232,249,.14), transparent 58%),
   var(--bg);
  color:var(--fg);font:15px/1.6 "Inter","Segoe UI","Noto Sans SC",system-ui,sans-serif;
  min-height:100vh}
.wrap{max-width:1180px;margin:0 auto;padding:48px 28px 80px}
header{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;flex-wrap:wrap}
h1{margin:0;font-size:26px;letter-spacing:.4px;font-weight:650}
h1 span{background:linear-gradient(92deg,var(--violet),var(--cyan));-webkit-background-clip:text;background-clip:text;color:transparent}
.meta{color:var(--dim);font-size:13px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(168px,1fr));gap:14px;margin:28px 0 34px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:18px 18px 16px;
  backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px)}
.card .k{color:var(--dim);font-size:12px;letter-spacing:.6px;text-transform:uppercase}
.card .v{font-size:27px;font-weight:640;margin-top:6px;font-variant-numeric:tabular-nums}
.card .s{color:var(--dim);font-size:12px;margin-top:2px}
section{margin:34px 0}
.sec-head{display:flex;align-items:baseline;justify-content:space-between;gap:16px;margin-bottom:12px}
h2{margin:0;font-size:16px;font-weight:600;letter-spacing:.3px}
h2::before{content:"◆";color:var(--violet);margin-right:8px;font-size:11px;vertical-align:2px}
input[type=search]{background:rgba(255,255,255,.04);border:1px solid var(--line);color:var(--fg);
  border-radius:10px;padding:7px 12px;font-size:13px;width:220px;outline:none}
input[type=search]:focus{border-color:rgba(167,139,250,.55)}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:13.5px}
th,td{padding:9px 12px;text-align:left;border-bottom:1px solid rgba(255,255,255,.055)}
th{color:var(--dim);font-weight:520;font-size:12px;letter-spacing:.5px;text-transform:uppercase;
  position:sticky;top:0;background:rgba(10,10,18,.86);backdrop-filter:blur(8px)}
td.num{text-align:right;font-variant-numeric:tabular-nums}
tbody tr:hover{background:rgba(167,139,250,.06)}
.id{font-family:"JetBrains Mono",ui-monospace,Consolas,monospace;font-size:12.5px;color:#d6d6ea}
.pill{display:inline-block;padding:1px 8px;border-radius:999px;font-size:11.5px;border:1px solid var(--line);color:var(--dim)}
.pill.up{color:var(--lime);border-color:rgba(190,242,100,.35)}
.note{color:var(--dim);font-size:13px;background:var(--panel);border:1px solid var(--line);
  border-radius:12px;padding:14px 16px}
.table-wrap{max-height:520px;overflow:auto;overscroll-behavior:contain;border:1px solid var(--line);border-radius:14px;
  background:rgba(255,255,255,.018)}
footer{margin-top:48px;color:var(--dim);font-size:12px;text-align:center}
"""

_JS = """
document.querySelectorAll('input[data-filter]').forEach(function(box){
  box.addEventListener('input', function(){
    var q = box.value.trim().toLowerCase();
    var table = document.getElementById(box.dataset.filter);
    if(!table) return;
    table.querySelectorAll('tbody tr').forEach(function(row){
      row.style.display = row.innerText.toLowerCase().indexOf(q) > -1 ? '' : 'none';
    });
  });
});
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


def _table(table_id: str, frame: pl.DataFrame, columns: list[tuple[str, str, str]]) -> str:
    """columns: (字段名, 表头, 类型) 类型取 text/num/code。"""
    head = "".join(f"<th>{_esc(title)}</th>" for _, title, _ in columns)
    rows: list[str] = []
    for record in frame.iter_rows(named=True):
        cells: list[str] = []
        for name, _, kind in columns:
            value = record.get(name)
            if kind == "num":
                cells.append(f'<td class="num">{_num(value)}</td>')
            elif kind == "code":
                cells.append(f'<td class="id">{_esc(value)}</td>')
            else:
                cells.append(f"<td>{_esc(value)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    body = "".join(rows) or '<tr><td colspan="99" class="note">暂无数据</td></tr>'
    return (
        f'<div class="table-wrap"><table id="{table_id}"><thead><tr>{head}</tr></thead>'
        f"<tbody>{body}</tbody></table></div>"
    )


class Reporter:
    def __init__(self, catalog: GoldCatalog) -> None:
        self._catalog = catalog

    def _overview(self) -> dict[str, object]:
        return self._catalog.run(
            """
            SELECT (SELECT count(*) FROM current) AS datasets,
                   (SELECT count(*) FROM current WHERE status = 'disappeared') AS gone,
                   (SELECT count(*) FROM card) AS cards,
                   (SELECT count(*) FROM v_growth) AS growth_ready,
                   (SELECT count(*) FROM observations
                     WHERE snapshot_date = (SELECT max(snapshot_date) FROM observations)) AS today_obs
            """
        ).row(0, named=True)

    def render(self, *, top: int = 20, overview: dict[str, object] | None = None) -> str:
        catalog = self._catalog
        overview = overview or self._overview()

        downloads = catalog.run(
            """
            SELECT d.id, d.pretty_name, d.downloads, d.downloads_all_time, d.likes, d.license_name
            FROM datasets AS d
            ORDER BY d.downloads DESC NULLS LAST
            LIMIT ?
            """,
            [top],
        )
        alltime = catalog.run(
            """
            SELECT d.id, d.pretty_name, d.downloads_all_time, d.downloads, d.license_name
            FROM datasets AS d
            ORDER BY d.downloads_all_time DESC NULLS LAST
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
            SELECT id, downloads_start, downloads_now, growth, span_days
            FROM v_growth WHERE growth > 0 ORDER BY growth DESC LIMIT ?
            """,
            [top],
        )

        growth_block = (
            _table(
                "growth",
                growth,
                [
                    ("id", "数据集", "code"),
                    ("downloads_start", "起点", "num"),
                    ("downloads_now", "当前", "num"),
                    ("growth", "增量", "num"),
                    ("span_days", "跨度(天)", "num"),
                ],
            )
            if growth.height
            else (
                '<div class="note">增速榜暂为空：全库有 '
                f"{overview['growth_ready']:,} 个数据集具备两次跨日读数，"
                "而 HF 的 downloads 为批量更新，需再等一个自然日才会出现正增长。</div>"
            )
        )

        generated = datetime.now().strftime("%Y-%m-%d %H:%M")
        cards = [
            ("数据集总数", f"{overview['datasets']:,}", f"已下架 {overview['gone']:,}"),
            ("今日读数", f"{overview['today_obs']:,}", "本日新增或变动"),
            ("卡片补齐", f"{overview['cards']:,}", "含正式名/许可证"),
            ("可测增速", f"{overview['growth_ready']:,}", "具备两次读数"),
        ]
        card_html = "".join(
            f'<div class="card"><div class="k">{_esc(k)}</div>'
            f'<div class="v">{_esc(v)}</div><div class="s">{_esc(s)}</div></div>'
            for k, v, s in cards
        )

        return f"""<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HF 数据集底座 · 榜单</title><style>{_CSS}</style></head>
<body><div class="wrap">
<header>
  <div>
    <h1>Hugging Face <span>数据集底座</span></h1>
    <div class="meta">快照 {generated} · 数据源本地 parquet · 口径见 v_growth / datasets 视图</div>
  </div>
  <div class="meta">生成于 {_esc(generated)}</div>
</header>

<div class="cards">{card_html}</div>

<section>
  <div class="sec-head"><h2>增速榜</h2></div>
  {growth_block}
</section>

<section>
  <div class="sec-head"><h2>30 天下载 Top {top}</h2>
    <input type="search" placeholder="筛选 id / 名称 / 许可证" data-filter="downloads"></div>
  {_table("downloads", downloads, [
      ("id", "数据集", "code"), ("pretty_name", "正式名", "text"),
      ("downloads", "30 天", "num"), ("downloads_all_time", "累计", "num"),
      ("likes", "点赞", "num"), ("license_name", "许可证", "text")])}
</section>

<section>
  <div class="sec-head"><h2>累计下载 Top {top}</h2>
    <input type="search" placeholder="筛选" data-filter="alltime"></div>
  {_table("alltime", alltime, [
      ("id", "数据集", "code"), ("pretty_name", "正式名", "text"),
      ("downloads_all_time", "累计", "num"), ("downloads", "30 天", "num"),
      ("license_name", "许可证", "text")])}
</section>

<section>
  <div class="sec-head"><h2>任务类别</h2></div>
  {_table("categories", categories, [
      ("category", "类别", "text"), ("datasets", "集数", "num"), ("downloads", "下载", "num")])}
</section>

<section>
  <div class="sec-head"><h2>许可证分布</h2></div>
  {_table("licenses", licenses, [
      ("license_name", "许可证", "text"), ("datasets", "集数", "num")])}
</section>

<footer>hf-data-crawl · 单文件离线可看</footer>
</div><script>{_JS}</script></body></html>
"""

    def write(self, path: Path, *, top: int = 20) -> ReportSummary:
        overview = self._overview()
        content = self.render(top=top, overview=overview)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return ReportSummary(
            path=path,
            bytes=len(content.encode("utf-8")),
            datasets=int(overview["datasets"]),  # type: ignore[arg-type]
        )
