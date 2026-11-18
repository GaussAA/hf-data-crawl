"""从 README 正文抽出卡片侧栏那几个字段。

它们**不在 YAML frontmatter 里**，只在正文的 Markdown 列表里：

    - **Homepage:** https://blog.einstein.ai/the-wikitext-...
    - **Paper:** [Pointer Sentinel Mixture Models](https://arxiv.org/abs/1609.07843)
    - **Point of Contact:** [Stephen Merity](mailto:smerity@salesforce.com)

HF 的页面侧栏会把它们渲染出来，但 `cardData` 不含——所以
`carddata.card_homepage` 对这类库是空的（`Salesforce/wikitext` 就是）。

命中率（2026-11-18 统计 95,471 条有正文的 README）：

    Homepage            11.6%
    Paper               14.3%
    Point of Contact     2.6%
    Total amount of disk 0.3%

只抽前三个。其余不到半个百分点，不值得为它维护一条正则——真要看，`text`
原文就在 `readme` 表里。
"""

from __future__ import annotations

import re
from typing import Any

#: 侧栏字段名 → 允许的写法。README 的格式很杂：`- **Homepage:** x`、
#: `**Homepage**: x`、`Homepage: x` 都见过，统一成「行首可选的列表符号 +
#: 可选的加粗 + 字段名 + 冒号（全角也收）+ 值」。
_FIELD_NAMES: dict[str, tuple[str, ...]] = {
    "homepage": ("Homepage",),
    "paper": ("Paper",),
    "point_of_contact": ("Point of Contact",),
}

#: `[标题](链接)`；标题可能为空（`[](url)`），也可能含嵌套方括号，这里取最内层。
_LINK = re.compile(r"\[([^\]]*)\]\(([^)]+)\)")

#: 整段就是一个裸链接（作者没写成 Markdown 链接）。这种要归到 url 而不是标题。
_BARE_URL = re.compile(r"^https?://\S+$")

#: `## Citation` 段落；到下一个标题（任意级别）为止。
_CITATION_SECTION = re.compile(r"(?m)^#{1,3}[ \t]+Citation[ \t]*$(.*?)(?=^#{1,3}[ \t]|\Z)", re.S)

#: 围栏代码块（```、```bibtex、```python 都收）。
_FENCE = re.compile(r"```[^\n]*\n(.*?)```", re.S)

#: BibTeX 条目的开头。
_BIBTEX_ENTRY = re.compile(r"@\w+\{")


def _compile() -> dict[str, re.Pattern[str]]:
    patterns: dict[str, re.Pattern[str]] = {}
    for key, names in _FIELD_NAMES.items():
        alternatives = "|".join(re.escape(name) for name in names)
        # 注意 `(?:\*\*)?` 出现三次：README 里两种写法都有——
        # `**Homepage:** v`（加粗把冒号也包进去）与 `**Homepage**: v`（冒号在外），
        # 只认前者会把值留成 `** v`（2026-11-18 实测踩到）。
        patterns[key] = re.compile(
            rf"(?im)^[ \t]*[-*+]?[ \t]*(?:\*\*)?(?:{alternatives})(?:\*\*)?[ \t]*"
            rf"[:：][ \t]*(?:\*\*)?[ \t]*(.+?)[ \t]*$"
        )
    return patterns


_PATTERNS = _compile()


def _clean(value: str) -> str:
    """去掉行尾的 Markdown 装饰。

    `**Homepage:** https://x` 会被正则吃到 `**` 之后，但作者也可能写成
    `Homepage: https://x **bold**`，故只剥「包裹整段的」星号与尖括号。
    """
    text = value.strip()
    text = text.strip("<>").strip()
    if text.startswith("**") and text.endswith("**") and len(text) > 4:
        text = text[2:-2].strip()
    return text


def _split_link(value: str) -> tuple[str | None, str | None]:
    """把 `[标题](链接)` 拆成 (标题, 链接)。

    - Markdown 链接 → 拆开
    - 裸链接（作者直接写 URL）→ (None, url)，别把 URL 当成标题
    - 其余 → 整段当标题
    """
    match = _LINK.search(value)
    if match:
        title = match.group(1).strip() or None
        url = match.group(2).strip() or None
        return (title, url)
    if _BARE_URL.match(value):
        return (None, value)
    return (value or None, None)


def _extract_citation(text: str) -> str | None:
    """从 `## Citation` 段落里抽 BibTeX。

    只要围栏块里含 `@xxx{` 就取那一段——README 的写法有 ```bibtex 也有裸 ```，
    统一按「第一个含 BibTeX 条目的围栏块」取。没有围栏块时，从第一个 `@xxx{`
    截到段落末尾（作者直接贴的情况）。

    占位符 `[More Information Needed]` 不含 `@xxx{`，自然被排除——这正是要的：
    54.7% 的 Citation 段落是 HF 模板留下的空壳，不能当成「有引用」。
    """
    section = _CITATION_SECTION.search(text)
    if not section:
        return None
    body = section.group(1)
    for fence in _FENCE.finditer(body):
        candidate = fence.group(1).strip()
        if _BIBTEX_ENTRY.search(candidate):
            return candidate
    loose = _BIBTEX_ENTRY.search(body)
    if loose:
        candidate = body[loose.start() :].strip()
        return candidate or None
    return None


def extract_fields(text: str | None) -> dict[str, Any]:
    """从一条 README 正文里抽侧栏字段。

    抽不到一律 None——这几个字段在上游本就稀疏，**不要用空串冒充「有」**，
    否则下游「非空即有效」的判据会把它们当成抓到了。
    """
    fields: dict[str, Any] = {
        "homepage": None,
        "paper_title": None,
        "paper_url": None,
        "point_of_contact": None,
        "citation_bibtex": None,
    }
    if not text:
        return fields

    homepage = _PATTERNS["homepage"].search(text)
    if homepage:
        value = _clean(homepage.group(1))
        title, url = _split_link(value)
        fields["homepage"] = url or title

    paper = _PATTERNS["paper"].search(text)
    if paper:
        value = _clean(paper.group(1))
        title, url = _split_link(value)
        fields["paper_title"] = title
        fields["paper_url"] = url

    contact = _PATTERNS["point_of_contact"].search(text)
    if contact:
        value = _clean(contact.group(1))
        title, _ = _split_link(value)
        fields["point_of_contact"] = title

    fields["citation_bibtex"] = _extract_citation(text)

    return fields
