"""从 README 正文抽侧栏字段：格式容错与「空壳不能算有」。"""

from __future__ import annotations

from hf_data_crawl.pipeline.readme_fields import extract_fields

WIKITEXT = """---
pretty_name: WikiText
---

# Dataset Card

- **Homepage:** [https://blog.einstein.ai/x/](https://blog.einstein.ai/x/)
- **Paper:** [Pointer Sentinel Mixture Models](https://arxiv.org/abs/1609.07843)
- **Point of Contact:** [Stephen Merity](mailto:smerity@salesforce.com)

## Citation

```bibtex
@misc{merity2016pointer,
  title={Pointer Sentinel Mixture Models},
  year={2016}
}
```

## License
CC BY-SA
"""


def test_抽出三类侧栏字段() -> None:
    """`Salesforce/wikitext` 的真实形状：这三个字段只在正文，不在 YAML。"""
    fields = extract_fields(WIKITEXT)

    assert fields["homepage"] == "https://blog.einstein.ai/x/"
    assert fields["paper_title"] == "Pointer Sentinel Mixture Models"
    assert fields["paper_url"] == "https://arxiv.org/abs/1609.07843"
    assert fields["point_of_contact"] == "Stephen Merity"


def test_抽出_bibtex() -> None:
    """正文是 citation 的主来源：底库 YAML 只有 3,844 条，正文有 55,382 条。"""
    bibtex = extract_fields(WIKITEXT)["citation_bibtex"]

    assert bibtex is not None
    assert bibtex.startswith("@misc{merity2016pointer")
    assert "Pointer Sentinel" in bibtex


def test_模板占位符不算有引用() -> None:
    """54.7% 的 Citation 段落是 HF 模板留下的 `[More Information Needed]`。

    把它当成「有引用」会让 `citation` 字段的覆盖率凭空翻一倍，而里面一个字
    都没有引用内容。
    """
    text = """## Citation

**BibTeX:**

```bibtex
[More Information Needed]
```
"""
    assert extract_fields(text)["citation_bibtex"] is None


def test_无围栏的_bibtex_也能抽() -> None:
    """作者直接把条目贴在段落里，没有 ``` 包裹。"""
    text = "## Citation\n\n@misc{foo,\n  title={Bar}\n}\n"
    bibtex = extract_fields(text)["citation_bibtex"]

    assert bibtex is not None
    assert bibtex.startswith("@misc{foo")


def test_裸链接归到_url_而不是标题() -> None:
    """`**Paper**: http://...` 没有 Markdown 链接语法，别把 URL 当标题。"""
    fields = extract_fields("**Paper**: http://arxiv.org/abs/0000.0000")

    assert fields["paper_title"] is None
    assert fields["paper_url"] == "http://arxiv.org/abs/0000.0000"


def test_容忍列表符号与全角冒号() -> None:
    """README 的写法很杂：`- **X:**`、`**X**:`、`X：` 都出现过。"""
    assert extract_fields("- **Homepage:** https://a.example")["homepage"] == "https://a.example"
    assert extract_fields("**Homepage**: https://b.example")["homepage"] == "https://b.example"
    assert extract_fields("Homepage：https://c.example")["homepage"] == "https://c.example"


def test_模板占位符不算有值() -> None:
    """HF 模板预置的 `[More Information Needed]` 不是内容。

    `Paper:` 行里它占绝大多数——不排除的话 `paper_title` 会有 88,872 条「有值」，
    而每一格都是同一句话。这类行还常带残留的 `**`（值自己以加粗开头）。
    """
    fields = extract_fields(
        "- **Paper:** ** [More Information Needed]\r\n- **Homepage:** ** [More Information Needed]"
    )

    assert fields["paper_title"] is None
    assert fields["paper_url"] is None
    assert fields["homepage"] is None


def test_值自己带加粗也能剥干净() -> None:
    assert extract_fields("**Homepage:** ** https://a.example")["homepage"] == "https://a.example"


def test_抽不到一律_none() -> None:
    """上游本就稀疏，别用空串冒充「有」。"""
    for text in ("", "## Dataset\n\nSome text.\n", None):
        fields = extract_fields(text)
        assert fields["homepage"] is None
        assert fields["paper_title"] is None
        assert fields["paper_url"] is None
        assert fields["point_of_contact"] is None
        assert fields["citation_bibtex"] is None
