# Issue tracker：本地 Markdown

Issue 与规格是 `.scratch/<feature>/` 下的 Markdown。`.scratch/` 不入库（见 `.gitignore`），
它是工作现场，不是交付物。

## 约定

- 一个特性一个目录：`.scratch/<feature-slug>/`
- 规格：`.scratch/<feature-slug>/spec.md`
- 实现票据：一票一文件，`.scratch/<feature-slug>/issues/<NN>-<slug>.md`，从 `01` 编号，
  不合成为一张总表
- 状态：文件顶部一行 `Status:`，取值 `needs-triage` / `needs-info` / `ready-for-agent` /
  `ready-for-human` / `wontfix`
- 对话与结论追加在文件末尾的 `## Comments` 下

## 领取与完成

- 一个票据一次只由一个人处理；开头与结论都落在文件里，不靠口头交接。
- 完成后在文件里写结论与证据（命令、输出、commit），再改 `Status`。
- 一个票据只回答一个问题；范围变大就拆票，编号免费。

## 归档

v1.0 的切片清单与 PRD 是**历史档案**，不是活台账。两者编号互相引用，删一份会让另一份的
编号失去参照；它们按原样保留，不随后续重构改写。索引在 [docs 索引](../README.md)：

- [docs/ISSUES.md](../ISSUES.md) — S1–S8 切片、四轮整改、两次事故复盘
- [docs/PRD-hf-data-crawl-v1.0.md](../PRD-hf-data-crawl-v1.0.md) — 切片「覆盖故事」编号
  对应的 20 条 User Stories
