# 文档索引与所有权

文档按「谁的事实」分工，**一份事实只写一处**。改文档前先查这张表；如果你要说的话
放不进任何一行，要么是放错了地方，要么该新增一份文档并在这里登记。

| 文档 | 它拥有什么 | 什么时候读 |
|---|---|---|
| [README](../README.md) | 项目是什么、怎么跑、命令与参数、数据布局、关键机制索引 | 第一次上手、加命令、换机器 |
| [GLOSSARY](../GLOSSARY.md) | 领域词表；编号决策与不变量（#1–#16 接口事实，#17 起设计与取舍） | 命名一个概念、改一处机制、想知道「为什么是这样」 |
| [DATA.md](DATA.md) | data 目录、表结构、字段含义（**生成物，不手改**） | 查字段、查表；改 `ops/fielddocs.py` 后重新生成 |
| [operations/RECOVERY.md](operations/RECOVERY.md) | 故障恢复、人工复位、归档 / 压缩回滚 | 数据或派生坏了 |
| [operations/MONITORING.md](operations/MONITORING.md) | 台账字段、监控指标与告警阈值、调度清单 | 排障、核对例行任务 |
| [operations/PERFORMANCE.md](operations/PERFORMANCE.md) | 体积与延迟基线、各表压缩参数、怎么重测 | 调压缩 / 分区、怀疑变慢、磁盘吃紧 |
| [adr/](adr/) | 决策**当时**的样子与被否掉的方案；不改写历史 | 要推翻或修订一项旧决策时 |
| [TODO.md](TODO.md) | 还没做的事 | 找活干 |
| [agents/](agents/) | issue 追踪、领域词表的使用约定 | 提 issue、写规格、做领域建模 |
| [.agents/skills/hf-request/SKILL.md](../.agents/skills/hf-request/SKILL.md) | 需求交付的规程（受理 → 方法 → 交付 → 变更） | 接需求、交付、改口径 |

## 写文档的三条规则

1. **决策进 GLOSSARY，操作进 README / operations，字段进 DATA.md（生成），决策当时的快照进 ADR。**
   README 不把「为什么」讲到最后一段——那一段属于词表。词表编号是稳定引用键，只增不改。
2. **不抄数**：列数、行数、检查项数量从代码或生成器取，文档只写判据与指向。
   数字会漂，判据不会；抄下来的数一旦过期，读者无法判断该信哪一份。
3. **改了文件名或新增文档**，更新本页并跑 `python scripts/check_docs_links.py`；
   它校验引用是否落地、编号是否完整，不校验内容是否过期——过期靠 Review 对照代码发现。

## v1.0 历史归档

以下两份是 2026-11-03 首版的需求与实现追溯记录，属**历史档案**，不是当前待办。
切片的验收状态、实测数据与事故复盘保留原文，不随后续重构改写；当前待办只看
[TODO.md](TODO.md)。

- [PRD v1.0](PRD-hf-data-crawl-v1.0.md) — Problem Statement、20 条 User Stories、
  实施与测试决策、Out of Scope、Changelog
- [ISSUES.md](ISSUES.md) — S1–S8 垂直切片与验收清单（「覆盖故事」编号对应 PRD 的
  User Stories）、四轮整改索引、两处真实事故复盘、已知遗留

二者构成「需求 → 切片」的完整追溯链，删去任一份都会让另一份的编号失去参照。

**读历史档案时注意口径漂移**：PRD 里「每周全量对账」等调度口径已于 2026-11-08
改为每日（底库行龄实测证明周节奏会让长尾长期拿旧值），以 [GLOSSARY](../GLOSSARY.md)
#43 为准；历史档案按上一段的约定不改写。

**交付档案同理**：`requests/REQ-0001-*` 的 `manifest.json` 由早期引擎生成
（`SELECT *`、72 列宽视图全列，且没有 `kind` / `spec_sha256` / `method` 段），
与现行默认字段契约（见 [GLOSSARY](../GLOSSARY.md) #62–#66）不同版。
它是历史留痕、不是可照抄的模板；新需求以项目级 skill `hf-request` 与
`request new` 生成的骨架为准。

## 校验

相对链接与行内代码里的仓库路径由 [check_docs_links.py](../scripts/check_docs_links.py)
逐条解析校验，CI 的 `Check documentation` 与 pre-commit 共用这一入口；同一入口还会校验
`GLOSSARY #nn` 引用指向真有条目、编号不重。本地可跑：

```bash
python scripts/check_docs_links.py
```
