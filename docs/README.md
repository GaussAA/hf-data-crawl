# 文档索引

本页是 `docs/` 的入口。仓库根目录的 [README](../README.md) 讲项目是什么、怎么跑；
[GLOSSARY](../GLOSSARY.md) 定义术语与口径；[AGENTS](../AGENTS.md) 与
[CODING_STANDARDS](../CODING_STANDARDS.md) 约束改动方式。

## 数据与口径

- [DATA.md](DATA.md) — Bronze / Silver / Gold 分层、表结构与字段口径；第八节讲单文件宽表导出的定位与边界
- [GLOSSARY.md](../GLOSSARY.md) — 状态四值、30 天下载量与累计下载量等术语的唯一定义

## 运维

- [RECOVERY.md](operations/RECOVERY.md) — 故障恢复与人工复位手册
- [MONITORING.md](operations/MONITORING.md) — 监控指标与巡检项

## 决策与待办

- [ADR-0001：元数据刷新一致性加固](adr/0001-metadata-refresh-consistency-hardening.md) — 水位线、派生原子性、null sha 漏报三项缺陷的决策记录
- [TODO.md](TODO.md) — 低优先级可选改进

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

## Agent 协作

- [domain.md](agents/domain.md) — 领域边界
- [issue-tracker.md](agents/issue-tracker.md) — 问题追踪约定
- [triage-labels.md](agents/triage-labels.md) — 分诊标签

## 校验

相对链接由 [check_docs_links.py](../scripts/check_docs_links.py) 逐条解析校验，
CI 的 `Check documentation` 与 pre-commit 共用这一入口。本地可跑：

```bash
python scripts/check_docs_links.py
```

新增文档或改动文件名后，请一并更新本页，避免索引与实物漂移。
