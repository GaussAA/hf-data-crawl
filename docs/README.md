# 文档索引

本页是 `docs/` 的入口。仓库根目录的 [README](../README.md) 讲项目是什么、怎么跑；
[GLOSSARY](../GLOSSARY.md) 定义术语与口径；[AGENTS](../AGENTS.md) 与
[CODING_STANDARDS](../CODING_STANDARDS.md) 约束改动方式。

## 数据与口径

- [DATA.md](DATA.md) — Bronze / Silver / Gold 分层、表结构与字段口径
- [GLOSSARY.md](../GLOSSARY.md) — 状态四值、30 天下载量与累计下载量等术语的唯一定义

## 运维

- [RECOVERY.md](operations/RECOVERY.md) — 故障恢复与人工复位手册
- [MONITORING.md](operations/MONITORING.md) — 监控指标与巡检项

## 决策与待办

- [ADR-0001：元数据刷新一致性加固](adr/0001-metadata-refresh-consistency-hardening.md) — 水位线、派生原子性、null sha 漏报三项缺陷的决策记录
- [TODO.md](TODO.md) — 低优先级可选改进

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
