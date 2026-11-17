# hf-data-crawl — Agent 导航

HF 公开数据集的元信息底库：采集 → Raw → Bronze → Silver / 观测 → 当前态 → Gold 视图，
另有「需求 → 交付档案」一条交付链。不下载数据集文件。

## 项目形状（改代码前必读）

`src/hf_data_crawl/` 是一个包、六个垂直切片，依赖只能单向下行：

```
platform < pipeline < {acquire, query} < delivery < ops
```

- 切片按「数据流经谁」划分，不按文件类型：`platform` 锁 / 原子写 / 存储 / 词表 / 运行台账；
  `pipeline` 派生；`acquire` 扫描与探测；`query` Gold 视图；`delivery` 需求与档案；
  `ops` 体检 / 归档 / 清理 / 文档生成 / CLI。
- `acquire/hf` 是 `acquire` 的子包，不是切片；`acquire` 与 `query` 同档、互不依赖。
- `ops/cli.py` 是唯一组合根：只调各切片公开的 `run_*`，不碰下划线开头的实现。
- 派生顺序固定 bronze → silver → observations → current，由 `pipeline/derive.py` 定义。
- `tests/test_architecture.py` 强制以上规则，含函数内导入与 `TYPE_CHECKING` 块。

## 按任务找文档

| 你要动 | 先读 | 它回答 |
|---|---|---|
| 采集 / 对账 / 探测 | GLOSSARY #5–#6、#10、#15、#17、#20、#27、#29–#30、#68、#72–#73 | 排序与异常值、扫描时序、来源命名、状态判定、探测限流 |
| 派生 / 重建 / 恢复 / 留存 | GLOSSARY #18–#19、#28、#31、#44–#45、#47、#78、#93；`docs/operations/RECOVERY.md` | 标记文件、后缀重放、派生凭据、冷热分层与回滚、换压缩格式 |
| 体检 / 告警 | GLOSSARY #74、#76、#81–#84；`docs/operations/MONITORING.md` | 判据基准（快照日而非日历）、Finding 的身份、连续天数 |
| 调度 / 日志 | `scripts/daily.ps1`（日常链的顺序）、`scripts/setup-schedule.ps1`（独立任务与补跑策略）、GLOSSARY #67、#77、#92 | 为什么是一条链、哪些任务不补跑、重试边界、批量补采卡住怎么看得出来 |
| 交付 / 需求 | `.agents/skills/hf-request/SKILL.md`、GLOSSARY #51–#58、#69–#71、#79、#85–#87 | 受理 → 口径 → 方法 → 预演 → 交付 → 变更 |
| 字段 / 数据形状 | `docs/DATA.md`（生成物）、GLOSSARY #62–#66、`delivery/fields.py` | 表结构、字段含义、默认交付契约 |
| 架构决策 / 术语 | `docs/adr/`、GLOSSARY #75、`docs/agents/domain.md` | 当时的取舍与事故；术语怎么用 |
| 提 issue / 写规格 | `docs/agents/issue-tracker.md` | `.scratch/<feature>/` 的目录与 Status 约定 |

术语一律以 [GLOSSARY.md](GLOSSARY.md) 为准，新词先入表再用。引用编号是稳定键
（`GLOSSARY #74`），只增不改；`python scripts/check_docs_links.py` 会校验它指向的真有条目。

## 硬规则

- **生成物不手改**：`docs/DATA.md` 由 `hf-data-crawl docs` 生成；字段含义改
  `ops/fielddocs.py` 后重新生成。
- **一份事实只写一处**：`docs/README.md` 的「谁拥有什么」表说明每类事实该落在哪份文件；
  改文档前先查表，别在 README 里复述词表。
- **提交前**：`uv run pytest -q` 与 `python scripts/check_docs_links.py`。
- **测试名与 docstring 用中文**，沿用现状；机械规则见 `CODING_STANDARDS.md`。
  文档里写「一定 / 必然」时，先问有没有一条能打红的测试。

## 历史档案

`docs/PRD-hf-data-crawl-v1.0.md` 与 `docs/ISSUES.md` 是 v1.0 的追溯记录，**不改写**；
当前待办只看 `docs/TODO.md`。
