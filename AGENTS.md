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
| 采集 / 对账 / 探测 | DECISIONS #5–#6、#10、#15、#17、#20、#27、#29–#30、#68、#72–#73、#123 | 排序与异常值、扫描时序、来源命名、状态判定、探测限流、连接池的两处坑 |
| 派生 / 重建 / 恢复 / 留存 | DECISIONS #18–#19、#28、#31、#44–#45、#47、#78、#93、#124、#126；`docs/operations/RECOVERY.md` | 标记文件、后缀重放、派生凭据、冷热分层与回滚、换压缩格式、大表别整块进内存、合并时数留下的行 |
| 体检 / 告警 | DECISIONS #74、#76、#81–#84、#127；`docs/operations/MONITORING.md` | 判据基准（快照日而非日历）、Finding 的身份、连续天数、富化资产怎么判新旧 |
| 调度 / 日志 | `ops/refresh.py`（刷新鲜编排器，ADR-0008）、`scripts/refresh.ps1`（计划任务入口）、`scripts/setup-schedule.ps1`、DECISIONS #67、#77、#92、#118、#132–#135 | 为什么是一条链、串并怎么切、哪些步骤不补跑、重试边界、卡住了怎么看得出与怎么掐断 |
| 交付 / 需求 | `.agents/skills/hf-request/SKILL.md`、DECISIONS #51–#58、#69–#71、#79、#85–#87 | 受理 → 口径 → 方法 → 预演 → 交付 → 变更 |
| 字段 / 数据形状 | `data/DATA.md`（生成物）、DECISIONS #62–#66、#128、`delivery/fields.py` | 表结构、字段含义、默认交付契约、同名列的两份事实与空值语义 |
| 架构决策 / 术语 | `docs/adr/`、`DECISIONS.md`（编号条目）、`docs/agents/domain.md` | 当时的取舍与事故；术语怎么用 |
| 提 issue / 写规格 | `docs/agents/issue-tracker.md` | `.scratch/<feature>/` 的目录与 Status 约定 |
| 改提交钩子 / 质量门槛 | DECISIONS #125、#142；`.pre-commit-config.yaml`（五项与 CI 一一对应）；`.github/workflows/ci.yml` | 本地与 CI 为什么必须是同一把尺子、mypy 与 basedpyright 各自的盲区、严格档为什么只关 Unknown 家族 |

术语一律以 [GLOSSARY.md](GLOSSARY.md) 为准，新词先入表再用；带编号的设计决策与
接口事实在 [DECISIONS.md](DECISIONS.md)，编号是稳定键（`DECISIONS #74`），只增不改。
`python scripts/tools/check_docs_links.py` 会校验编号指向的真有条目。

## 硬规则

- **提交前先 `git fetch`**：`git push` 被拒是**事后**通知，那时提交已经写好、可能已经
  rebase 过。若 `origin/main` 有本地没有的提交，先 `git pull --rebase`。2026-11-20 实测：
  另一个会话推了 4 个提交重构同一文件，推送被拒 → rebase 冲突 → 只能丢掉重做。
- **多行 Python 写脚本，不用 `python -c`**：PowerShell 对 `"` 与反引号的解析叠在 Python
  字符串转义上，`python -c "..."` 几乎必错（2026-11-20 一次会话失败 8 次：反引号被吞、
  `\"` 丢失、`\d` 触发 SyntaxWarning）。多行逻辑或含引号的命令写成 `.scratch/<name>.py`
  再 `uv run python` 跑；`.scratch/` 已在 `.gitignore` 里。
- **生成物不手改**：`data/DATA.md` 由 `hf-data-crawl docs` 生成；字段含义改
  `ops/fielddocs.py` 后重新生成。
- **一份事实只写一处**：`docs/README.md` 的「谁拥有什么」表说明每类事实该落在哪份文件；
  改文档前先查表，别在 README 里复述词表。
- **提交前**：先 `uv run pre-commit install` 装一次钩子（新 clone 必做）；之后每次提交
  由它跑 ruff / mypy / basedpyright / pytest / 文档链接，五项与 CI 一一对应。手动兜底
  `uv run pre-commit run --all-files`，别只跑 `pytest`——2026-11-18 三次 CI 红全是
  「本地漏跑某一项」造成的（ruff format 两次、类型检查一次），钩子没装时没人拦得住。
- **mypy 与 basedpyright 是两把尺子，不是重复**：`pyarrow` 有内联注解却缺 `py.typed`，
  mypy 整包当 Any、basedpyright 读得到——涉及 pyarrow（百万行表的流式读写）的改动，
  只跑 mypy 等于没查。详见 DECISIONS #125；basedpyright 的 strict 档与例外见 #142。
- **覆盖率是观测，不是闸门**：CI 的 test 步骤跑 `--cov=hf_data_crawl`，但不设
  `--cov-fail-under`，pre-commit 也不含这一项。它答的是「哪块没人看过」，没有正确
  阈值，设死只会诱导为数字补空测试；口径与 `branch = true` 的理由见
  `CODING_STANDARDS.md`。
- **测试名与 docstring 用中文**，沿用现状；机械规则见 `CODING_STANDARDS.md`。
  文档里写「一定 / 必然」时，先问有没有一条能打红的测试。

## 历史档案

`docs/PRD-hf-data-crawl-v1.0.md` 与 `docs/ISSUES.md` 是 v1.0 的追溯记录，**不改写**；
当前待办只看 `docs/TODO.md`。
