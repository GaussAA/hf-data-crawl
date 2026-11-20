# TODO

本页只记**还没做**的事。文末「已落地」一段是历史，留着是为了知道当时把原议收窄到了哪一步，
不是待办清单。

## 自动化改进

### 类型检查维护
类型检查已是单一入口，工具版本由 `uv.lock` 锁定，口径见
[CODING_STANDARDS.md](../CODING_STANDARDS.md) 的「工具链口径」：

```bash
uv run mypy          # src + tests，当前 0 错
uv run basedpyright  # src 严格档（为什么只关 Unknown 家族五条见 DECISIONS #142）
```

新增模块若出现类型错误，须改真实类型（显式注解、Protocol、让类型承载不变式），
不得用 `type: ignore` 或临时豁免绕过。

### 测试审计遗留（2026-11-20）
套件按「失效 / 不实 / 无用 / 冗余」清过一轮，剩两类没有动，需要时再收：

- **只有测试在调用的生产函数**：`refresh.Plan.by_name`、`health.HealthRecord.healthy`、
  `presets.list_presets`、`readme.RateLimiter.enabled`、`raw.RawDay.observed`、
  `raw.read_batches`、`batch.STAGING_ROOT`、`checkpoint.MemoryCheckpoint`、
  `sizes.staged_shards` / `sizes.staging_root`、`tables.TableSpec.filename`、
  `tables.BRONZE_SOURCE_FIELDS` 等。要么接上真实调用点，要么删函数并把断言移到
  真正被调用的入口——现在它们让人误以为有覆盖。
- **中等置信、证据不足未改的用例**：`test_commands.py` 的四条 `--full` 闸门
  （与 `test_rebuild.py` 重复）、`test_gold.py` 的重名列表、`test_doctor.py` 的
  字段新鲜度基准日、`test_report.py` 的补齐矩阵数据侧。要动先补「能打红」的
  反例再改。

### CI 性能（已定口径，别再照着旧建议改）
本地钩子的 `pytest` 用 `-n 8` 并行（140 秒 → 约 45 秒），**CI 保持串行全量**。
这不是漏做优化：本机 4P+8E，实测 `-n 4/6/8` 落在 34~54 秒且差异在噪声内（瓶颈是磁盘
IO），`-n auto` 反而回到 56 秒；而 GitHub runner 只有 2–4 核，并行收益小。更要紧的是
**总得留一处串行全量**——测试间的顺序依赖一旦出现，本地与 CI 会一起把它藏起来。

CI 里不要动的地方：

- **不要移除 Windows job**：它曾抓出 cp1252 控制台下打印中文导致的 `UnicodeEncodeError`
  （文档链接校验器「校验通过」被误判为失败），本机 UTF-8 终端复现不到，见
  `scripts/tools/check_docs_links.py`。
- **不要给 CI 的 pytest 加 `-n`**：理由同上，串行那一份是留给顺序依赖的探针。
- 若 CI 真的慢到碍事，先看哪一步占比最大（`gh run view --log`），再针对性处理；
  「并行运行测试」在 2 核 runner 上通常换不到时间。

---

## 文档维护

文档该写进哪份、哪类事实归谁，见 [docs/README.md](README.md) 的「谁拥有什么」表。
本页不再重复一份清单。

---

## 需求与交付（2026-11-18）

11-17 那轮审计的缺口与能力见 [ADR-0003](adr/0003-delivery-archive-hygiene.md)、
[ADR-0004](adr/0004-failure-semantics.md)、[ADR-0005](adr/0005-delivery-retention.md)。
11-18 把当时留下的三条都收了：

- **外发记账** → `request handoff` 写 `requests/<id>/HANDOFF.jsonl`，`show` 列最近三次
  （DECISIONS #119）。
- **代码内容指纹** → `manifest.method.env.code_sha256` 按 `CODE_PATHS` 的内容算，
  跨机器可比、脏着交付也说得清（DECISIONS #120）。单机假设里**只剩依赖与运行环境**
  没有钉：`uv.lock` 的内容进了指纹，Python 小版本与系统库不在其中。
- **小尾巴** → `request new --header-row` 让回填需求受理时就定表头行；四份既有需求的
  `METHOD.md` 补了「需求成立性」一节（补记日期与 manifest 指纹的先后差异写在各节里）。
- **样例需求可识别**（作者澄清后加）→ `request new --sample` / `request sample <id>`；
  `usage` 不计入影响面，`run-due` 跳过，`list` / `show` / `INDEX` 标注（DECISIONS #121）。

**仍然没做**：

- 多机一致性里「依赖与运行环境」那一层。等真出现第二台机器，再决定要不要把 Python 版本、
  平台与 `uv.lock` 一起编进一个 `env_sha256`——现在做，只是多一列没人核的账。
- 样例需求的保留策略。现在 REQ-0001..3 三份样例永久保留，作为最全的边界样本；
  数量多了要不要归档到 `requests-samples/`，等它真的碍事再定。

---

## 已落地（2026-11-15，历史）

下面三条原为低优先级待办，本轮按「一项一改一测」收口。**Q10 与 Q9 的原议做了收窄**，理由随条列出，免得日后照着旧命令行去实现一个会破坏既有契约的修复。

### Q10: 快照损坏诊断 → 只读诊断 + 处置指引

原议：`doctor --fix-snapshot` 自动修复损坏快照。

落地：`pipeline/rebuild.py::snapshot_health()` 只读诊断状态留存快照的三态（不存在 / 可解析 / 损坏），`doctor` 新增「状态快照」检查项，把 `retain` 对坏快照的静默回退变成可见告警，并给出分情况的处置指引。快照存在本身也报——它只表示「重建进行中」，写回成功后必须删除。

**未做自动修复**：`doctor` 的契约是只读（只写 `ALERT.txt` 与体检流水）；坏快照是重建被打断的唯一现场证据，自动删/盖等于销毁证据，也修不回已经清空的 state。若确需「隔离坏文件」，应另立命令、先备份留证，不混进体检。

### Q5: 观测覆盖前行数警告 → 覆盖前告警，不阻断

原议：`if after_count < before_count * 0.5: logger.warning(...)`。

落地：`OBSERVATION_SHRINK_SHARE = 0.5`，在覆盖当日观测分区前比较「该日已有行数」与「本轮新写入行数」，不足半数即打印告警（含日期与两侧行数）。ADR-0001 已认定「同日幂等，不丢失」是已接受的缝隙，故只补可观测性、不改写入语义；项目统一用 `print` 而非 logger。重建顺推（`carry`）时旧分区已清、`before=0`，不误报。

### Q9: 探测失败降级 → 记历史 + 单独报告，不驱逐

原议：记录探测失败历史，连续 3 轮失败 → 单独报告。

落地：`PROBE_DEGRADE_AFTER = 3`；轮转档案增 `failures`（逐库连续失败轮数，拿到结论即清零，只保留仍在 missing 的 id 以免膨胀）；达线的库写进差异清单 `kind=probe_degraded`、`ReconcileSummary.probe_degraded` 与 `runs` 台账，`doctor` 新增「探测顽固失败」检查项单独报告，命令行也点名数量。

**未做排序降级**：上一轮刚修好「失败项不消费轮转额度、下轮优先再探」，把连续失败的库挪到批次末尾，等于把最该复核的推远、退回「先别动变成一直没看」。DECISIONS #73 的降级对象是「已有明确结论的非在架库」，与「判不出结论」性质不同。这里的「降级」落在报告层面：从自动复核交给人去核对配额与上游状态。
