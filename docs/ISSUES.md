# Issues — hf-data-crawl v1.0

每条为一条**垂直切片**（tracer bullet）：贯穿 transport → 存储 → CLI → 测试，单独完成即可验证。按依赖顺序排列。

---

## S1 · HF 列表客户端：稳定游标、限流感知、断点续爬

- **Blocked by**: 无，可立即开工
- **覆盖故事**: 6、8、9、19、20
- **要建的东西**：一个能按 `sort` 稳定遍历列表的客户端。给定 transport，逐页产出 `Page(records, next_cursor, rate_remaining)`；`limit` 上限自动收敛到 1000；`expand[]` 按白名单构造；解析 `RateLimit` 头并在配额将尽时等待；单条记录缺 `id` 或 `lastModified` 时跳过并计数，不中断整轮；扫描过程每页落 checkpoint，可断点续跑。
- **验收**：
  - [ ] 不传 `sort` 时抛错（默认序不稳定，视为编程错误）
  - [ ] `limit=5000` 被收敛为 1000
  - [ ] 假 transport 给 3 页，末页无 `Link` 头时正常终止
  - [ ] 429 触发退避且最终成功；退避次数可见
  - [ ] 中途中断后以同一 checkpoint 续跑，产出的 `id` 序列与一次跑完完全一致
  - [ ] 缺 `id` 的记录被跳过并计数，其余正常产出

## S2 · Raw 落盘与幂等 Bronze

- **Blocked by**: S1
- **覆盖故事**: 7、12
- **要建的东西**：每页原始响应写 `raw/dt=YYYY-MM-DD/*.json.gz`；Bronze 一行一条 Record，带 `snapshot_date` 与原始 JSON 列。同一天重跑同一页不产生重复行。
- **验收**：
  - [x] 同页写两次，Raw 文件数不变、Bronze 行数不变
  - [x] Bronze 可由 Raw 单独重建，不联网
  - [x] 任一 Raw 文件可独立解压解析
- **状态**: 已完成（2026-11-03）。Raw 按内容摘要命名——同内容幂等、异内容另存，不做覆盖式写入。

## S3 · Silver 派生与状态表

- **Blocked by**: S2
- **覆盖故事**: 3、4、5、16
- **要建的东西**：把 Record 展开成结构化 Dataset——标签拆成维度（`task_categories`、`languages`、`size_categories`、`license`）；`downloads` 与 `downloadsAllTime` 分明；生成 `first_seen_at` / `last_seen_at`；维护 Current 表（每集一行最新态）与 Observations 表（指标变化才追加）。
- **验收**：
  - [x] 指标未变的集不追加观测行
  - [x] 指标变化的集追加一行且旧行保留
  - [x] 同一日重跑，Observations 不新增重复行
  - [x] Current 表行数等于去重后的 Dataset 数
- **状态**: 已完成（2026-11-03）。观测以 dt 分区存放，同日重跑先重建该日分区；current 由「本轮 Silver + 未出现行的原值」勾稽，下架判定留给 S5。

## S4 · `collect` 子命令（日增量）

- **Blocked by**: S3
- **覆盖故事**: 1、2、6、17、18
- **要建的东西**：串起 S1–S3 的日增量入口。按 `lastModified&direction=-1` 扫描，连续 N 页无新记录即停；lean 档默认；输出本轮摘要（页数、条数、耗时、配额余量、新增数）。
- **验收**：
  - [x] 首轮（空库）可跑通并产出可用数据
  - [x] 次轮在无变更时于 N 页内停止，且不写重复数据
  - [x] `--dry-run` 只探一页并打印字段，不落盘
  - [x] 摘要中的配额余量与响应头一致
- **状态**: 已完成（2026-11-03）。真实有界试跑 30 页 30000 条 / 149s（≈201 条/秒）；续跑 10 页后新增恰 10000 条、零重叠，current 40000 行唯一、观测无同 id 同日重复。

## S5 · `reconcile` 全量核对

- **Blocked by**: S4
- **覆盖故事**: 10、11
- **要建的东西**：按 `createdAt` 稳定序遍历全站，输出三类差异：新增、下架（Disappeared）、`sha` 变更。下架只标状态，不删观测史。
- **验收**：
  - [x] 已下架集被标为 Disappeared 且其 Observations 完好
  - [x] 重新出现的集状态复原
  - [x] 差异清单落盘为可读表格
  - [x] 中途中断可续跑
- **状态**: 已完成（2026-11-03）。真实有界对账 40 页 40000 条 / 86s：新增 10626、sha 变更 126、**下架 0**，50626 行全部 active——「未扫完不判下架」这条底线在真实数据上验过。

## S6 · `build` 从 Raw 重建派生层

- **Blocked by**: S3
- **覆盖故事**: 12、13
- **要建的东西**：只读 Raw，重算 Bronze/Silver/Current/Observations。派生规则变更后一条命令即可回填历史。
- **验收**：
  - [x] 断网状态下可完整重建
  - [x] 重建前后的 Current 表内容一致（除时间戳列）
- **状态**: 已完成（2026-11-03）。真实 4 万条：`build` 全程未联网，重建后 current 与 observations **逐行完全一致**（`equals` 为 True，连时间戳列都无需豁免）。

## S7 · Gold 视图与 `query`

- **Blocked by**: S3
- **覆盖故事**: 1、2、4、5、14、15
- **要建的东西**：DuckDB 视图（近 7 日增速榜、30 天与累计双榜、分类榜）+ `query` 子命令执行任意 SQL 或打印内置榜单。
- **验收**：
  - [x] 一条命令出「近 7 日 downloads 增速 Top 50」
  - [x] 按 `license=gated` 过滤可用
  - [x] 视图口径改动无需重跑采集
- **状态**: 已完成（2026-11-03）。视图直读 parquet，`downloads`/`alltime`/`categories`/`growth` 四榜与任意 SQL 均可用；真实 4 万条验证：任务类别榜首 robotics 3589 个，cc-by-4.0 + downloads>10 万过滤正常。增速榜需两日观测，单日时给出可读提示。

## S8 · 调度与运行报告

- **Blocked by**: S4、S5
- **覆盖故事**: 8、18
- **要建的东西**：本机定时（日增量、周对账）+ 每轮运行摘要落 `runs/YYYY-MM-DD.json`，含耗时、页数、字节、重试次数、终止原因。
- **验收**：
  - [x] 定时任务失败时留下可读原因，不静默
  - [x] 连续两轮摘要可对比，看得出退化
- **状态**: 已完成（2026-11-03）。`runs/` 每轮一条 JSON（成功与失败都写，失败带 `type: message`）；同一秒内连跑不会互相覆盖（撞名加序号）。调度示例见 README。
