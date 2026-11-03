# hf-data-crawl

Hugging Face 公开数据集元信息底座。周期性抓取元信息、留存原始响应、维护指标流水，用 DuckDB 直接查询。

不下载任何数据集文件，只抓元信息。

## 快速开始

```bash
uv sync

# 本机需经代理访问 HF 时（可选）
export HF_PROXY=http://127.0.0.1:7897
export HF_TOKEN=hf_xxx          # 可选；匿名额度为 5 分钟 500 次

uv run hf-data-crawl collect --dry-run          # 先探一页，确认连通与字段
uv run hf-data-crawl collect --max-pages 30     # 有界试跑 3 万条
uv run hf-data-crawl query --board downloads --limit 20
```

## 命令

| 命令 | 作用 | 联网 |
|---|---|---|
| `collect` | 日增量：按 `lastModified` 倒序扫到水位线 | 是 |
| `reconcile` | 全量对账：按 `createdAt` 稳定序扫全站，产出新增/下架/sha 变更 | 是 |
| `build` | 从 Raw 重建全部派生层 | 否 |
| `query` | 查内置榜单或执行任意只读 SQL | 否 |

常用参数：

```bash
hf-data-crawl collect   [--date YYYY-MM-DD] [--limit 1000] [--max-pages N] [--pages-without-new 3] [--dry-run]
hf-data-crawl reconcile [--date YYYY-MM-DD] [--limit 1000] [--max-pages N]
hf-data-crawl query     [--board growth|downloads|alltime|categories] [--limit 50] [--sql "..."]
```

全局参数 `--data-dir` 需放在子命令之前，默认取 `HF_DATA_DIR`，否则 `./data`。

内置榜单：

- `growth` 近 7 日 downloads 增量（需至少两天观测）
- `downloads` 30 天下载榜
- `alltime` 累计下载榜
- `categories` 任务类别聚合

## 数据布局

```
data/
├── raw/dt=YYYY-MM-DD/part-<页号>-<内容摘要>.json.gz   # 原始响应，永久保留
├── bronze/dt=YYYY-MM-DD/bronze.parquet                # 一行一条，含 raw 列
├── silver/dt=YYYY-MM-DD/silver.parquet                # 标签已拆维度
├── state/
│   ├── current.parquet                                # 每库一行最新态
│   └── observations/dt=YYYY-MM-DD/obs.parquet         # 指标变化流水
├── reports/reconcile-YYYY-MM-DD.csv                   # 对账差异清单
└── runs/YYYYMMDDTHHMMSS-<command>.json                # 每轮运行摘要
```

Raw 文件名带内容摘要：同页同内容重写为同一文件（幂等），同页内容变了则另存（绝不覆盖丢数据）。

## 设计要点

- **两档采样**：日增量走 lean 档（不含 `cardData`，约 1.6 KB/条）；`cardData` 只在需要时单独取。实测 full 档约 6 KB/条，全站日拉会白白多传数倍字节。
- **排序必须显式**：不传 `sort` 时 HF 的游标基于 `trendingScore`，会随时间漂移，翻页必漏。全量用 `createdAt`（不可变），增量用 `lastModified`。
- **水位线不用 `max(lastModified)`**：实测存在 2028 年的异常时间戳，任何单调性假设都会失效。改为「连续 N 页无新记录」。
- **下架判定只在扫完时做**：`reconcile` 用 `--max-pages` 有界跑时不会标记下架，否则未扫到的部分会被整片误判。
- **派生层可重建**：`build` 只读 Raw，断网即可回填。派生规则变更后一条命令重算历史。

## 定时

```powershell
pwsh -File scripts\setup-schedule.ps1              # 注册：每日 03:10 增量、每周日 04:00 对账
pwsh -File scripts\setup-schedule.ps1 -Uninstall   # 卸载
schtasks /Query /TN hf-collect /V /FO LIST         # 核对
```

计划任务调 `scripts/collect.cmd` 与 `scripts/reconcile.cmd` 两个包装脚本：代理默认取 `HF_PROXY`，日志追加到 `data\logs\`，`uv` 找不到时回退到 `%USERPROFILE%\.local\bin\uv.exe`。

每轮摘要落在 `runs/`，失败也写并带错误原因，不会静默。

## 开发

```bash
uv run pytest          # 63 项，全部离线
```

测试只在一条缝上打桩：`HfDatasetApi` 的 transport。除此之外全部走真实代码路径。
