---
name: hf-request
description: 接收、响应并交付 hf-data-crawl 的数据需求。当用户说「要一份数据集清单」「按这张表补齐信息」「把这批库的情况导出来」「上次那份改一下」「又来了一份新表」时使用；也用于核对已有交付、解释某一格数据从哪来、以及口径变更后的影响面比对。负责判定需求形态、写口径与过程记录、预演、交付、变更与比对。
agent_created: true
---

# 需求交付规程

人只负责提需求，Agent 负责把它变成一份**能自证的档案**。所有动作都落在
`requests/REQ-xxxx-<kind>-<标题>/` 里：口径在 `spec.yaml`，做法在 `method/`，
输入在 `input/`，产物在 `deliveries/<时刻>/`。硬约束由 `hf-data-crawl` 命令执行，
本规程管的是「怎么用这些命令把事情做对」。

开工前先读：`GLOSSARY.md` 的 #51（词表口径）、#57–#58（生命周期与自包含）、
#69（过程记录）、#70（变更治理）；字段契约见 `docs/DATA.md` 第八节。
词条含义一律以 `hf-data-crawl request show` 打出的定义为准，**不凭记忆**。

## 一、接收：先判定，再动手

判错一项，后面全白做：

| 判定 | 依据 | 判错的后果 |
| --- | --- | --- |
| 形态 | 要一份**新表** → `query`；要把**给定的表填满** → `backfill` | 拿回填做查询，会把对方表结构当需求 |
| 编号 | 扫 `requests/` 取最大号 +1 | 撞号会静默覆盖口径 |
| 节奏 | 一次性 → `once`；定期给人 → `daily/weekly/monthly` | `once` 重交需 `--force`；节奏错会天天问 |
| 输入 | `backfill` 必须收进原件副本 | 直接读外部文件，目录就搬不走了 |

只问真正影响口径的问题——**给谁看**（决定字段与排序）、**拿去做什么**（决定阈值）、
**多久要一次**（决定 cadence）。其余按最保守假设写进 spec 并注明，别十问八问。

分支处理：

- 「上次那份改一下」→ 改同一个 spec 并把 `version` 加一，**不要新建需求**。
- 「又来一份新表，字段不一样」→ `request input <id> --add <新表>` 收进 `input/v<n>/`
  并读差异；再改 `fill.source.path`、升版本。**不要覆盖旧快照**——旧档案里的
  sha256 会对不上磁盘，自证链就断在文件系统上。
- 「这批只要 20 个库」→ 先看能否用现有 backfill 的 `overrides.json` 表达，不要新建需求。
- 「要一份全站报表 / 看板」→ 不是本规程的事，走 `report` 命令。

## 二、响应：把口径与做法写下来

1. **建需求**

   ```bash
   hf-data-crawl request new REQ-0001 --title 标题 --requester 姓名 \
       [--purpose 用途] [--cadence once] [--kind query|backfill] [--source 表.xlsx]
   ```

   骨架自带注释，按注释改。`kind: backfill` 的 `--source` 会把原表复制进 `input/`。

2. **写口径**（`spec.yaml`）
   - `criteria.metric` 只写词表词条；歧义词（下载量 vs 累计下载量）当场澄清。
   - 不写 `output.columns` 时按 31 列默认契约投影——这就是「不指定时给什么」的定义。
   - 阈值、排序、护栏都要能在注释里回答「为什么是这个数」。

3. **写做法**（`method/METHOD.md` + `method/scripts/`）
   - 步骤要能让别人照着重跑一遍；人判与机算分列成表，判据写死。
   - 一次性脚本放 `method/scripts/` 存原件，再跑 `request method <id>` 登记指纹。
   - 删掉骨架里的 `<!-- 待填：` 那一行——留着等同于「没有方法」，交付会被拦。

4. **预演**

   ```bash
   hf-data-crawl request run REQ-0001 --dry-run
   ```

   看行数量级与逐列覆盖。0 行、几十万行、或某列一格未填，先回头改口径，
   **不要先正式交付再看结果**。

5. **汇报**：行数、列数、哪些列是判读的、哪些留空及原因，一次说清。

## 三、完成：交付并自证

```bash
hf-data-crawl request run REQ-0001
```

交付后读 `deliveries/<时刻>/README.md`，核对三件事：口径是否与你说的一致、
`source.current_md5` 是哪版底库、`method` 段是否钉住了你写的方法与脚本。
再告诉用户：产物路径、行数、判读来源、留空原因。**不要只说「已完成」**。

被拦下时按错误信息里的指引修，见 `references/blocks.md`。任何情况下都不许手改
`manifest.json` / `README.md`，也不许把 `deliveries/` 里的产物复制出来充数——
档案由引擎写，手改等于伪造。

## 四、变更：让「改了什么」有据可查

| 场景 | 命令 | 要点 |
| --- | --- | --- |
| 改口径 | 改 spec → `version` +1 → `request diff <id>` | 版本号不可复用，没升版会被拦 |
| 换输入表 | `request input <id> --add 新表` → 改 `fill.source.path` → 升版 | 旧快照留在 `input/v<n>/` |
| 看两版口径差 | `request diff <id> [vA] [vB]` | 默认最近两版；只比决定产物的部分 |
| 看两次交付差 | `request compare <id> [A] [B]` | 逐列报改值格数并抽样 |
| 体检 | `hf-data-crawl doctor` | 看「需求自包含」「口径版本」两项 |

变更后**必须**在 `method/METHOD.md` 里记一句「换了什么、为什么换」。这句话是
六个月后唯一能解释这次变更的东西，不是可选项。

## 硬规矩

1. **不猜**：判读不出来就 `manual` 并写清原因；编一个「看起来对」的值比留空危险得多。
2. **不改原件、不外写产物**：`input/` 只读，产物只进 `deliveries/<时刻>/`。
3. **不跳预演**：`--dry-run` 是交付前唯一能发现「0 行」的机会。
4. **不手改档案**：manifest 与 README 是引擎产物。
5. **不复用版本号**：内容变了就升版，哪怕只改一个阈值。
6. **不留无方法的交付**：`method/` 不齐，货出不了门。
7. **不 `SELECT *`**：未指定投影时给 31 列默认契约，那是定义，不是偷懒。
8. **不碰内部流程列**：对方表里「处理人 / 质检结果 / 存储路径」这类列无权填，留空。

## 反模式（见到就停）

- 「下载量我记得是历史累计」→ 错。词条默认近 30 天，累计要写「累计下载量」。
- 把对方表复制到 `deliveries/` 里手工填 → 错。必须经 `fill:` 规则，否则没有逐列账。
- 交付被拦就加 `--force` 或改 manifest → 错。拦下是结论，不是障碍。
- 一个需求号下并存两套口径给两个部门 → 错。拆号，编号免费。
- 把「字段不一样的新表」直接覆盖 `input/` → 会毁掉旧交付的自证链。

## 命令速查

```bash
hf-data-crawl request new REQ-0001 --title … --requester … [--kind backfill --source 表.xlsx]
hf-data-crawl request list
hf-data-crawl request show REQ-0001              # 口径、编译后 SQL、版本与指纹、方法状态、台账
hf-data-crawl request method REQ-0001 [--check]  # 重算脚本索引 / 只判不写
hf-data-crawl request input REQ-0001 --add 新表.xlsx
hf-data-crawl request diff REQ-0001 [v1 v2]
hf-data-crawl request compare REQ-0001 [A B]
hf-data-crawl request run REQ-0001 [--dry-run] [--force]
hf-data-crawl request run-due
hf-data-crawl doctor
```

## 交付前自检

- [ ] `request method <id> --check` 返回 0（方法齐、脚本指纹对得上）
- [ ] `--dry-run` 行数在预期量级，逐列覆盖无异常空格
- [ ] 改过口径的，`version` 已加一且 `request diff` 能说清差异
- [ ] 换过输入表的，`fill.source.path` 指向 `input/v<n>/`，旧快照仍在
- [ ] `manual` 列都有 `overrides.json` 或明确留空理由
- [ ] 交付后 README 的「口径」「基于哪一版底库」「复现」三段能看懂
