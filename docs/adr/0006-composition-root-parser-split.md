# ADR-0006: 组合根拆出参数表

**日期**: 2026-11-18
**状态**: 已实施

---

## 背景

`ops/cli.py` 是唯一组合根（ADR-0002），职责是「谁加锁、谁记账、谁恢复」。
但它同时装着全部子命令的参数定义：785 行里有 **492 行是 `build_parser`**，
占 63%。

两件事的增长方式完全不同：

- 参数定义随功能线性增长，每加一个开关就多几行；
- 锁策略只在事故后收紧（`SELF_LOCKED_COMMANDS` 的注释里挂着 2026-11-07 与
  2026-11-18 两次事故）。

混在一处之后，每次改 CLI 都要在近五百行里定位，冲突面最大；而真正该被逐行
review 的锁集合，反而被埋在参数注册里看不见。

---

## 决策

### 1. 参数表独立成 `ops/parsers.py`

`build_parser` 与它的全部子命令注册整体搬过去，连同两个只服务参数默认值与
帮助文本的等锁常量（`SNAPSHOT_LOCK_WAIT` / `DOCTOR_LOCK_WAIT`）。
`cli` 从 `parsers` 取 `build_parser`，方向单一，不成环。

### 2. 参数表只许 import 常量与枚举

新增 `test_参数表只声明参数不承担执行`：`parsers.py` 不得 import 任何 `run_*`，
也不得 import 任何 `*.commands` 模块。

原来的「组合根只经各切片的公开入口」只盯着 `cli.py` 一个文件（ADR-0002 的
第 3 条）。参数表若偷偷 import `run_*`，它就变成了第二个组合根，而十条规则
一条都不会说话。边界只有落成机制才算边界。

反向验证：往 `parsers.py` 末尾插一行
`from ..acquire.commands import run_collect`，该测试报出
`parsers.py:528 from hf_data_crawl.acquire.commands import run_collect`。

### 3. 测试改为直接引用参数表

`tests/acquire/test_cli_acquire.py` 与 `tests/ops/test_presets.py` 原先写
`from hf_data_crawl.ops.cli import build_parser`，靠的只是 `cli` 把它 import
进了模块命名空间。改成 `from hf_data_crawl.ops.parsers import ...`，来源才
与事实一致。

---

## 后果

**正面**

- `cli.py` 785 → 273 行；剩下的是锁策略、恢复、台账与分派，可以一次读完。
- 参数表覆盖率 **100%**（167 语句，0 未覆盖）——每条子命令都至少被一条测试
  真正解析过一次，这是拆分前没人知道的。
- 改一个 CLI 开关只碰 `parsers.py`，与锁策略的改动不再冲突。

**负面**

- 多一层间接：读命令行行为要从 `parsers` 跳到 `cli` 看分派。
- `cli.py` 自身覆盖率由 95.8% 降为 91.8%——搬走的是被充分覆盖的参数段，
  剩下的错误分支更难造场景。这是分母变小后的真实值，不是退步。

---

## 刻意没做

- **没把锁集合（`WRITE_COMMANDS` 等）也拆出去。** 它们只被 `main` 与
  `_execute` 使用，而这两处正是「施加锁」本身；分开会让「为什么这条命令
  要锁」与「锁是怎么加的」各在一处。
- **没动 `main` 里那 20 条 `if args.command == ...` 的分派。** 换成分发表能
  再短几十行，但那是另一种重构（把命令与处理函数绑成登记表），与本次
  「参数与执行分离」不是同一件事，混做会让 diff 说不清改了什么。
- **没有改任何命令行为。** 参数名、默认值、帮助文本一字未动。

---

## 验证

- ✅ `uv run pytest -q -n 8`：1308 通过（新增 1 条架构规则）
- ✅ `uv run ruff check .` / `uv run ruff format --check .` / `uv run mypy` / `uv run pyright` 全绿
- ✅ 覆盖率：`parsers.py` 100%（167 语句）· `cli.py` 91.8%（113 语句）· 全仓 89.0%
- ✅ 反向验证：参数表插入执行函数后，新规则打红并给出文件与行号

---

## 参考

- ADR-0002: 切片依赖边显式登记（组合根规则的由来）
- DECISIONS #125: 同一把尺子（本地与 CI 必须跑同一组检查）
- `tests/test_architecture.py`: 规则 11（参数表不承担执行）
