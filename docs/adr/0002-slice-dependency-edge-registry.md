# ADR-0002: 切片依赖边显式登记

**日期**: 2026-11-17
**状态**: 已实施

---

## 背景

2026-11-11 夜到 11-13 凌晨的垂直切片重排（DECISIONS #75）给 `tests/test_architecture.py`
留了四条规则加一条私有名规则。它们都只判断**方向**：只许依赖次序更低的切片，`platform`
不依赖任何切片，模块图无环，跨切片不用下划线名。

方向合法不等于关系合理。`RANK` 给出的是 `delivery`(3) > `acquire`(2)，于是
`delivery` import `acquire` 在规则上完全合法——可数据流里 `acquire` 在 `delivery`
上游，`delivery` 消费的是 `query` 的 Gold 视图，从不碰采集。一旦有人为了复用一段
探测或对账逻辑把它塞进 `delivery`，五条规则全绿，环也没有，唯一的变化是「谁依赖谁」
这件事从此说不清了。

同一次检查还发现两处**形式上的缝**：

1. `_imported_names()` 只遍历 `ast.ImportFrom`。写成 `import hf_data_crawl.delivery.backfill`
   再取属性，就能绕过「跨切片禁止私有名」，也能绕过 `test_档案层只认契约不认形态`。
2. `ops/cli.py` 的「组合根只调各切片公开的 `run_*`」只写在 docstring 里。多抓一个
   `<切片>.recovery` 不会有任何测试变红——它现在就是这么做的（`pipeline.recovery`），
   而这条唯一例外没有任何登记。

---

## 决策

### 1. 真实存在的依赖边单独登记

新增 `EDGES: dict[str, set[str]]`，按片登记它**实际** import 的切片，与 `RANK` 并列：

```
platform → {}
pipeline → {platform}
acquire  → {pipeline, platform}
query    → {pipeline, platform}
delivery → {query, platform}
ops      → {delivery, query, acquire, pipeline, platform}
```

`test_跨切片依赖边与登记表一致` 从源码汇总实际边并逐条比对，未登记的新边直接打红，
消失了却没同步的旧边也打红——单边删除会让表变成历史遗迹。

**折衷**：每新增一条跨切片 import 多一行 diff，而且写代码的人得先判断「这条边讲不讲得
通」。这是有意的摩擦。与「无环」相比，它守的不是能不能跑，而是依赖图的可解释性。

### 2. 私有名与契约检查补上 `ast.Import`

`_imported_names()` 增加 `ast.Import` 分支：`import pkg.slice._impl` 与
`from pkg.slice import _impl` 是同一条规则下的两种写法。
`test_档案层只认契约不认形态` 同样按 `import` 形式判违规。

### 3. 组合根入口登记为规则

`test_组合根只经各切片的公开入口`：`ops/cli.py` 只能 import `<切片>` 或
`<切片>.commands`。判据走 `_imports()`，并把每个别名按「`SRC` 下是否真有这个子模块」
过滤——否则 `from ..platform import DatasetStore` 里的类名会被误判成子模块。
`pipeline.recovery` 作为唯一例外写在测试里，附上原因（避环，见
`src/hf_data_crawl/pipeline/__init__.py`）。

---

## 后果

**正面**

- 语义反过来的边（典型：`delivery → acquire`）走不进主干，除非有人在 `EDGES` 里明写。
- 「唯一组合根」与「档案层只认契约」从说明变成了机制。
- 三处改动都不改变运行时行为，实施当天全部测试绿。

**负面**

- 每新增一条跨切片依赖都多一次判断成本。图省事的路径变长了：以前 `import` 一行，现在
  还得改测试文件。
- `EDGES` 是**抄写**下来的事实：`RANK` 变了它不会跟着变，得靠测试比对来发现漂移。
  它保证的是「不会悄悄变」，不是「写对了」。

---

## 刻意没做

- **没有改 `RANK`，也没有把 `derive_day` 的触发从 `collect` / `reconcile` 挪到组合根。**
  这一改能让 `acquire` 落到 `pipeline` 之下、次序与数据流一致，但要连带碰
  marker 的崩溃恢复、lineage 的戳记语义与 `build` 的既有含义。换来的是编号好看，
  不是故障面变小。
- **没有拆 `ops`。** 它同时是组合根与全系统视图（体检、`data/DATA.md` 生成），所以
  必然 import 其余五片。要给它一个 rank 5 的上层，组合根就得搬出切片阶梯（包根
  `__main__.py` 或新增 `cli` 切片），`test_every_module_lives_in_a_known_slice` 也要
  开例外；而 `docs` 本来就要读四片，耦合只是挪位置。要动先单独提。

---

## 验证

- ✅ `uv run pytest -q`：1125 → 1126 通过，1 跳过
- ✅ `uv run ruff check .` / `uv run ruff format --check .` / `uv run mypy` / `uv run pyright` 全绿
- ✅ 反向验证（三条规则各自打红，插进去再删掉）：
  - 把 `import hf_data_crawl.delivery.backfill` 插进 `delivery/delivery.py` →
    `test_档案层只认契约不认形态` 报出该文件与行号
  - 把 `from ..acquire import session` 插进 `ops/cli.py` →
    `test_组合根只经各切片的公开入口` 报出 `hf_data_crawl.acquire.session`
  - 把同一条边插进 `delivery/commands.py` → `test_slices_only_depend_downwards` 仍绿，
    `test_跨切片依赖边与登记表一致` 报「未登记：delivery -> acquire」

---

## 参考

- DECISIONS #75: 按垂直切片重排（六切片与方向规则的由来）
- DECISIONS #101: 切片依赖边要显式登记
- `tests/test_architecture.py`: 规则的唯一实现
- `src/hf_data_crawl/pipeline/__init__.py`: 为什么 `check_and_recover` 不从包里导出
