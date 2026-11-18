# ADR-0007: 切片门面改为「登记制」

**日期**: 2026-11-19
**状态**: 已实施

---

## 背景

ADR-0002 管住了切片之间的**方向**：谁能依赖谁，登记在 `EDGES` 里，多一条少一条
都失败。但方向正确不等于耦合可控——150 条跨切片导入里，**135 条直达内部子模块**：

```
platform.store      33
platform.atomic     19
platform.tables     18
```

`delivery` 抓 `query.gold.share_text` 与抓 `query.gold.GoldCatalog` 一样畅通。
前者是填充率的展示口径，后者是数据读取入口；哪个算契约、哪个只是眼下顺手拿的，
事后无从分辨。改 `share_text` 的签名会同时打断 `delivery` 的四个模块，而没有任何
测试会提前说话。

`pipeline/__init__.py` 更彻底：`__all__` 是空列表。整个切片的「公开面」为空，
等于声明「这里没有接口，全是内部实现」。

---

## 决策

**用登记约束依赖，不用转发。** 各切片的 `__init__.py` 的 `__all__` 是它对外提供
的名字集合；跨切片取用一个名字时，这个名字必须出现在目标切片的 `__all__` 里。

已实施的登记：

| 切片 | 粒度 | 内容 |
|---|---|---|
| `platform` | 符号级（52 个） | store / atomic / tables / coerce / vocab / runs / retention / lock / presets / env 的对外符号 |
| `query` | 符号级（7 个） | `BOARDS`、`GoldCatalog`、`GoldNotReady`、三个填充率口径、宽表列名 |
| `pipeline` | 子模块级（12 个） | `raw`、`bronze`、`silver`、`state`、`derive`、`rebuild`、`lineage`、`marker`、`recovery`、`card`、`readme_fields`、`commands` |

规则实现在 `tests/test_architecture.py` 的「跨切片只能依赖登记过的符号」一条：
`from ..X.Y import Z` 里 `Y` 或 `Z` 任一登记即可；`from ..X import Z` 里 `Z` 必须
登记。两种写法表达同一件事，只认一种的话换个写法就绕过去了。

另有一条「门面登记的名字都真实存在」：`__all__` 里写了却取不到，等于给了外部
一张空头契约，这条逐名核对。

**`ops` 不在约束范围内。** 它是组合根兼运维层：CLI 要调每个 `run_*`，体检要读各
切片的诊断函数。要求它只碰登记符号，只会逼着各切片把诊断函数一并登记，登记的
筛选作用就没了。

---

## 为什么不「强制转发」

一度打算把所有跨切片导入改成 `from ..platform import write_bytes`（走门面）。
放弃的理由：**信息量会下降**。`platform.atomic.write_bytes` 一眼可知「原子写」，
`platform.write_bytes` 要到门面里查它出自哪一层；而门面本身不提供任何封装收益
——它 re-export 的是同一个对象，没有转发逻辑、没有适配、没有收窄。

登记制保留了调用点的分层信息，同时让「谁依赖什么」有据可查。要收紧时改登记表，
不必动 148 处调用点。

---

## 两次碰壁的记录

**其一，`pipeline` 的符号级门面不可行。** 试过用 PEP 562 的 `__getattr__` 把 15
个符号提到顶层，两处同时报警：

- pyright 对 `__all__` 里未出现的名字报 15 条 `reportUnsupportedDunderAll`
  ——静态分析看不见惰性属性；
- 环检测把 `__init__ → derive` 与 `derive → __init__`（后者来自 `from . import bronze`
  的父包边）连成一条环。

两条都是真信号。这个切片的公开面本来就是子模块，硬提到符号级只换来「写法好看」。

**其二，`state` 通过 `from ..pipeline import state` 依赖，`state` 却不在旧注释
所说的「会成环」之列。** 旧注释说 `check_and_recover` 引入循环依赖，实测它经
`rebuild` 拉进 `derive`，并未成环，只是急切加载下会改变初始化时序。登记制不加载，
该顾虑不存在——`pipeline/__init__.py` 因此不 import 任何子模块。

---

## 反向验证

新增规则后当场插入一行未登记依赖：

```python
# delivery/contract.py 末尾
from ..query.gold import COPY_COMPRESSION
```

```
E  delivery\contract.py:84 delivery -> hf_data_crawl.query.gold.COPY_COMPRESSION
   （要就登记进 query/__init__.py 的 __all__，要就别依赖）
```

插入即红，随后还原。

**附带发现**：第一次做这个验证时没红。原因是 pytest 复用了上一次的字节码缓存，
源码改动没有生效；加 `-p no:cacheprovider` 后正常。凡做「插入—报红—还原」的验证，
必须带这个参数，否则会得出「规则不管用」的错误结论。

---

## 后果

- 新增一条跨切片依赖，必须先在目标切片的 `__init__.py` 里登记，否则测试失败。
- 改内部实现时，若名字已登记，说明有外部依赖，改前先看谁在用；未登记的可以自由改。
- `pipeline` 的 `__all__` 用文件级 `# pyright: reportUnsupportedDunderAll=false`
  抑制，理由已写在文件 docstring 里，真实性由架构测试核对。
