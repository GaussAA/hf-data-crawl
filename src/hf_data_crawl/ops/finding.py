"""体检结论：一条判定、一句话、一个标识。

为什么独立成模块
----------------
`Finding` 原先定义在 `doctor` 里，`health`（体检流水）要认这个类型，于是
`doctor → health`（调 `streak`）与 `health → doctor`（注解 `Finding`）成了一个环。
看上去可以靠 `TYPE_CHECKING` 绕过去，但架构测试把延迟导入也算环——理由是：
延迟导入只是把环藏起来，拆的时候一样要跨两个文件看。

拆出来之后，`doctor` 负责产出结论，`health` 负责记录结论，两者都只认这个类型，
谁也不认识谁。

三件事必须分开
--------------
`problem` 说「这是不是真问题」，`text` 给人读，`code` 给程序用。三者混成一个
字符串时，告警去重、连续天数、历史对比就都得靠子串匹配——文案里混进一个日期
或数字就匹配不上，历史连续性静默归零，而且不报错。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Finding:
    """一条体检结论。

    `code` 与**检查项**一一对应（`collect_freshness`、`raw_gaps` …），由 `diagnose`
    在编排处贴，所以它标识的是「哪一项检查」而不是「哪一句话」。文案可以随便改，
    code 不能：改了等于让历史记录断代。

    `degraded` 只在一处为真：没拿到写锁时，跨表读数可能跨时点。此时 `problem`
    **保留原判断**——「它是不是问题」与「这次读数算不算数」是两件事，用一个布尔值
    二选一会丢掉信息，于是「本来是红、因降级变黄」这件事在数据里就不存在了，
    只能靠字符串开头那个 `[非快照]` 前缀去猜。消费者判 `Finding.counts`。
    """

    problem: bool
    text: str
    #: 只有在一份一致快照上才有意义；非快照模式下置 `degraded`，不参与退出码。
    snapshot_dependent: bool = False
    #: 检查项标识，由 `diagnose` 按登记表贴。空值说明有人手工造了条结论而没贴码。
    code: str = ""
    #: 本次读数不算数（非快照模式），`problem` 保持原值。
    degraded: bool = False

    @property
    def counts(self) -> bool:
        """这条结论算不算数：是问题、且不是非快照模式下的降级读数。

        「它是不是问题」与「这次读数算不算数」是两件事，所以判定要落成一个名字。
        原先这条式子写在两个消费者里（退出码与告警、体检流水的计数），改一处漏一处
        的结果是同一个数在两处不一样——#81 修过的那类错。
        """
        return self.problem and not self.degraded
