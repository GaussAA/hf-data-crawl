#Requires -Version 7.0
<#
.SYNOPSIS
    README 全文保鲜：只重取 commit 已变的那批，不做全量。
.DESCRIPTION
    README 是仓库里的一个文件，它属于哪个版本由 commit 决定——所以这里的判据是
    `source_sha != current.sha`，不是 `lastModified`（后者会被「上传数据文件」这类
    无关改动推高，按它重抓等于白烧配额）。判据与实现都在
    `acquire.readme.stale_readme_ids`，本脚本只是把它排进日程。

    为什么只做增量：全量一轮约 21 小时 / 113 万条，属于「人工择机跑」的量级
    （2026-11-17 已跑过一次，1,135,833 行入库）。日常防两件事：**悄悄变旧**
    （commit 变了）与**从没抓过**（新库）——每轮 8000 个名额按下载量在这两者间
    分配，榜单与报告用到的都在头部。

    为什么必须带上「从没抓过」：`stale_readme_ids` 是 inner join，只挑已有记录
    的库；表里没有行的新库永远进不了候选（2026-11-19 实测 2,945 个，与 `size`
    早先的缺口问题是同一个形状，见 DECISIONS #131）。

    增量可续：结果先落 `data/.staging/readme/` 的原子分片，收尾才用短锁并入
    `enrich/readme.parquet`；中断后分片还在，下一轮按 id 跳过已抓的，不重复请求。

    两步不是一件事：抓回正文之后还要重抽侧栏字段（`parse-readme`），否则
    `readme_fields` 会停在旧正文上——那是安静的错误，查询结果看着像真的。

    命中率参考（前 2 万名实测）：约 64% 有正文、26% 作者本来没写、10% 受门禁限制。

.EXAMPLE
    pwsh -File scripts\readme.ps1
    pwsh -File scripts\readme.ps1 --changed 20000    # 临时加大一趟
#>
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Rest = @()
)

. (Join-Path $PSScriptRoot "_common.ps1")

$code = Invoke-HfTask -Name "readme" -Arguments (@(
    "--lock-wait", "900",
    "fetch-readme", "--changed", "8000"
) + $Rest) -TimeoutSeconds 3600

# 正文换了新版，侧栏字段就得跟着重抽——否则 `readme_fields` 会安静地停在旧正文上。
# `parse-readme` 按 (id, source_sha) 跳过未变的，常态只处理刚刷新的那几千条；
# 全量重抽 78 万条要 340 秒，只有第一次跑才该付这个价。
if ($code -eq 0) {
    $code = Invoke-HfTask -Name "readme-fields" -Arguments @(
        "--lock-wait", "900", "parse-readme"
    ) -TimeoutSeconds 1800
}

exit $code
