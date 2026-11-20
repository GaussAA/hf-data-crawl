"""采集命令层的边角：按需补卡片、补跑后重放、传输构造与打印分支。

`test_preview_commands.py` 盯的是「候选选得对不对」，这一份盯的是另外三类：

- **按需补卡片**（`enrich --ids`）：交付命中的常常只有几十条，逐条问一次比全量扫
  十几小时划算。它的输出必须能区分「卡片本来就是空的」与「抓取失败」——两者看着
  都是 0 行，含混过去就会有人去查并不存在的网络问题。
- **补跑过去日期**：单日派生会把 `current` 写成那天的状态，于是「补了一天」的代价
  是「底库退回那天」。这条重放必须真的发生。
- **传输构造**：`fetch-readme` 关掉空闲连接复用是一个事故换来的参数（10 个连接同
  时变坏后吞吐从 14/s 掉到 0.33/s），它一旦被后来者「顺手统一」掉，症状会在十几
  小时的长跑里才浮现。
"""

from __future__ import annotations

import io
import sys
from argparse import Namespace
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

import polars as pl
import pytest

from hf_data_crawl.acquire import commands
from hf_data_crawl.acquire.collect import CollectSummary
from hf_data_crawl.acquire.reconcile import ReconcileSummary
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import table

if TYPE_CHECKING:
    from hf_data_crawl.acquire.hf.api import Transport

TODAY = date(2026, 11, 19)
PAST = TODAY - timedelta(days=3)


class NoCards:
    """替身 API：一律答「没有卡片」。长尾上传确实常常如此，不是错误。"""

    def record(self, _dataset_id: str) -> None:
        return None


def enrich_args(**overrides: object) -> Namespace:
    base: dict[str, object] = {
        "ids": None,
        "top": 10,
        "limit": None,
        "date": TODAY.isoformat(),
    }
    base.update(overrides)
    return Namespace(**base)


def not_network(monkeypatch: pytest.MonkeyPatch, api: object | None = None) -> None:
    """把联网入口整体换掉：本组测试只验编排，不发请求、也不读本机凭据。"""
    monkeypatch.setattr(commands, "_open_transport", lambda _transport: None)
    monkeypatch.setattr(commands, "HfDatasetApi", lambda **_kwargs: api or NoCards())


# ---------------------------------------------------------------- 按需补卡片


def test_按需补卡片只问点名的库(tmp_path: Path, monkeypatch, capsys) -> None:
    store = DatasetStore(tmp_path / "data")
    asked: list[str] = []

    class Recorder:
        def record(self, dataset_id: str) -> None:
            asked.append(dataset_id)

    not_network(monkeypatch, Recorder())

    outcome = commands.run_enrich(enrich_args(ids="a/x, a/y"), store, None)

    # 逗号后的空格是人工手抄名单的常态；不去掉就会去问一个带空格的 id。
    assert asked == ["a/x", "a/y"]
    assert outcome.code == 0
    assert outcome.payload["requested"] == 2


def test_按需补卡片的名单为空时拒绝(tmp_path: Path, monkeypatch, capsys) -> None:
    """`--ids " , "` 传进来是一串分隔符：此时「跑完 0 行、状态 success」会骗人。"""
    store = DatasetStore(tmp_path / "data")
    not_network(monkeypatch)

    outcome = commands.run_enrich(enrich_args(ids=" , "), store, None)

    assert outcome.code == 1
    assert outcome.reason == "空 id 列表"
    assert "没有有效" in capsys.readouterr().out


def test_一张卡片都没取到时点明不是抓取失败(tmp_path: Path, monkeypatch, capsys) -> None:
    """0 张卡片有两种成因：网络不通，或这些库本就没有卡片。

    含混过去，读到「写入 0 行」的人会去查网络——那是白查。
    """
    store = DatasetStore(tmp_path / "data")
    not_network(monkeypatch)

    outcome = commands.run_enrich(enrich_args(ids="a/x"), store, None)

    out = capsys.readouterr().out
    assert outcome.payload["with_card"] == 0
    assert "按需补卡片：点名 1 个" in out
    assert "本身为空" in out


# ---------------------------------------------------------------- 补跑后重放


def collect_args(**overrides: object) -> Namespace:
    base: dict[str, object] = {
        "preset": None,
        "max_pages": None,
        "refresh_top": None,
        "pages_without_new": None,
        "skip_if_done": False,
        "dry_run": False,
        "limit": None,
        "cards": False,
        "card_pages": None,
        "full_scan": False,
        "date": PAST.isoformat(),
    }
    base.update(overrides)
    return Namespace(**base)


def summary_on(day: date) -> CollectSummary:
    return CollectSummary(
        snapshot_date=day,
        pages=1,
        records=10,
        skipped=0,
        new_ids=10,
        bronze_rows=10,
        silver_rows=10,
        appended_observations=10,
        current_rows=100,
        rate_remaining=99,
        stopped_reason="end",
        elapsed_seconds=1.0,
    )


def patch_collector(monkeypatch: pytest.MonkeyPatch, day: date) -> None:
    class FakeCollector:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def run(self, *_args: object, **_kwargs: object) -> CollectSummary:
            return summary_on(day)

    monkeypatch.setattr(commands, "Collector", FakeCollector)
    monkeypatch.setattr(commands, "_open_transport", lambda _transport: None)


def test_补跑过去日期后重放后缀(tmp_path: Path, monkeypatch, capsys) -> None:
    """单日派生把 `current` 写成那天的状态；不重放，底库就退回那一天。"""
    store = DatasetStore(tmp_path / "data")
    patch_collector(monkeypatch, PAST)
    rebuilt: list[date] = []
    monkeypatch.setattr(commands.rebuild, "replay_suffix", lambda _s, day: [day, PAST])
    monkeypatch.setattr(
        commands.rebuild, "rebuild_from_raw", lambda _s, since: rebuilt.append(since)
    )

    commands.run_collect(collect_args(), store, None)

    out = capsys.readouterr().out
    assert "重放后缀 2 天" in out
    assert rebuilt == [PAST], "不能只打印不重放"


def test_补跑过去日期但只有一天时不重放(tmp_path: Path, monkeypatch, capsys) -> None:
    """后缀只有它自己时没有「回到最新」这回事，重建一次纯属白跑。"""
    store = DatasetStore(tmp_path / "data")
    patch_collector(monkeypatch, PAST)
    rebuilt: list[date] = []
    monkeypatch.setattr(commands.rebuild, "replay_suffix", lambda _s, day: [day])
    monkeypatch.setattr(
        commands.rebuild, "rebuild_from_raw", lambda _s, since: rebuilt.append(since)
    )

    commands.run_collect(collect_args(), store, None)

    assert rebuilt == []
    assert "重放后缀" not in capsys.readouterr().out


def test_干跑不触发重放(tmp_path: Path, monkeypatch) -> None:
    """干跑什么都没写，重放会拿一份没变的 raw 去重建整库。"""
    store = DatasetStore(tmp_path / "data")
    patch_collector(monkeypatch, PAST)

    def boom(_store: object, _day: date) -> list[date]:
        raise AssertionError("干跑不该走到重放")

    monkeypatch.setattr(commands.rebuild, "replay_suffix", boom)

    commands.run_collect(collect_args(dry_run=True), store, None)


# ---------------------------------------------------------------- 传输构造


class RecordingTransport:
    """记下构造参数，代替真实的 httpx 客户端。"""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs


def test_注入的传输原样传出() -> None:
    """注入的传输必须原样传出：测试与离线回放靠的就是这个口子，不能被包装掉。"""
    sentinel = cast("Transport", object())

    assert commands._files_transport(sentinel, timeout=60.0) is sentinel
    assert commands._readme_transport(sentinel, timeout=60.0) is sentinel


def test_文件抓取用自定义超时(tmp_path: Path, monkeypatch) -> None:
    """整库文件清单可能几十 MB，默认 30 秒不够。"""
    monkeypatch.setattr(commands, "HttpxTransport", RecordingTransport)
    monkeypatch.setattr(commands, "load_env_file", lambda: None)

    made = commands._files_transport(None, timeout=120.0)

    assert isinstance(made, RecordingTransport)
    assert made.kwargs == {"timeout": 120.0}


def test_readme抓取关掉空闲连接复用(monkeypatch) -> None:
    """代理会单方面丢掉空闲连接，而 httpx 仍当它可用——下一次复用就卡在 TLS 上。

    2026-11 实测：10 个连接同时变坏后吞吐从 14/s 掉到 0.33/s，重启才恢复。
    `max_keepalive=0` 是这条事故的结论，不能被后来者「顺手统一」掉。
    """
    monkeypatch.setattr(commands, "HttpxTransport", RecordingTransport)
    monkeypatch.setattr(commands, "load_env_file", lambda: None)

    made = commands._readme_transport(None, timeout=45.0)

    assert isinstance(made, RecordingTransport)
    assert made.kwargs == {"timeout": 45.0, "max_keepalive": 0}


# ---------------------------------------------------------------- 打印分支


def test_干跑采集打印样本字段(tmp_path_factory) -> None:
    """干跑的价值就在「先看一眼字段」，这行不打出来，干跑等于没跑。"""
    summary = replace(
        summary_on(TODAY),
        stopped_reason="dry_run",
        rate_remaining=7,
        preview=[{"id": "a/x", "downloads": 3}],
    )

    buffer = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "stdout", buffer)
        commands._print_collect(summary)

    out = buffer.getvalue()
    assert "dry-run：探得 1 条样本" in out and "配额余量 7" in out
    assert "a/x" in out


def test_首轮采集说明卡片趟为何跳过() -> None:
    """没水位线 ≠ 出错：那是首轮全量补齐的地盘，说一句免得人去查配置。"""
    summary = replace(summary_on(TODAY), card_reason="no_watermark")

    buffer = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "stdout", buffer)
        commands._print_collect(summary)

    out = buffer.getvalue()
    assert "变化集卡片：无水位线" in out
    assert "不是增量" in out


def test_对账打印探测复核的三个数(tmp_path: Path) -> None:
    """探测复核 0 个与「没做探测」在终端里长得一样，所以只在真探测过时才打印。"""
    summary = ReconcileSummary(
        snapshot_date=TODAY,
        pages=1,
        records=10,
        new_ids=1,
        disappeared=0,
        sha_changed=0,
        reactivated=0,
        report_path=tmp_path / "diff.json",
        stopped_reason="end",
        elapsed_seconds=2.0,
        probe_probed=5,
        probe_failed=1,
        probe_deferred=2,
    )

    buffer = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "stdout", buffer)
        commands._print_reconcile(summary)

    out = buffer.getvalue()
    assert "探测复核 5 个" in out and "失败 1" in out and "顺延 2" in out


def test_对账打印顽固失败并指向清单(tmp_path: Path) -> None:
    summary = ReconcileSummary(
        snapshot_date=TODAY,
        pages=1,
        records=10,
        new_ids=0,
        disappeared=0,
        sha_changed=0,
        reactivated=0,
        report_path=tmp_path / "diff.json",
        stopped_reason="end",
        elapsed_seconds=2.0,
        probe_degraded=3,
    )

    buffer = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "stdout", buffer)
        commands._print_reconcile(summary)

    out = buffer.getvalue()
    assert "探测顽固失败 3 个" in out and "probe_degraded" in out


def test_进度行在非终端按十等分打印() -> None:
    """日志收 `\\r` 会糊成一行或冲成几百行——先判断给谁看。"""
    buffer = io.StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sys, "stdout", buffer)
        commands._progress_line(3, 100)
        assert buffer.getvalue() == "", "非整十不该打印"

        commands._progress_line(10, 100)
        commands._progress_line(100, 100)

    out = buffer.getvalue()
    assert "10/100" in out and "100/100" in out


def test_规模既无陈旧也无缺口时不做请求(tmp_path: Path, monkeypatch, capsys) -> None:
    """刷新链每天都会带 `--changed`；没什么可刷时要立刻收工，不能让下游以为抓过。"""
    store = DatasetStore(tmp_path / "data")
    store.upsert(
        "current",
        pl.DataFrame(
            [
                {
                    "id": "a/x",
                    "url": "https://huggingface.co/datasets/a/x",
                    "downloads": 10,
                    "last_modified": datetime(2026, 10, 20, tzinfo=UTC),
                }
            ],
            schema=dict(table("current").schema),
        ),
    )
    store.upsert(
        "size",
        pl.DataFrame(
            [
                {
                    "id": "a/x",
                    "snapshot_date": TODAY,
                    "fetched_at": datetime(2026, 11, 19, 6, tzinfo=UTC),
                    "status": "ok",
                    "num_rows": 5,
                }
            ],
            schema=dict(table("size").schema),
        ),
    )

    def boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("没有候选时不该构造 fetcher")

    monkeypatch.setattr(commands, "SizeFetcher", boom)

    args = Namespace(
        date=TODAY.isoformat(),
        changed=5,
        changed_all=False,
        ids=None,
        top=10,
        workers=1,
        retry_unavailable=False,
        flush_every=50,
    )
    outcome = commands.run_fetch_sizes(args, store, None)

    assert outcome.code == 0
    assert outcome.payload == {"changed": 0, "refreshed": 0, "gap": 0}
    assert "没有需要重取的库" in capsys.readouterr().out


# ---------------------------------------------------------------- 文件与 README 的早退


def seed_current(store: DatasetStore, *, sha: str = "sha-1") -> None:
    store.upsert(
        "current",
        pl.DataFrame(
            [
                {
                    "id": "a/x",
                    "url": "https://huggingface.co/datasets/a/x",
                    "downloads": 10,
                    "sha": sha,
                }
            ],
            schema=dict(table("current").schema),
        ),
    )


def test_点名的库都不在架时拒绝抓文件(tmp_path: Path, capsys) -> None:
    """名单过滤后为空却继续跑，会得到「跑完 0 条、状态 success」——比报错更像正常。"""
    store = DatasetStore(tmp_path / "data")
    seed_current(store)
    args = Namespace(
        date=TODAY.isoformat(),
        refresh_truncated=False,
        max_pages=3,
        ids="b/nope",
        timeout=30.0,
        max_retries=1,
        workers=1,
        top=10,
        retry_unavailable=False,
        flush_every=50,
    )

    outcome = commands.run_fetch_files(args, store, None)

    assert outcome.code == 1
    assert "都不存在" in capsys.readouterr().out


def readme_args(**overrides: object) -> Namespace:
    base: dict[str, object] = {
        "date": TODAY.isoformat(),
        "changed": 0,
        "changed_all": False,
        "ids": None,
        "top": 10,
        "timeout": 30.0,
        "max_retries": 1,
        "rate": 10.0,
        "workers": 1,
        "flush_every": 50,
        "retry_denied": False,
    }
    base.update(overrides)
    return Namespace(**base)


def test_README全都有结论时立刻收工(tmp_path: Path, capsys) -> None:
    """`--changed` 是每天的常态；全站都有结论时要一句话收工，而不是空跑一趟。"""
    store = DatasetStore(tmp_path / "data")
    seed_current(store, sha="sha-1")
    store.upsert(
        "readme",
        pl.DataFrame(
            [{"id": "a/x", "url": "u", "source_sha": "sha-1", "status": "ok"}],
            schema=dict(table("readme").schema),
        ),
    )

    outcome = commands.run_fetch_readme(readme_args(changed=50), store, None)

    assert outcome.code == 0
    assert outcome.payload == {"changed": 0, "refreshed": 0}
    assert "既无陈旧也无缺口" in capsys.readouterr().out


def test_changed_all单独给出也成立(tmp_path: Path, monkeypatch, capsys) -> None:
    """刷新链只传 `--changed-all`、不传 `--changed`。

    判据原先只看 `args.changed > 0`，于是链上这一步静默退回「按 --top 抓头部」：
    陈旧库一个没刷，退出码 0，日志里也看不出少了什么。
    """
    store = DatasetStore(tmp_path / "data")
    seed_current(store, sha="sha-2")
    store.upsert(
        "readme",
        pl.DataFrame(
            [{"id": "a/x", "url": "u", "source_sha": "sha-1", "status": "ok"}],
            schema=dict(table("readme").schema),
        ),
    )

    class Chosen(Exception):
        """候选已选出——证明没走「既无陈旧也无缺口」那条早退。"""

    def boom(*_args: object, **_kwargs: object) -> object:
        raise Chosen

    monkeypatch.setattr(commands, "ReadmeFetcher", boom)

    with pytest.raises(Chosen):
        commands.run_fetch_readme(readme_args(changed=0, changed_all=True), store, None)

    assert "随内容刷新" in capsys.readouterr().out


def test_README点名不到在架库时拒绝(tmp_path: Path, capsys) -> None:
    store = DatasetStore(tmp_path / "data")
    seed_current(store)

    outcome = commands.run_fetch_readme(readme_args(ids="b/nope"), store, None)

    assert outcome.code == 1
    assert outcome.reason == "--ids 无有效 id"
    assert "没有有效" in capsys.readouterr().out


def test_files的top为零表示全量() -> None:
    """`--top 0` 必须是全量：原先的 `max(1, 0)` 会静默改成只抓一个库。

    四条抓取链路（files / readme / sizes / preview）对 `--top 0` 必须是同一个意思，
    否则「跑全量」这件事得靠人记住每条命令各自的方言。
    """
    assert commands.resolve_files_top(0, 1234, named=False) == 1234
    assert commands.resolve_files_top(-5, 1234, named=False) == 1234


def test_files按下载量截断且不超过候选数() -> None:
    assert commands.resolve_files_top(10, 1234, named=False) == 10
    assert commands.resolve_files_top(99_999, 1234, named=False) == 1234


def test_files点名时不按top截断() -> None:
    """名单本身就是范围：`--ids` 与 `--top` 同时给，点名优先。"""
    assert commands.resolve_files_top(10, 50, named=True) == 50
