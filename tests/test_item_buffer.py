from __future__ import annotations

import contextlib
import threading
from pathlib import Path
from typing import Any

import pytest

from netspy import Item, UpdateItem, setting
from netspy.buffer.item_buffer import ItemBuffer
from netspy.dedup import Dedup
from netspy.pipelines.base import BasePipeline
from netspy.utils.stats import Stats


class RecordingPipeline(BasePipeline):
    saved: list[tuple[str, list[dict[str, Any]]]] = []
    updated: list[tuple[str, list[dict[str, Any]], list[str]]] = []
    fail_tables: set[str] = set()

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        if table in self.fail_tables:
            return False
        RecordingPipeline.saved.append((table, items))
        return True

    def update_items(self, table: str, items: list[dict[str, Any]], update_keys: list[str]) -> bool:
        RecordingPipeline.updated.append((table, items, update_keys))
        return True


_PIPE = f"{__name__}.RecordingPipeline"


@pytest.fixture(autouse=True)
def _reset_recording() -> None:
    RecordingPipeline.saved.clear()
    RecordingPipeline.updated.clear()
    RecordingPipeline.fail_tables.clear()


def _buffer(**kw: Any) -> ItemBuffer:
    kw.setdefault("pipelines", [_PIPE])
    kw.setdefault("dedup", Dedup(filter_type="lite"))
    return ItemBuffer(Stats(), **kw)


class NewsItem(Item):
    __unique_key__ = ["url"]


def test_dicts_go_to_default_table() -> None:
    buf = _buffer()
    buf.put({"a": 1})
    buf.flush()
    assert RecordingPipeline.saved == [(setting.ITEM_DEFAULT_TABLE, [{"a": 1}])]


def test_items_grouped_by_table() -> None:
    buf = _buffer()
    buf.put(NewsItem(url="u1", title="a"))
    buf.put(NewsItem(url="u2", title="b"))
    a = NewsItem(title="x")
    a.table_name = "other"
    buf.put(a)
    buf.flush()

    tables = {t for t, _ in RecordingPipeline.saved}
    assert tables == {"news", "other"}
    news_rows = next(rows for t, rows in RecordingPipeline.saved if t == "news")
    assert len(news_rows) == 2


def test_item_level_dedup_within_and_across_flushes() -> None:
    buf = _buffer()
    buf.put(NewsItem(url="u1", title="a"))
    buf.put(NewsItem(url="u1", title="a-dup-same-batch"))
    buf.flush()
    buf.put(NewsItem(url="u1", title="a-dup-later"))
    buf.flush()

    saved_rows = [r for _, rows in RecordingPipeline.saved for r in rows]
    assert len(saved_rows) == 1
    assert buf._stats.get("item") == 1
    assert buf._stats.get("item_dedup_dropped") == 2


def test_update_item_routed_to_update_items() -> None:
    class PriceItem(UpdateItem):
        __update_key__ = ["sku"]

    buf = _buffer()
    buf.put(PriceItem(sku="S1", price=5))
    buf.flush()
    assert RecordingPipeline.updated == [("price", [{"sku": "S1", "price": 5}], ["sku"])]
    assert not RecordingPipeline.saved


def test_failed_save_dumps_and_skips_dedup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    RecordingPipeline.fail_tables.add("news")
    dedup = Dedup(filter_type="lite")
    buf = _buffer(dedup=dedup)

    buf.put(NewsItem(url="u1", title="a"))
    buf.flush()

    dump = tmp_path / "failed_items.jsonl"
    assert dump.exists()
    assert "u1" in dump.read_text(encoding="utf-8")
    assert buf._stats.get("item_failed") == 1
    # 写失败 -> 指纹没入库 -> 重试还能再来一次
    assert dedup.get(NewsItem(url="u1", title="a").fingerprint) is False


def test_per_item_pipeline_override() -> None:
    it = NewsItem(url="u1")
    it.pipelines = [_PIPE]  # item 级覆盖
    buf = _buffer(pipelines=["nonexistent.BrokenPipeline"])  # 全局管道故意无效
    buf.put(it)
    buf.flush()
    assert len(RecordingPipeline.saved) == 1  # 用了 item 覆盖，没碰无效的全局管道


def test_handler_bypasses_pipelines_and_dedup() -> None:
    got: list[Any] = []
    buf = ItemBuffer(Stats(), handler=got.extend)
    buf.put(NewsItem(url="u1"))
    buf.put(NewsItem(url="u1"))
    buf.flush()
    assert len(got) == 2
    assert not RecordingPipeline.saved


def test_resolve_pipelines_does_not_double_construct_under_concurrent_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回归测试：缓存冷启动时并发 `flush()` 不该把同一份管道建两遍。

    `flush()` 能从多个线程并发触发——`put()` 攒够 `ITEM_MAX_CACHED_COUNT`
    时在调用方线程上同步 flush，多个 worker 同时攒够的话就是多个线程各跑
    各的 `flush()`。旧实现的 `_resolve_pipelines()` 是无锁的「查 → 建 →
    存」：缓存还没命中过这个 key 时，两个线程会各建一份管道实例——构造管道
    可能是开数据库连接池这种有实际开销的操作，输的那份既不会被用来写
    数据，也不在最终的缓存里，`close()` 找不到它，连接就那样泄漏。

    用 `Barrier(2)` 强制两个线程的构造过程真正重叠，断言构造只发生一次。
    """
    construct_count = {"n": 0}
    count_lock = threading.Lock()
    barrier = threading.Barrier(2)

    class SlowPipeline(BasePipeline):
        def __init__(self) -> None:
            with count_lock:
                construct_count["n"] += 1
            with contextlib.suppress(threading.BrokenBarrierError):
                barrier.wait(timeout=2)

        def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
            return True

    monkeypatch.setattr("netspy.utils.tools.load_object", lambda path: SlowPipeline, raising=True)

    buf = _buffer(pipelines=["fake.path.SlowPipeline"])

    results: list[list[Any]] = []
    results_lock = threading.Lock()

    def worker() -> None:
        pipelines = buf._resolve_pipelines(None)
        with results_lock:
            results.append(pipelines)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert construct_count["n"] == 1, f"管道被构造了 {construct_count['n']} 次，应为 1"
    assert len(results) == 2
    assert results[0] is results[1], "两个线程应该拿到同一份缓存的管道实例"


def test_item_filter_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "ITEM_FILTER_ENABLE", False)
    buf = _buffer()
    buf.put(NewsItem(url="u1", title="a"))
    buf.put(NewsItem(url="u1", title="a"))
    buf.flush()
    saved_rows = [r for _, rows in RecordingPipeline.saved for r in rows]
    assert len(saved_rows) == 2
