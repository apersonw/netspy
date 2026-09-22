"""批次任务标「已完成」不能早于数据落库。

文档推荐的写法是：

    def parse(self, request, response, task):
        yield {"url": ..., "title": ...}          # item 进内存缓冲
        self.update_task(task["id"], ok=True)     # 立刻标 DONE

真 MySQL 实测（5 个任务照这个写法处理完、缓冲区未 flush）：任务表 **DONE 5 个**，
实际落库 **0 行**。而 `reset_lost_tasks` 只回收「处理中」—— 标了 DONE 的任务
永远不会被重跑，批次报告 100% 完成而数据一行没有。

这是 v4.14「销账早于落库」在批次任务表上的化身：v4.14 修的是请求队列的销账，
批次状态是另一个状态存储，同一个洞原样还在。
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from netspy.buffer.item_buffer import ItemBuffer
from netspy.core import context
from netspy.core.batch_store import MemoryBatchStore
from netspy.core.spiders.batch_spider import BatchSpider
from netspy.dedup import Dedup
from netspy.network.request import Request
from netspy.pipelines.base import BasePipeline
from netspy.utils.stats import Stats


class _Pipeline(BasePipeline):
    rows: list[dict[str, Any]] = []

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        _Pipeline.rows.extend(items)
        return True


class _Refusing(BasePipeline):
    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        return False


def _spider(store: Any) -> Any:
    """只借 update_task 这一个方法 —— 起整个 BatchSpider 要连 Redis 和 MySQL。"""
    obj = type("_S", (), {})()
    obj._store = store
    obj.update_task = BatchSpider.update_task.__get__(obj)
    return obj


@pytest.fixture(autouse=True)
def _reset(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Any:
    _Pipeline.rows = []
    from netspy import setting

    monkeypatch.setattr(setting, "ITEM_FILTER_ENABLE", False)
    # 别把 dump 写进仓库根目录
    monkeypatch.setattr(setting, "FAILED_ITEM_PATH", str(tmp_path / "failed_items.jsonl"))
    yield
    context.set_current(None, None)


def _buffer(pipeline: str) -> ItemBuffer:
    return ItemBuffer(Stats(), pipelines=[pipeline], dedup=Dedup(filter_type="lite"))


def test_done_waits_for_the_data_to_land() -> None:
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    buf = _buffer(f"{__name__}._Pipeline")
    spider = _spider(store)
    request = Request("http://example.com/p/1")

    context.set_current(request, buf)
    buf.put({"url": "http://example.com/p/1"}, owner=request)
    spider.update_task(1, ok=True)

    assert store.count_tasks().done == 0, (
        "数据还在内存缓冲里，任务就被标成了已完成 —— "
        "节点这时一死，防丢机制只回收「处理中」，这个任务永远不会被重跑"
    )
    buf.flush()
    assert store.count_tasks().done == 1, "落库了却没标 DONE —— 任务会被无限重跑"
    assert len(_Pipeline.rows) == 1


def test_task_without_items_is_marked_immediately() -> None:
    """没产出 item 的请求要立刻标 —— 否则永远等不到落库那一刻。"""
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    buf = _buffer(f"{__name__}._Pipeline")
    spider = _spider(store)

    context.set_current(Request("http://example.com/p/1"), buf)
    spider.update_task(1, ok=True)
    assert store.count_tasks().done == 1


def test_marking_failed_is_never_deferred() -> None:
    """标失败不取决于数据落没落库。"""
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    buf = _buffer(f"{__name__}._Pipeline")
    spider = _spider(store)
    request = Request("http://example.com/p/1")

    context.set_current(request, buf)
    buf.put({"url": "x"}, owner=request)
    spider.update_task(1, ok=False)
    assert store.count_tasks().failed == 1


def test_after_persist_if_pending_excludes_concurrent_flush() -> None:
    """回归测试：`ItemBuffer.after_persist_if_pending()` 判断「owner 还在不在
    pending 里」和登记「落库后回调」必须是同一次加锁做完的原子操作。

    这是实测复现出来的真竞态，不是假设性的——`BatchSpider.update_task()`
    原来是先调 `buffer.owns(request)` 查一遍，再独立加锁调
    `buffer.after_persist(...)` 登记钩子。这两次调用之间没有锁保护：只要
    `flush()` 恰好插在中间跑完，这一批（连同这个 owner）已经处理完、
    `_run_after_persist` 也跑过了（pop 到空，什么都没做）——等 `after_persist`
    真正登记上钩子时已经没人会再触发它了，`mark_task(DONE)` 永远不会被调用，
    任务卡在「处理中」直到租约到期才被重新捞回来重跑一遍。

    自然的多线程压力测试测不出这个窗口——`put()` 和紧跟着的检查之间都是纯
    Python 操作，没有 I/O 让出 GIL，运气好的话线程调度永远踩不中那几条字节码
    宽的窗口。所以这里直接验证根本的互斥性质：`after_persist_if_pending`
    持锁期间，并发的 `flush()` 必须被真正挡住，不能抢跑。
    """
    calls: list[Any] = []
    buf = ItemBuffer(Stats(), handler=lambda items: calls.append(list(items)))
    owner = object()
    buf.put({"x": 1}, owner=owner)

    entered = threading.Event()
    release = threading.Event()
    real_lock = buf._lock

    class _PausingLock:
        def __enter__(self) -> None:
            real_lock.acquire()
            entered.set()
            release.wait(timeout=2)

        def __exit__(self, *exc: object) -> None:
            real_lock.release()

    buf._lock = _PausingLock()  # type: ignore[assignment]

    hook_fired = threading.Event()
    registered: dict[str, bool] = {}

    def call_after_persist() -> None:
        registered["ok"] = buf.after_persist_if_pending(owner, lambda: hook_fired.set())

    t1 = threading.Thread(target=call_after_persist)
    t1.start()
    assert entered.wait(timeout=2), "没能进入临界区，测试前提不成立"

    flush_done = threading.Event()

    def call_flush() -> None:
        buf.flush()
        flush_done.set()

    t2 = threading.Thread(target=call_flush)
    t2.start()
    assert not flush_done.wait(timeout=0.3), (
        "flush() 在 after_persist_if_pending 还持锁的时候就跑完了——"
        "两者没有互斥，钩子随时可能被冲掉而永远不会执行"
    )

    release.set()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert registered.get("ok") is True
    assert flush_done.is_set()
    assert hook_fired.is_set(), "钩子应该在 flush 完成之后被正确执行"


def test_update_task_does_not_race_with_flush() -> None:
    """集成回归测试：确认 `BatchSpider.update_task` 走的是原子路径，
    「确认还在 pending 里」和「登记落库回调」之间不露没上锁的空档。

    用 monkeypatch 在 `owns()` 判定为真之后暂停：如果 `update_task` 退化回
    旧的 `owns()` + `after_persist()` 两步式写法，会精确卡在这个空档上，
    这时候插一次 `flush()` 进去，钩子就该再也不会被触发——最终任务
    标不上 DONE。走的是原子路径（`after_persist_if_pending`）的话，
    `update_task` 压根不会调 `owns()`，这个 monkeypatch 不会被触发，
    行为应该照常。
    """
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    buf = _buffer(f"{__name__}._Pipeline")
    spider = _spider(store)
    request = Request("http://example.com/p/1")

    buf.put({"url": "x"}, owner=request)  # put 本身跟 context 无关，主线程调没问题

    paused = threading.Event()
    proceed = threading.Event()
    orig_owns = buf.owns

    def patched_owns(owner: Any) -> bool:
        result = orig_owns(owner)
        if result:
            paused.set()
            proceed.wait(timeout=2)
        return result

    buf.owns = patched_owns  # type: ignore[method-assign]

    def worker() -> None:
        # context 是线程本地的（worker 就是多线程，这是故意的），必须在
        # 真正调用 update_task 的这个线程里设置，跟 ParserWorker 的真实用法一致——
        # 否则 update_task 会因为「查不到当前请求」直接走立即写路径，
        # 这个测试就测不到「查还在不在」和「登记回调」之间的空档了
        context.set_current(request, buf)
        try:
            spider.update_task(1, ok=True)
        finally:
            context.set_current(None, None)

    t = threading.Thread(target=worker)
    t.start()
    hit_old_path = paused.wait(timeout=0.3)
    if hit_old_path:
        buf.flush()  # 抢在旧路径的空档里跑一次 flush
        proceed.set()
    t.join(timeout=2)

    buf.flush()  # 收尾：如果走的是新路径，这里才是真正落库的地方
    assert store.count_tasks().done == 1, (
        "任务的 DONE 回调丢了——update_task 在「查还在不在」和「登记回调」"
        "之间露出了没上锁的空档，被并发的 flush() 抢先处理掉了"
    )


def test_no_request_context_marks_immediately() -> None:
    """master 那边不在请求上下文里调用，不能因此卡住。"""
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    spider = _spider(store)
    spider.update_task(1, ok=True)
    assert store.count_tasks().done == 1


def test_dumped_write_still_marks_the_task_done() -> None:
    """写库失败的数据会 dump 到 `failed_items.jsonl` —— 那也是持久介质，任务照样算完成。

    这条沿用 v4.14 定下的规则（「dump 也算落到了持久介质，可以销账」）。
    两套机制必须对「完成」用同一个定义，否则请求队列已经销账、批次任务却还挂着，
    谁也说不清这个批次到底跑没跑完。

    代价是批次报告完成时，有 N 行躺在 dump 文件里而不是库里 ——
    换来的是不必为「库抖了一下」把整页重抓一遍。框架会记 error，
    `netspy retry --items` 可回放。
    """
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    buf = _buffer(f"{__name__}._Refusing")
    spider = _spider(store)
    request = Request("http://example.com/p/1")

    context.set_current(request, buf)
    buf.put({"url": "x"}, owner=request)
    spider.update_task(1, ok=True)
    buf.flush()
    assert store.count_tasks().done == 1


def test_worker_actually_sets_the_context() -> None:
    """上面几条都是自己 set_current 之后测机制 —— 那验不到「worker 有没有接上」。

    第一次反向验证就栽在这里：把 worker 里设置上下文那行破坏掉，5 条用例照样全绿。
    这条走真实的 AirSpider 路径，从用户回调里面回头看上下文。
    """
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    import netspy as mw
    from netspy import setting
    from netspy.utils import log

    class _T(BaseHTTPRequestHandler):
        def log_message(self, *a: Any) -> None:
            return None

        def do_GET(self) -> None:
            body = b"<html><body><h1>ok</h1></body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _T)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    seen: list[tuple[Any, Any]] = []

    setting.LOG_LEVEL = "CRITICAL"
    setting.SPIDER_THREAD_COUNT = 1
    setting.ROBOTS_OBEY = False
    setting.ITEM_PIPELINES = []
    log.configure()

    class _Spider(mw.AirSpider):
        def start_requests(self) -> Any:
            yield mw.Request(f"http://127.0.0.1:{port}/p/1", callback=self.parse_page)

        def parse_page(self, request: Any, response: Any) -> Any:
            seen.append((context.get_current_request(), context.get_current_item_buffer()))
            return None

    try:
        _Spider().start()
    finally:
        server.shutdown()

    assert seen, "回调没被调用，这个用例什么都没验到"
    current_request, current_buffer = seen[0]
    assert current_request is not None, "worker 没有设置当前请求上下文 —— update_task 会退回立刻写"
    assert current_request.url.endswith("/p/1")
    assert current_buffer is not None, "上下文里没有 item 缓冲，无法把标 DONE 推迟到落库之后"
