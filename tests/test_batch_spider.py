from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import fakeredis
import pytest
from pytest_httpserver import HTTPServer

from netspy import BatchSpider, Request, setting
from netspy.core import redis_scheduler
from netspy.core.batch_monitor import BatchMonitor
from netspy.core.batch_store import DONE, FAILED, MemoryBatchStore, MysqlBatchStore
from netspy.exceptions import SpiderError


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_scheduler, "get_redis", lambda url=None: client)
    monkeypatch.setattr("netspy.db.redisdb.get_redis", lambda url=None: client)
    yield client
    client.flushall()


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "DONE_CHECK_INTERVAL": 0.05,
        "DONE_CHECK_TIMES": 2,
        "BUFFER_FLUSH_INTERVAL": 0.02,
        "HEARTBEAT_INTERVAL": 0.05,
        "HEARTBEAT_STALE": 5.0,
        "RANDOM_USER_AGENT": False,
        "SPIDER_THREAD_COUNT": 3,
        "TASK_POLL_INTERVAL": 0.03,
        "TASK_EXHAUST_POLLS": 2,
        "BATCH_MONITOR_INTERVAL": 0.03,
        "BATCH_LOST_TASK_STALE": 100.0,
    }.items():
        monkeypatch.setattr(setting, name, value)


# ====================================================================== MemoryBatchStore
def test_memory_store_claim_count_mark() -> None:
    store = MemoryBatchStore([{"id": 1}, {"id": 2}, {"id": 3}])
    assert store.count_tasks().todo == 3

    claimed = store.claim_tasks(2)
    assert [c["id"] for c in claimed] == [1, 2]
    assert store.count_tasks().doing == 2

    store.mark_task(1, DONE)
    store.mark_task(2, FAILED)
    counts = store.count_tasks()
    assert (counts.done, counts.failed, counts.todo, counts.settled) == (1, 1, 1, 2)


def test_memory_store_reset_all_and_lost() -> None:
    store = MemoryBatchStore([{"id": 1}, {"id": 2}])
    store.claim_tasks(2)
    assert store.reset_lost_tasks(100.0) == 0  # 刚认领不算丢
    assert store.reset_lost_tasks(0.0) == 2  # stale=0 → 全部算丢
    assert store.count_tasks().todo == 2

    store.claim_tasks(1)
    store.mark_task(1, DONE)
    store.reset_all_tasks()
    assert store.count_tasks().todo == 2


def test_memory_store_batch_record_lifecycle() -> None:
    store = MemoryBatchStore([{"id": 1}])
    assert store.latest_batch() is None

    batch = store.create_batch("d1", 10, 7.0, "day")
    assert store.latest_batch() is not None
    assert store.latest_batch().id == batch.id and not store.latest_batch().is_done

    store.update_batch_counts(batch.id, done=3, failed=1, total=10)
    assert store.latest_batch().done_count == 3

    store.finish_batch(batch.id)
    assert store.latest_batch().is_done
    assert store.batch_age_seconds(batch.id) >= 0


def test_memory_store_custom_fields() -> None:
    store = MemoryBatchStore([{"pk": 1, "state": 0}], id_field="pk", state_field="state")
    assert store.claim_tasks(5)[0]["pk"] == 1
    assert store.count_tasks().doing == 1
    store.mark_task(1, DONE)
    assert store.count_tasks().done == 1


def test_memory_store_mark_task_not_lost_to_concurrent_reset_lost_tasks() -> None:
    """回归测试：worker 线程 mark_task(DONE) 和 monitor 线程 reset_lost_tasks()
    并发时，不该出现「任务其实做完了，却被判定成丢了、放回待处理重新抓一遍」。

    这是实测撞到过的真竞态，不是假设性的——`reset_lost_tasks` 原来跟 `mark_task`
    一样不加锁，「检查 state==DOING 且租约过期」和「写回 TODO」之间不是原子的。
    只要 worker 线程在这个窗口里把任务标成真正完成，无锁版本就会把它的 DONE
    覆盖回 TODO，跟 `claim_tasks` 那条注释警告的是同一类问题——GIL 只让窗口变窄，
    不会让它消失。MySQL 版没有这个问题：`UPDATE ... WHERE state=DOING` 是数据库侧
    的原子条件更新；内存版的检查和写是两条 Python 语句，必须自己上锁补上等价的原子性。

    用 monkeypatch 在 `self._touched.get(tid, now)` 判定通过之后、`reset_lost_tasks`
    真正写 TODO 之前强制暂停，把本来靠运气才能踩中的窗口变成确定性的。
    """
    store = MemoryBatchStore([{"id": 1}])
    store.claim_tasks(1)
    store._touched[1] = time.time() - 1000  # 让 reset_lost_tasks 判定它「丢了」

    paused = threading.Event()
    proceed = threading.Event()

    class _PausingTouched(dict):  # type: ignore[type-arg]
        """在 `self._touched.get(tid, now)` 这一步暂停——这是 reset_lost_tasks
        判定「丢了」的最后一步，暂停点之后紧跟着就是写 TODO。暂停期间让
        mark_task 并发跑一遍，验证它的写不会被随后的 TODO 覆盖掉。

        必须在这里暂停而不是在 `_state()` 里：`mark_task` 自己也会更新
        `self._touched`，如果在 `_state()` 处暂停，等 `reset_lost_tasks` 走到
        这一步再读 `self._touched` 时会读到 mark_task 刚写的新鲜时间戳，
        「租约过期」判定就会变成假，压根走不到写 TODO 那一步，竞态也就测不出来。
        """

        def get(self, key: object, default: object = None) -> object:  # type: ignore[override]
            value = super().get(key, default)
            if key == 1:
                paused.set()
                proceed.wait(timeout=2)
            return value

    store._touched = _PausingTouched(store._touched)  # type: ignore[assignment]

    monitor = threading.Thread(target=lambda: store.reset_lost_tasks(100.0))
    monitor.start()
    assert paused.wait(timeout=2), "monitor 线程没有走到预期的暂停点，测试前提不成立"

    store.mark_task(1, DONE)  # worker 线程：这条任务真的做完了
    proceed.set()
    monitor.join(timeout=2)

    assert store._tasks[0]["batch_status"] == DONE, (
        "mark_task 刚写的 DONE 被 reset_lost_tasks 冲掉了——"
        "任务明明做完了却被当成丢失，会被重新放回待处理抓一遍"
    )


# ====================================================================== BatchMonitor
def _drain_worker(
    redis: Any, key: str, store: MemoryBatchStore, *, until: int, retry_ids: set[int]
) -> None:
    """测试替身 worker：从 pending 取任务，标完成；retry_ids 里的 id 第一次遇到时假装崩溃。"""
    seen: dict[int, int] = {}
    deadline = time.time() + 5
    done = 0
    while done < until and time.time() < deadline:
        raw = redis.lpop(key, 20)
        for item in raw or []:
            task = json.loads(item)
            tid = task["id"]
            seen[tid] = seen.get(tid, 0) + 1
            if tid in retry_ids and seen[tid] == 1:
                continue  # 假装崩了，任务留在「处理中」
            store.mark_task(tid, DONE)
            done += 1
        time.sleep(0.01)


def test_monitor_run_once_completes_batch(fake_redis: Any) -> None:
    store = MemoryBatchStore([{"id": i} for i in range(4)])
    mon = BatchMonitor(
        store=store, redis=fake_redis, ns="netspy:M", batch_interval=1, monitor_interval=0.01
    )
    worker = threading.Thread(
        target=_drain_worker,
        args=(fake_redis, "netspy:M:batch_pending", store),
        kwargs={"until": 4, "retry_ids": set()},
    )
    worker.start()
    batch = mon.run_once()
    worker.join(timeout=5)

    assert batch.is_done and batch.done_count == 4
    assert store.count_tasks().done == 4
    assert fake_redis.get("netspy:M:batch_done") == "1"


def test_monitor_reprocesses_lost_task(fake_redis: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "BATCH_LOST_TASK_STALE", 0.02)
    store = MemoryBatchStore([{"id": 1}, {"id": 2}])
    mon = BatchMonitor(
        store=store,
        redis=fake_redis,
        ns="netspy:L",
        batch_interval=1,
        monitor_interval=0.02,
        lost_stale=0.02,
    )
    worker = threading.Thread(
        target=_drain_worker,
        args=(fake_redis, "netspy:L:batch_pending", store),
        kwargs={"until": 2, "retry_ids": {2}},  # 任务 2 第一次「崩溃」
    )
    worker.start()
    batch = mon.run_once()
    worker.join(timeout=5)

    assert batch.is_done
    assert store.count_tasks().done == 2  # 丢失的任务被重置后重跑成功


def test_monitor_empty_task_table_finishes_immediately(fake_redis: Any) -> None:
    mon = BatchMonitor(
        store=MemoryBatchStore([]), redis=fake_redis, ns="netspy:Z", batch_interval=1
    )
    batch = mon.run_once()
    assert batch.is_done


def test_monitor_lock_blocks_second_master(fake_redis: Any) -> None:
    store = MemoryBatchStore([{"id": 1}])

    def make() -> BatchMonitor:
        return BatchMonitor(store=store, redis=fake_redis, ns="netspy:LK", batch_interval=1)

    guard = make()._hold_lock()
    with pytest.raises(SpiderError, match="monitor 在运行"):
        make()._hold_lock()
    guard.__exit__()
    make()._hold_lock()  # 释放后可以再拿


def test_monitor_new_batch_waits_for_interval(fake_redis: Any) -> None:
    store = MemoryBatchStore([{"id": 1}])
    batch = store.create_batch("old", 1, interval=7.0, unit="day")
    store.finish_batch(batch.id)
    mon = BatchMonitor(
        store=store, redis=fake_redis, ns="netspy:W", batch_interval=7, interval_unit="day"
    )
    # 上一批刚结束、远没到 7 天 → 不开新批次
    assert mon._ensure_batch(force=False) is None
    # 强制（run_once 语义）→ 开
    assert mon._ensure_batch(force=True) is not None


# ====================================================================== BatchSpider 端到端
class DemoBatch(BatchSpider):
    def __init__(self, base: str, **kw: Any) -> None:
        self._base = base
        self.items: list[Any] = []
        super().__init__(item_handler=self.items.extend, **kw)

    def task_requests(self, task: dict[str, Any]) -> Iterator[Request]:
        yield Request(f"{self._base}/item/{task['id']}", callback=self.parse)

    def parse(self, request: Request, response: Any, task: Any = None) -> Iterator[Any]:
        yield {"id": task["id"], "title": response.css("h1::text").get()}
        self.update_task(task["id"], ok=True)


def _serve(server: HTTPServer) -> str:
    for i in range(20):
        server.expect_request(f"/item/{i}").respond_with_data(
            f"<html><h1>item {i}</h1></html>", content_type="text/html"
        )
    server.expect_request("/item/boom").respond_with_data("nope", status=500)
    return server.url_for("/").rstrip("/")


def test_batch_end_to_end(httpserver: HTTPServer, fake_redis: Any) -> None:
    base = _serve(httpserver)
    store = MemoryBatchStore([{"id": 1}, {"id": 2}, {"id": 3}])
    spider = DemoBatch(base, batch_store=store, redis_key="E2E", keep_alive=False)

    monitor = threading.Thread(target=lambda: spider.start_monitor(once=True))
    monitor.start()
    spider.start()  # worker：任务耗尽即退出
    monitor.join(timeout=15)

    assert not monitor.is_alive()
    assert sorted(d["id"] for d in spider.items) == [1, 2, 3]
    assert store.count_tasks().done == 3
    latest = store.latest_batch()
    assert latest is not None and latest.is_done and latest.done_count == 3
    assert fake_redis.get("netspy:E2E:batch_done") == "1"


def test_failed_request_marks_task_failed(httpserver: HTTPServer, fake_redis: Any) -> None:
    base = _serve(httpserver)
    store = MemoryBatchStore([{"id": "boom"}])

    class S(DemoBatch):
        __custom_setting__ = {"SPIDER_MAX_RETRY_TIMES": 1}

        def parse(self, request: Request, response: Any, task: Any = None) -> Iterator[Any]:
            if response.status_code != 200:
                raise ValueError("bad")
            yield from ()

    spider = S(base, batch_store=store, redis_key="FAIL", keep_alive=False)
    monitor = threading.Thread(target=lambda: spider.start_monitor(once=True))
    monitor.start()
    spider.start()
    monitor.join(timeout=15)

    assert not monitor.is_alive()
    assert store.count_tasks().failed == 1
    latest = store.latest_batch()
    assert latest is not None and latest.is_done and latest.fail_count == 1


def test_task_auto_injected_into_cb_kwargs(fake_redis: Any) -> None:
    spider = DemoBatch("http://x", batch_store=MemoryBatchStore([]), redis_key="CB")
    reqs = list(spider._task_requests_tagged({"id": 9, "extra": "x"}))
    assert reqs[0].cb_kwargs["task"] == {"id": 9, "extra": "x"}


# ====================================================================== MysqlBatchStore SQL 形状
class FakeDB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.query_returns: dict[str, list[dict[str, Any]]] = {}
        self.insert_id = 7

    def query(self, sql: str, args: Any = None) -> list[dict[str, Any]]:
        self.calls.append(("query", sql, args))
        for sub, rows in self.query_returns.items():
            if sub in sql:
                return rows
        return []

    def execute(self, sql: str, args: Any = None) -> int:
        self.calls.append(("execute", sql, args))
        return 1

    def insert(self, sql: str, args: Any = None) -> int:
        self.calls.append(("insert", sql, args))
        return self.insert_id

    def close(self) -> None:
        self.calls.append(("close", "", None))

    # 认领改成了「一个事务里 SELECT ... FOR UPDATE + UPDATE」，假 DB 也要跟上
    @contextmanager
    def transaction(self) -> Iterator[FakeDB]:
        self.calls.append(("begin", "", None))
        yield self
        self.calls.append(("commit", "", None))

    @contextmanager
    def cursor(self) -> Iterator[FakeCursor]:
        yield FakeCursor(self)


class FakeCursor:
    def __init__(self, db: FakeDB) -> None:
        self._db = db
        self._rows: list[dict[str, Any]] = []

    def execute(self, sql: str, args: Any = None) -> None:
        self._db.calls.append(("execute", sql, args))
        self._rows = []
        for sub, rows in self._db.query_returns.items():
            if sub in sql:
                self._rows = rows

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


def test_mysql_store_claim_locks_rows_in_one_transaction() -> None:
    """认领必须在**一个事务**里、且带 `FOR UPDATE`。

    这只是个快速回归闸：假 DB 验证不了数据库到底会不会锁行。真正的保证
    由 `tests/test_batch_store_integration.py` 在真 MySQL 上给出 ——
    这个用例存在的意义仅仅是「有人把 FOR UPDATE 删了能立刻发现」，
    不需要起数据库。

    （它的上一版断言的恰恰是**有 bug 的那个行为**：「SELECT 然后 UPDATE」两条
    独立语句。SQL 字符串对了不等于并发下对，这是第二次栽在同一件事上了。）
    """
    db = FakeDB()
    db.query_returns["SELECT * FROM `crawl_task`"] = [{"id": 5}, {"id": 6}]
    rows = MysqlBatchStore("crawl_task", db=db).claim_tasks(10)

    assert rows == [{"id": 5}, {"id": 6}]
    kinds = [c[0] for c in db.calls]
    assert kinds == ["begin", "execute", "execute", "commit"], f"认领不在一个事务里：{kinds}"
    select_sql = db.calls[1][1]
    assert "FOR UPDATE" in select_sql, "SELECT 没有锁行，多个 worker 会领到同一批任务"
    assert "WHERE `batch_status`=0 LIMIT %s" in select_sql
    assert "SET `batch_status`=2 WHERE `id` IN (%s, %s)" in db.calls[2][1]
    assert db.calls[2][2] == (5, 6)


def test_mysql_store_count_tasks_aggregates_by_state() -> None:
    db = FakeDB()
    db.query_returns["GROUP BY"] = [{"s": 0, "c": 3}, {"s": 1, "c": 5}, {"s": -1, "c": 1}]
    counts = MysqlBatchStore("t", db=db).count_tasks()
    assert (counts.total, counts.todo, counts.done, counts.failed) == (9, 3, 5, 1)


def test_mysql_store_create_batch_uses_insert_id() -> None:
    db = FakeDB()
    db.insert_id = 42
    record = MysqlBatchStore("t", db=db).create_batch("2026-09-04 10:00:00", 100, 7.0, "day")
    assert record.id == 42 and record.total_count == 100 and record.is_done is False
    assert db.calls[0][0] == "insert"


def test_mysql_store_custom_fields_in_sql() -> None:
    db = FakeDB()
    store = MysqlBatchStore("t", db=db, id_field="tid", state_field="st", time_field="mt")
    store.mark_task(9, DONE)
    assert db.calls[-1][1] == "UPDATE `t` SET `st`=%s WHERE `tid`=%s"
    assert db.calls[-1][2] == (DONE, 9)

    store.reset_lost_tasks(600)
    assert "WHERE `st`=2 AND `mt` < DATE_SUB(NOW(), INTERVAL %s SECOND)" in db.calls[-1][1]


def test_mysql_store_ensure_schema_creates_record_table() -> None:
    db = FakeDB()
    MysqlBatchStore("crawl_task", db=db).ensure_schema()
    assert "CREATE TABLE IF NOT EXISTS `crawl_task_batch_record`" in db.calls[0][1]


def test_mysql_store_reset_all_and_finish() -> None:
    db = FakeDB()
    store = MysqlBatchStore("t", db=db)
    store.reset_all_tasks()
    assert db.calls[-1][1] == "UPDATE `t` SET `batch_status`=0"
    store.finish_batch(3)
    assert "SET `is_done`=1" in db.calls[-1][1] and db.calls[-1][2] == (3,)


# ====================================================================== master 锁的续期
def test_renew_does_not_steal_back_an_expired_lock(fake_redis: Any) -> None:
    """续期必须校验「锁还是不是我的」。

    ``_renew_lock`` 原来是无条件 SET —— 于是：

    1. master A 一次 tick 超过锁的 TTL，锁过期
    2. master B 用 SET NX 合法拿到锁
    3. **A 下一次续期把 key 覆盖成自己的 node_id，把锁从 B 手里抢回来**
    4. A 和 B 都认为自己持有锁，同时开始认领任务

    也就是说互斥不是「偶尔失效」，而是一旦超时一次就**永久破掉**（两个 master
    此后互相覆盖）。这正是 `claim_tasks` 的竞态在生产里被触发的途径。

    释放路径本来就校验了持有者（`__exit__` 里的 `get == node_id`），
    续期这边漏了 —— 同一份代码里的两半，一半对一半不对。
    """
    from netspy.core.batch_monitor import BatchMonitor

    monitor = BatchMonitor(
        store=MemoryBatchStore([{"id": 1}]),
        redis=fake_redis,
        ns="netspy:LOCK",
        batch_interval=1.0,
        push_limit=10,
        monitor_interval=0.01,
    )
    key = "netspy:LOCK:batch_monitor_lock"

    fake_redis.set(key, "master-B", ex=60)  # B 是当前合法持有者

    # 丢了锁的 master 必须**停下来**，而不是继续认领任务
    with pytest.raises(SpiderError, match="易主"):
        monitor._renew_lock()

    assert fake_redis.get(key) == "master-B", "续期把别人的锁抢过来了 —— 互斥形同虚设"


def test_renew_keeps_the_lock_when_we_still_hold_it(fake_redis: Any) -> None:
    from netspy.core.batch_monitor import BatchMonitor

    monitor = BatchMonitor(
        store=MemoryBatchStore([{"id": 1}]),
        redis=fake_redis,
        ns="netspy:LOCK2",
        batch_interval=1.0,
        push_limit=10,
        monitor_interval=0.01,
    )
    key = "netspy:LOCK2:batch_monitor_lock"
    fake_redis.set(key, monitor._node_id, ex=1)

    monitor._renew_lock()

    assert fake_redis.get(key) == monitor._node_id
    assert fake_redis.ttl(key) > 1, "续期没有把 TTL 推回去"
