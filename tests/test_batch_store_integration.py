"""``MysqlBatchStore`` 在**真 MySQL**下的并发正确性（v4.6）。

此前 `MysqlBatchStore` 从没碰过真数据库 —— 测的是 SQL 字符串形状
（``assert "WHERE `batch_status`=0 LIMIT %s" in ...``），那只验证了
「我写出了我想写的字符串」，验证不了数据库认不认、更验证不了并发下对不对。

而 `claim_tasks` 正是多 worker 抢任务的核心。真库实测（改之前）：
200 个任务、6 个 worker → **认领 1200 次，每个任务都被 6 个 worker 各领了一遍**。
"""

from __future__ import annotations

import os
import threading
from collections import Counter
from collections.abc import Iterator
from typing import Any

import pytest

from netspy.core.batch_store import DOING, DONE, TODO, MysqlBatchStore

pytestmark = pytest.mark.integration

TABLE = "mw_batch_claim_test"


@pytest.fixture
def task_table(mysql_db: Any) -> Iterator[Any]:
    mysql_db.execute(f"DROP TABLE IF EXISTS `{TABLE}`")
    # 照 docs/batch-spider.md 里那份表结构建 —— 少一列 update_time，
    # master 的丢失任务检测就会直接报 Unknown column
    mysql_db.execute(
        f"""CREATE TABLE `{TABLE}` (
            `id` INT NOT NULL,
            `batch_status` TINYINT NOT NULL DEFAULT 0,
            `update_time` DATETIME NOT NULL
                DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            INDEX(`batch_status`)
        ) ENGINE=InnoDB"""
    )
    yield mysql_db
    mysql_db.execute(f"DROP TABLE IF EXISTS `{TABLE}`")


def _fill(db: Any, n: int) -> None:
    db.executemany(
        f"INSERT INTO `{TABLE}` (id, batch_status) VALUES (%s, %s)",
        [(i, TODO) for i in range(n)],
    )


# ---- 基本行为：先确认真库上跑得通 ------------------------------------
def test_claim_marks_rows_doing(task_table: Any) -> None:
    _fill(task_table, 5)
    store = MysqlBatchStore(TABLE, db=task_table)

    rows = store.claim_tasks(3)

    assert len(rows) == 3
    states = task_table.query(f"SELECT batch_status FROM `{TABLE}` WHERE batch_status=%s", (DOING,))
    assert len(states) == 3, "认领后数据库里的状态没有真的变成「处理中」"


def test_mark_task_writes_back(task_table: Any) -> None:
    _fill(task_table, 3)
    store = MysqlBatchStore(TABLE, db=task_table)
    store.mark_task(1, DONE)

    row = task_table.query(f"SELECT batch_status FROM `{TABLE}` WHERE id=1")[0]
    assert row["batch_status"] == DONE


def test_count_tasks_matches_reality(task_table: Any) -> None:
    _fill(task_table, 10)
    store = MysqlBatchStore(TABLE, db=task_table)
    store.claim_tasks(4)
    store.mark_task(0, DONE)

    counts = store.count_tasks()
    assert counts.total == 10
    assert counts.done == 1


# ---- 核心：并发认领不能重复 ------------------------------------------
def test_concurrent_claim_never_hands_out_the_same_task_twice(
    task_table: Any, mysql_url: str
) -> None:
    """六个 worker 同时抢：每个任务只能被认领一次。

    这是 `BatchSpider` 「任务防丢」承诺的底座。改之前 `claim_tasks` 是
    非原子的「SELECT 然后 UPDATE」，六个 worker 会 SELECT 到同一批行 ——
    实测 200 个任务被认领 1200 次，目标站等于挨了 6 倍流量。

    判据是可数的事实：所有 worker 认领到的 id 加起来有没有重复。
    """
    from netspy.db.mysqldb import MysqlDB

    n_tasks, n_workers, batch = 200, 6, 10
    _fill(task_table, n_tasks)

    claimed: list[int] = []
    lock = threading.Lock()
    errors: list[BaseException] = []

    def worker() -> None:
        # 每个 worker 一条独立连接 —— 共用一条的话根本没有并发可言
        db = MysqlDB.from_url(mysql_url)
        try:
            store = MysqlBatchStore(TABLE, db=db)
            while True:
                rows = store.claim_tasks(batch)
                if not rows:
                    return
                with lock:
                    claimed.extend(r["id"] for r in rows)
        except BaseException as exc:
            errors.append(exc)
        finally:
            db.close()

    threads = [threading.Thread(target=worker) for _ in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"worker 线程抛异常：{errors[0]!r}"
    dupes = {k: v for k, v in Counter(claimed).items() if v > 1}
    assert not dupes, (
        f"{len(dupes)} 个任务被重复认领（最多被领 {max(dupes.values())} 次）—— "
        f"同一批 URL 会被抓这么多遍"
    )
    assert len(claimed) == n_tasks, f"认领了 {len(claimed)} 个，应为 {n_tasks} 个（有遗漏）"


# ======================================================================
# 端到端：真 MySQL 任务表 + 真 Redis 队列 + 真 master / worker 分离
#
# 既有的端到端用例跑在 fakeredis + 单进程里 —— master 和 worker 是同一个进程的
# 两个线程。而 BatchSpider 的设计就是两个独立进程，「任务防丢」这个承诺只有在
# 真正分开跑的时候才有意义。
# ======================================================================
_PROC_TIMEOUT = 90
N_TASKS = 20


def _worker(base: str, mysql_url: str, redis_url: str, table: str, key: str) -> None:
    """一个 worker 进程：从共享队列取任务、抓取、回写状态。"""
    import netspy as ns
    from netspy import setting
    from netspy.core.batch_store import MysqlBatchStore
    from netspy.core.spiders.batch_spider import BatchSpider
    from netspy.db.mysqldb import MysqlDB
    from netspy.utils import log

    setting.REDIS_URL = redis_url
    setting.ITEM_PIPELINES = []
    setting.LOG_LEVEL = "CRITICAL"
    setting.DONE_CHECK_INTERVAL = 0.2
    setting.DONE_CHECK_TIMES = 3
    setting.SPIDER_THREAD_COUNT = 2
    setting.RANDOM_USER_AGENT = False
    setting.ROBOTS_OBEY = False
    setting.CIRCUIT_FAILURE_THRESHOLD = 0
    log.configure()

    class Node(BatchSpider):
        def task_requests(self, task: dict[str, Any]) -> Any:
            yield ns.Request(f"{base}/item/{task['id']}", callback=self.parse)

        def parse(self, request: Any, response: Any, task: Any = None) -> Any:
            self.update_task(task["id"], ok=True)
            return None

    Node(
        redis_key=key,
        batch_store=MysqlBatchStore(table, db=MysqlDB.from_url(mysql_url)),
        keep_alive=False,
    ).start()


def _master(base: str, mysql_url: str, redis_url: str, table: str, key: str) -> None:
    """master 进程：认领任务、往队列里灌。"""
    import netspy as ns
    from netspy import setting
    from netspy.core.batch_store import MysqlBatchStore
    from netspy.core.spiders.batch_spider import BatchSpider
    from netspy.db.mysqldb import MysqlDB
    from netspy.utils import log

    setting.REDIS_URL = redis_url
    setting.LOG_LEVEL = "CRITICAL"
    log.configure()

    class Node(BatchSpider):
        def task_requests(self, task: dict[str, Any]) -> Any:
            yield ns.Request(f"{base}/item/{task['id']}", callback=self.parse)

        def parse(self, request: Any, response: Any, task: Any = None) -> Any:
            return None

    Node(
        redis_key=key,
        batch_store=MysqlBatchStore(table, db=MysqlDB.from_url(mysql_url)),
        keep_alive=False,
    ).start_monitor(once=True)


def test_master_worker_split_across_real_processes(
    task_table: Any, mysql_url: str, redis_url: str, httpserver: Any
) -> None:
    """一个 master + 两个 worker，各自独立进程：每个任务恰好被抓一次。

    **判据取自 HTTP 靶子的命中次数**，不是问框架自己完成了多少 —— 认领重复时
    框架自身的计数看不出异常（它以为那些都是正常任务），只有靶子知道同一个 URL
    被打了几遍。
    """
    import multiprocessing as mp

    import redis as redis_lib

    rc = redis_lib.from_url(redis_url, decode_responses=True)
    rc.flushdb()
    _fill(task_table, N_TASKS)
    key = f"BATCH{os.getpid()}"

    with mp.Manager() as mgr:
        hits: Any = mgr.list()

        def handler(request: Any) -> Any:
            from werkzeug.wrappers import Response as WResponse

            hits.append(request.path)
            return WResponse("<html><h1>ok</h1></html>", content_type="text/html")

        for i in range(N_TASKS):
            httpserver.expect_request(f"/item/{i}").respond_with_handler(handler)
        base = httpserver.url_for("/").rstrip("/")

        procs = [
            mp.Process(target=_master, args=(base, mysql_url, redis_url, TABLE, key)),
            *(
                mp.Process(target=_worker, args=(base, mysql_url, redis_url, TABLE, key))
                for _ in range(2)
            ),
        ]
        for p in procs:
            p.start()
        for p in procs:
            p.join(timeout=_PROC_TIMEOUT)
        alive = [p for p in procs if p.is_alive()]
        for p in alive:
            p.terminate()
            p.join(timeout=5)
        counted = Counter(list(hits))

    rc.flushdb()
    rc.close()
    assert not alive, f"{len(alive)} 个进程没有自行退出"
    assert len(counted) == N_TASKS, f"漏抓：只见到 {len(counted)}/{N_TASKS} 个任务"
    dupes = {p: c for p, c in counted.items() if c != 1}
    assert not dupes, f"同一个任务被抓了多遍：{dupes}"
