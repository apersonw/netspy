"""BatchSpider 的「全特性同开」跑批：master + worker + 真 MySQL + 真 Redis。

`test_batch_store_integration.py` 已经验了 master/worker 拆分与「每个任务恰好
被抓一次」，但那条用例把管道关掉了（`ITEM_PIPELINES = []`），也没有失败路径。
这里补两件它没覆盖的：

1. **整链**：抓取 → 解析 → ItemBuffer → 真 MySQL 数据表。
2. **失败任务的归宿**。这是数据完整性问题，而且这个项目栽过：0.13.3 修的正是
   「批次任务落库后才标完成」——在那之前，5 个任务全标 DONE、真库落 0 行。
   一个页面永远 500 的任务，最后必须落在**失败**，不能：
   - 被谎报成「完成」（任务被消耗掉，什么都没抓到，防丢机制只回收「处理中」，
     标了完成的任务永远不会重跑）
   - 卡在「处理中」（那会被 master 反复回收、无限重试）

worker / master 函数与 Spider 类必须在**模块顶层**：macOS 默认 spawn。
"""

from __future__ import annotations

import multiprocessing as mp
from collections.abc import Iterator
from typing import Any

import pytest

pytestmark = pytest.mark.integration

TASK_TABLE = "bfs_tasks"
DATA_TABLE = "bfs_items"
N_TASKS = 6
BAD_TASK = 4  # 这个任务的页面永远 500
_PROC_TIMEOUT = 120


def _task_url(base: str, task_id: int) -> str:
    return f"{base}/boom/500" if task_id == BAD_TASK else f"{base}/item/{task_id}"


@pytest.fixture
def clean_redis(redis_url: str) -> Iterator[str]:
    import redis as redis_lib

    client = redis_lib.from_url(redis_url, decode_responses=True)
    client.flushdb()
    yield redis_url
    client.flushdb()
    client.close()


@pytest.fixture
def tables(mysql_db: Any) -> Iterator[Any]:
    for name in (TASK_TABLE, DATA_TABLE):
        mysql_db.execute(f"DROP TABLE IF EXISTS `{name}`")
    # 照 docs/batch-spider.md 的表结构 —— 少一列 update_time，master 的
    # 丢失任务检测会直接报 Unknown column
    mysql_db.execute(
        f"""CREATE TABLE `{TASK_TABLE}` (
            `id` INT NOT NULL,
            `batch_status` TINYINT NOT NULL DEFAULT 0,
            `update_time` DATETIME NOT NULL
                DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`), INDEX(`batch_status`)
        ) ENGINE=InnoDB"""
    )
    mysql_db.execute(
        f"""CREATE TABLE `{DATA_TABLE}` (
            `url` VARCHAR(255) NOT NULL,
            `title` VARCHAR(255),
            PRIMARY KEY (`url`)
        ) ENGINE=InnoDB"""
    )
    mysql_db.executemany(
        f"INSERT INTO `{TASK_TABLE}` (id, batch_status) VALUES (%s, %s)",
        [(i, 0) for i in range(N_TASKS)],
    )
    yield mysql_db
    for name in (TASK_TABLE, DATA_TABLE):
        mysql_db.execute(f"DROP TABLE IF EXISTS `{name}`")


def _common_setting(mysql_url: str, redis_url: str) -> None:
    from netspy import setting
    from netspy.utils import log

    setting.reload()
    setting.REDIS_URL = redis_url
    setting.LOG_LEVEL = "CRITICAL"
    setting.DONE_CHECK_INTERVAL = 0.2
    setting.DONE_CHECK_TIMES = 3
    setting.SPIDER_THREAD_COUNT = 2
    setting.ROBOTS_OBEY = False
    # 熔断保持开着（默认阈值 10）—— 见 test_full_stack_integration 的说明
    setting.SPIDER_MAX_RETRY_TIMES = 1
    setting.DEDUP_FILTER = "redis"  # 多进程共享，见 test_distributed_full_stack
    from urllib.parse import urlparse

    parsed = urlparse(mysql_url)
    setting.MYSQL_HOST = parsed.hostname or "127.0.0.1"
    setting.MYSQL_PORT = parsed.port or 3306
    setting.MYSQL_USER = parsed.username or "root"
    setting.MYSQL_PASSWORD = parsed.password or ""
    setting.MYSQL_DB = (parsed.path or "/netspy").lstrip("/")
    log.configure()


def _worker(base: str, mysql_url: str, redis_url: str, key: str) -> None:
    import netspy as ns
    from netspy import setting
    from netspy.core.batch_store import MysqlBatchStore
    from netspy.core.spiders.batch_spider import BatchSpider
    from netspy.db.mysqldb import MysqlDB

    _common_setting(mysql_url, redis_url)
    setting.ITEM_PIPELINES = ["netspy.pipelines.mysql.MysqlPipeline"]

    class Row(ns.Item):
        __table_name__ = DATA_TABLE
        __unique_key__ = ["url"]
        url: str = ""
        title: str = ""

    class Node(BatchSpider):
        def task_requests(self, task: dict[str, Any]) -> Any:
            yield ns.Request(_task_url(base, task["id"]), callback=self.parse)

        def parse(self, request: Any, response: Any, task: Any = None) -> Any:
            title = response.css("h1.t::text").get()
            if title:
                yield Row(url=request.url, title=title)
            self.update_task(task["id"], ok=True)

    Node(
        redis_key=key,
        batch_store=MysqlBatchStore(TASK_TABLE, db=MysqlDB.from_url(mysql_url)),
        keep_alive=False,
    ).start()


def _master(base: str, mysql_url: str, redis_url: str, key: str) -> None:
    import netspy as ns
    from netspy import setting
    from netspy.core.batch_store import MysqlBatchStore
    from netspy.core.spiders.batch_spider import BatchSpider
    from netspy.db.mysqldb import MysqlDB

    _common_setting(mysql_url, redis_url)
    setting.ITEM_PIPELINES = []

    class Node(BatchSpider):
        def task_requests(self, task: dict[str, Any]) -> Any:
            yield ns.Request(_task_url(base, task["id"]), callback=self.parse)

        def parse(self, request: Any, response: Any, task: Any = None) -> Any:
            return None

    Node(
        redis_key=key,
        batch_store=MysqlBatchStore(TASK_TABLE, db=MysqlDB.from_url(mysql_url)),
        keep_alive=False,
    ).start_monitor(once=True)


def test_master_worker_full_chain_and_failed_task_is_marked_failed(
    tables: Any, mysql_url: str, clean_redis: str
) -> None:
    """一个 master + 两个 worker：数据进真库，失败任务落在「失败」。"""
    from integration_site import Site

    ctx = mp.get_context("spawn")
    with Site(n_items=N_TASKS) as site:
        master = ctx.Process(target=_master, args=(site.url, mysql_url, clean_redis, "bfs"))
        master.start()
        master.join(timeout=_PROC_TIMEOUT)

        workers = [
            ctx.Process(target=_worker, args=(site.url, mysql_url, clean_redis, "bfs"))
            for _ in range(2)
        ]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=_PROC_TIMEOUT)
        alive = [w for w in workers if w.is_alive()]
        for w in alive:
            w.kill()
        hits = dict(site.hits)

    assert not alive, "有 worker 没退出（挂起必须判失败）"

    states = {
        r["id"]: r["batch_status"]
        for r in tables.query(f"SELECT id, batch_status FROM `{TASK_TABLE}`")
    }

    # 失败任务：必须是「失败」，不能被谎报成完成、也不能卡在处理中
    assert states[BAD_TASK] == -1, (
        f"页面永远 500 的任务落在状态 {states[BAD_TASK]}："
        "1=被谎报成完成（任务被消耗掉却什么都没抓到，且永远不会重跑）、"
        "2=卡在处理中（会被 master 反复回收、无限重试）"
    )

    # 其余任务：全部完成
    for tid in range(N_TASKS):
        if tid != BAD_TASK:
            assert states[tid] == 1, f"任务 {tid} 停在状态 {states[tid]}，没跑完"

    # 整链：数据真进了 MySQL，且只进了成功的那些
    rows = tables.query(f"SELECT url, title FROM `{DATA_TABLE}`")
    assert len(rows) == N_TASKS - 1, f"库里 {len(rows)} 行，应为 {N_TASKS - 1}"
    assert all("boom" not in r["url"] for r in rows), "失败的页面竟然也落了数据"

    # 靶场：成功的任务各抓一次；失败的按重试次数抓
    for tid in range(N_TASKS):
        if tid != BAD_TASK:
            assert hits.get(f"/item/{tid}") == 1, f"/item/{tid} 抓了 {hits.get(f'/item/{tid}')} 次"
    assert hits.get("/boom/500") == 2, f"500 抓了 {hits.get('/boom/500')} 次（应为 1+1 次重试）"
