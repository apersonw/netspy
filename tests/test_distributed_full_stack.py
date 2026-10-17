"""分布式 Spider 的「全特性同开」跑批 —— 判据全取框架外部。

`test_distributed_integration.py` 已经把跨进程的**单项**能力验得很扎实
（共享队列去重、种子锁、心跳、优雅停止、SIGKILL 回收）。这里补的是**组合**：
三个真进程，同时开着 robots / 请求去重 / Item 去重 / 重试 / 真库管道，
看它们叠在一起还成不成立。

⚠️ **这个文件的第一版把功劳记错了地方。** 它断言「每个 URL 跨三进程只被抓一次」
并说这证明了共享去重 —— 其实那来自**共享队列**（入了队的任务只被 pop 一次），
跟去重后端无关。跑完之后去翻 Redis，里面**一个去重键都没有**：
`DEDUP_FILTER` 默认是 `memory`，进程内布隆，三个节点各一份。

真正能分辨的场景是**两个节点各自解析出同一个 URL**（靶场的 `/a` 和 `/b` 都链到
`/shared`）。实测：`memory` 下 `/shared` 被抓 **2** 次，`redis` 下 1 次。

worker 函数与 Spider 类必须在**模块顶层**：macOS 默认 spawn，子进程重新 import。
"""

from __future__ import annotations

import multiprocessing as mp
import os
from collections.abc import Iterator
from typing import Any
from urllib.parse import urlparse

import pytest

pytestmark = pytest.mark.integration

N_ITEMS = 10
_PROC_TIMEOUT = 120  # 挂起必须判失败，不能让 CI 卡死
TABLE = "dist_full_items"


@pytest.fixture
def clean_redis(redis_url: str) -> Iterator[str]:
    import redis as redis_lib

    client = redis_lib.from_url(redis_url, decode_responses=True)
    client.flushdb()
    yield redis_url
    client.flushdb()
    client.close()


def _pg_fields(url: str) -> dict[str, Any]:
    """把连接串拆成框架要的分字段配置。

    框架没有 POSTGRES_URL 这一项 —— 写那个名字会被静默接受、完全不生效
    （v4.37 就是这么踩出来的，现在会有警告）。
    """
    p = urlparse(url)
    return {
        "POSTGRES_HOST": p.hostname or "127.0.0.1",
        "POSTGRES_PORT": p.port or 5432,
        "POSTGRES_USER": p.username or "postgres",
        "POSTGRES_PASSWORD": p.password or "",
        "POSTGRES_DB": (p.path or "/postgres").lstrip("/"),
    }


def _node(base: str, redis_url: str, pg_url: str, seed: bool) -> None:
    """一个爬虫节点（独立进程）。"""
    import netspy as ns
    from netspy import setting

    setting.reload()
    setting.REDIS_URL = redis_url
    setting.SPIDER_THREAD_COUNT = 4
    setting.USE_SESSION = True
    setting.ROBOTS_OBEY = True
    setting.SPIDER_MAX_RETRY_TIMES = 2
    setting.RETRY_BACKOFF = 0.01
    # 熔断保持开着（默认阈值 10）—— 见 test_full_stack_integration 的说明
    setting.ITEM_FILTER_ENABLE = True
    # **分布式必须用共享后端**：默认的 memory 是进程内布隆，三个节点各一份。
    # docs/distributed.md 的示例里就是这么写的，框架现在也会在没设时告警。
    setting.DEDUP_FILTER = "redis"
    setting.CONCURRENT_REQUESTS_PER_DOMAIN = 4
    setting.DONE_CHECK_INTERVAL = 0.1
    setting.DONE_CHECK_TIMES = 3
    setting.SPIDER_STARTUP_GRACE = 6.0
    setting.BUFFER_FLUSH_INTERVAL = 0.05
    setting.LOG_LEVEL = "ERROR"
    setting.ITEM_PIPELINES = ["netspy.pipelines.postgres.PostgresPipeline"]
    for key, value in _pg_fields(pg_url).items():
        setattr(setting, key, value)
    from netspy.utils import log

    log.configure()

    class Row(ns.Item):
        __table_name__ = TABLE
        __unique_key__ = ["url"]
        url: str = ""
        title: str = ""

    class Node(ns.Spider):
        def start_requests(self) -> Any:
            yield ns.Request(base + "/", callback="index", use_session=True)

        def index(self, request: Any, response: Any) -> Any:
            for href in response.css("a::attr(href)").getall():
                yield ns.Request(response.urljoin(href), callback="item", use_session=True)

        def item(self, request: Any, response: Any) -> Any:
            title = response.css("h1.t::text").get()
            if title:
                yield Row(url=request.url, title=title)

    Node(redis_key=f"dfs:{os.environ['DFS_NS']}").start()


def _run_nodes(base: str, redis_url: str, pg_url: str, n: int) -> list[int]:
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_node, args=(base, redis_url, pg_url, i == 0)) for i in range(n)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=_PROC_TIMEOUT)
    codes = [p.exitcode for p in procs]
    for p in procs:
        if p.is_alive():
            p.kill()
    return codes  # type: ignore[return-value]


def test_three_nodes_with_every_feature_on(
    clean_redis: str, postgres_db: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """三进程 + robots + 去重 + 重试 + 真库并发写，判据全由靶场和库给出。"""
    from integration_site import Site

    monkeypatch.setenv("DFS_NS", "combo")
    pg_url = os.environ["NETSPY_TEST_POSTGRES_URL"]
    postgres_db.execute(f"DROP TABLE IF EXISTS {TABLE}")
    postgres_db.execute(f"CREATE TABLE {TABLE} (url text primary key, title text)")

    with Site(n_items=N_ITEMS) as site:
        codes = _run_nodes(site.url, clean_redis, pg_url, 3)
        hits = dict(site.hits)

    assert all(c == 0 for c in codes), f"有节点异常退出：{codes}"

    # robots：三个进程里没有任何一个碰过被禁的路径
    assert "/private/secret" not in hits, f"robots 在分布式下失效：{hits}"

    # 跨进程去重：每个 item 页在**所有进程加起来**只被抓一次
    for i in range(N_ITEMS):
        got = hits.get(f"/item/{i}")
        assert got == 1, f"/item/{i} 跨进程被抓了 {got} 次（共享去重失效）"

    # 只有一个节点注入种子
    assert hits.get("/") == 1, f"首页被抓了 {hits.get('/')} 次，种子锁失效"

    # 三个进程并发写同一张表：行数必须精确，没有重复也没有丢
    rows = postgres_db.query(f"SELECT url, title FROM {TABLE}")
    assert len(rows) == N_ITEMS + 2, f"库里 {len(rows)} 行（应为 {N_ITEMS + 2}）"
    assert len({r["url"] for r in rows}) == len(rows), "同一 url 落了多行"


def _dedup_node(base: str, redis_url: str, backend: str, ns: str) -> None:
    """只验去重的精简节点：种子是 /a 和 /b，两者都链到 /shared。"""
    import netspy as ns
    from netspy import setting

    setting.reload()
    setting.REDIS_URL = redis_url
    setting.DEDUP_FILTER = backend
    setting.SPIDER_THREAD_COUNT = 2
    setting.ROBOTS_OBEY = False
    setting.ITEM_PIPELINES = []
    setting.DONE_CHECK_INTERVAL = 0.1
    setting.DONE_CHECK_TIMES = 3
    setting.SPIDER_STARTUP_GRACE = 4.0
    setting.LOG_LEVEL = "ERROR"
    from netspy.utils import log

    log.configure()

    class Node(ns.Spider):
        def start_requests(self) -> Any:
            yield ns.Request(base + "/a", callback="idx")
            yield ns.Request(base + "/b", callback="idx")

        def idx(self, request: Any, response: Any) -> Any:
            for href in response.css("a::attr(href)").getall():
                yield ns.Request(response.urljoin(href), callback="leaf")

        def leaf(self, request: Any, response: Any) -> None:
            return None

    Node(redis_key=f"dfsd:{ns}").start()


def _run_dedup_nodes(base: str, redis_url: str, backend: str, ns: str) -> None:
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_dedup_node, args=(base, redis_url, backend, ns)) for _ in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=_PROC_TIMEOUT)
    for p in procs:
        if p.is_alive():
            p.kill()


@pytest.mark.parametrize(
    ("backend", "second_is_fresh"), [("memory", True), ("lite", True), ("redis", False)]
)
def test_dedup_backend_decides_whether_two_processes_share_fingerprints(
    clean_redis: str, backend: str, second_is_fresh: bool
) -> None:
    """**这条才真的在验跨进程去重**，而且是确定性的。

    两个独立的 `Dedup` 实例代表两个节点。同一个指纹：

    - `memory` / `lite`（进程内）：第二个实例仍然认为它是**新的** → 会重复抓 / 重复入库
    - `redis`：第二个实例认得出来 → 收敛

    为什么不用端到端跑批来验：那条**天生不确定**。`/a` 和 `/b` 都链到 `/shared`，
    但如果两个入口页碰巧被**同一个**节点领走，它自己的本地去重就把 `/shared`
    收敛了，`memory` 下也只抓一次。第一版就是这么写的，实测两种结果都出现过
    （独立演示里是 2 次、放进用例里跑出 1 次）—— 不能交一个看运气的用例。

    端到端的证据留在 roadmap 里：同样两节点两入口，`memory` 下 `/shared` 被抓
    **2** 次、`redis` 下 1 次。
    """
    from netspy import setting
    from netspy.dedup import get_request_filter

    setting.reload()
    setting.REDIS_URL = clean_redis
    setting.DEDUP_FILTER = backend

    fp = "f" * 32
    node_a = get_request_filter(name="shared-ns")
    node_b = get_request_filter(name="shared-ns")

    assert node_a.add(fp) is True, "第一个节点该认为它是新的"
    assert node_b.add(fp) is second_is_fresh, (
        f"DEDUP_FILTER={backend}：第二个节点"
        f"{'仍然当成新的（进程内去重不跨节点）' if second_is_fresh else '应该认得出来'}"
    )


def test_distributed_warns_about_local_dedup(
    clean_redis: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """分布式跑着进程内去重时必须出声 —— 那是个**静默**错配。

    共享队列让大部分现象看起来正常（入了队的任务只被取走一次），所以错配不报错，
    只会悄悄多抓、多写。`docs/distributed.md` 的示例里写着 `DEDUP_FILTER = "redis"`，
    但此前没设的人得不到任何提示。

    ⚠️ 项目用的是 loguru，`caplog` **抓不到** —— 照 `test_dedup_scale.py` 的做法
    写文件再读。第一版正是用 caplog 写的：警告明明打在 stderr 上，断言却是 False。

    ⚠️ 而且必须**真的 `start()` 一遍**，不能直接调那个私有方法。第一版就是直接调的，
    于是把 `_on_start` 里的调用整个删掉，用例照样 5 条全绿 —— 验的是方法本身，
    不是它有没有被接上去。
    """
    import netspy as ns
    from netspy import setting
    from netspy.utils import log

    class Quiet(ns.Spider):
        def start_requests(self) -> Any:
            return iter(())

    for backend, should_warn in (("memory", True), ("lite", True), ("redis", False)):
        logfile = tmp_path / f"{backend}.log"
        setting.reload()
        setting.REDIS_URL = clean_redis
        setting.DEDUP_FILTER = backend
        setting.ITEM_PIPELINES = []
        setting.LOG_FILE = str(logfile)
        setting.LOG_LEVEL = "WARNING"
        log.configure()

        setting.SPIDER_STARTUP_GRACE = 0.2
        setting.DONE_CHECK_INTERVAL = 0.05
        setting.DONE_CHECK_TIMES = 2
        Quiet(redis_key=f"warn:{backend}").start()  # 走真实启动路径

        text = logfile.read_text(encoding="utf-8") if logfile.exists() else ""
        hit = "进程内" in text
        assert hit is should_warn, (
            f"DEDUP_FILTER={backend} 时{'应该' if should_warn else '不该'}告警；"
            f"日志内容：{text[:200]!r}"
        )
