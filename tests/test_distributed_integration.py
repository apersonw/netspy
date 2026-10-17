"""分布式能力在**真 Redis + 真多进程**下的验证。

此前 839 行分布式代码全部只用 fakeredis + 单进程测过 —— 那既不含真正的并发竞争，
也不跨进程。而分布式恰恰是这个框架相对 AirSpider 的全部价值所在。

**判据由 HTTP 靶子给出，而不是问框架自己**：靶子记录每个路径被请求的次数，
这是框架外部的地面真相。「每个 URL 恰好一次」才是共享队列 + 共享去重的意义。

worker 函数与 Spider 类必须定义在**模块顶层**：macOS 的 multiprocessing 默认
spawn，子进程会重新 import 本模块，闭包和局部类没法 pickle。
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import signal
import time
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WRequest
from werkzeug.wrappers import Response as WResponse

pytestmark = pytest.mark.integration

PAGES = 24
_PROC_TIMEOUT = 90  # 挂起必须判失败，不能让 CI 卡死


@pytest.fixture
def clean_redis(redis_url: str) -> Iterator[str]:
    import redis as redis_lib

    client = redis_lib.from_url(redis_url, decode_responses=True)
    client.flushdb()
    yield redis_url
    client.flushdb()
    client.close()


def _serve(server: HTTPServer, hits: Any, page_delay: float = 0.0) -> None:
    """靶子：种子页给出 PAGES 个链接；每次请求都记一笔。

    ``page_delay`` 给续爬测试用 —— 本地靶子太快的话，第一个进程会在被杀之前
    就把全部页面抓完，续爬这条路径就根本没被触发（第一版就栽在这里）。
    """

    def seed(request: WRequest) -> WResponse:
        hits.append("/seed")
        links = "".join(f'<a href="/p/{i}">p{i}</a>' for i in range(PAGES))
        return WResponse(f"<html><body>{links}</body></html>", content_type="text/html")

    def page(request: WRequest) -> WResponse:
        hits.append(request.path)
        if page_delay:
            time.sleep(page_delay)
        return WResponse("<html><body><h1>ok</h1></body></html>", content_type="text/html")

    server.expect_request("/seed").respond_with_handler(seed)
    for i in range(PAGES):
        server.expect_request(f"/p/{i}").respond_with_handler(page)


def _serve_timed(server: HTTPServer, hits: Any) -> None:
    """同上，但连**到达时刻**一起记 —— 限速的判据是节奏，不是次数。"""

    def seed(request: WRequest) -> WResponse:
        hits.append(("/seed", time.time()))
        links = "".join(f'<a href="/p/{i}">p{i}</a>' for i in range(PAGES))
        return WResponse(f"<html><body>{links}</body></html>", content_type="text/html")

    def page(request: WRequest) -> WResponse:
        hits.append((request.path, time.time()))
        return WResponse("<html><body><h1>ok</h1></body></html>", content_type="text/html")

    server.expect_request("/seed").respond_with_handler(seed)
    for i in range(PAGES):
        server.expect_request(f"/p/{i}").respond_with_handler(page)


# ---- 顶层定义，供子进程 spawn 后重新 import ---------------------------
class SlowFilePipeline:
    """落库靶子。慢是**关键**：真实的库批量写入要花时间（MySQL 批插 ~1 秒很常见），
    而销账早于落库的洞恰恰是在这段时间里张开的。先 sleep 再写 ——
    真库写到一半被硬杀是不会提交的。"""

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        import json
        import os

        time.sleep(float(os.environ.get("MW_TEST_PIPE_DELAY", "0")))
        with Path(os.environ["MW_TEST_STORE"]).open("a") as f:
            for it in items:
                f.write(json.dumps(it) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return True

    def close(self) -> None:
        return None


def _run_node(
    seed_url: str,
    redis_url: str,
    redis_key: str,
    keep: float = 0.0,
    delay: float = 0.0,
    global_throttle: bool = False,
    lease: float | None = None,
    store: bool = False,
    run_id: str = "",
    dedup: str | None = None,
) -> None:
    """一个「节点」：独立进程里跑一个 Spider。

    子进程不需要 monkeypatch —— get_redis() 读 setting.REDIS_URL，
    配上它就会连真 Redis，这正是生产里的真实路径。
    """
    import netspy as ns
    from netspy import setting
    from netspy.utils import log

    setting.REDIS_URL = redis_url
    setting.RUN_ID = run_id
    if dedup is not None:
        setting.DEDUP_FILTER = dedup
    setting.ITEM_PIPELINES = [f"{__name__}.SlowFilePipeline"] if store else []
    setting.LOG_LEVEL = "CRITICAL"
    setting.DONE_CHECK_INTERVAL = 0.2
    setting.DONE_CHECK_TIMES = 3
    setting.HEARTBEAT_INTERVAL = 0.3
    setting.HEARTBEAT_STALE = 5.0
    setting.SPIDER_THREAD_COUNT = 3
    setting.RANDOM_USER_AGENT = False
    setting.ROBOTS_OBEY = False
    setting.CONCURRENT_REQUESTS_PER_DOMAIN = 0
    setting.CIRCUIT_FAILURE_THRESHOLD = 0
    if keep:
        setting.SPIDER_MAX_RUNTIME = keep
    setting.DOWNLOAD_DELAY = delay
    # 抖动会让间隔在 ±50% 浮动，测出来的最小间隔就没法和配置值对账了
    setting.RANDOMIZE_DOWNLOAD_DELAY = False
    setting.GLOBAL_THROTTLE = global_throttle
    if lease is not None:
        setting.SPIDER_TASK_LEASE = lease
    log.configure()

    class NodeSpider(ns.Spider):
        __redis_key__ = redis_key

        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(seed_url, callback=self.parse_seed)

        def parse_seed(self, request, response):  # type: ignore[no-untyped-def]
            for href in response.css("a::attr(href)").getall():
                yield ns.Request(response.urljoin(href), callback=self.parse_page)

        def parse_page(self, request, response):  # type: ignore[no-untyped-def]
            if store:
                yield {"url": request.url}

    NodeSpider(redis_key=redis_key).start()


def _spawn(n: int, seed_url: str, redis_url: str, key: str, **node_kw: Any) -> list[mp.Process]:
    procs = [
        mp.Process(target=_run_node, args=(seed_url, redis_url, key), kwargs=node_kw, daemon=False)
        for _ in range(n)
    ]
    for p in procs:
        p.start()
    return procs


def _join_all(procs: list[mp.Process], timeout: float = _PROC_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    for p in procs:
        p.join(timeout=max(deadline - time.monotonic(), 1))
    alive = [p for p in procs if p.is_alive()]
    for p in alive:
        p.terminate()
        p.join(timeout=5)
    assert not alive, f"{len(alive)} 个节点没有自行退出 —— 结束检测在多进程下失效"


def _wait_until_fetching(hits: Any, want: int, timeout: float = 30.0) -> int:
    """等到靶子真的收到 `want` 个页面请求再动手。

    原来这里是固定 `sleep`，机器一忙（macOS 的 spawn 要重启解释器、重新 import）
    子进程还没抓到东西就被杀了 —— 用例里的「这个用例什么都没验到」守卫会报警。
    那是守卫在正确工作，但每次都得人来判断是真缺陷还是负载。等条件就没这问题。
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = len([h for h in list(hits) if h.startswith("/p/")])
        if got >= want:
            return got
        time.sleep(0.05)
    return len([h for h in list(hits) if h.startswith("/p/")])


# ---- 核心：多节点不重复、不遗漏 ---------------------------------------
def test_three_nodes_no_duplicate_no_missing(httpserver: HTTPServer, clean_redis: str) -> None:
    """三个真进程共享一个 Redis 队列：每个 URL 恰好被抓一次。

    这是共享队列 + 共享去重的**全部意义**。fakeredis 单进程测不出来。
    """
    with mp.Manager() as mgr:
        hits = mgr.list()
        _serve(httpserver, hits)
        key = f"dist{os.getpid()}"
        procs = _spawn(3, httpserver.url_for("/seed"), clean_redis, key)
        _join_all(procs)
        counted = Counter(list(hits))

    pages = {p: c for p, c in counted.items() if p.startswith("/p/")}
    assert len(pages) == PAGES, f"漏抓：只见到 {len(pages)}/{PAGES} 个页面"
    dupes = {p: c for p, c in pages.items() if c != 1}
    assert not dupes, f"重复抓取：{dupes}"


def test_seed_lock_only_one_node_seeds(httpserver: HTTPServer, clean_redis: str) -> None:
    """acquire_once 的种子锁在真并发下必须只让一个进程注入种子。

    单进程里根本不存在这个竞争 —— 这是 SET NX EX 第一次被真正检验。
    """
    with mp.Manager() as mgr:
        hits = mgr.list()
        _serve(httpserver, hits)
        key = f"seed{os.getpid()}"
        procs = _spawn(3, httpserver.url_for("/seed"), clean_redis, key)
        _join_all(procs)
        counted = Counter(list(hits))

    assert counted["/seed"] == 1, f"种子页被抓了 {counted['/seed']} 次，种子锁没生效"


@pytest.mark.parametrize(
    ("sig", "label"),
    [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")],
)
def test_graceful_stop_returns_tasks_to_redis(
    httpserver: HTTPServer, clean_redis: str, sig: int, label: str
) -> None:
    """节点被信号停掉时，本地缓冲里的任务必须还回 Redis，不能永久丢失。

    **SIGTERM 曾经会丢**：框架只装了 SIGINT 处理器，而 collector 一次
    ``get_batch(COLLECTOR_TASK_COUNT-1)`` 会把上百个任务从 Redis 领走
    （``zpopmin`` 取走即删）搬进本地内存。SIGTERM 没有处理器 → 进程直接终止 →
    ``finally: _teardown()`` 不执行 → ``_on_shutdown`` 的「推回 Redis」逻辑没机会跑。

    实测那次：24 个任务里 SIGTERM 丢了 20 个，SIGINT 全数恢复。
    而 SIGTERM 正是 docker stop / K8s 驱逐 / systemctl stop 发的信号 ——
    NetspyHub 停任务用的也是它。
    """
    with mp.Manager() as mgr:
        hits = mgr.list()
        # 慢靶子：保证第一个进程来不及抓完，本地缓冲里确实压着任务
        _serve(httpserver, hits, page_delay=0.2)
        key = f"sig{label}{os.getpid()}"
        url = httpserver.url_for("/seed")

        first = mp.Process(target=_run_node, args=(url, clean_redis, key), daemon=False)
        first.start()
        time.sleep(1.0)
        os.kill(first.pid, sig)
        first.join(timeout=30)
        partial = len({h for h in hits if h.startswith("/p/")})

        second = mp.Process(target=_run_node, args=(url, clean_redis, key), daemon=False)
        second.start()
        _join_all([second])
        counted = Counter([h for h in list(hits) if h.startswith("/p/")])

    assert partial < PAGES, f"第一个进程就抓完了（{partial}），这轮没验到恢复"
    assert len(counted) == PAGES, (
        f"{label} 后永久丢失 {PAGES - len(counted)} 个任务 —— 本地缓冲没被推回 Redis"
    )
    dupes = {p: c for p, c in counted.items() if c != 1}
    assert not dupes, f"恢复过程造成重复：{dupes}"


def test_heartbeat_visible_across_processes(httpserver: HTTPServer, clean_redis: str) -> None:
    """多进程下心跳要真的写进共享 Redis，否则结束检测无从判断别的节点。"""
    import redis as redis_lib

    with mp.Manager() as mgr:
        hits = mgr.list()
        _serve(httpserver, hits)
        key = f"hb{os.getpid()}"
        procs = _spawn(2, httpserver.url_for("/seed"), clean_redis, key)

        client = redis_lib.from_url(clean_redis, decode_responses=True)
        seen_nodes = 0
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and seen_nodes < 2:
            seen_nodes = max(seen_nodes, client.hlen(f"netspy:{key}:heartbeat") or 0)
            time.sleep(0.2)
        client.close()
        _join_all(procs)

    assert seen_nodes >= 2, f"只看到 {seen_nodes} 个节点的心跳，多进程心跳没生效"


# ---- 全局限速：N 个节点合起来才是配置的速率 ---------------------------
def test_global_throttle_lua_spaces_tickets(clean_redis: str, monkeypatch: Any) -> None:
    """先单独验 Lua 的取号算术，多进程测出问题时好分辨是算法还是并发。"""
    from netspy import setting
    from netspy.network import global_throttle

    # 用 monkeypatch 而不是直接赋值：跑测试时 pytest-randomly 会打乱顺序，
    # 改了不还原的全局 setting 会漏到别的用例里
    monkeypatch.setattr(setting, "REDIS_URL", clean_redis)
    global_throttle.reset()
    waits = [global_throttle.take_ticket("lua.example", 1.0) for _ in range(3)]

    assert all(w is not None for w in waits), "真 Redis 下不该走兜底分支"
    # 连号必须依次让开 1 秒：0、1、2
    assert waits[0] == pytest.approx(0.0, abs=0.05)
    assert waits[1] == pytest.approx(1.0, abs=0.05)
    assert waits[2] == pytest.approx(2.0, abs=0.05)


def _max_per_second(stamps: list[float]) -> int:
    """最繁忙的 1 秒窗口里落了几个请求 —— 也就是站点实际承受的峰值速率。"""
    ordered = sorted(stamps)
    return max(
        (sum(1 for t in ordered if s <= t < s + 1.0) for s in ordered),
        default=0,
    )


def _peak_rate(
    httpserver: HTTPServer,
    redis_url: str,
    hits: Any,
    tag: str,
    delay: float,
    global_on: bool,
) -> int:
    """跑一轮三节点，返回靶子看到的峰值到达速率。

    只看本轮**新增**的那些记录（`hits` 是两轮共用的）—— 不能靠重新注册 handler
    来隔离两轮：`expect_request` 是先注册先匹配，第二次注册根本不会被用到，
    于是第二轮读到的永远是空列表。
    """
    seen_before = len(hits)
    procs = _spawn(
        3,
        httpserver.url_for("/seed"),
        redis_url,
        f"thr{os.getpid()}{tag}",
        delay=delay,
        global_throttle=global_on,
    )
    _join_all(procs)
    fresh = list(hits)[seen_before:]
    stamps = [ts for path, ts in fresh if path.startswith("/p/")]
    assert len(stamps) == PAGES, f"漏抓：只见到 {len(stamps)}/{PAGES} 个页面"
    return _max_per_second(stamps)


def test_global_throttle_caps_rate_across_nodes(httpserver: HTTPServer, clean_redis: str) -> None:
    """三个真进程压同一个域：打开全局限速后，站点承受的速率才是配置的那个。

    **判据取自靶子记录的到达时刻**，不是问框架自己等了多久 —— 站点感受到的
    负载才是这件事的意义所在。

    开关两种情况**在同一次测试里各跑一遍**，而不是拆成两个参数化用例：
    对照组只用绝对阈值的话，在慢机器上（CI runner）可能仅仅因为跑不快就
    「没超速」，于是测试变成绿的却什么都没证明。同机比较则是自校准的 ——
    机器慢时两轮一起慢，而两者的**差距**仍然成立。
    """
    delay = 0.3
    allowed = round(1 / delay) + 1  # 3.33/s，放一格余量给网络抖动
    with mp.Manager() as mgr:
        hits = mgr.list()
        _serve_timed(httpserver, hits)
        peak_off = _peak_rate(httpserver, clean_redis, hits, "off", delay, global_on=False)
        peak_on = _peak_rate(httpserver, clean_redis, hits, "on", delay, global_on=True)

    assert peak_on <= allowed, f"打开全局限速后仍超速：峰值 {peak_on}/s > 允许的 {allowed}/s"
    assert peak_off > peak_on, (
        f"关掉全局限速后峰值没升高（{peak_off}/s vs {peak_on}/s）—— "
        "这轮对照不成立，说明测试根本没压到限速线"
    )


# ---- 启动竞态：晚一步的节点不能空跑退出 -------------------------------
def _run_counting_node(
    seed_url: str, redis_url: str, redis_key: str, delay: float, out: Any
) -> None:
    """跑一个节点，把它自己抓到的请求数回报出来。

    ``delay`` 用来放大真实部署里的启动错位 —— 容器起来的时刻、pip install 的
    耗时、start_requests 里的 DB 查询，都会让某个节点比别的晚几秒。
    """
    import time as _t

    if delay:
        _t.sleep(delay)

    import netspy as ns
    from netspy import setting
    from netspy.utils import log

    setting.REDIS_URL = redis_url
    setting.ITEM_PIPELINES = []
    setting.LOG_LEVEL = "CRITICAL"
    setting.DONE_CHECK_INTERVAL = 0.2
    setting.DONE_CHECK_TIMES = 3
    setting.HEARTBEAT_INTERVAL = 0.3
    setting.SPIDER_THREAD_COUNT = 2
    setting.RANDOM_USER_AGENT = False
    setting.ROBOTS_OBEY = False
    setting.DEDUP_FILTER = "redis"
    setting.CIRCUIT_FAILURE_THRESHOLD = 0
    # 一次只领 3 个：默认 100 会让先到的节点把整个队列一口气搬空，
    # 那样再怎么等也轮不到第二个节点，测的就不是启动竞态了
    setting.COLLECTOR_TASK_COUNT = 3
    log.configure()

    class Node(ns.Spider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(seed_url, callback=self.parse_seed)

        def parse_seed(self, request, response):  # type: ignore[no-untyped-def]
            for href in response.css("a::attr(href)").getall():
                yield ns.Request(response.urljoin(href), callback=self.parse_page)

        def parse_page(self, request, response):  # type: ignore[no-untyped-def]
            return None

    spider = Node(redis_key=redis_key)
    spider.start()
    out.put(spider.scheduler.stats.get("request_ok"))


def _seed_later(redis_url: str, redis_key: str, seed_url: str, delay: float) -> None:
    """隔 ``delay`` 秒后才把种子推进队列，模拟「别的节点正在慢慢播种」。"""
    import time as _t

    _t.sleep(delay)
    from netspy import setting

    setting.REDIS_URL = redis_url
    import netspy as ns
    from netspy.core.task_queue import RedisTaskQueue
    from netspy.db.redisdb import get_redis

    ns = f"{setting.REDIS_KEY_PREFIX}:{redis_key}"
    RedisTaskQueue(ns, get_redis()).put(ns.Request(seed_url, callback="parse_seed"))


def test_node_waits_when_another_is_still_seeding(httpserver: HTTPServer, clean_redis: str) -> None:
    """种子锁已被别人拿走、队列还空着时，本节点必须等，而不是空跑退出。

    构造是确定的：**测试自己先占住种子锁**（模拟另一个节点正在播种），
    节点起来后发现锁拿不到、队列也是空的 —— 正是线上第二个 worker 撞上的局面。
    3 秒后种子才进队列。

    没有启动宽限时，节点会在 `DONE_CHECK_TIMES × DONE_CHECK_INTERVAL`
    （这里 0.6 秒）内判定「抓完了」退出，一个请求都不抓。心跳挡不住 ——
    那一刻心跳表里根本没有别的节点。
    """
    import redis as redis_lib

    key = f"race{os.getpid()}"
    ns = f"netspy:{key}"
    client = redis_lib.from_url(clean_redis, decode_responses=True)
    # 先占住种子锁：本节点将走「另一节点已注入种子，直接消费队列」那条路
    client.set(f"{ns}:lock:seed", "1", nx=True, ex=600)

    with mp.Manager() as mgr:
        hits = mgr.list()
        out: Any = mp.Queue()
        _serve(httpserver, hits)
        url = httpserver.url_for("/seed")
        procs = [
            mp.Process(target=_run_counting_node, args=(url, clean_redis, key, 0.0, out)),
            mp.Process(target=_seed_later, args=(clean_redis, key, url, 3.0)),
        ]
        for p in procs:
            p.start()
        _join_all(procs)
        done = out.get()
        # 必须在 with 块内取值：manager 一关，代理对象就用不了了
        seen_pages = len([p for p in Counter(list(hits)) if p.startswith("/p/")])

    client.close()
    assert done > 0, (
        "节点一个请求都没抓 —— 它在种子进队列之前就判定「抓完了」退出了。"
        "线上表现为：配 N 个 worker，实际只有 1 个在干活"
    )
    assert seen_pages == PAGES, f"漏抓：只见到 {seen_pages}/{PAGES}"


# ---- 硬杀之后的任务恢复（租约）---------------------------------------
def test_sigkilled_node_tasks_are_reclaimed(httpserver: HTTPServer, clean_redis: str) -> None:
    """节点被 **SIGKILL** 硬杀后，它领走的任务必须能被别人捡回来。

    这是优雅停止（0.8.1）与退出落盘（0.10.3）**结构上覆盖不到**的场景 ——
    那两条都要求进程还活着，而 OOM Killer / 断电 / `docker kill` 不给这个机会。

    队列用 `zpopmin`：取走即删。任务进了某个节点的内存后 Redis 里就不存在了，
    进程一没，它们就随之消失。修复前实测：24 个任务硬杀后只剩 5 个。

    判据取自 HTTP 靶子：**每个页面恰好被抓一次**。
    """
    import redis as redis_lib

    key = f"kill{os.getpid()}"
    with mp.Manager() as mgr:
        hits = mgr.list()
        # 慢靶子：保证被杀那一刻，本地缓冲里确实压着已领走的任务
        _serve(httpserver, hits, page_delay=0.2)
        url = httpserver.url_for("/seed")

        # 租约是**部署级**设置：在途任务的到期时刻由领走它的那个节点写下，
        # 所以第一个节点也得用短租约，否则要等它写的 600 秒默认值到期
        first = mp.Process(
            target=_run_node, args=(url, clean_redis, key), kwargs={"lease": 2.0}, daemon=False
        )
        first.start()
        # 等它真的抓起来再杀 —— 固定 sleep 在满负载的机器上会杀在启动阶段，
        # 那样什么都验不到
        _wait_until_fetching(hits, 3)
        os.kill(first.pid, signal.SIGKILL)  # 不给任何清理机会
        first.join(timeout=30)

        client = redis_lib.from_url(clean_redis, decode_responses=True)
        stranded = client.zcard(f"netspy:{key}:z_inflight")
        assert stranded > 0, "被硬杀的节点没有留下在途任务，这个用例就没验到东西"
        # 把租约调短，让第二个节点能在测试时限内回收
        second = mp.Process(
            target=_run_node, args=(url, clean_redis, key), kwargs={"lease": 2.0}, daemon=False
        )
        second.start()
        _join_all([second])
        counted = Counter(list(hits))
        client.close()

    pages = {p: c for p, c in counted.items() if p.startswith("/p/")}
    assert len(pages) == PAGES, f"漏抓：只见到 {len(pages)}/{PAGES} —— 被硬杀节点领走的任务没能回收"


# ---- 硬杀之后的数据恢复（落库之后才销账）-----------------------------
def test_sigkilled_node_does_not_lose_scraped_data(
    httpserver: HTTPServer, clean_redis: str, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """节点被 SIGKILL 硬杀后，它**已经抓到手的数据**必须也能落库。

    上面那条用例验的是**任务**回来了，判据取自靶子（每页恰好一次）——
    而它和其余全部分布式用例一样写着 `ITEM_PIPELINES = []`，
    **数据这条路从来没被验过**。

    实测（400 页、管道每批 1.5 秒）：靶子确实发出全部 400 页、爬虫 exitcode=0
    报告抓完，**只落库 340 行**。少掉的 60 行重跑一遍一行都补不回来 ——
    请求指纹是入队前写的，新节点看一眼去重就跳过了。

    根因是顺序：worker 在 `finally` 里销账，那一刻 item 还只在内存缓冲里。
    """
    store = tmp_path / "store.jsonl"
    store.touch()
    monkeypatch.setenv("MW_TEST_STORE", str(store))
    monkeypatch.setenv("MW_TEST_PIPE_DELAY", "0.6")

    key = f"data{os.getpid()}"
    with mp.Manager() as mgr:
        hits = mgr.list()
        _serve(httpserver, hits, page_delay=0.2)
        url = httpserver.url_for("/seed")

        first = mp.Process(
            target=_run_node,
            args=(url, clean_redis, key),
            kwargs={"lease": 2.0, "store": True},
            daemon=False,
        )
        first.start()
        _wait_until_fetching(hits, 3)
        os.kill(first.pid, signal.SIGKILL)
        first.join(timeout=30)

        fetched_at_kill = len({p for p in list(hits) if p.startswith("/p/")})
        assert 0 < fetched_at_kill < PAGES, (
            f"被杀时抓了 {fetched_at_kill}/{PAGES} 页 —— 节点没在「抓了一半」时被杀，"
            "这个用例什么都没验到"
        )

        second = mp.Process(
            target=_run_node,
            args=(url, clean_redis, key),
            kwargs={"lease": 2.0, "store": True},
            daemon=False,
        )
        second.start()
        _join_all([second])
        pages = {p for p in list(hits) if p.startswith("/p/")}

    stored = {json.loads(line)["url"] for line in store.read_text().splitlines() if line.strip()}
    assert len(pages) == PAGES, f"漏抓：只见到 {len(pages)}/{PAGES}"
    assert len(stored) == PAGES, (
        f"靶子发出了 {len(pages)} 页，只有 {len(stored)} 行落库 —— "
        f"少掉的 {PAGES - len(stored)} 条数据永久丢失："
        "任务已销账不会被回收，请求指纹又是入队前写的，重跑也补不回来"
    )


# ======================================================================
# 定时重跑：生产上那 49 小时缩成三轮
# ======================================================================
def _round(httpserver: HTTPServer, hits: Any, redis_url: str, key: str, **node_kw: Any) -> int:
    """起 2 个节点跑完一轮，返回这一轮靶子新增的页面命中数。"""
    before = len([h for h in list(hits) if h.startswith("/p/")])
    _join_all(_spawn(2, httpserver.url_for("/seed"), redis_url, key, **node_kw))
    return len([h for h in list(hits) if h.startswith("/p/")]) - before


def test_scheduled_rerun_is_a_noop_without_run_id(httpserver: HTTPServer, clean_redis: str) -> None:
    """**生产复现**：一个 2 节点的分布式任务每 5 分钟定时跑一次，49 小时里
    1160 个实例有 1158 个「请求成功 0」，全部 exit 0。这里把它缩成三轮：
    第一轮真抓，之后每轮两个节点都干净地退出、一个请求都不发。

    这条用例记录的是**默认语义**（一个 redis_key = 一个可续跑的作业），
    它会一直绿 —— 修的不是这个行为，是让它（a）出声、（b）有 RUN_ID 可选。
    """
    hits: Any = mp.Manager().list()
    _serve(httpserver, hits)

    assert _round(httpserver, hits, clean_redis, "rerun-noop", dedup="redis") == PAGES
    assert _round(httpserver, hits, clean_redis, "rerun-noop", dedup="redis") == 0
    assert _round(httpserver, hits, clean_redis, "rerun-noop", dedup="redis") == 0


def test_scheduled_rerun_crawls_every_time_with_run_id(
    httpserver: HTTPServer, clean_redis: str
) -> None:
    """同样三轮，每轮一个新 RUN_ID（平台就是这么注入的）：每轮都把全部页面抓一遍，
    且每轮里每个页面**恰好一次**（两个节点分摊，不重复）。"""
    hits: Any = mp.Manager().list()
    _serve(httpserver, hits)

    for n in range(3):
        got = _round(httpserver, hits, clean_redis, "rerun-fresh", dedup="redis", run_id=f"r{n}")
        assert got == PAGES, f"第 {n + 1} 轮只抓到 {got}/{PAGES} 页"
    # 每轮每页恰好一次：三轮就是每页 3 次
    counts = Counter(h for h in list(hits) if h.startswith("/p/"))
    assert all(c == 3 for c in counts.values()), {k: v for k, v in counts.items() if v != 3}

    # key 落在各自运行下；运行索引里三个都在
    import redis as redis_lib

    client = redis_lib.from_url(clean_redis, decode_responses=True)
    try:
        assert client.zcard("netspy:rerun-fresh:runs") == 3
        assert not client.exists("netspy:rerun-fresh:lock:seed"), "作业级种子锁不该出现"
        for n in range(3):
            assert client.exists(f"netspy:rerun-fresh:run:r{n}:lock:seed")
            assert client.ttl(f"netspy:rerun-fresh:run:r{n}:lock:seed") > 0, "运行 key 没有 TTL"
    finally:
        client.close()
