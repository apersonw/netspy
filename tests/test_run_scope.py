"""运行作用域（RUN_ID）：同一个爬虫**重跑**时到底是「续上次」还是「从头来」。

**生产上撞到的**（NetspyHub，2026-09-11 → 09-13）：一个 2 节点的分布式任务
每 5 分钟定时跑一次，49 小时里 1160 个实例有 **1158 个「请求成功 0，入库 0 条」**，
全部 exit 0、全部被平台记成 success。只有第一轮真干了活。

机制：`{ns}:lock:seed` 是 24 小时 TTL 的一次性锁，第一轮之后所有节点都走
「另一节点已注入种子」；每 24 小时抢到锁的那一个，种子 URL 又被**无 TTL 的
Redis 布隆**去重掉。框架的模型是「一个 redis_key = 一个可续跑的作业」，
而定时任务要的是「每次触发 = 一次全新运行」。两边各自都对，缝是错的。

这里先把那个症状原样复现出来（第一条用例），再验修法：
设了 RUN_ID，队列 / 种子锁 / 在途 / 心跳 / 失败列表 / 去重都落到
`{ns}:run:{RUN_ID}` 下，每次运行互不相干。
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import fakeredis
import pytest
from pytest_httpserver import HTTPServer

from netspy import Item, Request, Spider, setting
from netspy.core import redis_scheduler
from netspy.pipelines.base import BasePipeline
from netspy.utils import log

NS = "netspy"


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_scheduler, "get_redis", lambda url=None: client)
    # 去重 / Item 去重走的是各自模块里的 get_redis，也要指到同一个假实例
    from netspy.dedup import redis_filter

    monkeypatch.setattr(redis_filter, "_default_redis", lambda: client)
    yield client
    client.flushall()


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DONE_CHECK_INTERVAL", 0.05)
    monkeypatch.setattr(setting, "DONE_CHECK_TIMES", 2)
    monkeypatch.setattr(setting, "BUFFER_FLUSH_INTERVAL", 0.02)
    monkeypatch.setattr(setting, "HEARTBEAT_INTERVAL", 0.05)
    monkeypatch.setattr(setting, "HEARTBEAT_STALE", 5.0)
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 2)
    # 空跑的节点会等启动宽限；生产上是 10 秒，测试里别等
    monkeypatch.setattr(setting, "SPIDER_STARTUP_GRACE", 0.2)
    # 分布式该用的去重后端 —— 生产上就是它把种子 URL 挡掉的
    monkeypatch.setattr(setting, "DEDUP_FILTER", "redis")


@pytest.fixture
def logfile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    # 项目用 loguru，caplog 抓不到 —— 照 test_log.py 的做法写文件再读
    path = tmp_path / "ns.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(path))
    monkeypatch.setattr(setting, "LOG_LEVEL", "INFO")
    log.configure()
    return path


PAGES = 3


def _serve(server: HTTPServer, hits: list[str]) -> None:
    def seed(_: Any) -> Any:
        from werkzeug import Response

        hits.append("/seed")
        links = "".join(f'<a href="/p/{i}">p{i}</a>' for i in range(PAGES))
        return Response(f"<html><body>{links}</body></html>", content_type="text/html")

    def page(request: Any) -> Any:
        from werkzeug import Response

        hits.append(request.path)
        return Response("<html><body><h1>ok</h1></body></html>", content_type="text/html")

    server.expect_request("/seed").respond_with_handler(seed)
    for i in range(PAGES):
        server.expect_request(f"/p/{i}").respond_with_handler(page)


class Book(Item):
    __table_name__ = "books"
    __unique_key__ = ["url"]

    url: str


class CollectPipeline(BasePipeline):
    """把入库的行收进一个模块级列表。

    **必须走管道，不能走 item_handler**：handler 那条路直接交付、不过 Item 去重，
    用它写的话「Item 指纹跟不跟运行」这条断言永远是绿的（第一版就是这么假绿的，
    把 `_make_item_dedup` 整个删掉测试照样全过）。
    """

    rows: list[dict[str, Any]] = []

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        CollectPipeline.rows.extend(items)
        return True


class Crawl(Spider):
    def __init__(self, seed_url: str, **kw: Any) -> None:
        self._seed_url = seed_url
        super().__init__(pipelines=[f"{__name__}.CollectPipeline"], **kw)

    @property
    def items(self) -> list[dict[str, Any]]:
        return CollectPipeline.rows

    def start_requests(self) -> Iterator[Request]:
        yield Request(self._seed_url, callback=self.parse_seed)

    def parse_seed(self, request: Request, response: Any) -> Iterator[Any]:
        for href in response.css("a::attr(href)").getall():
            yield Request(response.urljoin(href), callback=self.parse_page)

    def parse_page(self, request: Request, response: Any) -> Iterator[Any]:
        item = Book()
        item.url = response.url
        yield item


def _run(url: str, **kw: Any) -> tuple[Crawl, list[dict[str, Any]]]:
    """跑一轮，返回 (spider, 这一轮入库的行)。"""
    before = len(CollectPipeline.rows)
    spider = Crawl(url, redis_key="rerun", **kw)
    spider.start()
    return spider, CollectPipeline.rows[before:]


@pytest.fixture(autouse=True)
def _reset_rows() -> Iterator[None]:
    CollectPipeline.rows = []
    yield
    CollectPipeline.rows = []


# ======================================================================
# 1. 症状本身：不设 RUN_ID，重跑就是空转 —— 而且是 exit 0 的那种
# ======================================================================
def test_rerun_without_run_id_does_nothing_and_exits_clean(
    httpserver: HTTPServer, fake_redis: Any
) -> None:
    hits: list[str] = []
    _serve(httpserver, hits)
    url = httpserver.url_for("/seed")

    first, rows1 = _run(url)
    assert first.scheduler.stats.get("request_ok") == PAGES + 1
    assert len(rows1) == PAGES

    second, rows2 = _run(url)
    # 这就是生产上那 1158 个实例：0 请求，0 入库，正常退出
    assert second.scheduler.stats.get("request_ok") == 0
    assert rows2 == []
    assert len(hits) == PAGES + 1, "第二轮一个请求都没发出去"
    # 原因摆在 Redis 里：种子锁还在，布隆里有全部 URL
    assert fake_redis.exists(f"{NS}:rerun:lock:seed")
    assert int(fake_redis.get(f"{NS}:rerun:dedup:bloom:count") or 0) == PAGES + 1


def test_a_stale_job_is_reported_loudly(
    httpserver: HTTPServer, fake_redis: Any, logfile: Path
) -> None:
    """空转可以是用户要的（续爬一个已完成的作业），但**不能是静默的**。

    第二个进程一启动就该看得出来：种子锁是上一轮留下的、队列空、没有活节点。
    这时要告诉用户为什么本轮什么都不会干、以及怎么改。

    ⚠️ 判据是「锁的年龄 > 启动宽限」，用来把「和我同时启动的节点」和「上一轮的
    残留」分开。**不能靠两个 _run 之间真实流逝的时间**去凑那个年龄 —— 第一版
    就是这么写的，本地第一轮爬完花了 1 秒多（age=1）过了，CI 上快到不足 1 秒
    （TTL 还没掉一整秒，age=0）就不触发，四个 Python 版本齐红。
    这里显式把种子锁「变旧」，模拟定时任务两轮之间那 5 分钟间隔 —— 确定性的。
    """
    hits: list[str] = []
    _serve(httpserver, hits)
    url = httpserver.url_for("/seed")
    _run(url)

    # 模拟「上一轮是 5 分钟前跑的」：把锁的剩余 TTL 往下压，等价于它老了 300 秒
    seed_key = f"{NS}:rerun:lock:seed"
    fake_redis.expire(seed_key, int(setting.SPIDER_SEED_LOCK_TTL - 300))
    _run(url)

    text = logfile.read_text(encoding="utf-8")
    assert "WARNING" in text
    assert "已完成" in text and "RUN_ID" in text, text[-600:]


# ======================================================================
# 2. 修法：RUN_ID 让每次运行互不相干
# ======================================================================
def test_run_id_makes_each_run_start_fresh(
    httpserver: HTTPServer, fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    hits: list[str] = []
    _serve(httpserver, hits)
    url = httpserver.url_for("/seed")

    monkeypatch.setattr(setting, "RUN_ID", "run-1")
    first, rows1 = _run(url)
    monkeypatch.setattr(setting, "RUN_ID", "run-2")
    second, rows2 = _run(url)

    # 两轮都真的抓了：靶子被打了两遍
    assert first.scheduler.stats.get("request_ok") == PAGES + 1
    assert second.scheduler.stats.get("request_ok") == PAGES + 1
    assert len(hits) == 2 * (PAGES + 1)
    # Item 去重也是按运行来的：第二轮的 3 本书没被第一轮的指纹挡掉。
    # 不然就是「请求成功 4，入库 0」—— 请求重抓了，数据一条没落，另一种空转
    assert len(rows1) == PAGES and len(rows2) == PAGES, (len(rows1), len(rows2))
    assert (second.scheduler.stats.get("item_dedup_dropped") or 0) == 0

    # key 落在各自的运行下，作业级别的 key 一个都没碰
    assert fake_redis.exists(f"{NS}:rerun:run:run-1:lock:seed")
    assert fake_redis.exists(f"{NS}:rerun:run:run-2:lock:seed")
    assert not fake_redis.exists(f"{NS}:rerun:lock:seed")
    assert not fake_redis.exists(f"{NS}:rerun:dedup:bloom")
    assert fake_redis.exists(f"{NS}:rerun:run:run-2:dedup:bloom")


def test_same_run_id_resumes_that_run(httpserver: HTTPServer, fake_redis: Any) -> None:
    """RUN_ID 相同 = 同一次运行 —— 续跑语义**没有丢**，只是挪到了运行这一层。

    平台上「重跑这个实例」传同一个 RUN_ID 就是续上次，不会从头再抓一遍。
    """
    hits: list[str] = []
    _serve(httpserver, hits)
    url = httpserver.url_for("/seed")

    setting.RUN_ID = "same"
    _run(url)
    second, _ = _run(url)
    assert second.scheduler.stats.get("request_ok") == 0
    assert len(hits) == PAGES + 1


def test_dedup_scope_spider_keeps_fingerprints_across_runs(
    httpserver: HTTPServer, fake_redis: Any, logfile: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """增量爬：运行是新的，但抓过的 URL 不再抓 —— 这是个**显式**选择。"""
    hits: list[str] = []
    _serve(httpserver, hits)
    url = httpserver.url_for("/seed")
    monkeypatch.setattr(setting, "DEDUP_SCOPE", "spider")

    monkeypatch.setattr(setting, "RUN_ID", "run-1")
    _run(url)
    monkeypatch.setattr(setting, "RUN_ID", "run-2")
    second, _ = _run(url)

    # 种子锁是新的（本轮确实种了），但种子 URL 被作业级去重挡下 —— 0 请求
    assert fake_redis.exists(f"{NS}:rerun:run:run-2:lock:seed")
    assert second.scheduler.stats.get("request_ok") == 0
    assert fake_redis.exists(f"{NS}:rerun:dedup:bloom"), "去重该在作业级别"
    assert not fake_redis.exists(f"{NS}:rerun:run:run-2:dedup:bloom")
    # 这种「本轮不会重抓」的行为必须出声 —— 否则又是一个 exit 0 的空转
    text = logfile.read_text(encoding="utf-8")
    assert "DEDUP_SCOPE" in text and "spider" in text, text[-600:]


def test_run_keys_expire_instead_of_piling_up(
    httpserver: HTTPServer, fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """每 5 分钟一次的定时任务，一天就是 288 套 key —— 不带 TTL 的话 Redis 会被撑爆。"""
    hits: list[str] = []
    _serve(httpserver, hits)
    monkeypatch.setattr(setting, "RUN_ID", "ttl-run")
    monkeypatch.setattr(setting, "RUN_TTL", 3600)
    _run(httpserver.url_for("/seed"))

    run_keys = [k for k in fake_redis.keys("*") if ":run:ttl-run:" in k]
    assert run_keys, "运行下面一个 key 都没有？"
    for key in run_keys:
        ttl = fake_redis.ttl(key)
        assert 0 < ttl <= 3600, f"{key} 没有 TTL（ttl={ttl}）"
    # 运行索引也在，且带 TTL —— 监控要靠它列出「这个爬虫最近跑过哪些运行」
    assert fake_redis.zscore(f"{NS}:rerun:runs", "ttl-run") is not None
    assert 0 < fake_redis.ttl(f"{NS}:rerun:runs") <= 3600


def test_keys_are_touched_while_the_run_is_alive(
    httpserver: HTTPServer, fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TTL 由心跳续期：一个跑了 8 天的运行不能在第 7 天被自己的 TTL 清空。"""
    hits: list[str] = []
    _serve(httpserver, hits)
    monkeypatch.setattr(setting, "RUN_ID", "long-run")
    monkeypatch.setattr(setting, "RUN_TTL", 3600)
    spider = Crawl(httpserver.url_for("/seed"), redis_key="rerun")
    sched = spider.scheduler
    sched._seed()
    key = f"{NS}:rerun:run:long-run:lock:seed"
    # 人为把 TTL 拨小，看心跳会不会把它拨回去
    fake_redis.expire(key, 5)
    assert fake_redis.ttl(key) <= 5
    sched._on_start()
    try:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and fake_redis.ttl(key) <= 5:
            time.sleep(0.05)
        assert fake_redis.ttl(key) > 5, "心跳没有续期运行下的 key"
    finally:
        sched._on_shutdown()
