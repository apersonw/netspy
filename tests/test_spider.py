from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import fakeredis
import pytest
from pytest_httpserver import HTTPServer
from werkzeug import Response as WResponse

from netspy import Request, Spider, setting
from netspy.core import redis_scheduler
from netspy.core.task_queue import RedisTaskQueue
from netspy.exceptions import ValidationError


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_scheduler, "get_redis", lambda url=None: client)
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
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 3)


NS = "netspy"  # setting.REDIS_KEY_PREFIX


def _setup_pages(server: HTTPServer, pages: int, per: int) -> None:
    for page in range(1, pages + 1):
        lis = "".join(f"<li>p{page}i{i}</li>" for i in range(per))
        nxt = f'<a id="next" href="/page/{page + 1}">n</a>' if page < pages else ""
        server.expect_request(f"/page/{page}").respond_with_data(
            f"<html><body><ul>{lis}</ul>{nxt}</body></html>",
            content_type="text/html; charset=utf-8",
        )


class Crawl(Spider):
    def __init__(self, start_url: str, **kw: Any) -> None:
        self._start_url = start_url
        self.items: list[Any] = []
        super().__init__(item_handler=self.items.extend, **kw)

    def start_requests(self) -> Iterator[Request]:
        yield Request(self._start_url, callback=self.parse_page)

    def parse_page(self, request: Request, response: Any) -> Iterator[Any]:
        for text in response.css("li::text").getall():
            yield {"v": text}
        nxt = response.css("a#next::attr(href)").get()
        if nxt:
            yield Request(response.urljoin(nxt), callback=self.parse_page)


# ----------------------------------------------------------------------
def test_spider_crawls_to_completion(httpserver: HTTPServer, fake_redis: Any) -> None:
    _setup_pages(httpserver, pages=3, per=4)
    spider = Crawl(httpserver.url_for("/page/1"), redis_key="t1")
    spider.start()

    assert sorted(d["v"] for d in spider.items) == sorted(
        f"p{p}i{i}" for p in (1, 2, 3) for i in range(4)
    )
    assert spider.scheduler.stats.get("request_ok") == 3
    assert fake_redis.zcard(f"{NS}:t1:z_requests") == 0


def test_seed_lock_prevents_double_seed(httpserver: HTTPServer, fake_redis: Any) -> None:
    _setup_pages(httpserver, pages=1, per=1)
    sched = Crawl(httpserver.url_for("/page/1"), redis_key="t2").scheduler

    sched._seed()
    assert sched._task_queue.qsize() == 1
    sched._seed()  # 第二个节点：锁已被占，跳过
    assert sched._task_queue.qsize() == 1


def test_resume_consumes_queued_request(httpserver: HTTPServer, fake_redis: Any) -> None:
    httpserver.expect_request("/only").respond_with_data(
        "<html><body><li>resumed</li></body></html>", content_type="text/html"
    )
    RedisTaskQueue(f"{NS}:t3b", fake_redis).put(
        Request(httpserver.url_for("/only"), callback="parse_page")
    )

    class S(Crawl):
        def start_requests(self) -> Iterator[Request]:
            raise AssertionError("不该被调用")

    spider = S("unused", redis_key="t3b")
    spider.start()
    assert spider.items == [{"v": "resumed"}]


def test_redis_dedup_across_requests(httpserver: HTTPServer, fake_redis: Any) -> None:
    hits = {"n": 0}

    def handler(_: Any) -> WResponse:
        hits["n"] += 1
        return WResponse("<html><body><li>x</li></body></html>", content_type="text/html")

    httpserver.expect_request("/dup").respond_with_handler(handler)

    class S(Crawl):
        def start_requests(self) -> Iterator[Request]:
            for _ in range(5):
                yield Request(self._start_url, callback=self.parse_page)

    S(httpserver.url_for("/dup"), redis_key="t4").start()
    assert hits["n"] == 1


def test_failed_requests_go_to_redis_list(httpserver: HTTPServer, fake_redis: Any) -> None:
    httpserver.expect_request("/bad").respond_with_data("no", status=500)

    class S(Spider):
        __custom_setting__ = {"SPIDER_MAX_RETRY_TIMES": 1}

        def start_requests(self) -> Iterator[Request]:
            yield Request(httpserver.url_for("/bad"), callback=self.parse)

        def validate(self, request: Request, response: Any) -> bool:
            if response.status_code != 200:
                raise ValidationError("bad")
            return True

        def parse(self, request: Request, response: Any) -> None:
            return None

    S(redis_key="t5").start()
    failed = fake_redis.lrange(f"{NS}:t5:failed_requests", 0, -1)
    assert len(failed) == 1
    assert "/bad" in failed[0]


def test_heartbeat_registered_and_cleaned(httpserver: HTTPServer, fake_redis: Any) -> None:
    _setup_pages(httpserver, pages=1, per=1)
    hkey = f"{NS}:t6:heartbeat"
    seen: dict[str, int] = {}

    class S(Crawl):
        def parse_page(self, request: Request, response: Any) -> Iterator[Any]:
            # 等一下再看，别比谁快：工作线程先于心跳线程启动（心跳在 _on_start 里起），
            # 只有一页的时候，worker 可能在第一次心跳写进去之前就解析完了。整个文件连着跑、
            # 机器热起来之后这场比赛就会输，用例随机红。要验的是「跑着的时候登记了心跳」
            deadline = time.monotonic() + 2
            while fake_redis.hlen(hkey) == 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            seen["fields"] = fake_redis.hlen(hkey)
            yield from Crawl.parse_page(self, request, response)

    S(httpserver.url_for("/page/1"), redis_key="t6").start()
    assert seen["fields"] == 1
    assert fake_redis.hlen(hkey) == 0


def test_all_nodes_idle_logic(httpserver: HTTPServer, fake_redis: Any) -> None:
    _setup_pages(httpserver, pages=1, per=1)
    sched = Crawl(httpserver.url_for("/page/1"), redis_key="t7").scheduler
    hkey = f"{NS}:t7:heartbeat"
    now = time.time()

    fake_redis.hset(hkey, "idle-node", f"{now:.3f}:0")
    assert sched._all_nodes_idle() is True

    fake_redis.hset(hkey, "busy-node", f"{now:.3f}:4")
    assert sched._all_nodes_idle() is False

    fake_redis.hset(hkey, "busy-node", f"{now - 999:.3f}:4")  # 陈旧 -> 忽略
    assert sched._all_nodes_idle() is True


def test_keep_alive_never_auto_done(httpserver: HTTPServer, fake_redis: Any) -> None:
    _setup_pages(httpserver, pages=1, per=1)
    sched = Crawl(httpserver.url_for("/page/1"), redis_key="t8", keep_alive=True).scheduler
    assert sched._is_done() is False
