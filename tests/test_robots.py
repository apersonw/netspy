"""robots.txt 支持。"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WRequest
from werkzeug.wrappers import Response as WResponse

from netspy import setting
from netspy.network import robots, throttle
from netspy.network.robots import RobotsCache, robots_url

RULES = "User-agent: *\nDisallow: /private\nAllow: /\n"


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    robots.reset()
    throttle.reset()
    monkeypatch.setattr(setting, "ROBOTS_OBEY", True)
    monkeypatch.setattr(setting, "ROBOTS_USER_AGENT", "*")
    monkeypatch.setattr(setting, "CONCURRENT_REQUESTS_PER_DOMAIN", 0)
    monkeypatch.setattr(setting, "DOWNLOAD_DELAY", 0.0)
    yield
    robots.reset()
    throttle.reset()


def _serve(httpserver: HTTPServer, body: str, status: int = 200) -> None:
    httpserver.expect_request("/robots.txt").respond_with_data(
        body, status=status, content_type="text/plain"
    )


# ---- 基本判定 --------------------------------------------------------
def test_robots_url_is_derived_from_any_url() -> None:
    assert robots_url("https://a.com/deep/path?x=1") == "https://a.com/robots.txt"


def test_disallow_is_honored(httpserver: HTTPServer) -> None:
    _serve(httpserver, RULES)
    cache = RobotsCache()
    assert cache.allowed(httpserver.url_for("/public")) is True
    assert cache.allowed(httpserver.url_for("/private/x")) is False


def test_missing_robots_allows_everything(httpserver: HTTPServer) -> None:
    """404 = 没有 robots.txt = 没有限制，这是标准行为。"""
    _serve(httpserver, "nope", status=404)
    assert RobotsCache().allowed(httpserver.url_for("/anything")) is True


def test_server_error_fails_open(httpserver: HTTPServer) -> None:
    """5xx 时放行 —— 一次瞬时 500 不该让整个爬虫停摆。"""
    _serve(httpserver, "boom", status=503)
    assert RobotsCache().allowed(httpserver.url_for("/anything")) is True


def test_unreachable_host_fails_open() -> None:
    assert RobotsCache().allowed("http://127.0.0.1:1/x") is True


def test_malformed_robots_fails_open(httpserver: HTTPServer) -> None:
    _serve(httpserver, "\x00\xff 这不是 robots.txt")
    assert RobotsCache().allowed(httpserver.url_for("/x")) is True


# ---- 缓存 ------------------------------------------------------------
def test_fetched_once_then_cached(httpserver: HTTPServer) -> None:
    hits = []

    def handler(request: WRequest) -> WResponse:
        hits.append(1)
        return WResponse(RULES, content_type="text/plain")

    httpserver.expect_request("/robots.txt").respond_with_handler(handler)
    cache = RobotsCache()
    for _ in range(5):
        cache.allowed(httpserver.url_for("/x"))
    assert len(hits) == 1, f"robots.txt 应只抓一次，实际 {len(hits)}"


def test_concurrent_first_access_fetches_once(httpserver: HTTPServer) -> None:
    """多个 worker 同时首访同一个域，不该各抓一遍。"""
    hits: list[int] = []
    lock = threading.Lock()

    def handler(request: WRequest) -> WResponse:
        with lock:
            hits.append(1)
        time.sleep(0.1)  # 拉长窗口，让并发真的重叠
        return WResponse(RULES, content_type="text/plain")

    httpserver.expect_request("/robots.txt").respond_with_handler(handler)
    cache = RobotsCache()
    threads = [
        threading.Thread(target=lambda: cache.allowed(httpserver.url_for("/x"))) for _ in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(hits) == 1, f"并发首访应只抓一次，实际 {len(hits)}"


def test_cache_expires_after_ttl(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "ROBOTS_CACHE_TTL", 0.15)
    hits = []

    def handler(request: WRequest) -> WResponse:
        hits.append(1)
        return WResponse(RULES, content_type="text/plain")

    httpserver.expect_request("/robots.txt").respond_with_handler(handler)
    cache = RobotsCache()
    cache.allowed(httpserver.url_for("/x"))
    time.sleep(0.25)
    cache.allowed(httpserver.url_for("/x"))
    assert len(hits) == 2


# ---- Crawl-delay 喂给限速 --------------------------------------------
def test_crawl_delay_is_applied_to_throttle(httpserver: HTTPServer) -> None:
    _serve(httpserver, "User-agent: *\nCrawl-delay: 0.3\nAllow: /\n")
    cache = RobotsCache()
    cache.allowed(httpserver.url_for("/x"))

    domain = throttle.domain_of(httpserver.url_for("/x"))
    assert cache.crawl_delay(httpserver.url_for("/x")) == pytest.approx(0.3)
    # 该域的间隔应被限速器接管
    th = throttle._default
    with th._lock:
        assert th._domain_delay.get(domain) == pytest.approx(0.3)


def test_global_delay_wins_when_larger(monkeypatch: pytest.MonkeyPatch) -> None:
    """站点声明的节奏不该反过来放宽全局设置。"""
    monkeypatch.setattr(setting, "DOWNLOAD_DELAY", 1.0)
    monkeypatch.setattr(setting, "RANDOMIZE_DOWNLOAD_DELAY", False)
    th = throttle.DomainThrottle()
    th.set_domain_delay("a.com", 0.1)
    th._take_ticket("a.com")
    with th._lock:
        scheduled = th._next_at["a.com"] - time.monotonic()
    assert scheduled == pytest.approx(1.0, abs=0.1), "应取 max(全局, 站点声明)"


# ---- 总开关 ----------------------------------------------------------
def test_obey_disabled_makes_no_request(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """关闭时不但放行，而且**完全不抓** robots.txt。"""
    monkeypatch.setattr(setting, "ROBOTS_OBEY", False)
    hits = []

    def handler(request: WRequest) -> WResponse:
        hits.append(1)
        return WResponse(RULES, content_type="text/plain")

    httpserver.expect_request("/robots.txt").respond_with_handler(handler)
    assert robots.allowed(httpserver.url_for("/private/x")) is True
    assert hits == [], "关闭时不该产生任何 robots.txt 请求"


def test_module_level_allowed_respects_rules(httpserver: HTTPServer) -> None:
    _serve(httpserver, RULES)
    assert robots.allowed(httpserver.url_for("/private/x")) is False
    assert robots.allowed(httpserver.url_for("/ok")) is True


# ---- 端到端：完整 AirSpider 链路 --------------------------------------
def test_disallowed_url_is_skipped_not_failed(httpserver: HTTPServer) -> None:
    """被 robots 拦下是**有意跳过**，不该计入失败、也不该产生请求。"""
    import netspy as ns
    from netspy.utils import stats as sk

    _serve(httpserver, RULES)
    hits: list[str] = []

    def handler(request: WRequest) -> WResponse:
        hits.append(request.path)
        return WResponse("<h1>ok</h1>", content_type="text/html")

    httpserver.expect_request("/private/secret").respond_with_handler(handler)
    httpserver.expect_request("/public/page").respond_with_handler(handler)

    setting.ITEM_PIPELINES = []
    setting.SPIDER_THREAD_COUNT = 1
    parsed: list[str] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/private/secret"), callback=self.parse)
            yield ns.Request(httpserver.url_for("/public/page"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            parsed.append(response.url)

    spider = S()
    spider.start()
    stats = spider._scheduler.stats.as_dict()  # type: ignore[attr-defined]

    assert "/private/secret" not in hits, "被禁止的 URL 不该真的发出请求"
    assert "/public/page" in hits
    assert len(parsed) == 1
    assert stats.get(sk.ROBOTS_DROPPED, 0) == 1
    assert stats.get(sk.REQUEST_FAILED, 0) == 0, "robots 跳过不该计入失败"
