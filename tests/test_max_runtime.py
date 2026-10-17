"""运行时长上限与熔断的端到端行为。"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WRequest
from werkzeug.wrappers import Response as WResponse

import netspy as ns
from netspy import setting
from netspy.network import circuit, robots, throttle
from netspy.network.downloader import close_default_downloaders
from netspy.utils import stats as sk


@pytest.fixture(autouse=True)
def _clean() -> Iterator[None]:
    circuit.reset()
    throttle.reset()
    robots.reset()
    setting.ITEM_PIPELINES = []
    setting.LOG_LEVEL = "CRITICAL"
    from netspy.utils import log

    log.configure()
    yield
    close_default_downloaders()
    circuit.reset()
    throttle.reset()
    robots.reset()


# ---- 运行时长上限 ----------------------------------------------------
def test_max_runtime_stops_gracefully(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """到点优雅停止，**正常返回而不抛异常** —— 定时任务不该每次都报错。"""
    monkeypatch.setattr(setting, "SPIDER_MAX_RUNTIME", 1.0)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 2)

    def handler(request: WRequest) -> WResponse:
        time.sleep(0.05)
        return WResponse("<h1>ok</h1>", content_type="text/html")

    httpserver.expect_request("/slow").respond_with_handler(handler)
    seen: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(2000):  # 远超 1s 能跑完的量
                yield ns.Request(
                    httpserver.url_for("/slow") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            seen.append(response.status_code)

    t0 = time.monotonic()
    S().start()  # 不该抛异常
    elapsed = time.monotonic() - t0

    assert elapsed < 15, f"应在上限附近停止，实际 {elapsed:.1f}s"
    assert seen, "停止前应该抓到了一些"
    assert len(seen) < 2000, "不该跑完全部"


def test_zero_means_no_limit(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "SPIDER_MAX_RUNTIME", 0.0)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 2)
    httpserver.expect_request("/q").respond_with_data("<h1>ok</h1>", content_type="text/html")
    seen: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(6):
                yield ns.Request(
                    httpserver.url_for("/q") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            seen.append(response.status_code)

    S().start()
    assert len(seen) == 6, "不限时应跑完"


# ---- 熔断的端到端 ----------------------------------------------------
def test_persistent_503_trips_the_breaker(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setting, "CIRCUIT_FAILURE_THRESHOLD", 2)
    monkeypatch.setattr(setting, "CIRCUIT_COOLDOWN", 30.0)
    monkeypatch.setattr(setting, "SPIDER_MAX_RETRY_TIMES", 0)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 1)
    monkeypatch.setattr(setting, "RETRY_AFTER_MAX", 0.0)
    httpserver.expect_request("/down").respond_with_data("boom", status=503)

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(2):
                yield ns.Request(
                    httpserver.url_for("/down") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            return None

    S().start()
    domain = throttle.domain_of(httpserver.url_for("/down"))
    th = throttle._default
    with th._lock:
        scheduled = th._next_at.get(domain, 0.0) - time.monotonic()
    assert scheduled > 25, "持续 503 应让该域被熔断冷却"


def test_repeated_404_does_not_trip(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """按 ID 探测的经典场景：连续 404 不该把熔断器打开。"""
    monkeypatch.setattr(setting, "CIRCUIT_FAILURE_THRESHOLD", 2)
    monkeypatch.setattr(setting, "SPIDER_MAX_RETRY_TIMES", 0)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 1)
    httpserver.expect_request("/gone").respond_with_data("nope", status=404)

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(6):
                yield ns.Request(
                    httpserver.url_for("/gone") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            return None

    spider = S()
    spider.start()
    stats = spider._scheduler.stats.as_dict()  # type: ignore[attr-defined]

    domain = throttle.domain_of(httpserver.url_for("/gone"))
    th = throttle._default
    with th._lock:
        scheduled = th._next_at.get(domain, 0.0) - time.monotonic()
    assert scheduled <= 0, "连续 404 不该触发熔断"
    assert stats.get(sk.REQUEST_FAILED, 0) == 6, "404 仍然计失败，只是不熔断"
