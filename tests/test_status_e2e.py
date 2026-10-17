"""状态码策略的端到端行为：真 socket + 完整 AirSpider 链路。

单测证明分类函数对，这里证明「跑起来真的按分类走」—— 429 会等待重试、
404 不再进 parse、3xx 仍能到回调。
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WRequest
from werkzeug.wrappers import Response as WResponse

import netspy as ns
from netspy import setting
from netspy.network.downloader import close_default_downloaders


@pytest.fixture(autouse=True)
def _quiet() -> Iterator[None]:
    from netspy.network import throttle

    throttle.reset()
    setting.ITEM_PIPELINES = []
    setting.SPIDER_THREAD_COUNT = 1
    setting.LOG_LEVEL = "CRITICAL"
    from netspy.utils import log

    log.configure()
    yield
    close_default_downloaders()
    throttle.reset()


def _run(spider: ns.AirSpider) -> None:
    spider.start()


def test_404_never_reaches_parse(httpserver: HTTPServer) -> None:
    """0.7.0 破坏性变更的核心：错误页不再被当成数据。"""
    httpserver.expect_request("/gone").respond_with_data("<h1>Not Found</h1>", status=404)
    parsed: list[Any] = []
    failed: list[Any] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/gone"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            parsed.append(response.status_code)

        def failed_request(self, request, response):  # type: ignore[no-untyped-def]
            failed.append(response.status_code if response else None)
            return None

    _run(S())
    assert parsed == [], "404 不该进 parse"
    assert failed == [404], "404 应走 failed_request 钩子"


def test_404_reaches_parse_when_accepted(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ACCEPT_STATUS_CODES 是把特定码放回 parse 的正规口子。"""
    monkeypatch.setattr(setting, "ACCEPT_STATUS_CODES", [404])
    httpserver.expect_request("/gone").respond_with_data("gone", status=404)
    parsed: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/gone"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            parsed.append(response.status_code)

    _run(S())
    assert parsed == [404]


def test_429_retries_and_honors_retry_after(httpserver: HTTPServer) -> None:
    """429 要退避重试，并且真的按 Retry-After 等待。"""
    hits: list[float] = []

    def handler(request: WRequest) -> WResponse:
        hits.append(time.monotonic())
        if len(hits) == 1:
            return WResponse("slow down", status=429, headers={"Retry-After": "1"})
        return WResponse("<h1>ok</h1>", content_type="text/html")

    httpserver.expect_request("/limited").respond_with_handler(handler)
    parsed: list[str] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/limited"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            parsed.append(response.xpath("//h1/text()").get() or "")

    _run(S())
    assert parsed == ["ok"], "重试后应拿到正常内容"
    assert len(hits) == 2
    assert hits[1] - hits[0] >= 0.9, f"应按 Retry-After 等约 1s，实际 {hits[1] - hits[0]:.2f}s"


def test_retry_after_too_long_gives_up(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retry-After 超上限时直接判失败，不占着 worker 干等。"""
    monkeypatch.setattr(setting, "RETRY_AFTER_MAX", 2.0)
    httpserver.expect_request("/limited").respond_with_data(
        "later", status=429, headers={"Retry-After": "3600"}
    )
    failed: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/limited"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            return None

        def failed_request(self, request, response):  # type: ignore[no-untyped-def]
            failed.append(response.status_code if response else -1)
            return None

    t0 = time.monotonic()
    _run(S())
    assert failed == [429]
    assert time.monotonic() - t0 < 10, "不该真的去等 3600s"


def test_3xx_still_reaches_parse_when_redirects_disabled(httpserver: HTTPServer) -> None:
    """手工处理重定向是正当用法，不能被状态码策略打断。"""
    httpserver.expect_request("/from").respond_with_data(
        "", status=302, headers={"Location": "/to"}
    )
    got: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(
                httpserver.url_for("/from"), callback=self.parse, allow_redirects=False
            )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            got.append(response.status_code)

    _run(S())
    assert got == [302]


def test_check_disabled_restores_0_6_behavior(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """提供可回退的开关 —— 破坏性变更必须留退路。"""
    monkeypatch.setattr(setting, "CHECK_STATUS_CODE", False)
    httpserver.expect_request("/gone").respond_with_data("<h1>404 page</h1>", status=404)
    parsed: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            yield ns.Request(httpserver.url_for("/gone"), callback=self.parse)

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            parsed.append(response.status_code)

    _run(S())
    assert parsed == [404], "关掉开关后应回到 0.6.0 的全部放行"


# ---- per-domain 限速的端到端行为 --------------------------------------
def test_concurrency_cap_observed_by_server(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """服务端观测到的并发必须被压到上限 —— 证明限速真的作用在完整链路上。"""
    from netspy.network import throttle

    throttle.reset()
    monkeypatch.setattr(setting, "CONCURRENT_REQUESTS_PER_DOMAIN", 2)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 8)

    live = 0
    peak = 0
    lock = __import__("threading").Lock()

    def handler(request: WRequest) -> WResponse:
        nonlocal live, peak
        with lock:
            live += 1
            peak = max(peak, live)
        time.sleep(0.05)
        with lock:
            live -= 1
        return WResponse("<h1>ok</h1>", content_type="text/html")

    httpserver.expect_request("/p").respond_with_handler(handler)

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(12):
                yield ns.Request(
                    httpserver.url_for("/p") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            return None

    _run(S())
    throttle.reset()
    assert peak <= 2, f"单域并发应被压到 2，服务端观测到 {peak}"


def test_default_cap_does_not_bind_default_threads(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """默认上限 8 > 默认线程数 4 —— 对默认配置应当完全无感。"""
    from netspy.network import throttle

    throttle.reset()
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 4)
    httpserver.expect_request("/q").respond_with_data("<h1>ok</h1>", content_type="text/html")
    seen: list[int] = []

    class S(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(8):
                yield ns.Request(
                    httpserver.url_for("/q") + f"?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            seen.append(response.status_code)

    t0 = time.monotonic()
    _run(S())
    throttle.reset()
    assert len(seen) == 8
    assert time.monotonic() - t0 < 5, "默认配置不该被限速拖慢"
