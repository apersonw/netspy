from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from netspy import AirSpider, Request, Response, setting
from netspy.network.middleware import DownloaderMiddleware, MiddlewareManager

_MOD = __name__


class AddHeader(DownloaderMiddleware):
    def process_request(self, request: Request) -> Request:
        request.requests_kwargs.setdefault("headers", {})["X-MW"] = "1"
        return request


class ShortCircuit(DownloaderMiddleware):
    def process_request(self, request: Request) -> Response:
        return Response(
            url=request.url,
            status_code=200,
            content=b"<p>cached</p>",
            headers={"content-type": "text/html"},
        )


class RescheduleOn202(DownloaderMiddleware):
    def process_response(self, request: Request, response: Response) -> Response | Request:
        if response.status_code == 202:
            return Request(response.url + "?retried=1")
        return response


def test_process_request_chain_mutates() -> None:
    out = MiddlewareManager([f"{_MOD}.AddHeader"]).process_request(Request("https://x"))
    assert isinstance(out, Request)
    assert out.requests_kwargs["headers"]["X-MW"] == "1"


def test_process_request_short_circuits_with_response() -> None:
    out = MiddlewareManager([f"{_MOD}.ShortCircuit"]).process_request(Request("https://x"))
    assert isinstance(out, Response)
    assert "cached" in out.text


def test_process_response_can_reschedule() -> None:
    mgr = MiddlewareManager([f"{_MOD}.RescheduleOn202"])
    out = mgr.process_response(Request("https://x"), Response(url="https://x", status_code=202))
    assert isinstance(out, Request)
    assert "retried=1" in out.url


def test_empty_manager_is_passthrough() -> None:
    mgr = MiddlewareManager()
    req = Request("https://x")
    assert mgr.process_request(req) is req
    assert not mgr


def test_spider_uses_global_middleware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(setting, "DONE_CHECK_INTERVAL", 0.04)
    monkeypatch.setattr(setting, "DONE_CHECK_TIMES", 2)
    monkeypatch.setattr(setting, "BUFFER_FLUSH_INTERVAL", 0.02)
    monkeypatch.setattr(setting, "DOWNLOADER_MIDDLEWARES", [f"{_MOD}.ShortCircuit"])

    class S(AirSpider):
        def start_requests(self) -> Iterator[Request]:
            yield Request("https://never-fetched.invalid/", callback=self.parse)

        def parse(self, request: Request, response: Any) -> Iterator[Any]:
            yield {"body": response.text}

    items: list[Any] = []
    S(item_handler=items.extend).start()
    assert items == [{"body": "<p>cached</p>"}]


def test_spider_middleware_adds_header(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setting, "DONE_CHECK_INTERVAL", 0.04)
    monkeypatch.setattr(setting, "DONE_CHECK_TIMES", 2)
    monkeypatch.setattr(setting, "BUFFER_FLUSH_INTERVAL", 0.02)
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    monkeypatch.setattr(setting, "DOWNLOADER_MIDDLEWARES", [f"{_MOD}.AddHeader"])
    seen: dict[str, str] = {}

    def handler(request: Any) -> Any:
        from werkzeug import Response as WResponse

        seen["ns"] = request.headers.get("X-MW", "")
        return WResponse("<html></html>", content_type="text/html")

    httpserver.expect_request("/p").respond_with_handler(handler)

    class S(AirSpider):
        def start_requests(self) -> Iterator[Request]:
            yield Request(httpserver.url_for("/p"), callback=self.parse)

        def parse(self, request: Request, response: Any) -> None:
            return None

    S().start()
    assert seen["ns"] == "1"
