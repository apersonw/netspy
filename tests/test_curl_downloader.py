"""CurlDownloader（curl_cffi）—— TLS / HTTP2 指纹伪装。

respx 只能拦 httpx，所以这里一律打真实本地 socket（pytest-httpserver）。
"""

from __future__ import annotations

import threading
from collections.abc import Iterator

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WerkzeugRequest
from werkzeug.wrappers import Response as WerkzeugResponse

from netspy import Request, RequestError, setting
from netspy.network.downloader import close_default_downloaders, get_default_downloader
from netspy.network.downloader._common import resolve_impersonate
from netspy.network.downloader._curl import CurlDownloader
from netspy.network.proxy_pool import ProxyPool, close_proxy_pool
from netspy.network.user_agent import USER_AGENTS

IMPERSONATE = "chrome"


@pytest.fixture
def downloader() -> Iterator[CurlDownloader]:
    dl = CurlDownloader()
    try:
        yield dl
    finally:
        dl.close()


@pytest.fixture(autouse=True)
def _cleanup() -> Iterator[None]:
    yield
    close_default_downloaders()
    close_proxy_pool()


def _echo_ua(httpserver: HTTPServer, path: str = "/ua") -> str:
    """让服务端把收到的 User-Agent 回显出来。"""

    def handler(request: WerkzeugRequest) -> WerkzeugResponse:
        return WerkzeugResponse(request.headers.get("User-Agent", ""), content_type="text/plain")

    httpserver.expect_request(path).respond_with_handler(handler)
    return httpserver.url_for(path)


# ----------------------------------------------------------------------
def test_download_returns_response(downloader: CurlDownloader, httpserver: HTTPServer) -> None:
    httpserver.expect_request("/").respond_with_data(
        "<h1>Hi</h1>", content_type="text/html; charset=utf-8"
    )
    resp = downloader.download(Request(httpserver.url_for("/")))
    assert resp.status_code == 200
    assert resp.ok
    assert resp.xpath("//h1/text()").get() == "Hi"
    assert resp.request is not None
    assert isinstance(resp.elapsed, float)


def test_chinese_content_decodes(downloader: CurlDownloader, httpserver: HTTPServer) -> None:
    httpserver.expect_request("/zh").respond_with_data(
        "<p>中文内容</p>", content_type="text/html; charset=utf-8"
    )
    resp = downloader.download(Request(httpserver.url_for("/zh")))
    assert resp.css("p::text").get() == "中文内容"


def test_impersonate_suppresses_random_ua(
    downloader: CurlDownloader, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """伪装时绝不能再塞 UA 池里的 UA。

    TLS 握手说 Chrome、UA 头说别的浏览器 —— 这种自相矛盾比不伪装还容易被识破，
    所以 impersonate 一旦生效就必须让 curl_cffi 自己给出整套自洽的浏览器头。
    """
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", True)
    url = _echo_ua(httpserver)

    ua = downloader.download(Request(url, impersonate=IMPERSONATE)).text
    assert ua, "服务端应收到 User-Agent"
    assert ua not in USER_AGENTS, "伪装时不应使用 UA 池里的 UA"
    assert "Chrome/" in ua

    # 同一伪装目标应稳定复现同一 UA（UA 池是随机的，不会稳定）
    again = downloader.download(Request(url, impersonate=IMPERSONATE)).text
    assert again == ua


def test_random_ua_still_applies_without_impersonate(
    downloader: CurlDownloader, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", True)
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    url = _echo_ua(httpserver)
    assert downloader.download(Request(url)).text in USER_AGENTS


def test_explicit_headers_win_over_impersonate(
    downloader: CurlDownloader, httpserver: HTTPServer
) -> None:
    url = _echo_ua(httpserver)
    resp = downloader.download(
        Request(url, impersonate=IMPERSONATE, headers={"User-Agent": "my-bot/1.0"})
    )
    assert resp.text == "my-bot/1.0"


def test_redirects_followed_by_default(downloader: CurlDownloader, httpserver: HTTPServer) -> None:
    httpserver.expect_request("/from").respond_with_data(
        "", status=302, headers={"Location": httpserver.url_for("/to")}
    )
    httpserver.expect_request("/to").respond_with_data("done")
    resp = downloader.download(Request(httpserver.url_for("/from")))
    assert resp.status_code == 200
    assert resp.text == "done"
    assert resp.history


def test_allow_redirects_false_stops_at_302(
    downloader: CurlDownloader, httpserver: HTTPServer
) -> None:
    httpserver.expect_request("/from").respond_with_data(
        "", status=302, headers={"Location": httpserver.url_for("/to")}
    )
    resp = downloader.download(Request(httpserver.url_for("/from"), allow_redirects=False))
    assert resp.status_code == 302


def test_network_error_becomes_request_error(downloader: CurlDownloader) -> None:
    # 127.0.0.1:1 上不会有人监听
    with pytest.raises(RequestError):
        downloader.download(Request("http://127.0.0.1:1/"))


def test_session_reuse_keeps_one_session(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/s").respond_with_data("ok")
    dl = CurlDownloader(use_session=True)
    try:
        dl.download(Request(httpserver.url_for("/s")))
        first, _, _ = dl._session_for(Request(httpserver.url_for("/s")))
        dl.download(Request(httpserver.url_for("/s")))
        again, _, _ = dl._session_for(Request(httpserver.url_for("/s")))
        # 断言的是复用本身，不是某个内部字段 —— 后者一重构就误报，
        # 而真正该守的行为反而没人守
        assert again is first
    finally:
        dl.close()
    assert len(dl._sessions) == 0


def test_cookie_jar_shared_across_session_shards(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """回归测试：同一个代理（含"没配代理"这个值）的不同分片必须共享同一个
    cookie jar，不能因为分片就各过各的、互相看不见对方种下的 cookie。

    不是假设性的——`_jar_for()`/`_session_for_proxy()` 的文档字符串里明确
    记着这是个真出过的 bug："第一版只有它的 1/片数，代理一多 jar 先被换出，
    同代理的两个分片就分家了"。修复本身当时没有测试守着，升级/重构很容易
    悄悄退化回去。

    用真实线程而不是直接摆弄内部计数器——分片用的 `shard_index` 是按线程
    分配的 thread-local 值，伪造它反而测不出真实场景；两个线程顺序执行
    （full join 再起下一个）保证拿到的是连续的分片计数，`% 2` 后必然落在
    两个不同分片，断言不会因为测试执行顺序而偶发抖动。
    """
    monkeypatch.setattr(setting, "CURL_SESSION_SHARD_THREADS", 1)
    monkeypatch.setattr(setting, "SPIDER_THREAD_COUNT", 2)

    seen_cookies: list[str] = []

    def handler(request: WerkzeugRequest) -> WerkzeugResponse:
        seen_cookies.append(request.headers.get("Cookie", ""))
        resp = WerkzeugResponse("ok")
        resp.set_cookie("sid", "abc123")
        return resp

    httpserver.expect_request("/c").respond_with_handler(handler)
    url = httpserver.url_for("/c")

    dl = CurlDownloader(use_session=True)
    sessions: list[object] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            dl.download(Request(url))
            session, _, _ = dl._session_for(Request(url))
            sessions.append(session)
        except BaseException as exc:  # 子线程异常要带回主线程才能断言
            errors.append(exc)

    try:
        t1 = threading.Thread(target=worker)
        t1.start()
        t1.join()
        t2 = threading.Thread(target=worker)
        t2.start()
        t2.join()
    finally:
        dl.close()

    assert not errors, f"子线程里出错：{errors}"
    assert len(sessions) == 2
    assert sessions[0] is not sessions[1], "两个线程没有真的分到两片，测试前提不成立"
    assert seen_cookies[1] == "sid=abc123", "第二个分片应该能看到第一个分片种下的 cookie"


def test_close_is_idempotent() -> None:
    dl = CurlDownloader(use_session=True)
    dl.close()
    dl.close()


# ---- 代理池接入 ------------------------------------------------------
class _SpyPool(ProxyPool):
    def __init__(self) -> None:
        self.handed: list[str] = []
        self.bad: list[str] = []

    def get_proxy(self) -> str:
        self.handed.append("http://127.0.0.1:1")
        return "http://127.0.0.1:1"

    def report_bad(self, proxy: str) -> None:
        self.bad.append(proxy)


def test_uses_proxy_pool_and_reports_bad(
    downloader: CurlDownloader, monkeypatch: pytest.MonkeyPatch
) -> None:
    spy = _SpyPool()
    monkeypatch.setattr("netspy.network.downloader._common.get_proxy_pool", lambda: spy)
    with pytest.raises(RequestError):
        downloader.download(Request("http://example.invalid/"))
    assert spy.handed == ["http://127.0.0.1:1"]
    assert spy.bad == ["http://127.0.0.1:1"]


# ---- impersonate 的解析与传播 ----------------------------------------
def test_resolve_impersonate_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "safari17_0")
    assert resolve_impersonate(Request("http://x/")) == "safari17_0"
    assert resolve_impersonate(Request("http://x/", impersonate="chrome131")) == "chrome131"

    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    assert resolve_impersonate(Request("http://x/")) is None


def test_impersonate_survives_serialization() -> None:
    """分布式模式下 Request 要经 Redis round-trip，丢了 impersonate 等于 worker 裸奔。"""
    restored = Request.from_dict(Request("http://x/", impersonate="chrome131").to_dict())
    assert restored.impersonate == "chrome131"


def test_default_downloader_picks_curl_when_impersonating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "chrome")
    assert isinstance(get_default_downloader(Request("http://x/")), CurlDownloader)


def test_render_beats_impersonate(monkeypatch: pytest.MonkeyPatch) -> None:
    """render 要真浏览器，浏览器自带真实指纹，不该被 curl 抢走。"""
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "chrome")
    assert not isinstance(get_default_downloader(Request("http://x/", render=True)), CurlDownloader)


def test_no_impersonate_keeps_httpx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    monkeypatch.setattr(setting, "DOWNLOADER_ASYNC", False)
    assert not isinstance(get_default_downloader(Request("http://x/")), CurlDownloader)


# ---- 真指纹验证（需要外网，CI 用 -m "not network" 排除）----------------
@pytest.mark.network
def test_ja3_actually_differs_from_httpx() -> None:
    """唯一能真正证明「伪装生效」的测试：同一端点，两个下载器的 JA3 必须不同。"""
    from netspy.network.downloader._httpx import HttpxDownloader

    url = "https://tls.peet.ws/api/all"
    with HttpxDownloader() as plain, CurlDownloader() as curl:
        bare = plain.download(Request(url)).json()
        faked = curl.download(Request(url, impersonate=IMPERSONATE)).json()

    assert bare["tls"]["ja3_hash"] != faked["tls"]["ja3_hash"], "伪装后 JA3 应当改变"
    assert "Chrome" in faked["http_version"] or faked["tls"]["ja3_hash"]
