"""渲染请求必须跟另外三个下载器一样走代理池。

实测复现的真 bug：`PlaywrightDownloader` 原来**根本不问代理池**，只认启动时
写死的 `WEBDRIVER["proxy"]`。`PROXY_ENABLE=True` 时普通请求都走代理，
`render=True` 的请求却从源 IP 直连目标站——文档承诺「拿不到代理时**不会**
直连」，渲染这条路连代理池都没问过，而且一声不吭。

前三组不起浏览器，验「出口怎么选」；最后两条起真 chromium，验「选出来的
出口真的被用上了」——参数传对了不等于浏览器真走了那个代理。
"""

from __future__ import annotations

import http.server
import itertools
import queue
import threading
import urllib.request
from collections.abc import Iterator
from typing import Any
from unittest import mock

import pytest

from netspy import setting
from netspy.exceptions import ProxyUnavailableError, RequestError
from netspy.network.downloader import _playwright as pw
from netspy.network.downloader._playwright import PlaywrightDownloader
from netspy.network.proxy_pool.base import ProxyPool
from netspy.network.request import Request
from netspy.network.response import Response


class _SpyPool(ProxyPool):
    def __init__(self, proxies: list[str | None]) -> None:
        self._it = itertools.cycle(proxies)
        self.asked = 0
        self.good: list[str] = []
        self.bad: list[str] = []
        self.suspect: list[str] = []

    def get_proxy(self) -> str | None:
        self.asked += 1
        return next(self._it)

    def report_good(self, proxy: str) -> None:
        self.good.append(proxy)

    def report_bad(self, proxy: str) -> None:
        self.bad.append(proxy)

    def report_suspect(self, proxy: str) -> None:
        self.suspect.append(proxy)


def _use_pool(monkeypatch: pytest.MonkeyPatch, pool: ProxyPool) -> None:
    monkeypatch.setattr(setting, "PROXY_ENABLE", True)
    monkeypatch.setattr("netspy.network.downloader._common.get_proxy_pool", lambda: pool)


def _stub_render_pool(dl: PlaywrightDownloader) -> mock.Mock:
    fake = mock.Mock()
    fake.submit.return_value = Response(url="http://t/", status_code=200, content=b"ok")
    dl._pool = fake  # type: ignore[assignment]
    return fake


# ---------------------------------------------------------------- 出口怎么选（不起浏览器）
def test_render_request_takes_its_proxy_from_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _SpyPool(["http://p:1"])
    _use_pool(monkeypatch, spy)
    dl = PlaywrightDownloader({"headless": True, "timeout": 5})
    render_pool = _stub_render_pool(dl)

    dl.download(Request("http://t/", render=True))

    assert spy.asked == 1, "渲染请求没问代理池——会从源 IP 直连"
    assert render_pool.submit.call_args.args[1] == "http://p:1"
    assert spy.good == ["http://p:1"], "成功一次要给代理池报喜，清零连续失败计数"


def test_render_refuses_to_go_direct_when_the_pool_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """池空且没开 PROXY_ALLOW_DIRECT：抛错，**一个字节都不发**。"""
    _use_pool(monkeypatch, _SpyPool([None]))
    monkeypatch.setattr(setting, "PROXY_ALLOW_DIRECT", False)
    monkeypatch.setattr(setting, "PROXY_WAIT_TIMEOUT", 0.05)
    monkeypatch.setattr(setting, "PROXY_MIN_INTERVAL", 0.0)
    dl = PlaywrightDownloader({"headless": True, "timeout": 5})
    render_pool = _stub_render_pool(dl)

    with pytest.raises(ProxyUnavailableError):
        dl.download(Request("http://t/", render=True))
    render_pool.submit.assert_not_called()


def test_explicit_render_proxy_beats_the_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _SpyPool(["http://pool:1"])
    _use_pool(monkeypatch, spy)
    dl = PlaywrightDownloader({"headless": True, "timeout": 5, "proxy": "http://fixed:9"})
    render_pool = _stub_render_pool(dl)

    dl.download(Request("http://t/", render=True))

    assert spy.asked == 0
    assert render_pool.submit.call_args.args[1] == "http://fixed:9"
    assert spy.good == [], "不是从池里拿的代理，不该替它向池汇报"


def _render_error(cause: str) -> RequestError:
    """模拟 `_RenderPool.submit()` 抛出的那种错误：外层 RequestError，原因挂在 __cause__。"""
    err = RequestError("渲染失败")
    err.__cause__ = RuntimeError(cause)
    return err


def test_proxy_fault_bans_the_proxy_other_failures_only_mark_suspect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = _SpyPool(["http://p:1"])
    _use_pool(monkeypatch, spy)
    dl = PlaywrightDownloader({"headless": True, "timeout": 5})
    render_pool = _stub_render_pool(dl)

    render_pool.submit.side_effect = _render_error("net::ERR_PROXY_CONNECTION_FAILED at http://t/")
    with pytest.raises(RequestError):
        dl.download(Request("http://t/", render=True))
    assert spy.bad == ["http://p:1"]

    render_pool.submit.side_effect = _render_error("Timeout 30000ms exceeded")
    with pytest.raises(RequestError):
        dl.download(Request("http://t/", render=True))
    assert spy.suspect == ["http://p:1"], "说不清是谁的错的失败不该直接拉黑代理"


# ---------------------------------------------------------------- 一个出口一个 context（假浏览器）
class _Ctx:
    def __init__(self, kwargs: dict[str, Any]) -> None:
        self.kwargs = kwargs
        self.closed = False
        self.route = mock.Mock()
        self.add_init_script = mock.Mock()

    def close(self) -> None:
        self.closed = True


class _Browser:
    def __init__(self) -> None:
        self.contexts: list[_Ctx] = []

    def new_context(self, **kwargs: Any) -> _Ctx:
        ctx = _Ctx(kwargs)
        self.contexts.append(ctx)
        return ctx


def _worker_with_fake_browser(config: dict[str, Any]) -> tuple[pw._RenderWorker, _Browser]:
    worker = pw._RenderWorker(0, queue.Queue(), config)
    browser = _Browser()
    worker._browser = browser
    worker._context = browser.new_context(user_agent="UA-1", locale="zh-CN")
    worker._ctx_kwargs = {"user_agent": "UA-1", "locale": "zh-CN"}
    return worker, browser


def test_each_proxy_gets_its_own_context_with_the_same_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Chromium 的代理是 context 级的：换出口就得换 context，但画像（UA / 时区……）
    要跟默认 context 一致——否则换个代理，指纹也跟着变了。"""
    monkeypatch.setattr(pw, "_stealth_script", lambda: "/* stealth */")
    worker, _ = _worker_with_fake_browser({"headless": True})

    a = worker._context_for("http://a:1")
    b = worker._context_for("http://b:1")
    assert worker._context_for("http://a:1") is a, "同一个出口要复用 context"
    assert a is not b and a is not worker._context
    for ctx, server in ((a, "http://a:1"), (b, "http://b:1")):
        assert ctx.kwargs["proxy"] == {"server": server}
        assert ctx.kwargs["user_agent"] == "UA-1" and ctx.kwargs["locale"] == "zh-CN"
        ctx.add_init_script.assert_called_once()  # 反检测脚本也得挂上
    assert worker._context_for(None) is worker._context


def test_proxy_contexts_are_bounded_and_evicted_ones_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pw, "_stealth_script", lambda: "")
    monkeypatch.setattr(setting, "SESSION_CACHE_SIZE", 2)
    worker, _ = _worker_with_fake_browser({"headless": True})

    first = worker._context_for("http://1:1")
    worker._context_for("http://2:1")
    worker._context_for("http://3:1")
    assert first.closed, "被换出的 context 不关的话，它的页面 / 连接就泄漏了"
    assert len(worker._proxy_contexts) == 2

    worker._teardown()
    assert not worker._proxy_contexts


def test_persistent_profile_fails_closed_instead_of_leaking_the_ip() -> None:
    """持久化 profile 只有启动时那一个 context，出口改不了。拿它去渲染一个本该
    走别的代理的请求，等于从错误的出口打出去——必须报错，不能凑合。"""
    worker = pw._RenderWorker(0, queue.Queue(), {"headless": True, "user_data_dir": "/x"})
    worker._context = object()  # persistent 模式：有 context，没有 browser
    with pytest.raises(RequestError, match="WEBDRIVER"):
        worker._context_for("http://pool:1")


# ---------------------------------------------------------------- 真浏览器：真的走了那个代理
def _serve(handler: type[http.server.BaseHTTPRequestHandler]) -> http.server.ThreadingHTTPServer:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


@pytest.fixture
def target() -> Iterator[tuple[str, list[str]]]:
    direct: list[str] = []

    class Target(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            direct.append(self.path)
            body = f"<html><title>{self.path}</title></html>".encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    server = _serve(Target)
    # `localhost.` 而不是 127.0.0.1：Chromium 对回环地址默认绕过代理（<-loopback>），
    # 用 127.0.0.1 的话测的就不是代理了
    yield f"http://localhost.:{server.server_address[1]}", direct
    server.shutdown()


def _forwarding_proxy(hits: list[str]) -> http.server.ThreadingHTTPServer:
    class Proxy(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # 对 http 目标，浏览器发绝对 URI 的 GET 给代理
            hits.append(self.path)
            with urllib.request.urlopen(self.path) as upstream:
                body = upstream.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    return _serve(Proxy)


@pytest.mark.render
def test_real_browser_rotates_through_pool_proxies(
    target: tuple[str, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("playwright")
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    hits_a: list[str] = []
    hits_b: list[str] = []
    proxy_a, proxy_b = _forwarding_proxy(hits_a), _forwarding_proxy(hits_b)
    _use_pool(
        monkeypatch,
        _SpyPool(
            [
                f"http://127.0.0.1:{proxy_a.server_address[1]}",
                f"http://127.0.0.1:{proxy_b.server_address[1]}",
            ]
        ),
    )
    base, _ = target
    dl = PlaywrightDownloader({"headless": True, "timeout": 20, "pool_size": 1})
    try:
        titles = [
            dl.download(Request(f"{base}/p{i}", render=True)).css("title::text").get()
            for i in range(4)
        ]
    finally:
        dl.close()
        proxy_a.shutdown()
        proxy_b.shutdown()
    assert titles == ["/p0", "/p1", "/p2", "/p3"]
    assert len(hits_a) == 2 and len(hits_b) == 2, (
        f"渲染请求没按代理池轮换出口：A={len(hits_a)} B={len(hits_b)}"
    )


@pytest.mark.render
def test_real_browser_never_reaches_the_target_directly_through_a_dead_proxy(
    target: tuple[str, list[str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """代理连不上时请求该失败，而不是绕过代理去直连——那正是源 IP 暴露的样子。"""
    pytest.importorskip("playwright")
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    _use_pool(monkeypatch, _SpyPool(["http://127.0.0.1:1"]))
    base, direct = target
    dl = PlaywrightDownloader({"headless": True, "timeout": 10, "pool_size": 1})
    try:
        with pytest.raises(RequestError):
            dl.download(Request(f"{base}/secret", render=True))
    finally:
        dl.close()
    assert direct == [], f"目标站被直连了 {len(direct)} 次——源 IP 暴露"
