from __future__ import annotations

import threading
from collections.abc import Iterator
from unittest import mock

import httpx
import pytest
import respx

from netspy import Request, RequestError, setting
from netspy.network.downloader import (
    HttpxDownloader,
    close_default_downloaders,
    download_request,
    get_default_downloader,
)
from netspy.network.user_agent import USER_AGENTS


@pytest.fixture(autouse=True)
def _close_downloaders() -> Iterator[None]:
    yield
    close_default_downloaders()


@respx.mock
def test_download_returns_response() -> None:
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, html="<h1>Hi</h1>"))
    resp = Request("https://example.com/").download()
    assert resp.status_code == 200
    assert resp.ok
    assert resp.xpath("//h1/text()").get() == "Hi"
    assert resp.request is not None


@respx.mock
def test_query_and_headers_are_sent() -> None:
    route = respx.get("https://example.com/s").mock(return_value=httpx.Response(200))
    Request("https://example.com/s", params={"q": "x"}, headers={"X-Test": "1"}).download()
    sent = route.calls.last.request
    assert sent.url.params["q"] == "x"
    assert sent.headers["x-test"] == "1"


@respx.mock
def test_random_user_agent_injected_by_default() -> None:
    route = respx.get("https://example.com/").mock(return_value=httpx.Response(200))
    Request("https://example.com/").download()
    assert route.calls.last.request.headers["user-agent"] in USER_AGENTS


@respx.mock
def test_explicit_user_agent_not_overridden() -> None:
    route = respx.get("https://example.com/").mock(return_value=httpx.Response(200))
    Request("https://example.com/", headers={"User-Agent": "mine/1.0"}).download()
    assert route.calls.last.request.headers["user-agent"] == "mine/1.0"


@respx.mock
def test_random_user_agent_disabled() -> None:
    route = respx.get("https://example.com/").mock(return_value=httpx.Response(200))
    Request("https://example.com/", random_user_agent=False).download()
    assert route.calls.last.request.headers["user-agent"] not in USER_AGENTS


@respx.mock
def test_allow_redirects_false_stops_at_302() -> None:
    respx.get("https://example.com/a").mock(
        return_value=httpx.Response(302, headers={"Location": "https://example.com/b"})
    )
    resp = Request("https://example.com/a", allow_redirects=False).download()
    assert resp.status_code == 302


@respx.mock
def test_redirects_followed_by_default() -> None:
    respx.get("https://example.com/a").mock(
        return_value=httpx.Response(302, headers={"Location": "https://example.com/b"})
    )
    respx.get("https://example.com/b").mock(return_value=httpx.Response(200, text="B"))
    resp = Request("https://example.com/a").download()
    assert resp.status_code == 200
    assert resp.text == "B"
    assert resp.history == ["https://example.com/a"]


@respx.mock
def test_network_error_becomes_request_error() -> None:
    respx.get("https://example.com/").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(RequestError, match="下载失败"):
        Request("https://example.com/").download()


@respx.mock
def test_session_downloader_reuses_client() -> None:
    respx.get("https://example.com/").mock(return_value=httpx.Response(200))
    req = Request("https://example.com/", use_session=True)
    downloader = get_default_downloader(req)
    assert isinstance(downloader, HttpxDownloader)
    download_request(req)
    download_request(req)
    # 断言的是**复用**本身，不是某个内部字段还在不在：
    # 两次请求只能留下一个连接池
    assert len(downloader._clients) == 1


def test_render_true_routes_to_playwright_downloader() -> None:
    from netspy.network.downloader._playwright import PlaywrightDownloader

    dl = get_default_downloader(Request("https://example.com/", render=True))
    assert isinstance(dl, PlaywrightDownloader)


@respx.mock
def test_explicit_downloader_and_context_manager() -> None:
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, text="ok"))
    with HttpxDownloader(use_session=True) as dl:
        resp = Request("https://example.com/").download(dl)
        assert resp.text == "ok"
        assert len(dl._clients) == 1
    assert len(dl._clients) == 0  # __exit__ 已 close


# ---- USE_SESSION 曾是死配置（benchmark 里两行数字一模一样才暴露）------------
def test_use_session_setting_is_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    """`setting.USE_SESSION` 必须真的生效 —— 它一度只是个装饰品。"""
    from netspy.network.downloader import get_default_downloader

    monkeypatch.setattr(setting, "USE_SESSION", True)
    monkeypatch.setattr(setting, "DOWNLOADER_ASYNC", False)
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    dl = get_default_downloader(Request("http://x/"))
    assert isinstance(dl, HttpxDownloader)
    assert dl._use_session is True


def test_request_use_session_overrides_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    from netspy.network.downloader import get_default_downloader

    monkeypatch.setattr(setting, "USE_SESSION", True)
    monkeypatch.setattr(setting, "DOWNLOADER_ASYNC", False)
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    dl = get_default_downloader(Request("http://x/", use_session=False))
    assert dl._use_session is False


def test_use_session_default_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    from netspy.network.downloader import get_default_downloader

    monkeypatch.setattr(setting, "DOWNLOADER_ASYNC", False)
    monkeypatch.setattr(setting, "DOWNLOADER_IMPERSONATE", "")
    assert get_default_downloader(Request("http://x/"))._use_session is False


# ---- SSL context 缓存：每请求新建 Client 的最大单项开销 --------------------
def test_ssl_context_is_cached() -> None:
    """同一个 verify 值必须拿到同一个 SSLContext —— 否则每请求 ~33ms 白花。"""
    from netspy.network.downloader._common import ssl_context_for

    assert ssl_context_for(True) is ssl_context_for(True)


def test_ssl_context_passthrough_for_uncacheable() -> None:
    import ssl as _ssl

    from netspy.network.downloader._common import ssl_context_for

    assert ssl_context_for(False) is False
    ctx = _ssl.create_default_context()
    assert ssl_context_for(ctx) is ctx


# ---- close_default_downloaders() 必须跟 get_default_downloader() 共用一把锁 --
def test_close_default_downloaders_does_not_race_with_get_default_downloader() -> None:
    """回归测试：关闭全部默认下载器时不能被并发的首次构造打断。

    这是实测复现出来的真 bug，跟 `redisdb.close_redis()` 是一模一样的坑——
    调度器停工作线程用的是**有超时的** `join()`，一个慢请求的 worker 完全
    可能在下载器整体关闭时还在跑，这时它正并发调 `get_default_downloader()`
    首次构造某个下载器并写进 `_defaults`。原来的 `close_default_downloaders()`
    遍历 `_defaults.values()` 时完全不等 `get_default_downloader()` 用的那把
    锁，字典大小在遍历中途被改变会直接抛
    `RuntimeError: dictionary changed size during iteration`；
    即使侥幸没抛，也可能把「刚建好、调用方还没来得及用上」的下载器
    一起摘走关掉，造成 use-after-close。

    用一个在 `close()` 里暂停的假下载器，强制另一个线程的
    `get_default_downloader()` 插入恰好落在遍历中途。
    """
    from netspy.network import downloader as dl_mod

    dl_mod._defaults.clear()

    paused = threading.Event()
    proceed = threading.Event()

    pausing = mock.Mock()

    def pausing_close() -> None:
        paused.set()
        proceed.wait(timeout=2)

    pausing.close.side_effect = pausing_close
    dl_mod._defaults["existing"] = pausing
    for i in range(1, 5):
        dl_mod._defaults[f"key{i}"] = mock.Mock()

    errors: list[BaseException] = []

    def closer() -> None:
        try:
            dl_mod.close_default_downloaders()
        except BaseException as exc:
            errors.append(exc)

    t = threading.Thread(target=closer)
    t.start()
    assert paused.wait(timeout=2), "没能让 close 卡在遍历中途，测试前提不成立"

    with dl_mod._lock:
        dl_mod._defaults["brand-new-mid-iteration"] = mock.Mock()

    proceed.set()
    t.join(timeout=2)

    assert not errors, f"close_default_downloaders() 不该抛异常：{errors}"
