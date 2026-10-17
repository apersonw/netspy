from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import respx

from netspy import Request, RequestError, setting
from netspy.network.downloader import close_default_downloaders, get_default_downloader
from netspy.network.downloader._async_httpx import AsyncHttpxDownloader, loop_count
from netspy.network.user_agent import USER_AGENTS


@pytest.fixture
def downloader() -> Iterator[AsyncHttpxDownloader]:
    dl = AsyncHttpxDownloader(concurrency=8)
    try:
        yield dl
    finally:
        dl.close()


@respx.mock
def test_download_returns_response(downloader: AsyncHttpxDownloader) -> None:
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, html="<h1>Hi</h1>"))
    resp = downloader.download(Request("https://example.com/"))
    assert resp.status_code == 200
    assert resp.ok
    assert resp.xpath("//h1/text()").get() == "Hi"
    assert resp.request is not None


@respx.mock
def test_params_headers_and_random_ua(downloader: AsyncHttpxDownloader) -> None:
    route = respx.get("https://example.com/s").mock(return_value=httpx.Response(200))
    downloader.download(Request("https://example.com/s", params={"q": "x"}, headers={"X-T": "1"}))
    sent = route.calls.last.request
    assert sent.url.params["q"] == "x"
    assert sent.headers["x-t"] == "1"
    assert sent.headers["user-agent"] in USER_AGENTS


@respx.mock
def test_random_ua_disabled(downloader: AsyncHttpxDownloader) -> None:
    route = respx.get("https://example.com/").mock(return_value=httpx.Response(200))
    downloader.download(Request("https://example.com/", random_user_agent=False))
    assert route.calls.last.request.headers["user-agent"] not in USER_AGENTS


@respx.mock
def test_redirects_followed_by_default(downloader: AsyncHttpxDownloader) -> None:
    respx.get("https://example.com/a").mock(
        return_value=httpx.Response(302, headers={"Location": "https://example.com/b"})
    )
    respx.get("https://example.com/b").mock(return_value=httpx.Response(200, text="B"))
    resp = downloader.download(Request("https://example.com/a"))
    assert resp.text == "B"
    assert resp.history == ["https://example.com/a"]


@respx.mock
def test_allow_redirects_false_stops_at_302(downloader: AsyncHttpxDownloader) -> None:
    respx.get("https://example.com/a").mock(
        return_value=httpx.Response(302, headers={"Location": "https://example.com/b"})
    )
    resp = downloader.download(Request("https://example.com/a", allow_redirects=False))
    assert resp.status_code == 302


@respx.mock
def test_network_error_becomes_request_error(downloader: AsyncHttpxDownloader) -> None:
    respx.get("https://example.com/").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(RequestError, match="下载失败"):
        downloader.download(Request("https://example.com/"))


@respx.mock
def test_worker_threads_do_not_each_get_a_loop(downloader: AsyncHttpxDownloader) -> None:
    """12 个 worker 不该开出 12 个事件循环线程。

    v4.34 之后循环数是 `ceil(线程数 / ASYNC_THREADS_PER_LOOP)`，不再恒为 1 ——
    但「不是每个 worker 一个」这个意图不变，所以断言改成跟 `loop_count()` 对齐。
    **匹配线程名用前缀**：线程名带上了分片序号，用 `==` 匹配会一个都匹配不到，
    断言就变成空过（`test_close_is_idempotent` 当时正是这么假绿的）。
    """
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, text="ok"))
    results: list[str] = []

    def hit() -> None:
        results.append(downloader.download(Request("https://example.com/")).text)

    workers = [threading.Thread(target=hit) for _ in range(12)]
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=5)

    assert results == ["ok"] * 12
    loop_threads = [t for t in threading.enumerate() if t.name.startswith("async-downloader")]
    assert len(loop_threads) == loop_count()
    assert len(loop_threads) < 12


@respx.mock
def test_cookie_jar_shared_across_event_loop_shards() -> None:
    """回归测试：不同事件循环分片必须共享同一份 cookie jar，登录态这类状态
    不能因为落到不同分片就丢。

    这是实测撞到过的真 bug，不是假设性的——`AsyncHttpxDownloader` 原来完全
    没有 `_httpx.py`/`_curl.py` 那套「跨分片共享 jar」的机制，每个分片的
    `AsyncClient` 各自持有独立的 cookie jar，第一个分片落的登录 cookie，
    下一个请求分到另一个分片就看不见了，而且是静默的、不报错。
    """
    seen_cookies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_cookies.append(request.headers.get("cookie", ""))
        return httpx.Response(200, headers={"set-cookie": "sid=abc123"})

    respx.get("https://example.com/c").mock(side_effect=handler)

    dl = AsyncHttpxDownloader(concurrency=8, loops=2)
    try:
        assert len(dl._shards) == 2, "没有真的分到两片，测试前提不成立"

        def worker() -> None:
            dl.download(Request("https://example.com/c"))

        t1 = threading.Thread(target=worker)
        t1.start()
        t1.join()
        t2 = threading.Thread(target=worker)
        t2.start()
        t2.join()
    finally:
        dl.close()

    assert seen_cookies == ["", "sid=abc123"], "第二个分片应该能看到第一个分片种下的 cookie"


def test_close_is_idempotent() -> None:
    """关两次不抛，且循环线程真的都收掉了。

    ⚠️ 这条曾经假绿：线程名从 `async-downloader` 变成 `async-downloader-0` 之后，
    `t.name == "async-downloader"` 一个都匹配不到，`not any(...)` 恒为真。
    先建一个**多片**的下载器，确保「每一片都要收」这件事真被验到。
    """
    dl = AsyncHttpxDownloader(concurrency=2, loops=3)
    assert sum(t.name.startswith("async-downloader") for t in threading.enumerate()) == 3
    dl.close()
    dl.close()  # 不抛
    assert not any(
        t.name.startswith("async-downloader") and t.is_alive() for t in threading.enumerate()
    )


def test_close_does_not_hang_a_worker_stuck_on_an_in_flight_download(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回归测试：`close()` 时一个还没跑完的下载协程，不该让调用线程永久卡死。

    `_Shard.submit()` 是 `run_coroutine_threadsafe(coro, loop).result()`，
    不带超时。`close()` 原来只是 `loop.call_soon_threadsafe(loop.stop)`——
    这只让 `run_forever()` 在当前这轮跑完后返回，**不会主动结束还挂起的
    协程**。调度器停工作线程用的是**有超时的** join，一个慢请求的 worker
    完全可能还卡在 `shard.submit(...).result()` 里；loop 一停，那个协程
    从此再也没有机会被推进，它的 `Future` 永远停在 pending，调用方的
    `.result()` 就跟着永久卡死——**不是超时失败，是真正无限期地挂起**，
    这比其它「优雅停止失效」的坑更严重：连一个异常都不会有。

    用一个卡在 `run_in_executor` 上、只能被外部信号唤醒的协程模拟"一个还
    没跑完的慢请求"，然后并发调用 `close()`，断言调用线程能在有限时间内
    收到结果（无论是异常还是别的），而不是永远醒不过来。
    """
    dl = AsyncHttpxDownloader(concurrency=4)
    never_release = threading.Event()
    started = threading.Event()

    async def hang_forever(self: AsyncHttpxDownloader, request: Request, shard: Any) -> None:
        started.set()
        await asyncio.get_event_loop().run_in_executor(None, never_release.wait)

    monkeypatch.setattr(AsyncHttpxDownloader, "_download", hang_forever)

    result: dict[str, Any] = {}

    def worker() -> None:
        try:
            result["response"] = dl.download(Request("https://example.com/"))
        except Exception as exc:
            result["error"] = exc

    t = threading.Thread(target=worker, daemon=True)
    t.start()
    try:
        assert started.wait(timeout=2), "下载协程没能真正开始，测试前提不成立"
        time.sleep(0.1)

        closer = threading.Thread(target=dl.close, daemon=True)
        closer.start()
        closer.join(timeout=10)
        assert not closer.is_alive(), "close() 本身不该被一个挂起的协程拖住"

        t.join(timeout=5)
        assert not t.is_alive(), "worker 线程永久卡住了——这正是要修的 bug"
        assert "error" in result, f"应该收到一个可捕获的异常，而不是别的：{result}"
    finally:
        never_release.set()


@respx.mock
def test_get_default_downloader_uses_async_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DOWNLOADER_ASYNC", True)
    respx.get("https://example.com/").mock(return_value=httpx.Response(200, text="ok"))
    try:
        dl = get_default_downloader(Request("https://example.com/"))
        assert isinstance(dl, AsyncHttpxDownloader)
        assert dl.download(Request("https://example.com/")).text == "ok"
    finally:
        close_default_downloaders()
