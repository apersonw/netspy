"""停止渲染池时，排队中的请求不能让调用线程永久挂起。

`submit()` 里 `job.event.wait()` 原本没有超时，而 `close()` 只给 worker 置停止位、
塞一个 None 哨兵，**不排空队列** —— worker 一看见停止位就退出，队列里剩下的任务
永远不会 set() 它们的 event。

实测（1 个在渲染、4 个排队时关闭）：**4 个调用线程永久挂起**。
worker 卡在 event.wait() 里，`stop()` 它不看、`join()` 它不动 ——
优雅停止就此失效。

不需要真浏览器：验的是池的生命周期逻辑，桩掉 _ensure_browser / _render。
"""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest import mock

import pytest

from netspy.exceptions import RequestError
from netspy.network.downloader import _playwright as pw
from netspy.network.request import Request
from netspy.network.response import Response

_CFG = {"pool_size": 1, "timeout": 5, "headless": True}


@pytest.fixture
def stub_browser(monkeypatch: pytest.MonkeyPatch) -> Any:
    """不起浏览器；渲染慢到足以让请求排上队。"""
    monkeypatch.setattr(pw._RenderWorker, "_ensure_browser", lambda self: None)

    def _slow(self: Any, request: Request) -> Response:
        time.sleep(0.5)
        return Response(
            url=request.url, status_code=200, content=b"<html>ok</html>", request=request
        )

    monkeypatch.setattr(pw._RenderWorker, "_render", _slow)


def _submit_many(pool: Any, n: int) -> tuple[list[threading.Thread], list[Any]]:
    results: list[Any] = []

    def call(i: int) -> None:
        try:
            pool.submit(Request(f"http://example.com/p/{i}"))
            results.append(("ok", i))
        except RequestError as exc:
            results.append((type(exc).__name__, i))

    threads = [threading.Thread(target=call, args=(i,), daemon=True) for i in range(n)]
    for t in threads:
        t.start()
    return threads, results


def test_queued_requests_do_not_hang_their_callers(stub_browser: Any) -> None:
    """关闭时排队的任务要被收尾，调用者拿到失败 —— 而不是永远等下去。"""
    pool = pw._RenderPool(dict(_CFG))
    threads, results = _submit_many(pool, 5)
    time.sleep(0.2)  # 1 个在渲染，其余排队
    pool.close()

    # 总时限卡死在 5 秒：必须是 close() **当场**收尾，而不是靠 submit() 那个
    # 兜底超时（渲染超时 ×2 + 30 秒）把人熬回来。
    # 第一版这里写的是「每个线程 join(10)」，5 个线程加起来能等到 50 秒 ——
    # 兜底超时正好在 40 秒，于是把排空缺失完全掩盖了，反向验证照样全绿
    deadline = time.monotonic() + 5
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    hung = [i for i, t in enumerate(threads) if t.is_alive()]
    assert not hung, (
        f"关闭 5 秒后仍有 {len(hung)} 个调用线程没返回 —— 停止爬虫时它们 join 不掉，优雅停止失效"
    )
    assert len(results) == 5


def test_normal_rendering_still_works(stub_browser: Any) -> None:
    """别用「全都失败」换「不挂起」—— 正常渲染必须原样通过。"""
    pool = pw._RenderPool(dict(_CFG))
    try:
        response = pool.submit(Request("http://example.com/ok"))
        assert response.status_code == 200
        assert b"ok" in response.content
    finally:
        pool.close()


def test_close_is_idempotent(stub_browser: Any) -> None:
    pool = pw._RenderPool(dict(_CFG))
    pool.submit(Request("http://example.com/ok"))
    pool.close()
    pool.close()


def test_finished_job_is_not_overwritten_by_drain() -> None:
    """排空和 worker 退出是并发的：worker 可能正好取走了这个 job。
    收尾只能生效一次，后到的那次不许覆盖已有结果。"""
    job = pw._Job(Request("http://example.com/x"))
    job.response = Response(
        url="http://example.com/x", status_code=200, content=b"done", request=job.request
    )
    job.event.set()
    job.fail(RuntimeError("晚到的收尾"))
    assert job.error is None, "已经完成的任务被排空覆盖成失败了"


def test_downloader_close_does_not_race_with_in_flight_download() -> None:
    """`PlaywrightDownloader.close()` 不能让并发中的 `download()` 崩掉。

    这是实测复现出来的真 bug——`download()` 原来的写法是双重检查锁建完
    `self._pool` 之后，最后一行 `return self._pool.submit(request)`
    **在锁外重新读了一次** `self._pool`。调度器停工作线程用的是**有超时的**
    join，一个慢渲染请求的 worker 完全可能在下载器整体 `close()` 时还在跑；
    `close()` 恰好在这次重读之前把 `self._pool` 置 None 的话，会直接
    `AttributeError: 'NoneType' object has no attribute 'submit'`——跟
    `proxy_pool.get_proxy_pool()` 是同一类坑。

    用一个临时包住 `__getattribute__` 的技巧模拟真实的线程抢占：`download()`
    第一次读 `self._pool` 时，**先正常拿到那个真实存在的池对象**，再暂停——
    这模拟的是「读取已经完成、线程被切换出去」，而不是「读取本身被推迟」。
    暂停期间让 `close()` 在另一侧把 `self._pool` 置 None，再放行。

    如果 `download()` 用的是第一次读取时已经拿到手的那个引用（修复之后：
    只读一次、存进局部变量），最终应该正常返回，不受并发 `close()` 影响；
    如果它后面又独立重新读了一次 `self._pool`（修复之前的行为），这次
    重读会读到 None，紧接着调用 `.submit()` 直接 `AttributeError`。
    """
    from netspy.network.downloader._playwright import PlaywrightDownloader

    dl = PlaywrightDownloader({"pool_size": 1, "timeout": 5, "headless": True})
    dl._pool = mock.Mock()  # type: ignore[assignment]
    dl._pool.submit.return_value = Response(
        url="http://example.com/x", status_code=200, content=b"ok"
    )

    paused = threading.Event()
    proceed = threading.Event()
    read_count = {"n": 0}
    orig_getattribute = PlaywrightDownloader.__getattribute__

    def patched_getattribute(self: Any, name: str) -> Any:
        if name == "_pool" and self is dl:
            read_count["n"] += 1
            if read_count["n"] == 1:
                # 先正常完成这次读取，拿到当前真实的池对象——
                # 暂停的是「读到之后」，不是「读取本身」
                value = orig_getattribute(self, name)
                paused.set()
                proceed.wait(timeout=2)
                return value
        return orig_getattribute(self, name)

    result: dict[str, Any] = {}

    def downloader_thread() -> None:
        try:
            result["response"] = dl.download(Request("http://example.com/x"))
        except AttributeError as exc:
            result["error"] = exc

    PlaywrightDownloader.__getattribute__ = patched_getattribute  # type: ignore[method-assign]
    try:
        t1 = threading.Thread(target=downloader_thread)
        t1.start()
        assert paused.wait(timeout=2), "没能让 download() 卡在第一次读取 _pool 之后，测试前提不成立"

        dl.close()  # 这里的 self._pool 读取是第二次，不会被暂停，直接看到并清空当前值

        proceed.set()
        t1.join(timeout=2)
    finally:
        PlaywrightDownloader.__getattribute__ = orig_getattribute  # type: ignore[method-assign]

    assert "error" not in result, f"download() 崩了：{result.get('error')!r}"
    assert result["response"].status_code == 200


def test_wait_has_an_upper_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    """兜底：worker 被杀、浏览器卡死、或哪条路径漏了 set()，
    都不该让调用者永远等下去。"""
    monkeypatch.setattr(pw._RenderWorker, "_ensure_browser", lambda self: None)
    # 渲染永不返回，且不设置 event —— 模拟 worker 线程「不在了」
    monkeypatch.setattr(pw._RenderWorker, "run", lambda self: None)

    monkeypatch.setattr(pw, "_WAIT_MARGIN", 0.5)
    pool = pw._RenderPool({"pool_size": 1, "timeout": 0.01, "headless": True})
    started = time.monotonic()
    with pytest.raises(RequestError):
        pool.submit(Request("http://example.com/x"))
    assert time.monotonic() - started < 5
    pool.close()
