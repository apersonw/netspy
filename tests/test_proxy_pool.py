from __future__ import annotations

import threading
import time

import httpx
import pytest
import respx

from netspy import Request, RequestError, setting
from netspy.network.downloader import close_default_downloaders
from netspy.network.downloader._httpx import HttpxDownloader
from netspy.network.proxy_pool import ProxyPool, close_proxy_pool, get_proxy_pool
from netspy.network.proxy_pool.api import ApiProxyPool


@pytest.fixture(autouse=True)
def _cleanup() -> None:
    yield
    close_proxy_pool()
    close_default_downloaders()


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "PROXY_MIN_INTERVAL", 0.0)
    monkeypatch.setattr(setting, "PROXY_MAX_USE_TIMES", 100)


def test_parse_plain_and_json() -> None:
    assert ApiProxyPool._parse("1.1.1.1:80\n2.2.2.2:80") == ["1.1.1.1:80", "2.2.2.2:80"]
    assert ApiProxyPool._parse('["3.3.3.3:80", "4.4.4.4:80"]') == [
        "3.3.3.3:80",
        "4.4.4.4:80",
    ]


@respx.mock
def test_api_pool_fetches_and_rotates() -> None:
    respx.get("https://proxy.api/list").mock(
        return_value=httpx.Response(200, text="1.1.1.1:8000\n2.2.2.2:8000")
    )
    pool = ApiProxyPool("https://proxy.api/list")
    seen = {pool.get_proxy() for _ in range(6)}
    assert seen == {"http://1.1.1.1:8000", "http://2.2.2.2:8000"}


@respx.mock
def test_first_fetch_not_suppressed_by_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    """「从没抓过」必须能抓 —— 否则刚启动的容器里代理池永远是空的。

    间隔取一个比任何 uptime 都大的值：`monotonic()` 原点是开机，一旦拿 0.0 当
    「从没抓过」的哨兵，`now - 0.0 < PROXY_MIN_INTERVAL` 就恒真，第一次抓取被吞。
    这么写不用改动全局时钟，且在任何 uptime 的机器上都能复现。
    """
    monkeypatch.setattr(setting, "PROXY_MIN_INTERVAL", 1e12)
    route = respx.get("https://fresh/l").mock(return_value=httpx.Response(200, text="1.1.1.1:80"))

    pool = ApiProxyPool("https://fresh/l")

    assert pool.get_proxy() == "http://1.1.1.1:80"
    assert route.call_count == 1

    pool.get_proxy()  # 已经抓过，间隔内不该再打接口
    assert route.call_count == 1


@respx.mock
def test_report_bad_does_not_wait_for_unrelated_fetch() -> None:
    """回归测试：`_fetch()` 拉取列表期间，不该拿主锁卡住无关的 `report_bad()`。

    旧实现把 `httpx.get()` 整段包在 `self._lock` 里——池空时一个线程触发拉取，
    另一个线程哪怕只是想给一个跟这次拉取毫不相干的代理记一笔失败，也得先陪
    着干等整个网络往返。实测跟拉取无关的 `report_bad()` 被拖了整整一次拉取
    耗时。用一个人为放慢的响应制造这个窗口：拉取真正开始后再调
    `report_bad()`，旧实现下它要等拉取结束才返回，新实现应该几乎立即返回。
    """
    fetch_started = threading.Event()
    release_fetch = threading.Event()

    def _slow(request: httpx.Request) -> httpx.Response:
        fetch_started.set()
        assert release_fetch.wait(timeout=2), "拉取没能被外部按预期放行"
        return httpx.Response(200, text="1.1.1.1:80")

    respx.get("https://slow/list").mock(side_effect=_slow)
    pool = ApiProxyPool("https://slow/list")

    def fetcher() -> None:
        pool.get_proxy()  # 池是空的，会触发 _fetch()

    t = threading.Thread(target=fetcher)
    t.start()
    assert fetch_started.wait(timeout=2), "没能让拉取真正开始，测试前提不成立"

    start = time.monotonic()
    pool.report_bad("http://9.9.9.9:80")  # 跟这次拉取完全无关的一次报告
    elapsed = time.monotonic() - start

    release_fetch.set()
    t.join(timeout=2)

    assert elapsed < 0.5, f"report_bad() 花了 {elapsed:.2f}s —— 被无关的拉取卡住了"


@respx.mock
def test_report_bad_excludes_proxy() -> None:
    respx.get("https://p/l").mock(return_value=httpx.Response(200, text="1.1.1.1:80\n2.2.2.2:80"))
    pool = ApiProxyPool("https://p/l")
    pool.get_proxy()
    pool.report_bad("http://1.1.1.1:80")
    for _ in range(5):
        assert pool.get_proxy() != "http://1.1.1.1:80"


@respx.mock
def test_get_proxy_pool_singleton_and_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "PROXY_ENABLE", False)
    assert get_proxy_pool() is None

    monkeypatch.setattr(setting, "PROXY_ENABLE", True)
    monkeypatch.setattr(setting, "PROXY_EXTRACT_API", "https://p/l")
    respx.get("https://p/l").mock(return_value=httpx.Response(200, text="9.9.9.9:80"))
    first = get_proxy_pool()
    assert first is get_proxy_pool()
    close_proxy_pool()
    assert get_proxy_pool() is not first


def test_get_proxy_pool_never_returns_none_while_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`PROXY_ENABLE=True` 时 `get_proxy_pool()` 绝不能返回 None。

    这是实测复现出来的真缺陷——原来的写法是「查一次、需要就建、最后再重新
    读一次字典返回」。调度器停工作线程用的是**有超时的** join，一个慢请求
    的 worker 完全可能在下载器整体关闭时还在跑，这时它正调用
    `get_proxy_pool()`；如果 `close_proxy_pool()` 恰好插在「查」和「最后
    那次重读」之间把 `_state["pool"]` 置空，`get_proxy_pool()` 会返回 None，
    调用方（`_attempt_proxy`）把它当成「压根没开代理池」直接放行直连——
    而 `PROXY_ENABLE=True` 时绝不该直连，源 IP 会暴露给目标站，
    这正是开代理池要避免的事。比崩溃更糟：**它不报错、只是悄悄放行**。

    修法是全程只读一次 `_state["pool"]` 到局部变量，返回的是这个局部变量，
    不是函数末尾对字典的第二次独立读取——这样无论 `close_proxy_pool()`
    什么时候把字典置空，本次调用早先已经拿到手的池对象不受影响。
    """
    monkeypatch.setattr(setting, "PROXY_ENABLE", True)
    monkeypatch.setattr(setting, "PROXY_EXTRACT_API", "https://p2/l")
    with respx.mock:
        respx.get("https://p2/l").mock(return_value=httpx.Response(200, text="1.1.1.1:80"))
        existing = get_proxy_pool()
    assert existing is not None

    import threading

    from netspy.network import proxy_pool as pp_mod

    read_count = {"n": 0}
    paused = threading.Event()
    proceed = threading.Event()

    class _PausingState(dict):  # type: ignore[type-arg]
        def __getitem__(self, key: str) -> object:  # type: ignore[override]
            value = super().__getitem__(key)
            if key == "pool":
                read_count["n"] += 1
                if read_count["n"] == 1:
                    # 模拟：拿到局部变量之后，close_proxy_pool() 并发把字典置空
                    paused.set()
                    proceed.wait(timeout=2)
            return value

    monkeypatch.setattr(pp_mod, "_state", _PausingState(pp_mod._state))

    result: dict[str, object] = {}

    def getter() -> None:
        result["pool"] = pp_mod.get_proxy_pool()

    t1 = threading.Thread(target=getter)
    t1.start()
    assert paused.wait(timeout=2), "没能让 get_proxy_pool 卡在第一次读取，测试前提不成立"

    def closer() -> None:
        pp_mod._state["pool"] = None

    t2 = threading.Thread(target=closer)
    t2.start()
    t2.join(timeout=2)

    proceed.set()
    t1.join(timeout=2)

    assert result["pool"] is existing, (
        f"get_proxy_pool() 返回了 {result['pool']!r}——"
        "PROXY_ENABLE=True 时不该返回 None，那会让调用方误判成没开代理池直连"
    )


class _SpyPool(ProxyPool):
    def __init__(self) -> None:
        self.handed: list[str] = []
        self.bad: list[str] = []

    def get_proxy(self) -> str:
        self.handed.append("http://p:1")
        return "http://p:1"

    def report_bad(self, proxy: str) -> None:
        self.bad.append(proxy)


@respx.mock
def test_downloader_uses_pool_and_reports_bad(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _SpyPool()
    monkeypatch.setattr("netspy.network.downloader._common.get_proxy_pool", lambda: spy)
    respx.get("https://x.test/").mock(side_effect=httpx.ConnectError("boom"))
    with pytest.raises(RequestError):
        HttpxDownloader().download(Request("https://x.test/"))
    assert spy.handed == ["http://p:1"]
    assert spy.bad == ["http://p:1"]


@respx.mock
def test_explicit_request_proxy_skips_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = _SpyPool()
    monkeypatch.setattr("netspy.network.downloader._common.get_proxy_pool", lambda: spy)
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    respx.get("https://x.test/").mock(return_value=httpx.Response(200, text="ok"))
    resp = HttpxDownloader().download(Request("https://x.test/", proxy="http://explicit:9"))
    assert resp.text == "ok"
    assert spy.handed == []
