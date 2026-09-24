from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator
from typing import Any

import fakeredis
import pytest
from pytest_httpserver import HTTPServer

from netspy import (
    AirSpider,
    GuestUserPool,
    LocalUserPool,
    RedisUserPool,
    Request,
    Response,
    User,
    setting,
)
from netspy.network.user_pool.middleware import UserPoolMiddleware


# ---------------------------------------------------------------- LocalUserPool
def test_local_pool_round_robin_and_lazy_login() -> None:
    logins: list[str] = []

    def login(user: User) -> dict[str, str]:
        logins.append(user.username)
        return {"sid": f"tok-{user.username}"}

    pool = LocalUserPool([{"username": "a", "password": "pa"}, {"username": "b"}], login=login)
    u1, u2, u3 = pool.get(), pool.get(), pool.get()
    assert (u1.username, u2.username, u3.username) == ("a", "b", "a")
    assert u1.cookies == {"sid": "tok-a"}
    assert logins == ["a", "b"]  # 只登录一次，之后复用 cookie


def test_local_pool_report_bad_blocks_and_clears_cookie() -> None:
    pool = LocalUserPool([{"username": "a"}, {"username": "b"}])
    a = pool.get()
    a.cookies = {"x": "1"}
    pool.report_bad(a, block_seconds=999)
    assert a.cookies == {}
    for _ in range(4):
        assert pool.get().username == "b"  # a 被拉黑，只发 b


def test_local_pool_returns_none_when_all_blocked() -> None:
    pool = LocalUserPool([{"username": "a"}])
    pool.report_bad(pool.get(), block_seconds=999)
    assert pool.get() is None


def test_local_pool_login_failure_cools_down() -> None:
    def bad_login(user: User) -> dict[str, str]:
        raise RuntimeError("captcha")

    pool = LocalUserPool([{"username": "a"}], login=bad_login)
    assert pool.get() is None


def test_local_pool_logins_for_different_users_run_concurrently() -> None:
    """回归测试：不同账号的 `login()` 必须能真正并行，不能被一把全局锁串成队列。

    `login()` 通常是一次网络请求（可能几百毫秒到几秒）。旧实现把它整个包在
    `self._lock` 里——`get()` 变成了「谁在登录，其他所有线程（不管要哪个账号）
    都得干等」，`SPIDER_THREAD_COUNT` 开得再高，账号池这一环也会退化成串行。

    用 `Barrier(2)` 强制验证：两个线程各请求一个不同的、都需要登录的账号，
    只要两边的 `login()` 真的同时在跑，barrier 就会在超时前被撞开；
    退回旧的串行实现，第二个线程的 `login()` 要等第一个整个 `get()` 调用
    （含锁）结束才会开始，barrier 必然超时。
    """
    barrier = threading.Barrier(2)
    entered: set[str] = set()
    entered_lock = threading.Lock()

    def login(user: User) -> dict[str, str]:
        with entered_lock:
            entered.add(user.username)
        barrier.wait(timeout=2.0)  # 两边没同时进来就会超时抛 BrokenBarrierError
        return {"sid": f"tok-{user.username}"}

    pool = LocalUserPool(
        [{"username": "a"}, {"username": "b"}],
        login=login,
    )

    results: dict[str, User | None] = {}

    def worker(key: str) -> None:
        results[key] = pool.get()

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    t2.start()
    t1.join(timeout=5)
    t2.join(timeout=5)

    assert entered == {"a", "b"}
    assert results["a"] is not None and results["a"].cookies == {"sid": "tok-a"}
    assert results["b"] is not None and results["b"].cookies == {"sid": "tok-b"}


def test_local_pool_concurrent_get_for_same_user_logs_in_once() -> None:
    """回归测试：按用户名拆锁之后，同一个账号被并发拿到时仍然只登录一次。

    这是把 `login()` 挪出全局锁之后要守住的不变量——挪出去意味着两个线程可能
    同时选中同一个账号（比如账号池只有一个号），如果不额外按用户名加锁去重，
    会出现重复登录、cookie 互相覆盖。
    """
    login_calls: list[str] = []
    calls_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def login(user: User) -> dict[str, str]:
        with calls_lock:
            login_calls.append(user.username)
        with contextlib.suppress(threading.BrokenBarrierError):
            barrier.wait(timeout=0.3)
        time.sleep(0.05)
        return {"sid": "shared-token"}

    pool = LocalUserPool([{"username": "solo"}], login=login)

    results: list[User | None] = []
    results_lock = threading.Lock()

    def worker() -> None:
        user = pool.get()
        with results_lock:
            results.append(user)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert login_calls == ["solo"]
    assert len(results) == 2
    assert all(u is not None and u.cookies == {"sid": "shared-token"} for u in results)


# ---------------------------------------------------------------- GuestUserPool
def test_guest_pool_mints_guests() -> None:
    seen: set[str] = set()

    def login(user: User) -> dict[str, str]:
        seen.add(user.username)
        return {"g": user.username}

    pool = GuestUserPool(login=login, size=2)
    users = {pool.get().username for _ in range(6)}
    assert users == {"guest-1", "guest-2"}
    assert seen == {"guest-1", "guest-2"}


# ---------------------------------------------------------------- RedisUserPool
@pytest.fixture
def rds() -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    yield client
    client.flushall()


def test_redis_pool_checkout_and_return(rds: Any) -> None:
    pool = RedisUserPool("p1", [{"username": "a"}, {"username": "b"}], redis_client=rds)
    a = pool.get()
    b = pool.get()
    assert {a.username, b.username} == {"a", "b"}
    assert pool.get() is None  # 都借出去了

    pool.report_ok(a)
    assert pool.get().username == "a"  # 归还后能再借


def test_redis_pool_report_bad_cools_down(rds: Any) -> None:
    pool = RedisUserPool("p2", [{"username": "a"}], redis_client=rds)
    a = pool.get()
    pool.report_bad(a, block_seconds=999)
    assert pool.get() is None  # 冷却中

    pool2 = RedisUserPool("p2", [], redis_client=rds)
    pool2._r.zadd("p2:users:ready", {"a": time.time() - 1})  # 手动提前解封
    assert pool2.get().username == "a"


def test_redis_pool_caches_cookies(rds: Any) -> None:
    calls = {"n": 0}

    def login(user: User) -> dict[str, str]:
        calls["n"] += 1
        return {"sid": "x"}

    p = RedisUserPool("p3", [{"username": "a"}], login=login, redis_client=rds)
    u1 = p.get()
    p.report_ok(u1)
    u2 = p.get()
    assert u1.cookies == u2.cookies == {"sid": "x"}
    assert calls["n"] == 1
    assert rds.get("p3:cookie:a") is not None


def test_redis_pool_require_cookies_treats_missing_cookie_as_not_ready(rds: Any) -> None:
    # 没有 login 回调（Cookie 完全指望外部写进来），Redis 里也还没有缓存——
    # require_cookies=True 时不该发一个没有 Cookie 的 User 出去。
    pool = RedisUserPool(
        "p4",
        [{"username": "a"}],
        redis_client=rds,
        require_cookies=True,
        not_ready_retry_seconds=999,
    )
    assert pool.get() is None
    # 账号被放回冷却队列了，而不是丢失——手动提前解封后应该还借得到。
    rds.zadd("p4:users:ready", {"a": time.time() - 1})
    # 这次 Redis 里依然没有 Cookie，所以还是拿不到。
    assert pool.get() is None


def test_redis_pool_require_cookies_returns_user_once_published(rds: Any) -> None:
    # 模拟外部保活服务把 Cookie 写进了约定好的 key——RedisUserPool 只负责读。
    rds.set("p5:cookie:a", '{"web_session": "abc"}')
    pool = RedisUserPool("p5", [{"username": "a"}], redis_client=rds, require_cookies=True)
    user = pool.get()
    assert user is not None
    assert user.cookies == {"web_session": "abc"}


def test_redis_pool_malformed_cached_cookie_treated_as_not_ready(rds: Any) -> None:
    """回归测试：Redis 里缓存的 Cookie 值不是合法 JSON（外部服务写坏 / 写半截 /
    人工改坏）时，get() 不该直接把 JSONDecodeError 炸出去。

    `UserPoolMiddleware._wait_for_user()` 调 `pool.get()` 时没有包 try/except，
    一炸就是整条请求处理链路跟着崩——比"这个号暂时不可用"严重得多，应该跟
    `test_redis_pool_require_cookies_treats_missing_cookie_as_not_ready` 一样
    优雅降级，而不是崩。
    """
    rds.set("p7:cookie:a", "this is not valid json{{{")
    pool = RedisUserPool(
        "p7",
        [{"username": "a"}],
        redis_client=rds,
        require_cookies=True,
        not_ready_retry_seconds=999,
    )
    assert pool.get() is None  # 不炸，当成暂不可用
    # 放回冷却队列了，不是丢失
    assert rds.zscore("p7:users:ready", "a") is not None


def test_redis_pool_malformed_cached_cookie_without_require_cookies_falls_back_to_empty(
    rds: Any,
) -> None:
    """同样的坏数据，`require_cookies=False` 时该退回「没有 Cookie 但照常发号」
    这个既有行为（跟 `test_redis_pool_without_require_cookies_keeps_old_behavior`
    对称），而不是崩，也不是意外地判定成「不可用」。
    """
    rds.set("p8:cookie:a", "{not json")
    pool = RedisUserPool("p8", [{"username": "a"}], redis_client=rds)
    user = pool.get()
    assert user is not None
    assert user.cookies == {}


def test_redis_pool_without_require_cookies_keeps_old_behavior(rds: Any) -> None:
    # require_cookies 默认 False：没有 login、没有缓存 Cookie 时仍然发出账号——
    # 这是改动前就有的行为，不能因为新参数破坏掉。
    pool = RedisUserPool("p6", [{"username": "a"}], redis_client=rds)
    user = pool.get()
    assert user is not None
    assert user.cookies == {}


def test_redis_pool_shared_across_instances(rds: Any) -> None:
    a = RedisUserPool("shared", [{"username": "u1"}], redis_client=rds)
    b = RedisUserPool("shared", [{"username": "u1"}], redis_client=rds)
    got = a.get()
    assert got is not None
    assert b.get() is None  # 另一个实例看不到已借出的


# ---------------------------------------------------------------- middleware
def test_middleware_attaches_cookies_and_reports_ok() -> None:
    pool = LocalUserPool([{"username": "a", "cookies": {"sid": "s1"}}])
    reports: list[str] = []
    pool.report_ok = lambda u: reports.append(f"ok:{u.username}")  # type: ignore[method-assign]

    mw = UserPoolMiddleware(pool)
    req = mw.process_request(Request("https://x"))
    assert isinstance(req, Request)
    assert req.requests_kwargs["cookies"] == {"sid": "s1"}

    out = mw.process_response(req, Response(url="https://x", status_code=200))
    assert isinstance(out, Response)
    assert reports == ["ok:a"]


def test_middleware_rotates_on_login_failure() -> None:
    pool = LocalUserPool(
        [{"username": "a", "cookies": {"c": "1"}}, {"username": "b", "cookies": {"c": "2"}}]
    )
    mw = UserPoolMiddleware(pool, check_login=lambda resp: "登录" not in resp.text)

    req = mw.process_request(Request("https://x"))
    logged_out = Response(url="https://x", status_code=200, content="请先登录".encode())
    retry = mw.process_response(req, logged_out)
    assert isinstance(retry, Request)  # 触发重试
    assert "cookies" not in retry.requests_kwargs
    # a 被拉黑，下次拿到 b
    assert mw.process_request(Request("https://x")).requests_kwargs["cookies"] == {"c": "2"}


# ---------------------------------------------------------------- spider 集成
def test_spider_auto_wires_user_pool(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setting, "DONE_CHECK_INTERVAL", 0.04)
    monkeypatch.setattr(setting, "DONE_CHECK_TIMES", 2)
    monkeypatch.setattr(setting, "BUFFER_FLUSH_INTERVAL", 0.02)
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    seen_cookies: dict[str, str] = {}

    def handler(request: Any) -> Any:
        from werkzeug import Response as WResponse

        seen_cookies["sid"] = request.cookies.get("sid", "")
        return WResponse("<html>ok</html>", content_type="text/html")

    httpserver.expect_request("/p").respond_with_handler(handler)

    class S(AirSpider):
        def __init__(self, url: str, **kw: Any) -> None:
            self._url = url
            super().__init__(**kw)

        def user_pool(self) -> LocalUserPool:
            return LocalUserPool([{"username": "acc", "cookies": {"sid": "SECRET"}}])

        def start_requests(self) -> Iterator[Request]:
            yield Request(self._url, callback=self.parse)

        def parse(self, request: Request, response: Any) -> None:
            return None

    S(httpserver.url_for("/p")).start()
    assert seen_cookies["sid"] == "SECRET"
