"""账号池空了之后，不能匿名把请求发出去，更不能把登录墙当数据存。

实测（靶子在账号用满 3 次后封号，池里就一个号）：账号被封、池子空掉之后，
爬虫继续匿名抓，**12 行落库里 10 行是登录墙页面**，而且用户明明写了
`check_login` —— 那个钩子一次都没被调用。

链条有两截，各自独立：
  1. `process_request` 取不到账号只记一条 warning 就放行，请求照发、没有 cookie
  2. `process_response` 第一行 `if user is None: return response`，
     于是匿名请求一次都不检查 —— 而那正是最需要检查的时候
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from netspy import setting
from netspy.exceptions import UserUnavailableError
from netspy.network.circuit import counts_as_unhealthy
from netspy.network.request import Request
from netspy.network.response import Response
from netspy.network.user_pool.base import User, UserPool
from netspy.network.user_pool.middleware import UserPoolMiddleware


class _Pool(UserPool):
    def __init__(self) -> None:
        self.stock: list[User] = []
        self.ok: list[User] = []
        self.bad: list[User] = []

    def get(self) -> User | None:
        return self.stock.pop(0) if self.stock else None

    def report_ok(self, user: User) -> None:
        self.ok.append(user)

    def report_bad(self, user: User, *, block_seconds: float = 1800.0) -> None:
        self.bad.append(user)


@pytest.fixture(autouse=True)
def _strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "USER_POOL_ALLOW_ANONYMOUS", False)
    monkeypatch.setattr(setting, "USER_POOL_WAIT_TIMEOUT", 0.2)


def _req() -> Request:
    return Request("http://example.com/p/1")


def _resp(body: str) -> Response:
    return Response(url="http://example.com/p/1", status_code=200, content=body.encode())


def _wall() -> Response:
    return _resp("<html><body><h1>请先登录</h1></body></html>")


def _data() -> Response:
    return _resp("<html><body><h1>真实数据</h1></body></html>")


def _logged_in(response: Response) -> bool:
    return "请先登录" not in response.text


def test_empty_pool_does_not_send_the_request() -> None:
    """匿名发出去的话，需要登录的站会回一张登录墙 —— 而那会被当成数据入库。"""
    ns = UserPoolMiddleware(_Pool(), check_login=_logged_in)
    with pytest.raises(UserUnavailableError):
        ns.process_request(_req())


def test_anonymous_is_an_explicit_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "USER_POOL_ALLOW_ANONYMOUS", True)
    ns = UserPoolMiddleware(_Pool(), check_login=_logged_in)
    assert ns.process_request(_req()) is None


def test_check_login_runs_even_without_an_account(monkeypatch: pytest.MonkeyPatch) -> None:
    """`check_login` 判断的是**响应**，和有没有挂账号无关。

    以前 `user is None` 时直接 return response，登录墙就那样进了 parse()。
    """
    monkeypatch.setattr(setting, "USER_POOL_ALLOW_ANONYMOUS", True)
    pool = _Pool()
    ns = UserPoolMiddleware(pool, check_login=_logged_in)
    request = _req()
    ns.process_request(request)  # 没账号，匿名放行
    out = ns.process_response(request, _wall())
    assert isinstance(out, Request), "登录墙被当成有效响应交给 parse 了"
    assert pool.bad == [], "没有账号可拉黑，不该对 None 调 report_bad"


def test_valid_anonymous_response_is_not_wrongly_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """判据必须是 check_login 本身 —— 不能因为「这次没挂账号」就把响应一律判死，
    那会把允许匿名的场景全砸掉。"""
    monkeypatch.setattr(setting, "USER_POOL_ALLOW_ANONYMOUS", True)
    pool = _Pool()
    ns = UserPoolMiddleware(pool, check_login=_logged_in)
    request = _req()
    ns.process_request(request)
    assert isinstance(ns.process_response(request, _data()), Response)
    assert pool.ok == [], "没有账号，不该对 None 调 report_ok"


def test_bad_account_is_still_blacklisted() -> None:
    """挂了账号却掉登录时，原来的拉黑 + 换号重试不能被改坏。"""
    pool = _Pool()
    user = User(username="u1", cookies={"sid": "x"})
    pool.stock.append(user)
    ns = UserPoolMiddleware(pool, check_login=_logged_in)
    request = _req()
    ns.process_request(request)
    assert isinstance(ns.process_response(request, _wall()), Request)
    assert pool.bad == [user]


def test_waits_for_an_account_to_come_back() -> None:
    """游客池能现登一个，被拉黑的号也会到期放出来 —— 别一看空就立刻判死。"""
    pool = _Pool()
    user = User(username="late", cookies={"sid": "x"})

    def restock() -> None:
        time.sleep(0.05)
        pool.stock.append(user)

    threading.Thread(target=restock, daemon=True).start()
    ns = UserPoolMiddleware(pool, check_login=_logged_in)
    out = ns.process_request(_req())
    assert isinstance(out, Request)
    assert out.requests_kwargs["cookies"]["sid"] == "x"


def test_account_shortage_does_not_trip_the_circuit_breaker() -> None:
    """账号全被拉黑是我们这边没号可用，不是目标站挂了。"""
    assert counts_as_unhealthy(UserUnavailableError("没号"), None) is False


def test_existing_cookies_are_preserved() -> None:
    pool = _Pool()
    pool.stock.append(User(username="u1", cookies={"sid": "x"}))
    ns = UserPoolMiddleware(pool, check_login=_logged_in)
    request = _req()
    request.requests_kwargs["cookies"] = {"lang": "zh"}
    out = ns.process_request(request)
    assert isinstance(out, Request)
    assert out.requests_kwargs["cookies"] == {"lang": "zh", "sid": "x"}


def test_exhausted_retries_are_recoverable(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """重试耗尽的请求要落进 `failed_requests.jsonl`，能用 `retry --requests` 回放。

    验证 v4.18 的断言时才发现这条不成立：单机模式下 `_on_failed_request`
    是空实现，重试耗尽的请求就那样消失了 —— 于是「不匿名发出」只是把
    「存了脏数据」换成了「静默丢任务」。分布式模式一直把它们推进 Redis 失败列表，
    单机这边漏了，而那个文件的既定用途正是装这些请求。
    """
    import json

    from netspy.core.base_scheduler import BaseScheduler

    dump = tmp_path / "failed_requests.jsonl"
    monkeypatch.setattr(setting, "FAILED_REQUEST_PATH", str(dump))
    scheduler = BaseScheduler.__new__(BaseScheduler)  # 只测这一个钩子，不起整个调度器
    scheduler._on_failed_request(_req())

    assert dump.exists(), "重试耗尽的请求没落盘 —— 它此刻是唯一副本"
    record = json.loads(dump.read_text(encoding="utf-8").strip())
    assert record["url"] == "http://example.com/p/1"
