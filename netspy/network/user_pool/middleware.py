"""把账号池接进下载中间件链：请求前挂 cookie，响应后按登录态换号重试。"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from netspy import setting
from netspy.exceptions import UserUnavailableError
from netspy.network.middleware import DownloaderMiddleware
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from netspy.network.request import Request
    from netspy.network.response import Response
    from netspy.network.user_pool.base import User, UserPool

log = get_logger("user_pool")

CheckLogin = Callable[["Response"], bool]

#: 请求**自己**带的 cookie（挂账号之前的那份），存在 `request.custom` 里。
#:
#: 挂账号是直接改 `requests_kwargs["cookies"]` 的，而重试时重新入队的是**同一个**
#: 请求对象——原来下一次挂号是在「上一个号的 cookie」上再盖一层，上一个号有、
#: 这个号没有的 cookie 就留了下来。实测 alice 失败、换 bob 重试，bob 的请求带着
#: `sid=BOB; device=ALICE-PHONE` 出去：一个请求顶着两个号的身份，正是站点用来
#: 关联账号、一起封掉的那种信号。
#:
#: 放 `custom` 而不是普通属性：分布式下重试的请求会序列化进 Redis、被别的节点领走，
#: 只有 `to_dict()` 带得走的东西才不会在换节点时丢掉。
_BASE_COOKIES = "_user_pool_base_cookies"


def _base_cookies(request: Request) -> dict[str, str]:
    base = request.custom.get(_BASE_COOKIES)
    if base is None:
        base = dict(request.requests_kwargs.get("cookies") or {})
        request.custom[_BASE_COOKIES] = base
    return dict(base)


def _set_cookies(request: Request, cookies: dict[str, str]) -> None:
    if cookies:
        request.requests_kwargs["cookies"] = cookies
    else:
        request.requests_kwargs.pop("cookies", None)


class UserPoolMiddleware(DownloaderMiddleware):
    def __init__(self, pool: UserPool, *, check_login: CheckLogin | None = None) -> None:
        self._pool = pool
        self._check = check_login

    def process_request(self, request: Request) -> Request | None:
        base = _base_cookies(request)
        # 上一次尝试挂的号不算数了：不清掉的话，这次匿名发出去时 process_response
        # 还会拿着上一个号去 report_ok / report_bad
        request.__dict__.pop("_user", None)
        user = self._wait_for_user()
        if user is None:
            if not setting.USER_POOL_ALLOW_ANONYMOUS:
                # 匿名发出去的话，需要登录的站会回一张登录墙 ——
                # 而那张页面会被当成数据存进库。实测账号被封后
                # 12 行落库里有 10 行是登录墙
                raise UserUnavailableError(
                    f"账号池没有可用账号（等了 {setting.USER_POOL_WAIT_TIMEOUT} 秒），"
                    f"不匿名发出：{request.method} {request.url}。"
                    "确实想要「有号更好、没号也能抓」的话，"
                    "把 USER_POOL_ALLOW_ANONYMOUS 打开"
                )
            log.warning("账号池没有可用账号，按配置匿名发出：{}", request.url)
            _set_cookies(request, base)  # 匿名就真匿名，别带着上一个号的 cookie
            return None
        _set_cookies(request, {**base, **user.cookies})
        request.__dict__["_user"] = user
        return request

    def process_response(self, request: Request, response: Response) -> Response | Request:
        user = request.__dict__.get("_user")
        # check_login 判断的是**这个响应是不是有效数据**，和有没有挂账号无关。
        # 以前这里先 `if user is None: return response`，于是匿名发出的请求
        # 一次都不检查 —— 而那正是最需要检查的时候：登录墙就那样进了 parse()
        if self._check is not None and not self._logged_in(response):
            if user is not None:
                log.warning("账号 {} 登录失效，拉黑并换号重试", user)
                self._pool.report_bad(user)
            else:
                log.warning("匿名请求拿到的不是登录后的内容，重试：{}", request.url)
            retry = request.copy()
            # 去掉的只是失效账号的 cookie，请求自己带的那些要留着
            _set_cookies(retry, _base_cookies(retry))
            retry.filter_repeat = False
            return retry
        if user is not None:
            self._pool.report_ok(user)
        return response

    def _wait_for_user(self) -> User | None:
        """有界等待。游客池能现登一个，被拉黑的号也会到期放出来。

        等待要短：账号不像代理会「补货」，拉黑默认 30 分钟，
        真等下去多半没意义 —— 让请求走重试、最终落进 failed_requests 更有用。
        """
        deadline: float | None = None
        while True:
            user = self._pool.get()
            if user is not None:
                return user
            if setting.USER_POOL_ALLOW_ANONYMOUS:
                return None
            if deadline is None:
                deadline = time.monotonic() + setting.USER_POOL_WAIT_TIMEOUT
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.5, max(setting.USER_POOL_WAIT_TIMEOUT / 10, 0.05)))

    def _logged_in(self, response: Response) -> bool:
        if self._check is None:
            return True
        try:
            return bool(self._check(response))
        except Exception:
            log.exception("check_login 抛异常，当作已登录处理")
            return True
