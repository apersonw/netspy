"""Redis 账号池：多进程 / 多节点共享一批账号，不会两个节点同时用同一个号。

账号在 Redis sorted set ``<name>:users:ready`` 里，score = 可用时间戳（0 或过去 = 现在可用）。
``get`` 用 ``zpopmin`` 借走（其他节点看不到了），``report_ok`` / ``report_bad`` 归还
（后者带冷却时间）。cookies 缓存在 ``<name>:cookie:<username>``（带 TTL）。

``login=None`` 时，Cookie 完全由外部写入 ``<name>:cookie:<username>``——比如另一个
专门负责登录 / 保活的服务。这种「纯消费」场景下如果外部还没写进来（或者写的 TTL
已经到期），``require_cookies=True`` 会让这个账号被当成「暂不可用」放回冷却队列，
而不是发一个没有 Cookie 的 ``User`` 出去（那样调用方十有八九会拿着它去请求，
认证失败白打一次）。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from netspy.network.user_pool.base import User, UserPool
from netspy.network.user_pool.local import _coerce_user
from netspy.utils import tools
from netspy.utils.log import get_logger

log = get_logger("user_pool")

LoginFn = Callable[[User], "dict[str, str]"]


class RedisUserPool(UserPool):
    def __init__(
        self,
        name: str,
        accounts: list[User | dict[str, Any]] | None = None,
        *,
        login: LoginFn | None = None,
        redis_client: Any = None,
        cookie_ttl: int = 3600,
        require_cookies: bool = False,
        not_ready_retry_seconds: float = 30.0,
    ) -> None:
        self._r: Any = redis_client if redis_client is not None else _default_redis()
        self._ready = f"{name}:users:ready"
        self._cookie_prefix = f"{name}:cookie:"
        self._login = login
        self._cookie_ttl = cookie_ttl
        self._require_cookies = require_cookies
        self._not_ready_retry_seconds = not_ready_retry_seconds
        self._accounts = {u.username: u for u in (_coerce_user(a) for a in (accounts or []))}
        if self._accounts:
            self._r.zadd(self._ready, dict.fromkeys(self._accounts, 0.0), nx=True)

    def get(self) -> User | None:
        now = time.time()
        rows = self._r.zpopmin(self._ready, 1)
        if not rows:
            return None
        username, score = rows[0][0], float(rows[0][1])
        if score > now:
            self._r.zadd(self._ready, {username: score})  # 还在冷却，放回
            return None

        user = self._accounts.get(username) or User(username=username)
        cached = self._r.get(self._cookie_prefix + username)
        if cached:
            user.cookies = tools.loads_json(cached)
        elif self._login is not None:
            try:
                user.cookies = dict(self._login(user) or {})
            except Exception:
                log.exception("登录失败：{}", username)
                self._r.zadd(self._ready, {username: now + 300})
                return None
            self._r.set(
                self._cookie_prefix + username,
                tools.dumps_json(user.cookies),
                ex=self._cookie_ttl,
            )
        elif self._require_cookies:
            # 没有登录回调、Redis 里也没有缓存的 Cookie——外部保活服务大概率
            # 还没写进来，或者写的 TTL 到期了。当成暂不可用放回冷却队列，
            # 不发一个没有 Cookie 的 User 出去。
            self._r.zadd(self._ready, {username: now + self._not_ready_retry_seconds})
            return None
        return user

    def report_ok(self, user: User) -> None:
        self._r.zadd(self._ready, {user.username: time.time()})

    def report_bad(self, user: User, *, block_seconds: float = 1800.0) -> None:
        self._r.zadd(self._ready, {user.username: time.time() + block_seconds})
        self._r.delete(self._cookie_prefix + user.username)


def _default_redis() -> Any:
    from netspy.db.redisdb import get_redis

    return get_redis()
