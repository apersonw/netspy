"""进程内账号池。

``LocalUserPool``：给一批账号，轮流发放；某账号被 ``report_bad`` 后拉黑一段时间。
``GuestUserPool``：无账号，靠 ``login()`` 拿匿名 cookie，维护固定数量的游客会话。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from typing import Any

from netspy.network.user_pool.base import User, UserPool
from netspy.utils.log import get_logger

log = get_logger("user_pool")

LoginFn = Callable[[User], "dict[str, str]"]


def _coerce_user(raw: User | dict[str, Any]) -> User:
    if isinstance(raw, User):
        return raw
    return User(
        username=str(raw.get("username", "guest")),
        password=str(raw.get("password", "")),
        cookies=dict(raw.get("cookies") or {}),
        extra=dict(raw.get("extra") or {}),
    )


class LocalUserPool(UserPool):
    def __init__(
        self,
        users: list[User | dict[str, Any]] | None = None,
        *,
        login: LoginFn | None = None,
    ) -> None:
        self._login = login
        self._users: list[User] = [_coerce_user(u) for u in (users or [])]
        self._blocked: dict[str, float] = {}
        self._idx = 0
        self._lock = threading.Lock()
        #: 每个账号一把登录锁，懒创建。`login()` 通常是一次网络请求，
        #: 绝不能拿主锁 `self._lock` 裹住它 —— 那会让所有线程排成一队去登同一个
        #: 账号池，哪怕它们各自要的是不同账号。锁粒度按用户名拆开：不同账号的
        #: 登录能真正并行，同一个账号被并发拿到时仍然只登录一次（内层双重检查）。
        self._login_locks: dict[str, threading.Lock] = {}

    def add_user(self, user: User | dict[str, Any]) -> None:
        with self._lock:
            self._users.append(_coerce_user(user))

    def _login_lock(self, username: str) -> threading.Lock:
        with self._lock:
            lock = self._login_locks.get(username)
            if lock is None:
                lock = threading.Lock()
                self._login_locks[username] = lock
            return lock

    def get(self) -> User | None:
        now = time.monotonic()
        with self._lock:
            total = len(self._users)
        for _ in range(total):
            with self._lock:
                user = self._users[self._idx % total]
                self._idx += 1
                if self._blocked.get(user.username, 0.0) > now:
                    continue
            if user.cookies or self._login is None:
                return user
            with self._login_lock(user.username):
                if user.cookies:  # 等锁的时候被别的线程登录过了，直接复用
                    return user
                try:
                    user.cookies = dict(self._login(user) or {})
                except Exception:
                    log.exception("登录失败：{}", user.username)
                    with self._lock:
                        self._blocked[user.username] = time.monotonic() + 300
                    continue
            return user
        return None

    def report_ok(self, user: User) -> None:
        return None

    def report_bad(self, user: User, *, block_seconds: float = 1800.0) -> None:
        with self._lock:
            self._blocked[user.username] = time.monotonic() + block_seconds
            user.cookies = {}


class GuestUserPool(LocalUserPool):
    def __init__(self, *, login: LoginFn, size: int = 3) -> None:
        super().__init__(login=login)
        self._size = max(1, size)
        self._counter = 0

    def get(self) -> User | None:
        now = time.monotonic()
        with self._lock:
            live = sum(1 for u in self._users if self._blocked.get(u.username, 0.0) <= now)
            while live < self._size:
                self._counter += 1
                self._users.append(User(username=f"guest-{self._counter}"))
                live += 1
        return super().get()
