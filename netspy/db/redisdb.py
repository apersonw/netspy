"""Redis 连接管理 + 一次性锁。

按 URL 缓存 `redis.Redis` 实例（``decode_responses=True``）。分布式 Spider / 持久化去重
用到的所有 Redis 访问都从这里拿客户端。
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any

import redis

from netspy import setting

_lock = threading.Lock()
_clients: dict[str, Any] = {}


def get_redis(url: str | None = None) -> Any:
    url = url or setting.REDIS_URL
    client = _clients.get(url)
    if client is None:
        with _lock:
            client = _clients.get(url)
            if client is None:
                client = redis.Redis.from_url(url, decode_responses=True)
                _clients[url] = client
    return client


def close_redis() -> None:
    # 摘出快照再清空必须和 get_redis() 的写入共用同一把锁：不加锁的话，
    # get_redis() 并发插入新 key 会在这里遍历到一半改变字典大小，
    # 直接抛 RuntimeError（且抛在 for 循环自己的 __next__ 里，不在下面
    # 那个只包住 client.close() 的 suppress 范围内）——不仅崩，
    # 崩之前没关的连接、崩之后残留的新 key 都清不掉。
    with _lock:
        clients = list(_clients.values())
        _clients.clear()
    for client in clients:
        with contextlib.suppress(Exception):
            client.close()


def key(*parts: str) -> str:
    """拼一个带命名空间前缀的 Redis key。"""
    return ":".join((setting.REDIS_KEY_PREFIX, *parts))


def acquire_once(client: Any, name: str, ttl: int = 86400) -> bool:
    """尝试拿一个一次性锁（不阻塞、不主动释放，靠 TTL 过期）。

    多节点同时启动时用它保证 ``start_requests`` 只被执行一次。
    """
    return bool(client.set(name, "1", nx=True, ex=ttl))
