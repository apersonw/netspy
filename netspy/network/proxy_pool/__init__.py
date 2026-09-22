"""代理池（阶段 06）：ProxyPool 接口 + ApiProxyPool。

``get_proxy_pool()`` 按 ``PROXY_ENABLE`` / ``PROXY_POOL`` 返回单例，供下载器使用。
"""

from __future__ import annotations

import threading

from netspy import setting
from netspy.network.proxy_pool.base import ProxyPool
from netspy.utils import tools

__all__ = ["ProxyPool", "close_proxy_pool", "get_proxy_pool"]

_state: dict[str, ProxyPool | None] = {"pool": None}
_lock = threading.Lock()


def get_proxy_pool() -> ProxyPool | None:
    if not setting.PROXY_ENABLE:
        return None
    # 全程只读一次 _state["pool"] 到局部变量再返回它，不在函数末尾重新读
    # 字典——调度器停工作线程用的是**有超时的** join，`close_proxy_pool()`
    # 完全可能在这次调用的「查」和「返回」之间把 _state["pool"] 置空。
    # 原来函数末尾是 `return _state["pool"]`，会重新读一次字典：读到 None
    # 的话，调用方（`_attempt_proxy`）会把它当成「压根没开代理池」直连出去——
    # 而 `PROXY_ENABLE=True` 时绝不直连、源 IP 会暴露给目标站，正是开代理池
    # 要避免的那件事。这不是「关闭时偶尔多等一下」的可接受代价，是静默的
    # 隐私泄漏，比崩溃更糟。
    pool = _state["pool"]
    if pool is not None:
        return pool
    with _lock:
        pool = _state["pool"]
        if pool is None:
            pool = tools.load_object(setting.PROXY_POOL)()
            _state["pool"] = pool
        return pool


def close_proxy_pool() -> None:
    # 置空和读取必须和 get_proxy_pool() 共用同一把锁：不加锁的话，close()
    # 调用期间置空 _state["pool"] 可能正好夹在 get_proxy_pool() 的两次读取
    # 之间，产生上面同一种静默直连的风险。
    with _lock:
        pool = _state["pool"]
        _state["pool"] = None
    if pool is not None:
        pool.close()
