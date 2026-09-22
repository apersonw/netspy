"""下载器：HttpxDownloader（阶段 01）；PlaywrightDownloader（阶段 04）。

``download_request`` 是便捷入口：按 Request 选择合适的默认下载器并执行。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from netspy import setting
from netspy.network import cache, throttle
from netspy.network.downloader._common import (
    resolve_impersonate,
    set_effective_concurrency,
)
from netspy.network.downloader._httpx import HttpxDownloader
from netspy.network.downloader.base import Downloader

if TYPE_CHECKING:
    from netspy.network.request import Request
    from netspy.network.response import Response

__all__ = [
    "Downloader",
    "HttpxDownloader",
    "close_default_downloaders",
    "download_request",
    "get_default_downloader",
]

_defaults: dict[str, Downloader] = {}
_lock = threading.Lock()


def _wants_session(request: Request) -> bool:
    """请求级 > 全局 ``setting.USE_SESSION``。

    `Request.use_session` 默认 None，此前只看它、完全没读 setting，
    于是 `USE_SESSION` 成了一个「定义了、文档写了、但永远不生效」的死配置。
    """
    if request.use_session is not None:
        return bool(request.use_session)
    return bool(setting.USE_SESSION)


def get_default_downloader(request: Request) -> Downloader:
    want_session = _wants_session(request)
    if request.render:
        # 渲染必须走浏览器，浏览器自带真实指纹，无需 impersonate
        key = "playwright"
    elif resolve_impersonate(request):
        key = "curl-session" if want_session else "curl"
    elif setting.DOWNLOADER_ASYNC:
        key = "async"
    elif want_session:
        key = "httpx-session"
    else:
        key = "httpx"
    downloader = _defaults.get(key)
    if downloader is None:
        with _lock:
            downloader = _defaults.get(key)
            if downloader is None:
                downloader = _build(key, request)
                _defaults[key] = downloader
    return downloader


def _build(key: str, request: Request) -> Downloader:
    if key == "playwright":
        from netspy.network.downloader._playwright import PlaywrightDownloader

        return PlaywrightDownloader()
    if key.startswith("curl"):
        from netspy.network.downloader._curl import CurlDownloader

        return CurlDownloader(use_session=_wants_session(request))
    if key == "async":
        from netspy.network.downloader._async_httpx import AsyncHttpxDownloader

        return AsyncHttpxDownloader()
    return HttpxDownloader(use_session=_wants_session(request))


def download_request(request: Request, downloader: Downloader | None = None) -> Response:
    # 缓存查在限速之前：命中就该完全不碰网络，也不该占掉一个限速名额 ——
    # 否则「重跑不打扰目标站」这件事只做了一半
    cached = cache.load(request)
    if cached is not None:
        return cached
    # 限速放在这里而不是中间件里：parser_control 中 process_request 与下载处在两个
    # 独立的 try，下载抛异常时 process_response 不会执行，中间件拿的名额会泄漏。
    # 这里的 with 保证无论成功失败都释放。
    with throttle.slot(request.url):
        response = (downloader or get_default_downloader(request)).download(request)
    if setting.ANTIBOT_DETECT:
        # 放在这里而不是各下载器里：httpx / curl / async / playwright 一次覆盖
        from netspy.network import antibot

        antibot.raise_if_blocked(response)
    cache.store(request, response)
    return response


def close_default_downloaders() -> None:
    # 快照 + 清空必须和 get_default_downloader() 的写入共用同一把锁：调度器
    # 停工作线程用的是**有超时的** join，一个慢请求的 worker 完全可能在这里
    # 跑的时候还没退出、正并发调 get_default_downloader() 首次构造某个下载器
    # 并写进 _defaults。不加锁遍历的话，跟 close_redis() 是一模一样的坑——
    # 字典大小在遍历中途被改变，直接 RuntimeError；关闭动作本身也可能跟
    # 「刚建好、调用方还没来得及用上」的下载器发生 use-after-close。
    with _lock:
        downloaders = list(_defaults.values())
        _defaults.clear()
    for downloader in downloaders:
        downloader.close()
    # 连「真实并发是多少」一起收掉。它是累积取最大的，不重置就会跨爬虫
    # （以及跨用例）泄漏，让下一个爬虫按上一个的线程数分片。
    set_effective_concurrency(None)
