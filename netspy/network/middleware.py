"""下载中间件链。

每个中间件是一个普通类，可实现：

- ``process_request(request) -> Request | Response | None``
  返回 ``Response`` 则短路下载；返回 ``Request`` 则替换；``None`` 表示不改
- ``process_response(request, response) -> Response | Request``
  返回 ``Request`` 则丢回队列重新调度

通过 ``setting.DOWNLOADER_MIDDLEWARES``（点号路径列表）启用。
"""

from __future__ import annotations

from typing import Any, cast

from netspy.network.request import Request
from netspy.network.response import Response
from netspy.utils import tools
from netspy.utils.log import LoggerMixin, get_logger

log = get_logger("middleware")


class DownloaderMiddleware(LoggerMixin):
    """基类，子类按需覆写。子类里可以直接 ``self.logger.info(...)``。"""

    def process_request(self, request: Request) -> Request | Response | None:
        return None

    def process_response(self, request: Request, response: Response) -> Response | Request:
        return response


class MiddlewareManager:
    def __init__(self, paths: list[str] | None = None) -> None:
        self._mws: list[Any] = []
        for path in paths or []:
            self._mws.append(tools.load_object(path)())
            log.debug("已加载下载中间件 {}", path)

    def __bool__(self) -> bool:
        return bool(self._mws)

    def prepend(self, middleware: Any) -> None:
        self._mws.insert(0, middleware)

    def process_request(self, request: Request) -> Request | Response:
        for ns in self._mws:
            handler = getattr(ns, "process_request", None)
            if handler is None:
                continue
            out = handler(request)
            if out is None:
                continue
            if isinstance(out, Response):
                return out
            request = cast("Request", out)
        return request

    def process_response(self, request: Request, response: Response) -> Response | Request:
        for ns in reversed(self._mws):
            handler = getattr(ns, "process_response", None)
            if handler is None:
                continue
            out = handler(request, response)
            if isinstance(out, Request):
                return out
            response = cast("Response", out)
        return response
