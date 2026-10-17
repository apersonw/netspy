"""``RedisTaskScheduler`` —— 给 RedisScheduler 加一个「轮询任务源」的线程。

不走 ``start_requests``：一个后台线程每 ``TASK_POLL_INTERVAL`` 秒拉一批任务，
对每个任务调 ``task_requests(task)`` 生成请求丢进队列。多节点各自 ``lpop``，
任务天然分摊。``keep_alive=False`` 时，连续拉不到任务且队列空、各节点空闲即退出。
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

from netspy import setting
from netspy.core.redis_scheduler import RedisScheduler
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from netspy.core.base_parser import BaseParser
    from netspy.network.request import Request

log = get_logger("scheduler")


class _TaskPoller(threading.Thread):
    def __init__(
        self,
        *,
        fetch: Callable[[int], Iterable[Any]],
        make_requests: Callable[[Any], Iterable[Request] | None],
        request_buffer: Any,
    ) -> None:
        super().__init__(name="task-poller", daemon=True)
        self._fetch = fetch
        self._make_requests = make_requests
        self._request_buffer = request_buffer
        self._stop_event = threading.Event()
        self.exhausted = False
        self._empty_polls = 0

    def run(self) -> None:
        self._poll()
        while not self._stop_event.wait(setting.TASK_POLL_INTERVAL):
            self._poll()

    def _poll(self) -> None:
        try:
            tasks = list(self._fetch(setting.TASK_BATCH_SIZE))
        except Exception:
            log.exception("拉取任务失败")
            return
        if not tasks:
            self._empty_polls += 1
            if self._empty_polls >= setting.TASK_EXHAUST_POLLS:
                self.exhausted = True
            return
        self._empty_polls = 0
        self.exhausted = False
        count = 0
        for task in tasks:
            try:
                for request in self._make_requests(task) or ():
                    self._request_buffer.put(request)
                    count += 1
            except Exception:
                log.exception("task_requests 失败：{!r}", task)
        self._request_buffer.flush()
        log.info("拉取 {} 个任务 → {} 个请求", len(tasks), count)

    def stop(self) -> None:
        self._stop_event.set()


class RedisTaskScheduler(RedisScheduler):
    def __init__(
        self,
        parser: BaseParser,
        *,
        fetch_tasks: Callable[[int], Iterable[Any]],
        task_requests: Callable[[Any], Iterable[Request] | None],
        **kwargs: Any,
    ) -> None:
        self._fetch_tasks = fetch_tasks
        self._task_requests = task_requests
        self._poller: _TaskPoller | None = None
        super().__init__(parser, **kwargs)

    def _seed(self) -> None:
        log.info("TaskSpider：等待任务源（keep_alive={}）", self._keep_alive)
        # 没有 start_requests 可种，但上一轮重试耗尽的请求照样要能回放
        if self._replay_failed_requests():
            self._request_buffer.flush()

    def _on_start(self) -> None:
        super()._on_start()
        self._poller = _TaskPoller(
            fetch=self._fetch_tasks,
            make_requests=self._task_requests,
            request_buffer=self._request_buffer,
        )
        self._poller.start()

    def _is_done(self) -> bool:
        if self._keep_alive:
            return False
        if self._poller is None or not self._poller.exhausted:
            return False
        return self._local_idle() and self._task_queue.empty() and self._all_nodes_idle()

    def _on_shutdown(self) -> None:
        if self._poller is not None:
            self._poller.stop()
            self._poller.join(timeout=5)
        super()._on_shutdown()
