"""``TaskSpider`` —— 从任务源（默认 Redis list）持续拉任务来爬。

适合「有一堆待抓的 id / url，想用一个或多个常驻进程慢慢消费」的场景。
生产者 ``TaskSpider.push_tasks(...)``（或运行中 ``self.add_tasks(...)``）；
消费者在一台或多台机器上 ``MySpider().start()``。需要 ``pip install netspy[redis]``。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from netspy import setting
from netspy.core.spiders.spider import Spider
from netspy.utils import tools

if TYPE_CHECKING:
    from collections.abc import Iterable

    from netspy.buffer.item_buffer import ItemHandler
    from netspy.core.redis_task_scheduler import RedisTaskScheduler
    from netspy.network.request import Request


def _task_source_key(name: str) -> str:
    return f"{setting.REDIS_KEY_PREFIX}:{name}:tasks"


class TaskSpider(Spider):
    __custom_setting__: ClassVar[dict[str, Any]] = {}

    def __init__(
        self,
        *,
        redis_key: str | None = None,
        task_key: str | None = None,
        keep_alive: bool | None = None,
        thread_count: int | None = None,
        item_handler: ItemHandler | None = None,
        pipelines: list[str] | None = None,
    ) -> None:
        if self.__custom_setting__:
            setting.apply(self.__custom_setting__)
        try:
            from netspy.core.redis_task_scheduler import RedisTaskScheduler
            from netspy.core.task_source import RedisTaskSource
            from netspy.db.redisdb import get_redis
        except ImportError as exc:  # pragma: no cover
            raise ImportError("TaskSpider 需要 Redis：pip install netspy[redis]") from exc

        # ⚠️ task_key 的默认值是 rk（redis_key 解析后的结果），不是类名本身——
        # 自定义了 redis_key 却不显式传 task_key 的话，这个实例读的任务源是
        # `_task_source_key(redis_key)`。但 push_tasks() 是 classmethod，
        # 拿不到任何实例的 redis_key，默认值只能退回 cls.__name__。两边各自
        # 默认的话，自定义过 redis_key 的实例会读到跟 push_tasks() 完全不同的
        # Redis key——推的任务没人读，消费者看见空队列直接判定「任务耗尽」
        # 退出，没有任何报错。自定义 redis_key 时，push_tasks() 必须显式传
        # 同一个 task_key（或直接把 redis_key 的值传进去）才能对上。
        rk = redis_key or type(self).__name__
        self._task_source = RedisTaskSource(get_redis(), _task_source_key(task_key or rk))
        self._scheduler: RedisTaskScheduler = RedisTaskScheduler(
            self,
            redis_key=rk,
            keep_alive=keep_alive,
            thread_count=thread_count,
            item_handler=item_handler,
            pipelines=pipelines,
            fetch_tasks=self.fetch_tasks,
            task_requests=self.task_requests,
        )

    # ------------------------------------------------------------------
    # 用户覆写
    # ------------------------------------------------------------------
    def task_requests(self, task: Any) -> Iterable[Request]:
        """把一个任务（dict）变成一个或多个 Request。必须实现。"""
        raise NotImplementedError(f"{type(self).__name__} 需实现 task_requests(self, task)")

    def fetch_tasks(self, limit: int) -> list[Any]:
        """拉一批任务。默认从 Redis list 取；覆写以从 MySQL / Mongo 查。"""
        return [tools.loads_json(raw) for raw in self._task_source.fetch(limit)]

    # ------------------------------------------------------------------
    def add_tasks(self, *tasks: Any) -> None:
        """运行中追加任务（比如在 parse 里发现了新的待抓项）。"""
        self._task_source.push(*(tools.dumps_json(t) for t in tasks))

    @classmethod
    def push_tasks(cls, *tasks: Any, task_key: str | None = None) -> None:
        """生产者用：把任务塞进 Redis 任务源。

        ``task_key`` 默认是类名，**不是**消费者那边的 ``redis_key``——这里是
        classmethod，拿不到任何实例的 ``redis_key``。消费者如果自定义了
        ``redis_key`` 却没给它显式传 ``task_key``，它的任务源默认会跟着
        ``redis_key`` 走；这时必须在这里也显式传同一个 ``task_key``
        （通常就传那个 ``redis_key`` 的值），否则推的任务和消费者读的是两个
        不同的 Redis key——推了但没人读，消费者看到空队列直接判定「任务耗尽」
        退出，不会有任何报错。
        """
        from netspy.core.task_source import RedisTaskSource
        from netspy.db.redisdb import get_redis

        source = RedisTaskSource(get_redis(), _task_source_key(task_key or cls.__name__))
        source.push(*(tools.dumps_json(t) for t in tasks))

    def start(self) -> None:
        self._scheduler.run()

    def stop(self) -> None:
        self._scheduler.stop()

    @property
    def scheduler(self) -> RedisTaskScheduler:
        return self._scheduler
