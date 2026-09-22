"""``BatchSpider`` —— 周期性批次采集：MySQL 任务表 + 批次记录 + 进度 / 防丢。

两种角色，通常跑在不同进程：

- **master**：``spider.start_monitor()``。管理批次生命周期——到点开新批次（重置任务表）、
  把待处理任务推进 Redis 待抓队列、按任务表刷新进度、重置卡死任务、批次跑完收尾。
  同一命名空间只允许一个 master（Redis 锁）。``start_monitor(once=True)`` 跑完一个批次即返回，
  适合 cron。
- **worker**：``spider.start()``。和 ``TaskSpider`` 一样消费 Redis 队列，对每个任务调
  ``task_requests(task)``；解析完成后自己回写任务状态。可多进程 / 多机。

需要 ``pip install "netspy[redis,mysql]"``。
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from netspy import setting
from netspy.core import context
from netspy.core.base_parser import BaseParser
from netspy.core.batch_store import DONE, FAILED
from netspy.utils import tools
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable

    from netspy.buffer.item_buffer import ItemHandler
    from netspy.core.batch_monitor import BatchMonitor
    from netspy.core.batch_store import BatchStore
    from netspy.core.redis_task_scheduler import RedisTaskScheduler
    from netspy.network.request import Request
    from netspy.network.response import Response

log = get_logger("batch")


class BatchSpider(BaseParser):
    __custom_setting__: ClassVar[dict[str, Any]] = {}
    #: 任务表名；不设则用 redis_key 的下划线小写形式
    __task_table__: ClassVar[str | None] = None

    def __init__(
        self,
        *,
        redis_key: str | None = None,
        task_table: str | None = None,
        batch_store: BatchStore | None = None,
        batch_interval: float | None = None,
        keep_alive: bool | None = None,
        thread_count: int | None = None,
        item_handler: ItemHandler | None = None,
        pipelines: list[str] | None = None,
    ) -> None:
        if self.__custom_setting__:
            setting.apply(self.__custom_setting__)
        try:
            from netspy.core.redis_task_scheduler import RedisTaskScheduler
            from netspy.db.redisdb import get_redis
        except ImportError as exc:  # pragma: no cover - 缺 redis
            raise ImportError("BatchSpider 需要 Redis：pip install netspy[redis]") from exc

        self._rk = redis_key or type(self).__name__
        self._ns = f"{setting.REDIS_KEY_PREFIX}:{self._rk}"
        self._task_table = task_table or type(self).__task_table__ or _snake(self._rk)
        self._batch_interval = (
            batch_interval if batch_interval is not None else setting.BATCH_INTERVAL
        )
        self._redis = get_redis()
        self._pending_key = f"{self._ns}:batch_pending"
        self._store: BatchStore = batch_store if batch_store is not None else self._make_store()
        self._monitor: BatchMonitor | None = None

        keep = setting.SPIDER_KEEP_ALIVE if keep_alive is None else keep_alive
        self._scheduler: RedisTaskScheduler = RedisTaskScheduler(
            self,
            redis_key=self._rk,
            keep_alive=keep,
            # BatchSpider 的身份是「批次作业」：batch_pending / batch_done / master 锁
            # 都在作业命名空间下，批次本身就是它的重跑单位。RUN_ID 是给
            # 「同一个 Spider 定时重跑」用的，对它没有意义 —— 显式关掉，
            # 免得平台注入的 RUN_ID 把 worker 的队列和心跳挪到别处、监控找不到
            run_id="",
            thread_count=thread_count,
            item_handler=item_handler,
            pipelines=pipelines,
            fetch_tasks=self.fetch_tasks,
            task_requests=self._task_requests_tagged,
        )

    # ------------------------------------------------------------------
    # 用户覆写
    # ------------------------------------------------------------------
    def task_requests(self, task: dict[str, Any]) -> Iterable[Request]:
        """把一个任务（任务表的一行 dict）变成一个或多个 Request。必须实现。

        框架会自动给每个 Request 补上 ``cb_kwargs['task']``，回调里可直接拿到。
        """
        raise NotImplementedError(f"{type(self).__name__} 需实现 task_requests(self, task)")

    # ------------------------------------------------------------------
    # 任务状态回写
    # ------------------------------------------------------------------
    def update_task(self, task_id: Any, *, ok: bool = True) -> None:
        """回写任务状态：``ok=True`` → 已完成；``ok=False`` → 失败（不再重试）。

        标「已完成」会**等这次请求产出的数据真正落库之后**才写任务表。
        照文档在回调里 ``yield`` 完 item 紧接着调用它 —— 那一刻数据还只在内存缓冲里，
        立刻写 DONE 的话，节点一死数据就没了，而防丢机制**只回收「处理中」**，
        标了 DONE 的任务永远不会被重跑。真库实测：5 个任务全标 DONE，落库 0 行。

        三条例外走立即写入：没产出 item 的请求（否则永远等不到落库）、
        ``ok=False``（标失败不取决于数据）、以及不在请求上下文里调用（比如 master）。
        """
        if not ok:
            self._store.mark_task(task_id, FAILED)
            return
        buffer = context.get_current_item_buffer()
        request = context.get_current_request()
        # 用原子的 after_persist_if_pending 而不是「先 owns() 查再 after_persist()
        # 登记」——那样中间有没上锁的空档，flush() 一旦插进去，钩子就再也不会
        # 被执行，任务永远卡在「处理中」直到租约到期才被重新捞回来
        if (
            buffer is None
            or request is None
            or not buffer.after_persist_if_pending(
                request, lambda: self._store.mark_task(task_id, DONE)
            )
        ):
            self._store.mark_task(task_id, DONE)

    def failed_request(self, request: Request, response: Response | None) -> Iterable[Any] | None:
        """重试耗尽后：把对应任务标为失败。覆写时记得 super()。"""
        task = request.cb_kwargs.get("task")
        if isinstance(task, dict):
            tid = task.get(setting.BATCH_TASK_ID_FIELD)
            if tid is not None:
                self.update_task(tid, ok=False)
        return None

    # ------------------------------------------------------------------
    def fetch_tasks(self, limit: int) -> list[dict[str, Any]]:
        """worker：从 master 推来的 Redis 待抓队列取一批任务。"""
        raw = self._redis.lpop(self._pending_key, max(1, limit))
        if not raw:
            return []
        rows = raw if isinstance(raw, list) else [raw]
        return [tools.loads_json(item) for item in rows]

    def _task_requests_tagged(self, task: dict[str, Any]) -> Iterable[Request]:
        for request in self.task_requests(task) or ():
            request.cb_kwargs.setdefault("task", task)
            yield request

    def _make_store(self) -> BatchStore:
        from netspy.core.batch_store import MysqlBatchStore

        return MysqlBatchStore(
            self._task_table,
            id_field=setting.BATCH_TASK_ID_FIELD,
            state_field=setting.BATCH_TASK_STATE_FIELD,
            time_field=setting.BATCH_TASK_TIME_FIELD,
        )

    # ------------------------------------------------------------------
    def start(self) -> None:
        """worker：消费 master 推来的任务。"""
        self._scheduler.run()

    def start_monitor(self, *, once: bool = False) -> None:
        """master：管理批次生命周期。``once=True`` 跑完一个批次即返回。"""
        from netspy.core.batch_monitor import BatchMonitor

        self._monitor = BatchMonitor(
            store=self._store,
            redis=self._redis,
            ns=self._ns,
            batch_interval=self._batch_interval,
            interval_unit=setting.BATCH_INTERVAL_UNIT,
            monitor_interval=setting.BATCH_MONITOR_INTERVAL,
            lost_stale=setting.BATCH_LOST_TASK_STALE,
            push_limit=setting.BATCH_PUSH_LIMIT,
        )
        if once:
            self._monitor.run_once()
        else:
            self._monitor.run()

    def stop(self) -> None:
        self._scheduler.stop()
        if self._monitor is not None:
            self._monitor.stop()

    @property
    def scheduler(self) -> RedisTaskScheduler:
        return self._scheduler

    @property
    def store(self) -> BatchStore:
        return self._store


def _snake(name: str) -> str:
    import re

    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
