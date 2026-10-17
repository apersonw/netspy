"""``BatchMonitor`` —— BatchSpider 的 master：批次生命周期 + 任务防丢 + 进度。

一次巡检（``_tick``）做四件事：

1. 把卡在「处理中」太久的任务重置回「待处理」（防丢）
2. 认领「待处理」任务（状态置为处理中），推进 Redis 待抓队列 ``<ns>:batch_pending``
3. 按任务表统计刷新批次记录的 done / fail / total
4. 所有任务都已结算（完成 + 失败 == 总数）→ 收尾批次、置 ``<ns>:batch_done`` 标志

``run_once`` 把当前批次跑到完成即返回（适合 cron）；``run`` 常驻，一个批次跑完后
等到下一个间隔再开下一批。同一命名空间只允许一个 monitor（Redis 锁）。
"""

from __future__ import annotations

import math
import socket
import threading
import time
from typing import TYPE_CHECKING, Any

from netspy.core.batch_store import BatchRecord
from netspy.exceptions import SpiderError
from netspy.utils import tools
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from netspy.core.batch_store import BatchStore

log = get_logger("batch")

_LOCK_TTL = 60

#: 是我的才续期。返回 1 = 续上了，0 = 锁已经不是自己的
_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('EXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

#: 是我的才删
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class BatchMonitor:
    def __init__(
        self,
        *,
        store: BatchStore,
        redis: Any,
        ns: str,
        batch_interval: float,
        interval_unit: str = "day",
        monitor_interval: float = 10.0,
        lost_stale: float = 600.0,
        push_limit: int = 5000,
    ) -> None:
        self._store = store
        self._redis = redis
        self._batch_interval = batch_interval
        self._interval_unit = interval_unit
        self._monitor_interval = monitor_interval
        self._lost_stale = lost_stale
        self._push_limit = push_limit
        self._pending_key = f"{ns}:batch_pending"
        self._done_key = f"{ns}:batch_done"
        self._lock_key = f"{ns}:batch_monitor_lock"
        #: 锁的 TTL 必须盖过两次续期之间的间隔。续期只在每轮巡检开头做一次，
        #: 中间要睡满 `monitor_interval`——原来 TTL 写死 60 秒，
        #: `BATCH_MONITOR_INTERVAL` 一设到 60 以上（日更批次每几分钟看一眼是很
        #: 正常的配置），锁就在睡眠期间过期，下一次续期发现 key 没了，误判成
        #: 「锁已易主」，唯一的 master 自己把自己杀掉——全程根本没有第二个 master。
        #: ×3 给单轮巡检本身的耗时（认领、推送几千个任务）也留出余量。
        self._lock_ttl = max(_LOCK_TTL, math.ceil(monitor_interval * 3))
        self._node_id = f"{socket.gethostname()}-{time.time():.0f}"
        self._stop = threading.Event()

    # ------------------------------------------------------------------
    def run_once(self) -> BatchRecord:
        """把当前批次跑到完成并返回。未到下一批时间也会强制开一个新批次。"""
        with self._hold_lock():
            self._store.ensure_schema()
            batch = self._ensure_batch(force=True)
            assert batch is not None
            while not self._stop.is_set():
                self._renew_lock()
                if self._tick(batch):
                    return self._store.latest_batch() or batch
                self._wait(self._monitor_interval)
            return batch

    def run(self) -> None:
        """常驻：批次之间按 ``batch_interval`` 间隔调度。"""
        with self._hold_lock():
            self._store.ensure_schema()
            while not self._stop.is_set():
                self._renew_lock()
                batch = self._ensure_batch(force=False)
                if batch is None:
                    self._wait(self._monitor_interval)
                    continue
                while not self._stop.is_set():
                    self._renew_lock()
                    if self._tick(batch):
                        break
                    self._wait(self._monitor_interval)

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    def _ensure_batch(self, *, force: bool) -> BatchRecord | None:
        latest = self._store.latest_batch()
        if latest is not None and not latest.is_done:
            return latest
        if not force and latest is not None:
            age = self._store.batch_age_seconds(latest.id)
            if age < self._interval_seconds():
                return None
        self._store.reset_all_tasks()
        self._redis.delete(self._pending_key)
        self._redis.delete(self._done_key)
        total = self._store.count_tasks().total
        batch = self._store.create_batch(
            tools.format_date(), total, self._batch_interval, self._interval_unit
        )
        log.info("开新批次 {}（{} 个任务）", batch.batch_date, total)
        return batch

    def _tick(self, batch: BatchRecord) -> bool:
        lost = self._store.reset_lost_tasks(self._lost_stale)
        if lost:
            log.warning("重置 {} 个卡住的任务回待处理", lost)

        claimed = self._store.claim_tasks(self._push_limit)
        if claimed:
            self._redis.rpush(self._pending_key, *(tools.dumps_json(t) for t in claimed))
            log.info("推进 {} 个任务到待抓队列", len(claimed))

        counts = self._store.count_tasks()
        self._store.update_batch_counts(
            batch.id, done=counts.done, failed=counts.failed, total=counts.total
        )
        log.info("批次进度 {}/{}（失败 {}）", counts.settled, counts.total, counts.failed)

        if counts.total == 0 or counts.finished:
            self._store.finish_batch(batch.id)
            self._redis.set(self._done_key, "1", ex=86400)
            log.info("批次 {} 完成", batch.batch_date)
            return True
        return False

    def _interval_seconds(self) -> float:
        return self._batch_interval * (86400.0 if self._interval_unit == "day" else 3600.0)

    def _wait(self, seconds: float) -> None:
        self._stop.wait(seconds)

    # ---- Redis 锁：同一命名空间只允许一个 monitor ----
    def _hold_lock(self) -> _LockGuard:
        if not self._redis.set(self._lock_key, self._node_id, nx=True, ex=self._lock_ttl):
            raise SpiderError(f"已有 BatchSpider monitor 在运行（{self._lock_key}）")
        return _LockGuard(self._redis, self._lock_key, self._node_id)

    def _renew_lock(self) -> None:
        """只在锁**还是自己的**时候续期。

        原来是无条件 `SET` —— 于是一旦本节点的一次 tick 超过 `_LOCK_TTL`：
        锁过期 → 另一个 master 用 `SET NX` 合法拿到 → **本节点下次续期又把它
        覆盖回来**。两个 master 从此互相覆盖，互斥不是「偶尔失效」而是**永久破掉**，
        而两个 master 同时认领任务就意味着同一批 URL 被抓两遍。

        校验和续期必须原子完成：先 GET 再 SET 之间锁可能已经易主。
        （释放路径本来就校验了持有者，只是那边也是非原子的 GET-then-DEL，一并收拾。）
        """
        if not self._redis.register_script(_RENEW_LUA)(
            keys=[self._lock_key], args=[self._node_id, self._lock_ttl]
        ):
            # 锁已经不是自己的了：另一个 master 接管了，本节点不该再认领任务
            raise SpiderError(f"BatchSpider monitor 锁已易主（{self._lock_key}），本节点退出")


class _LockGuard:
    def __init__(self, redis: Any, key: str, node_id: str) -> None:
        self._redis = redis
        self._key = key
        self._node_id = node_id

    def __enter__(self) -> _LockGuard:
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            # 原子的「是我的才删」：GET 之后 DEL 之前锁可能已经过期并被别人拿走，
            # 那样就会删掉别人的锁
            self._redis.register_script(_RELEASE_LUA)(keys=[self._key], args=[self._node_id])
        except Exception:
            log.debug("释放 monitor 锁失败", exc_info=True)
