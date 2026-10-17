"""``RedisScheduler`` —— 分布式调度器：Redis 队列 + Redis 去重 + 多节点结束检测。

多进程 / 多机跑同一个 Spider：队列 / 去重都在 Redis，天然断点续爬。
``start_requests`` 靠一次性锁保证只被某个节点执行一次；每个节点写心跳，
只有「所有活跃节点都空闲 + 队列空」时才判定结束（``keep_alive=True`` 则永不自停）。
"""

from __future__ import annotations

import os
import socket
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

from netspy import setting
from netspy.core.base_scheduler import BaseScheduler
from netspy.core.task_queue import RedisTaskQueue
from netspy.db.redisdb import acquire_once, get_redis
from netspy.dedup import get_item_filter, get_request_filter
from netspy.exceptions import ConfigError
from netspy.network.request import Request
from netspy.utils import stats as sk
from netspy.utils import tools
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from netspy.core.base_parser import BaseParser
    from netspy.dedup import Dedup, Filter

log = get_logger("scheduler")

_FAILED_KEY = "failed_requests"


def _resolve_dedup_scope(run_id: str) -> str:
    """把 DEDUP_SCOPE 的 auto 落到 run / spider 之一。

    没有 RUN_ID 时 run 没有意义（没有「本次运行」这个东西），退化成 spider 并出声 ——
    用户显式要了 run 却没给运行标识，多半是漏配了。
    """
    scope = str(setting.DEDUP_SCOPE or "auto").strip().lower()
    if scope == "auto":
        return "run" if run_id else "spider"
    if scope not in {"run", "spider"}:
        raise ConfigError(f"未知的 DEDUP_SCOPE：{scope!r}（可选 auto / run / spider）")
    if scope == "run" and not run_id:
        log.warning(
            "DEDUP_SCOPE=run 但没有 RUN_ID —— 没有「本次运行」可言，退化为 spider"
            "（去重跨运行持久）。要每次从头抓，设 NETSPY_RUN_ID"
        )
        return "spider"
    return scope


class _Heartbeat(threading.Thread):
    def __init__(
        self,
        redis: Any,
        hkey: str,
        node_id: str,
        pending_fn: Callable[[], int],
        renew_fn: Callable[[], int] | None = None,
        touch_fn: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(name="heartbeat", daemon=True)
        self._redis = redis
        self._hkey = hkey
        self._node_id = node_id
        self._pending_fn = pending_fn
        #: 顺手续租约。挂在心跳上而不是另起线程：这个线程还在跑本身就代表
        #: 「本节点还活着」，它停了租约也就该到期 —— 两件事的判据天然是同一个
        self._renew_fn = renew_fn
        #: 同理顺手给运行作用域下的 key 续 TTL：节点活着，这次运行的 key 就不该过期
        self._touch_fn = touch_fn
        self._stop_event = threading.Event()

    def run(self) -> None:
        self._beat()
        while not self._stop_event.wait(setting.HEARTBEAT_INTERVAL):
            self._beat()

    def _beat(self) -> None:
        try:
            self._redis.hset(self._hkey, self._node_id, f"{time.time():.3f}:{self._pending_fn()}")
            self._redis.expire(self._hkey, max(2, int(setting.HEARTBEAT_STALE * 4)))
        except Exception:  # 心跳失败不该拖垮爬虫
            log.debug("心跳写入失败", exc_info=True)
        if self._renew_fn is not None:
            self._renew_fn()
        if self._touch_fn is not None:
            self._touch_fn()

    def stop(self) -> None:
        self._stop_event.set()


class RedisScheduler(BaseScheduler):
    def __init__(
        self,
        parser: BaseParser,
        *,
        redis_key: str,
        keep_alive: bool | None = None,
        run_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        #: 作业命名空间：一个 redis_key 一个。不设 RUN_ID 时所有 key 都在这下面
        self._job_ns = f"{setting.REDIS_KEY_PREFIX}:{redis_key}"
        #: 运行标识。None = 读配置；"" = 显式关掉（BatchSpider 用，它的身份是批次作业）
        self._run_id = str(setting.RUN_ID or "") if run_id is None else run_id
        #: 本次运行的命名空间：队列 / 种子锁 / 在途 / 心跳 / 失败列表都在这下面。
        #: 设了 RUN_ID 就是 <job>:run:<id>，否则退化成作业命名空间 —— 老行为一字不改
        self._ns = f"{self._job_ns}:run:{self._run_id}" if self._run_id else self._job_ns
        self._dedup_scope = _resolve_dedup_scope(self._run_id)
        self._redis = get_redis()
        self._keep_alive = setting.SPIDER_KEEP_ALIVE if keep_alive is None else keep_alive
        self._node_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self._hkey = f"{self._ns}:heartbeat"
        self._heartbeat: _Heartbeat | None = None
        #: 本节点是否拿到过任务 —— 启动宽限只保护「一个都没见过」的窗口
        self._seen_work = False
        self._started_at = time.monotonic()
        #: 自己留着引用：运行作用域下要给它们的 key 续 TTL
        self._req_dedup: Dedup | None = None
        self._item_dedup: Dedup | None = None
        super().__init__(parser, **kwargs)

    # ------------------------------------------------------------------
    def _make_task_queue(self) -> RedisTaskQueue:
        return RedisTaskQueue(self._ns, self._redis)

    def _summary_context(self) -> dict[str, Any]:
        ctx = super()._summary_context()
        # run_id 空串 = 单机语义没设运行作用域；namespace 是本次运行的实际前缀，
        # redis_key 是作业前缀 —— Hub 用 run_id 关联实例，用 namespace 去 Redis 查监控
        ctx["run_id"] = self._run_id
        ctx["namespace"] = self._ns
        ctx["redis_key"] = self._job_ns
        return ctx

    def _dedup_ns(self) -> str:
        """去重落在哪：run = 本次运行下（重跑从头抓）；spider = 作业下（增量爬）。"""
        return self._ns if self._dedup_scope == "run" else self._job_ns

    def _make_dedup(self) -> Filter:
        self._req_dedup = get_request_filter(name=self._dedup_ns(), redis_client=self._redis)
        return self._req_dedup

    def _make_item_dedup(self) -> Dedup | None:
        """运行作用域下 Item 指纹也得跟着走。

        不然请求按运行重抓了，每条 Item 却被上一次运行的指纹挡在入库之前 ——
        「请求成功 63，入库 0」，又是一种 exit 0 的空转。
        作业作用域（spider / 没设 RUN_ID）保持老行为：ItemBuffer 自己建，
        落在全局的 <prefix>:items 下。
        """
        if not setting.ITEM_FILTER_ENABLE or self._dedup_scope != "run":
            return None
        self._item_dedup = get_item_filter(name=f"{self._ns}:items", redis_client=self._redis)
        return self._item_dedup

    # ------------------------------------------------------------------
    # 运行作用域：key 的 TTL 与索引
    # ------------------------------------------------------------------
    def _run_keys(self) -> list[str]:
        """本次运行在 Redis 里占的全部 key（心跳除外，它有自己的短 TTL）。"""
        keys = [
            f"{self._ns}:z_requests",
            f"{self._ns}:z_inflight",
            f"{self._ns}:lock:seed",
            f"{self._ns}:{_FAILED_KEY}",
            self._replay_staging_key(),
            f"{self._job_ns}:runs",
        ]
        if self._dedup_scope == "run":
            for dedup in (self._req_dedup, self._item_dedup):
                if dedup is not None:
                    keys.extend(dedup.redis_keys())
        return keys

    def _touch_run_keys(self) -> None:
        """给运行作用域下的 key 续 TTL。心跳每跳一次调一次，退出时再调一次。

        没有 TTL 的话，每 5 分钟一次的定时任务一天就在 Redis 里留下 288 套 key。
        由心跳续期而不是一次性设死：一个跑了 8 天的运行不能在第 7 天被自己清掉。
        对不存在的 key 调 EXPIRE 是空操作，所以列表里多几个没用到的 key 无害。
        """
        if not self._run_id:
            return
        try:
            pipe = self._redis.pipeline()
            for key in self._run_keys():
                pipe.expire(key, setting.RUN_TTL)
            pipe.execute()
        except Exception:
            log.debug("续期运行 key 失败", exc_info=True)

    def _register_run(self) -> None:
        """把本次运行记进 <job>:runs（zset，score = 开始时刻）。

        监控要靠它列出「这个爬虫最近跑过哪些运行」—— 否则得 SCAN 整个 keyspace。
        """
        if not self._run_id:
            return
        try:
            self._redis.zadd(f"{self._job_ns}:runs", {self._run_id: time.time()}, nx=True)
        except Exception:
            log.debug("登记运行失败", exc_info=True)

    def _warn_if_dedup_is_process_local(self) -> None:
        """分布式跑着进程内去重时出声 —— 那是个静默错配。

        `DEDUP_FILTER` 默认是 `memory`（进程内布隆）。单机 AirSpider 下这没问题，
        但分布式下每个节点各有一份指纹，**互相不知道**：

        - 两个节点各自解析出同一个 URL 时会**重复抓**。实测两节点、两个入口页
          都链到同一个页面：`memory` 下那个页面被抓 **2** 次，`redis` 下 1 次。
        - Item 去重用的是同一个开关，于是任务被重放时（租约是**至少一次**语义，
          节点卡住或被硬杀就会重放）**重复入库**。SQL 管道有唯一键兜底，
          CSV / Mongo / 自定义管道没有。

        大部分现象看起来仍然正常 —— 共享队列保证了「入了队的任务只被取走一次」，
        所以错配不会报错，只会悄悄多抓、多写。`docs/distributed.md` 的示例里写着
        `DEDUP_FILTER = "redis"`，但此前没设的人得不到任何提示。
        """
        backend = str(setting.DEDUP_FILTER).lower()
        if backend in {"memory", "lite"}:
            log.warning(
                "DEDUP_FILTER={} 是**进程内**去重，分布式下每个节点各有一份指纹、"
                "互相不知道：两个节点解析出同一个 URL 会重复抓，任务被重放时会重复入库。"
                "改成 DEDUP_FILTER='redis'（布隆）或 'redis-set'（精确）。"
                "确实只想要进程内去重的话，忽略这条即可。",
                setting.DEDUP_FILTER,
            )

    def _warn_on_unsafe_timing(self) -> None:
        """心跳 / 租约几个间隔之间有硬约束，配反了不报错、只会悄悄出事。

        - 心跳间隔 ≥ `HEARTBEAT_STALE`：两次心跳之间，活着的节点在别人眼里就是「死了」，
          其它节点据此判定「所有节点都空闲」提前收工；
        - 心跳间隔 ≥ `SPIDER_TASK_LEASE`：租约是挂在心跳上续的，续上之前就过期了，
          别的节点会把还在处理的任务回收走重抓一遍。
        """
        interval = setting.HEARTBEAT_INTERVAL
        if interval >= setting.HEARTBEAT_STALE:
            log.warning(
                "HEARTBEAT_INTERVAL={}s 不小于 HEARTBEAT_STALE={}s：两次心跳之间活节点会被当成"
                "已死，其它节点可能提前收工。把 HEARTBEAT_STALE 设成心跳间隔的 3 倍以上",
                interval,
                setting.HEARTBEAT_STALE,
            )
        lease = setting.SPIDER_TASK_LEASE
        if lease > 0 and interval >= lease:
            log.warning(
                "HEARTBEAT_INTERVAL={}s 不小于 SPIDER_TASK_LEASE={}s：租约在心跳续上之前就过期，"
                "处理中的任务会被别的节点回收重抓。把租约设成心跳间隔的几倍以上",
                interval,
                lease,
            )

    def _on_start(self) -> None:
        self._warn_if_dedup_is_process_local()
        self._warn_on_unsafe_timing()
        self._register_run()
        self._touch_run_keys()
        self._heartbeat = _Heartbeat(
            self._redis,
            self._hkey,
            self._node_id,
            self._local_pending,
            self._renew_leases,
            touch_fn=self._touch_run_keys,
        )
        self._heartbeat.start()
        log.info("节点 {} 加入（命名空间 {}）", self._node_id, self._ns)
        if self._run_id and self._dedup_scope == "spider":
            # 这是用户的显式选择（增量爬），但后果要说出来：本轮不会重抓以前抓过的 URL。
            # 不说的话，「运行是新的、却 0 请求」和生产上那两天的空转长得一模一样
            log.info(
                "DEDUP_SCOPE=spider：去重跨运行持久（{}），本次运行不会重抓以前抓过的 URL。"
                "要每次从头抓，把它设成 run（或去掉，auto 在有 RUN_ID 时就是 run）",
                self._job_ns,
            )

    def _renew_leases(self) -> int:
        """把本节点还持有的任务租约往后推。

        没有这一步的话租约从**领走**那刻起算，而 collector 一次领
        `COLLECTOR_TASK_COUNT` 个 —— 排在后面的任务在被碰到之前就超时了，
        节点全程健康却被判定「已死」，任务被别人抢去重抓一遍。
        """
        try:
            return int(self._task_queue.renew(self._collector.held_requests()))
        except Exception:
            log.debug("续期租约失败", exc_info=True)
            return 0

    def _seed(self) -> None:
        self._seed_or_skip()
        # 放在种子之后：先把失败请求灌进去的话队列就非空了，start_requests 会被当成
        # 「已经种过」跳过。种子那条路里 _seed_requests() 已经回放过的，这里取到的是空的
        if self._replay_failed_requests():
            self._request_buffer.flush()

    def _seed_or_skip(self) -> None:
        seed_key = f"{self._ns}:lock:seed"
        if not acquire_once(self._redis, seed_key, ttl=setting.SPIDER_SEED_LOCK_TTL):
            self._explain_skipped_seed(seed_key)
            return
        if not self._task_queue.empty():
            log.info("队列非空（{} 条），跳过种子注入，继续消费", self._task_queue.qsize())
            return
        log.info("种子请求 {} 条", self._seed_requests())

    def _replay_staging_key(self) -> str:
        return f"{self._ns}:{_FAILED_KEY}:replaying:{self._node_id}"

    def _replay_failed_requests(self) -> int:
        """`RETRY_FAILED_ON_START` 的分布式版：把 Redis 失败列表灌回队列。

        原来分布式下重试耗尽的请求 RPUSH 进 `<ns>:failed_requests` 之后就**再也没有
        代码读它**——`RETRY_FAILED_ON_START` 只回放本地文件，而分布式节点根本不往
        文件里写。文档让用户「要把数据真正抓回来，开 RETRY_FAILED_ON_START」，对
        `Spider` / `TaskSpider` 这句话是空的：实测站点恢复后重跑，0 个请求发出，
        失败列表原封不动。

        跟文件版同一个思路：**先改名再消费**。`RENAME` 是原子的，多个节点同时启动
        也只有一个能拿到；灌进队列、flush 成功之后才删暂存 key——中途崩了暂存 key
        还在，不会因为「已经读过了」就丢掉。本机落盘的那份（推回 Redis 失败时的兜底）
        照旧由父类回放。
        """
        if not setting.RETRY_FAILED_ON_START:
            return 0
        count = super()._replay_failed_requests()
        staging = self._replay_staging_key()
        try:
            self._redis.rename(f"{self._ns}:{_FAILED_KEY}", staging)
        except Exception:
            # 列表不存在（没有失败请求），或别的节点先一步改了名
            return count
        replayed = 0
        for raw in self._redis.lrange(staging, 0, -1):
            try:
                request = Request.from_dict(tools.loads_json(raw))
            except Exception:
                log.exception("这条失败请求解析不了，跳过：{}", str(raw)[:120])
                continue
            # 指纹早就在去重里了——不清掉的话灌回去会被静默挡下
            request.filter_repeat = False
            self._request_buffer.put(request)
            replayed += 1
        self._request_buffer.flush()
        self._redis.delete(staging)
        if replayed:
            log.info("从 Redis 失败列表重新灌入 {} 条请求", replayed)
        return count + replayed

    def _explain_skipped_seed(self, seed_key: str) -> None:
        """拿不到种子锁 —— 是别的节点正在种，还是上一轮留下的？**要分得清。**

        生产上撞到的：定时任务每 5 分钟起一轮，第一轮之后每个节点都在这里
        打一句「另一节点已注入种子」然后消费一个空队列、exit 0。49 小时、
        1158 个实例，日志里没有一个字说这是空转。

        判据：锁已经拿了一阵子（超过启动宽限，不是和我同时启动的节点）、
        队列空、没有在途、没有活节点 —— 那就是一个已经跑完的作业，本轮无事可做。
        """
        try:
            ttl = int(self._redis.ttl(seed_key))
            age = setting.SPIDER_SEED_LOCK_TTL - ttl if ttl > 0 else None
            stale = (
                age is not None
                and age > setting.SPIDER_STARTUP_GRACE
                and self._task_queue.empty()
                and self._task_queue.inflight_count() == 0
                and self._live_node_count() == 0
            )
        except Exception:
            log.debug("判断种子锁状态失败", exc_info=True)
            stale = False
        if not stale:
            log.info("另一节点已注入种子，本节点直接消费队列")
            return
        log.warning(
            "命名空间 {} 里是一个**已完成**的作业：种子锁是 {} 秒前留下的（还有 {} 秒过期）、"
            "队列空、没有在途、没有活节点。本节点无事可做，会在启动宽限期后正常退出 ——"
            "这不是崩溃，是空转。要重新抓一遍：设 RUN_ID 让每次运行独立"
            "（NetspyHub 会自动注入），或换一个 redis_key，或手动删掉 {} 和去重 key"
            "（见 docs/distributed.md「重新注入种子」）。"
            "另一种可能是别的节点的 start_requests 还没跑完 —— 那它稍后会出现在心跳里。",
            self._ns,
            age,
            ttl,
            seed_key,
        )

    def _live_node_count(self) -> int:
        """心跳还新鲜的节点数（不含本节点 —— 它此刻还没开始跳）。"""
        now = time.time()
        entries: dict[str, str] = self._redis.hgetall(self._hkey)
        alive = 0
        for raw in entries.values():
            ts_str, _, _ = raw.partition(":")
            try:
                if now - float(ts_str) <= setting.HEARTBEAT_STALE:
                    alive += 1
            except ValueError:
                continue
        return alive

    def _is_done(self) -> bool:
        if self._keep_alive:
            return False
        if self._in_startup_grace():
            return False
        # 顺手回收租约到期的任务：结束检测本来就在周期性跑，不必另起线程。
        # 放在判定**之前**很关键 —— 否则「队列空了」会先成立、爬虫先退出，
        # 而那些被硬杀节点领走的任务还挂在在途表里没人捡
        if self._task_queue.reclaim_expired():
            return False
        if not self._local_idle() or not self._task_queue.empty():
            return False
        # 还有任务在别的节点手里没销账 —— 它们可能正在被处理，也可能属于一个
        # 已经死掉的节点。不能就此收工：租约到期后它们要被重新抓一遍
        if self._task_queue.inflight_count() > 0:
            return False
        return self._all_nodes_idle()

    def _in_startup_grace(self) -> bool:
        """本节点还在启动宽限期内、且**一个任务都没见过**。

        多节点同时启动时只有一个能拿到种子锁，其余节点看到的是空队列。没有这个
        宽限的话，它们会在 `DONE_CHECK_TIMES × DONE_CHECK_INTERVAL`（默认 1.5 秒）
        内判定「抓完了」直接退出 —— 而播种节点那时往往还没把种子推进队列。

        `_all_nodes_idle()` 挡不住这个：播种节点在那一刻的 pending 也是 0，
        它自己还没开始拉活，看上去和「闲着」没区别。

        只有从没拿到过任务的节点才等；拿到过活之后 `_seen_work` 为真，
        后续判定完全按原来的规则走，不会拖慢正常结束。
        """
        if self._seen_work:
            return False
        if self.stats.get(sk.REQUEST_OK) or self.stats.get(sk.REQUEST_FAILED):
            self._seen_work = True
            return False
        grace = setting.SPIDER_STARTUP_GRACE
        return grace > 0 and time.monotonic() - self._started_at < grace

    def _all_nodes_idle(self) -> bool:
        now = time.time()
        try:
            entries: dict[str, str] = self._redis.hgetall(self._hkey)
        except Exception:
            log.debug("读取心跳失败", exc_info=True)
            return False
        for raw in entries.values():
            ts_str, _, pending_str = raw.partition(":")
            try:
                if now - float(ts_str) > setting.HEARTBEAT_STALE:
                    continue  # 死节点
                if int(pending_str) > 0:
                    return False
            except ValueError:
                continue
        return True

    def _on_shutdown(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat.stop()
            self._heartbeat.join(timeout=5)
        try:
            self._redis.hdel(self._hkey, self._node_id)
        except Exception:
            log.debug("清理心跳失败", exc_info=True)

        # 本节点采集器 / buffer 里没跑完的推回 Redis 队列，交给其他节点 / 重启接管
        leftovers = [
            *self._request_buffer.drain_pending(),
            *self._collector.drain(),
        ]
        for i, request in enumerate(leftovers):
            # 已经过过去重了；不清掉的话推回去也会被自己的指纹挡掉
            request.filter_repeat = False
            try:
                self._task_queue.put(request)
                # 推回队列之后要销掉旧租约，否则这条任务同时躺在队列和在途表里：
                # 别的节点抓完队列后 inflight_count() 仍大于 0，会一直不肯收工。
                # 顺序不能反 —— 先销账再入队的话，中间崩掉任务就两头都不在了
                self._task_queue.done(request)
            except Exception:
                # 这个函数**恰恰是在「Redis 出问题」时被调用的** —— 推不回去就落盘，
                # 否则剩下的请求静默消失：它们既不在队列里、也不在缓冲区里了
                log.exception("推回 Redis 失败，改为落盘")
                self._dump_requests(leftovers[i:], "退出时推回 Redis 失败")
                pushed = i
                break
        else:
            pushed = len(leftovers)
        if pushed:
            log.info("已把 {} 条未完成请求推回 Redis 队列", pushed)
        # 心跳已经停了，最后再续一次：退出前才建出来的 key（比如失败列表）也要有 TTL
        self._touch_run_keys()

    # ------------------------------------------------------------------
    def _on_failed_request(self, request: Any) -> None:
        """把重试耗尽的请求推到 Redis 失败列表。"""
        try:
            self._redis.rpush(f"{self._ns}:{_FAILED_KEY}", tools.dumps_json(request.to_dict()))
        except Exception:
            log.debug("写失败请求列表失败", exc_info=True)
