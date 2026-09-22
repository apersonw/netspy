"""``ItemBuffer`` —— 收集 parse 产出的数据，批量去重后交给管道落库。

流程：``put`` 累积 → 定时 / 满量 ``flush`` → 按 (表, 是否 UpdateItem, 管道) 分组
→ Item 级去重（fingerprint）→ 逐管道 ``save_items`` / ``update_items``
→ 成功则写去重指纹；失败则 dump 到 ``FAILED_ITEM_PATH``。

**落库之后才给任务销账**：每条 item 记着产出它的那条请求（``owner``），一批写完
（入库成功、或 dump 进 failed_items —— 两者都落到了持久介质）才回调 ``ack``。
销账早于落库的话，节点被硬杀就会**静默丢数据**：数据只在内存里，而任务已经
销过账不会被回收，请求指纹又是入队前就写的，重跑一遍也补不回来。

给了 ``handler`` 时走调试快路径：直接把原始批次交给 handler，不去重、不落库。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from netspy import setting
from netspy.exceptions import ItemError
from netspy.network.item import Item, UpdateItem
from netspy.utils import stats as sk
from netspy.utils import tools
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from netspy.dedup import Dedup
    from netspy.pipelines.base import BasePipeline
    from netspy.utils.stats import Stats

log = get_logger("item_buffer")

ItemHandler = Callable[[list[Any]], None]


class _Norm(NamedTuple):
    table: str
    is_update: bool
    data: dict[str, Any]
    fingerprint: str | None
    update_keys: tuple[str, ...]
    pipelines: tuple[str, ...] | None


def _normalize(obj: Any) -> _Norm:
    if isinstance(obj, Item):
        obj.pre_to_db()
        is_update = isinstance(obj, UpdateItem)
        keys = tuple(obj.update_key) if isinstance(obj, UpdateItem) else ()
        fp = obj.fingerprint if setting.ITEM_FILTER_ENABLE else None
        pipes = tuple(obj.pipelines) if obj.pipelines else None
        return _Norm(obj.table_name, is_update, obj.to_dict(), fp, keys, pipes)
    if isinstance(obj, dict):
        return _Norm(setting.ITEM_DEFAULT_TABLE, False, obj, None, (), None)
    raise ItemError(f"无法入库的类型：{type(obj)!r}（需要 Item 或 dict）")


class ItemBuffer(threading.Thread):
    def __init__(
        self,
        stats: Stats,
        *,
        handler: ItemHandler | None = None,
        pipelines: list[str] | None = None,
        dedup: Dedup | None = None,
        ack: Callable[[Any], None] | None = None,
    ) -> None:
        super().__init__(name="item-buffer", daemon=True)
        self._stats = stats
        self._handler = handler
        self._pipeline_paths = pipelines
        self._pipeline_cache: dict[tuple[str, ...], list[BasePipeline]] = {}
        self._dedup = dedup
        #: 落库成功后给任务销账。分布式下就是 ``Collector.done``
        self._ack = ack
        #: (item, 产出它的请求) —— owner 为 None 表示没人等着它销账
        self._pending: list[tuple[Any, Any]] = []
        #: 按 owner 挂的「落库之后再做」回调。BatchSpider 用它把任务表的
        #: 「已完成」推迟到数据真的落库之后 —— 否则任务标了 DONE 而数据还在内存里，
        #: 节点一死，防丢机制只回收 DOING，这些任务永远不会被重跑
        self._after_persist: dict[int, list[Any]] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    def owns(self, owner: Any) -> bool:
        """`owner` 还有 item 压在缓冲里没落库吗。

        已经 flush 过就返回 False —— 那时数据已经在库里，调用方可以立刻动作。
        """
        with self._lock:
            return any(o is owner for _, o in self._pending)

    def after_persist(self, owner: Any, fn: Any) -> None:
        """`owner` 产出的数据整批落库之后再执行 `fn`。

        落库失败时**不执行** —— 让上游的状态留在「处理中」，由防丢机制回收重跑。

        ⚠️ 只有在能确定 `owner` **此刻**还压在缓冲里时才该调用这个方法——
        调用方如果先用 `owns()` 查了一遍再决定要不要调用这个方法，中间有
        没上锁的空档：`flush()` 完全可能正好夹在两次调用之间跑完，等这里真正
        登记上钩子时，那一批早就已经处理完、`_run_after_persist` 也跑过了
        （pop 到空，什么都没做）——钩子从此再也不会被执行。这种「先查后做」
        的场景应该用 `after_persist_if_pending()`，把检查和登记锁在同一次里。
        """
        with self._lock:
            self._after_persist.setdefault(id(owner), []).append(fn)

    def after_persist_if_pending(self, owner: Any, fn: Any) -> bool:
        """`owner` 还压在缓冲里就登记 `fn`（返回 True）；已经 flush 过就不登记
        （返回 False，调用方该走「立刻处理」的路径）。

        检查和登记在同一次加锁里做完，不给 `flush()` 留插队空档——
        这正是 `owns()` 后面紧跟一次独立加锁的 `after_persist()` 会踩的坑：
        `flush()` 一旦插进这两次调用中间，钩子就永远不会被执行了。
        """
        with self._lock:
            if not any(o is owner for _, o in self._pending):
                return False
            self._after_persist.setdefault(id(owner), []).append(fn)
            return True

    def put(self, item: Any, owner: Any = None) -> None:
        with self._lock:
            self._pending.append((item, owner))
            size = len(self._pending)
        if size >= setting.ITEM_MAX_CACHED_COUNT:
            self.flush()

    def is_empty(self) -> bool:
        with self._lock:
            return not self._pending

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def run(self) -> None:
        while not self._stop_event.wait(setting.BUFFER_FLUSH_INTERVAL):
            self.flush()
        self.flush()

    def stop(self) -> None:
        self._stop_event.set()

    def close(self) -> None:
        for pipelines in self._pipeline_cache.values():
            for pipeline in pipelines:
                try:
                    pipeline.close()
                except Exception:
                    log.exception("管道 {} close 异常", type(pipeline).__name__)
        self._pipeline_cache.clear()

    # ------------------------------------------------------------------
    def flush(self) -> None:
        with self._lock:
            batch = self._pending
            self._pending = []
        if not batch:
            return
        objs = [obj for obj, _ in batch]
        # 同一条请求可能产出多条 item，按 id 去重后只销一次账
        owners = {id(owner): owner for _, owner in batch if owner is not None}
        if self._handler is not None:
            self._handler(objs)
            self._stats.incr(sk.ITEM, len(objs))
            self._ack_all(owners.values())
            return
        try:
            self._persist(objs)
        except Exception:
            # 销账早于落库正是这个模块要堵的洞，所以这里**不能**销账。
            # 不销账的任务会在租约到期后被别的节点重抓 —— 数据至少还有第二次机会。
            # 顺带保住 flush 线程：以前这里一抛，整个 ItemBuffer 线程就悄悄死了
            log.exception("落库异常，本批 {} 条不销账，等租约到期重抓", len(objs))
            return
        self._ack_all(owners.values())

    def _ack_all(self, owners: Any) -> None:
        for owner in owners:
            self._run_after_persist(owner)
            if self._ack is None:
                continue
            try:
                self._ack(owner)
            except Exception:
                log.exception("任务销账失败")

    def _run_after_persist(self, owner: Any) -> None:
        with self._lock:
            hooks = self._after_persist.pop(id(owner), None)
        for fn in hooks or ():
            try:
                fn()
            except Exception:
                log.exception("落库后回调异常")

    def _persist(self, batch: list[Any]) -> None:
        dedup = self._get_dedup()
        seen: set[str] = set()
        groups: dict[tuple[str, bool, tuple[str, ...] | None], list[_Norm]] = defaultdict(list)
        degraded = False
        for obj in batch:
            norm = _normalize(obj)
            if norm.fingerprint is not None and dedup is not None:
                try:
                    duplicate = norm.fingerprint in seen or dedup.get(norm.fingerprint)
                except Exception:
                    # 查重也是一次 Redis 调用。抛出去的话异常会穿出这个循环，
                    # 后面的数据既不写库、也不 dump ——
                    # 实测 9 条数据在第 3 条上抖一次：整批 9 条全丢。
                    # 去重是优化，丢数据不是可选项：按「没见过」放行
                    degraded = True
                    duplicate = False
                if duplicate:
                    self._stats.incr(sk.ITEM_DEDUP_DROPPED)
                    continue
                seen.add(norm.fingerprint)
            groups[(norm.table, norm.is_update, norm.pipelines)].append(norm)

        for (table, is_update, pipe_paths), rows in groups.items():
            datas = [row.data for row in rows]
            update_keys = list(rows[0].update_keys)
            pipelines = self._resolve_pipelines(pipe_paths)
            ok = all(
                self._write(pipeline, table, is_update, datas, update_keys)
                for pipeline in pipelines
            )
            if ok:
                self._stats.incr(sk.ITEM, len(datas))
                if dedup is not None:
                    try:
                        for row in rows:
                            if row.fingerprint is not None:
                                dedup.add(row.fingerprint)
                    except Exception:
                        # 数据已经写进去了，指纹没记上 —— 重抓时会重复入库。
                        # 但让异常穿出去更糟：后面的分组会一起丢
                        # （实测第一组写成功、剩下 6 条凭空消失）
                        degraded = True
            else:
                self._dump_failed(
                    table,
                    datas,
                    update_keys=update_keys if is_update else None,
                    pipelines=pipe_paths,
                )

        if degraded:
            # 按批记一次，别每条都刷屏；但必须记 —— 静默降级就是把一种无声换成另一种
            self._stats.incr(sk.DEDUP_DEGRADED)
            log.warning("Item 去重不可用，本批按「没见过」放行 —— 可能重复入库")

    # ------------------------------------------------------------------
    def _get_dedup(self) -> Dedup | None:
        if not setting.ITEM_FILTER_ENABLE:
            return None
        if self._dedup is None:
            from netspy.dedup import get_item_filter

            self._dedup = get_item_filter()
        return self._dedup

    def _resolve_pipelines(self, paths: tuple[str, ...] | None) -> list[BasePipeline]:
        key = (
            paths
            if paths is not None
            else tuple(
                self._pipeline_paths if self._pipeline_paths is not None else setting.ITEM_PIPELINES
            )
        )
        cached = self._pipeline_cache.get(key)
        if cached is None:
            cached = [tools.load_object(path)() for path in key]
            self._pipeline_cache[key] = cached
        return cached

    def _write(
        self,
        pipeline: BasePipeline,
        table: str,
        is_update: bool,
        datas: list[dict[str, Any]],
        update_keys: list[str],
    ) -> bool:
        try:
            if is_update:
                return pipeline.update_items(table, datas, update_keys)
            return pipeline.save_items(table, datas)
        except Exception:
            log.exception("管道 {} 写入异常", type(pipeline).__name__)
            return False

    def _dump_failed(
        self,
        table: str,
        datas: list[dict[str, Any]],
        *,
        update_keys: list[str] | None = None,
        pipelines: tuple[str, ...] | None = None,
    ) -> None:
        """把写失败的一批落到磁盘，**带上回放所需的全部信息**。

        只记 table + data 是不够的：`UpdateItem` 回放时会退化成普通 INSERT，
        而 PG 默认 `ON CONFLICT DO NOTHING` 会让这条 INSERT 什么都不做**并返回成功** ——
        于是 retry 报告成功、删掉文件，那次更新永久消失。

        字段是可选的：老的 dump 文件没有它们，读的时候按普通插入处理（即今天的行为）。
        """
        path = Path(setting.FAILED_ITEM_PATH)
        extra: dict[str, Any] = {}
        if update_keys:
            extra["update_keys"] = list(update_keys)
        if pipelines:
            extra["pipelines"] = list(pipelines)
        with path.open("a", encoding="utf-8") as fh:
            for data in datas:
                fh.write(tools.dumps_json({"table": table, "data": data, **extra}) + "\n")
        self._stats.incr(sk.ITEM_FAILED, len(datas))
        log.error("[{}] {} 条数据写入失败，已 dump 到 {}", table, len(datas), path)
