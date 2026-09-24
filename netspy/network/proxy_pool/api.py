"""从一个 HTTP 接口拉取代理列表的代理池。

``PROXY_EXTRACT_API`` 返回的内容可以是每行一个代理，或一个 JSON 字符串数组。
拿到后轮流使用；某代理被 ``report_bad`` 或用满 ``PROXY_MAX_USE_TIMES`` 次后丢弃，
池空时（且距上次拉取超过 ``PROXY_MIN_INTERVAL``）重新拉取。

``report_bad`` 让代理**冷却**而不是永久出局：原来它只进不出，一次瞬时错误就把代理
永久拉黑，拉列表时又拒绝放回黑名单里的 —— 单代理池被打空后再也起不来，
配合 ``PROXY_ALLOW_DIRECT=False`` 就是每请求空等 ``PROXY_WAIT_TIMEOUT`` 再失败。
现在连续失败按 2 倍退避（见 ``PROXY_BAN_SECONDS``），冷却期一过自动重新可用。
"""

from __future__ import annotations

import contextlib
import threading
import time
from collections import deque

import httpx

from netspy import setting
from netspy.network.proxy_pool.base import ProxyPool
from netspy.utils import tools
from netspy.utils.log import get_logger

log = get_logger("proxy_pool")


class ApiProxyPool(ProxyPool):
    def __init__(self, api: str | None = None) -> None:
        self._api = api or setting.PROXY_EXTRACT_API
        self._lock = threading.Lock()
        #: 拉取时持有，`self._lock` 不持有 —— 拉取是一次 HTTP 请求（最长
        #: PROXY_MIN_INTERVAL 允许的等待之外还有 httpx 自己的 10s 超时）。
        #: 拿主锁裹住它的话，另一个线程哪怕只是想 report_bad() 一个跟这次拉取
        #: 毫不相干的代理，也得先干等这次拉取的整个网络往返 —— 实测跟拉取
        #: 完全无关的 report_bad() 被拖了整整一次拉取耗时（1.0s）。
        #: 这把独立的锁只用来防止多个线程同时打供应商接口（穿透 PROXY_MIN_INTERVAL）。
        self._fetch_lock = threading.Lock()
        self._pool: deque[str] = deque()
        self._use_count: dict[str, int] = {}
        # 代理 -> 解禁时刻（monotonic）。原来这里是个只进不出的 set，
        # 一次瞬时错误就把代理永久踢出，池被啃空后整个爬虫静默降级到
        # 「每请求空等 PROXY_WAIT_TIMEOUT 再失败」
        self._banned: dict[str, float] = {}
        # 代理 -> (连续失败次数, 上次失败时刻)，用来做指数退避
        self._fails: dict[str, tuple[int, float]] = {}
        #: 代理 -> (连续可疑失败次数, 上次时刻)，攒够 PROXY_SUSPECT_BAN_AFTER 才拉黑。
        #: 带时间戳是为了能按年龄回收 —— 见 `_prune`
        self._suspects: dict[str, tuple[int, float]] = {}
        # None = 从没抓过。不能用 0.0 当哨兵：monotonic 的原点是开机，刚启动的容器上
        # monotonic() - 0.0 < PROXY_MIN_INTERVAL 会把第一次抓取跳过，代理池起不来。
        self._last_fetch: float | None = None

    # ------------------------------------------------------------------
    def _prune(self, now: float) -> None:
        """回收已经没有意义的记账。**调用方必须持有 `self._lock`。**

        这四个 dict 都以代理为键，而代理是会轮换的 —— 很多代理商的提取接口
        每次返回**全新的 IP**。实测 3000 次 `get_proxy` 之后 `_use_count` 里
        躺着 1500 条，且只增不减。

        只做**语义等价**的回收，不顺手改行为：

        - `_banned` 里已过期的：`_is_banned` 本来也会在下次被查到时删掉，
          只是「下次」对一个再也不会出现的代理永远不来；
        - `_fails` / `_suspects` 里过了 `PROXY_BAN_MAX_SECONDS` 的：现有逻辑
          本来就把它们当作已归零（退避重新从头算），删掉不改变任何判断；
        - `_use_count` 里**既不在池中、也不在冷却中**的：这类代理已经退出流通，
          它的使用次数没有意义。**这一条同时修好了轮换**：代理用满
          `PROXY_MAX_USE_TIMES` 被 `_drop` 之后，`_fetch` 会把它放回池里，
          而计数不清零 —— 于是它只能再用一次就又被丢。实测 3 个代理的池在
          第一轮用完之后，稳态退化成**每秒只取到 3 个**（其余空手而归），
          而且每秒去拉一次供应商的列表接口，永远不停。
          文档写的是「用满这么多次后**轮换**」，不是终身配额。
        """
        for key, until in list(self._banned.items()):
            if until <= now:
                del self._banned[key]
        for key, (_, last) in list(self._fails.items()):
            if now - last > setting.PROXY_BAN_MAX_SECONDS:
                del self._fails[key]
        for key, (_, last) in list(self._suspects.items()):
            if now - last > setting.PROXY_BAN_MAX_SECONDS:
                del self._suspects[key]
        live = set(self._pool) | set(self._banned)
        for key in list(self._use_count):
            if key not in live:
                del self._use_count[key]

    def _fetch(self) -> None:
        """拉取新代理。**只在 `_fetch_lock` 内部持有 `self._lock`，且不横跨 HTTP 请求**：

        真正的网络往返（`httpx.get`）发生在两段锁之间 —— 这样另一个线程调
        `get_proxy()` / `report_bad()` 等只碰内存状态的操作，不会被这次拉取
        卡住。`_fetch_lock` 本身则保证同一时刻只有一个线程在打供应商接口，
        且 `PROXY_MIN_INTERVAL` 的判断在它内部做，不会被并发穿透。
        """
        if not self._api:
            return
        with self._fetch_lock:
            with self._lock:
                now = time.monotonic()
                if (
                    self._last_fetch is not None
                    and now - self._last_fetch < setting.PROXY_MIN_INTERVAL
                ):
                    return
                self._last_fetch = now
                # 先回收再补充：用满次数被丢出池的代理，计数在这里清零，
                # 补回来之后才是「轮换」而不是「只能再用一次」
                self._prune(now)
            try:
                body = httpx.get(self._api, timeout=10).text.strip()
            except httpx.HTTPError as exc:
                log.error("拉取代理失败：{!r}", exc)
                return
            proxies = self._parse(body)
            with self._lock:
                for proxy in proxies:
                    if not self._is_banned(proxy, time.monotonic()) and proxy not in self._pool:
                        self._pool.append(proxy)
                log.debug("代理池补充 {} 个，当前 {}", len(proxies), len(self._pool))

    @staticmethod
    def _parse(body: str) -> list[str]:
        if body.startswith("["):
            try:
                return [str(p) for p in tools.loads_json(body)]
            except ValueError:
                return []
        return [line.strip() for line in body.splitlines() if line.strip()]

    def _normalize(self, proxy: str) -> str:
        return proxy if "://" in proxy else f"http://{proxy}"

    # ------------------------------------------------------------------
    def get_proxy(self) -> str | None:
        with self._lock:
            empty = not self._pool
        if empty:
            # 在锁外面拉取：见 `_fetch` 的说明。拉取期间别的线程该干嘛干嘛，
            # 不会被这次可能耗时数秒的 HTTP 请求卡住
            self._fetch()
        with self._lock:
            for _ in range(len(self._pool)):
                proxy = self._pool[0]
                self._pool.rotate(-1)  # 轮转到队尾
                if self._is_banned(proxy, time.monotonic()):
                    self._drop(proxy)
                    continue
                self._use_count[proxy] = self._use_count.get(proxy, 0) + 1
                if self._use_count[proxy] >= setting.PROXY_MAX_USE_TIMES:
                    self._drop(proxy)
                return self._normalize(proxy)
            return None

    def _drop(self, proxy: str) -> None:
        with contextlib.suppress(ValueError):
            self._pool.remove(proxy)

    def _ban_seconds(self, fails: int) -> float:
        """第 N 次连续失败要冷却多久。``PROXY_BAN_SECONDS=0`` 时回到旧的「永久拉黑」。"""
        base = setting.PROXY_BAN_SECONDS
        if base <= 0:
            return float("inf")
        return float(min(base * (2 ** (fails - 1)), setting.PROXY_BAN_MAX_SECONDS))

    def _is_banned(self, proxy: str, now: float) -> bool:
        """冷却是否还没结束。**过期的顺手清掉** —— 这就是代理重新可用的路径。"""
        until = self._banned.get(proxy)
        if until is None:
            return False
        if until > now:
            return True
        del self._banned[proxy]
        log.debug("代理 {} 冷却结束，重新可用", proxy)
        return False

    def report_good(self, proxy: str) -> None:
        """成功一次 —— 连续失败计数清零。

        每个成功请求都会调用它，所以先做两次 dict 成员检查（GIL 下是原子的），
        绝大多数时候直接返回，不去碰池的全局锁 —— 那把锁在 `get_proxy` 的
        热路径上。最坏情况是漏掉一次清零，下一次成功会补上。

        ⚠️ **别把这条当成性能优化**：实测两种写法都在 ~3M 次/秒（每次约 0.3µs），
        而 httpx 这条路径每请求约 1.9ms —— 占比 0.016%，怎么写都无所谓。
        留着快路径只为不和 `get_proxy` 抢同一把锁，而**这一点没有单独量过**。
        """
        raw = proxy.split("://", 1)[-1]
        if raw not in self._fails and raw not in self._suspects:
            return
        with self._lock:
            self._fails.pop(raw, None)
            self._suspects.pop(raw, None)

    def report_suspect(self, proxy: str) -> None:
        """一次说不清是谁的错的失败：读超时、连接重置、响应畸形。

        这类失败**大多是目标站的锅**，直接拉黑会把整池健康代理清空
        （实测三个请求打一个慢 URL 就够了）。所以要连续攒够
        `PROXY_SUSPECT_BAN_AFTER` 次 —— 中间成功一次就由 `report_good` 清零。
        真正挂掉的代理会连续失败，照样会被拉黑，只是晚几次。
        """
        threshold = max(setting.PROXY_SUSPECT_BAN_AFTER, 1)
        raw = proxy.split("://", 1)[-1]
        with self._lock:
            now = time.monotonic()
            prev, last = self._suspects.get(raw, (0, 0.0))
            # 和 `_fails` 一个规矩：隔得太久就不算「连续」了
            if last and now - last > setting.PROXY_BAN_MAX_SECONDS:
                prev = 0
            n = prev + 1
            if n < threshold:
                self._suspects[raw] = (n, now)
                log.debug("代理 {} 第 {}/{} 次可疑失败（还不拉黑）", proxy, n, threshold)
                return
            self._suspects.pop(raw, None)
        log.warning("代理 {} 连续 {} 次可疑失败，按代理故障处理", proxy, threshold)
        self.report_bad(proxy)

    def report_bad(self, proxy: str) -> None:
        with self._lock:
            now = time.monotonic()
            raw = proxy.split("://", 1)[-1]
            fails, last = self._fails.get(raw, (0, 0.0))
            # 距上次失败已经超过最长冷却 —— 中间一直好好的，退避重新从头算，
            # 否则一个跑了几天的进程会把偶发失败累积成永久放逐
            if last and now - last > setting.PROXY_BAN_MAX_SECONDS:
                fails = 0
            fails += 1
            self._fails[raw] = (fails, now)
            self._suspects.pop(raw, None)
            seconds = self._ban_seconds(fails)
            # 两种写法都记：拉列表拿到的是裸 host:port，调用方报上来的带 scheme
            for key in (raw, proxy):
                self._banned[key] = now + seconds
            self._drop(raw)
            log.warning(
                "代理 {} 第 {} 次连续失败，冷却 {}",
                proxy,
                fails,
                "永久（PROXY_BAN_SECONDS=0）" if seconds == float("inf") else f"{seconds:.0f}s",
            )

    def close(self) -> None:
        with self._lock:
            self._pool.clear()
