"""基于 Playwright 的浏览器渲染下载器。

架构：``pool_size`` 个独立渲染线程，每个线程持有一个 chromium（Playwright 的 sync API
不能跨线程用）。工作线程把渲染任务丢进队列并阻塞等结果，因此 ``pool_size`` 就是真正的
并发浏览器上限；小于 ``SPIDER_THREAD_COUNT`` 时天然形成背压。

需要 ``pip install netspy[render] && playwright install chromium``。
"""

from __future__ import annotations

import queue
import threading
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from netspy import setting
from netspy.exceptions import RequestError
from netspy.network.downloader._common import check_size
from netspy.network.downloader.base import Downloader
from netspy.network.response import Response
from netspy.network.user_agent import get_random_user_agent
from netspy.utils.log import get_logger

if TYPE_CHECKING:
    from netspy.network.request import Request

log = get_logger("downloader.playwright")

_BLOCKED_RESOURCES = frozenset({"image", "media", "font"})


@lru_cache(maxsize=1)
def _stealth_script() -> str:
    """反检测注入脚本——第三方 MIT 库 ``playwright-stealth``（跟 Netspy 一样是
    宽松开源协议，随手能用；不是从任何 NON-COMMERCIAL 协议的项目抄的代码，是
    它们也在用的同一个上游库）生成的完整版 evasion 集合，换掉了早期这里手写
    的 4 行版本。那 4 行只处理了 `navigator.webdriver`/`plugins`/`languages`，
    覆盖不了 WebGL vendor/renderer 这类更深的指纹——无 GPU 的容器里跑
    headless Chrome，WebGL 默认会暴露 "SwiftShader"（软件渲染）这种明确的
    自动化信号，这是实测撞到过的真实拦截原因，不是理论风险。这个库默认会把
    WebGL 伪装成一块常见的 Intel 核显，同时还处理了 iframe.contentWindow、
    chrome.runtime、media codecs 等一整套 puppeteer-extra-plugin-stealth 的
    证据集合。

    语言覆盖沿用框架原来的选择（中文站点优先）；其余全部用库自己的默认值——
    这些默认值本身就是社区长期验证过的组合，没有实测依据之前不去手动调它们。

    结果只取决于常量参数，跨线程复用同一份是安全的，用 `lru_cache` 避免每个
    渲染线程都重新生成一遍这份 40KB+ 的脚本。
    """
    from playwright_stealth import Stealth

    payload: str = Stealth(navigator_languages_override=("zh-CN", "zh")).script_payload
    return payload


#: `submit()` 兜底等待在渲染超时之上再留的余量（秒）。
#: 主修复是关闭时排空队列；这里只防「worker 不在了却没人收尾」。
_WAIT_MARGIN = 30.0


class _Job:
    __slots__ = ("error", "event", "request", "response")

    def __init__(self, request: Request) -> None:
        self.request = request
        self.event = threading.Event()
        self.response: Response | None = None
        self.error: BaseException | None = None

    def fail(self, exc: BaseException) -> None:
        """收尾一个没能完成的任务。

        排空和 worker 退出是并发的 —— worker 可能正好取走了这个 job。
        `Event.is_set()` 让收尾只生效一次，后到的那次不会覆盖已有结果。
        """
        if self.event.is_set():
            return
        self.error = exc
        self.event.set()


class _RenderWorker(threading.Thread):
    def __init__(self, index: int, jobs: queue.Queue[_Job | None], config: dict[str, Any]) -> None:
        super().__init__(name=f"render-{index}", daemon=True)
        self._jobs = jobs
        self._config = config
        self._stop_event = threading.Event()
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None

    def stop(self) -> None:
        self._stop_event.set()

    # ------------------------------------------------------------------
    def run(self) -> None:
        try:
            while not self._stop_event.is_set():
                try:
                    job = self._jobs.get(timeout=0.3)
                except queue.Empty:
                    continue
                if job is None:
                    break
                try:
                    self._ensure_browser()
                    job.response = self._render(job.request)
                except Exception as exc:
                    job.error = exc
                finally:
                    job.event.set()
        finally:
            self._teardown()

    def _ensure_browser(self) -> None:
        if self._browser is not None:
            return
        from playwright.sync_api import sync_playwright

        cfg = self._config
        self._pw = sync_playwright().start()
        browser_type = getattr(self._pw, cfg.get("browser", "chromium"))
        launch_kwargs: dict[str, Any] = {"headless": cfg["headless"]}
        if cfg.get("proxy"):
            launch_kwargs["proxy"] = {"server": cfg["proxy"]}
        self._browser = browser_type.launch(**launch_kwargs)

        ua = cfg.get("user_agent")
        if not ua and setting.RANDOM_USER_AGENT:
            ua = get_random_user_agent()
        ctx_kwargs: dict[str, Any] = {}
        if ua:
            ctx_kwargs["user_agent"] = ua
        viewport = cfg.get("viewport")
        if viewport:
            ctx_kwargs["viewport"] = {"width": viewport[0], "height": viewport[1]}
        self._context = self._browser.new_context(**ctx_kwargs)

        if not cfg.get("load_images", False):
            self._context.route("**/*", _maybe_block)
        if cfg.get("stealth", True):
            self._context.add_init_script(_stealth_script())
        log.debug("渲染线程 {} 已启动 {}", self.name, cfg.get("browser", "chromium"))

    def _render(self, request: Request) -> Response:
        cfg = self._config
        timeout_ms = float(cfg["timeout"]) * 1000
        page = self._context.new_page()
        try:
            nav = page.goto(
                request.url, timeout=timeout_ms, wait_until=cfg.get("wait_until", "load")
            )
            wait_for = request.wait_for or cfg.get("wait_for")
            if wait_for:
                page.wait_for_selector(wait_for, timeout=timeout_ms)
            render_time = (
                request.render_time
                if request.render_time is not None
                else cfg.get("render_time", 0)
            )
            if render_time:
                page.wait_for_timeout(float(render_time) * 1000)
            if callable(request.render_script):
                request.render_script(page)

            html = page.content()
            body = html.encode("utf-8")
            # 渲染这条路走不了流式（内容是从浏览器里取的，没有可以边读边停的字节流），
            # 但上限还是要认：否则 render=True 就成了 MAX_RESPONSE_SIZE 的一个后门。
            # 注意此时浏览器进程早已把整页装下了 —— 这一刀挡的是「把巨大的字符串
            # 交给用户的 parse()」，挡不住浏览器那边的内存
            check_size(body, page.url)
            cookies = {c["name"]: c["value"] for c in self._context.cookies()}
            status = nav.status if nav is not None else 200
            headers = dict(nav.headers) if nav is not None else {}
            headers.setdefault("content-type", "text/html; charset=utf-8")
            return Response(
                url=page.url,
                status_code=status,
                content=body,
                headers=headers,
                cookies=cookies,
                request=request,
                encoding="utf-8",
            )
        finally:
            page.close()

    def _teardown(self) -> None:
        for obj, method in (
            (self._context, "close"),
            (self._browser, "close"),
            (self._pw, "stop"),
        ):
            if obj is None:
                continue
            try:
                getattr(obj, method)()
            except Exception:
                log.debug("渲染资源清理异常", exc_info=True)
        self._context = self._browser = self._pw = None


def _maybe_block(route: Any) -> None:
    try:
        if route.request.resource_type in _BLOCKED_RESOURCES:
            route.abort()
        else:
            route.continue_()
    except Exception:  # 页面已关闭等
        pass


class _RenderPool:
    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._jobs: queue.Queue[_Job | None] = queue.Queue()
        self._workers: list[_RenderWorker] = []
        self._lock = threading.Lock()
        self._started = False

    def _ensure_started(self) -> None:
        with self._lock:
            if self._started:
                return
            size = max(1, int(self._config.get("pool_size", 1)))
            for i in range(size):
                worker = _RenderWorker(i, self._jobs, self._config)
                worker.start()
                self._workers.append(worker)
            self._started = True
            log.info("渲染池启动，{} 个浏览器", size)

    def submit(self, request: Request) -> Response:
        self._ensure_started()
        job = _Job(request)
        self._jobs.put(job)
        # 兜底：worker 被杀、浏览器卡死、或者哪条路径漏了 set()，
        # 都不该让调用者永远等下去。上限取渲染超时 + 余量 ——
        # 比渲染本身的超时短的话，正常的慢渲染会被误判成失败
        if not job.event.wait(timeout=float(self._config["timeout"]) * 2 + _WAIT_MARGIN):
            job.fail(RuntimeError("等待渲染结果超时，渲染线程可能已经不在了"))
        if job.error is not None:
            raise RequestError(f"渲染失败 {request.url}：{job.error!r}") from job.error
        assert job.response is not None
        return job.response

    def close(self) -> None:
        with self._lock:
            if not self._started:
                return
            for worker in self._workers:
                worker.stop()
            for _ in self._workers:
                self._jobs.put(None)
            for worker in self._workers:
                worker.join(timeout=10)
            self._workers.clear()
            self._started = False
            # worker 一看见停止位就退出，队列里剩下的任务不会有人再碰它们 ——
            # 而它们的调用线程正等在 job.event 上。不收尾的话那些线程永远醒不来：
            # 实测 1 个在渲染、4 个排队时关闭，4 个调用线程永久挂起
            self._drain()

    def _drain(self) -> None:
        """把队列里没人处理的任务收尾掉，让它们的调用者拿到一个失败。"""
        stranded = 0
        while True:
            try:
                job = self._jobs.get_nowait()
            except queue.Empty:
                break
            if job is None:
                continue
            job.fail(RuntimeError("渲染池已关闭，这个请求没能开始渲染"))
            stranded += 1
        if stranded:
            log.warning("渲染池关闭，{} 个排队中的请求未渲染（调用方会收到失败）", stranded)


class PlaywrightDownloader(Downloader):
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self._config = {**setting.WEBDRIVER, **(config or {})}
        self._pool: _RenderPool | None = None
        self._lock = threading.Lock()

    def download(self, request: Request) -> Response:
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    self._pool = _RenderPool(self._config)
        return self._pool.submit(request)

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None
