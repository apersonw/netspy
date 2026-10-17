"""基于 Playwright 的浏览器渲染下载器。

架构：``pool_size`` 个独立渲染线程，每个线程持有一个 chromium（Playwright 的 sync API
不能跨线程用）。工作线程把渲染任务丢进队列并阻塞等结果，因此 ``pool_size`` 就是真正的
并发浏览器上限；小于 ``SPIDER_THREAD_COUNT`` 时天然形成背压。

需要 ``pip install netspy[render] && playwright install chromium``。

``WEBDRIVER["engine"] = "patchright"`` 可选切换到 patchright（CDP 层补丁，避免用
``Runtime.enable`` 等探测点，需 ``pip install netspy[render-patchright]``）；只支持
chromium，配 firefox/webkit 会在构造期直接报错。
"""

from __future__ import annotations

import queue
import threading
from collections import OrderedDict
from collections.abc import Mapping
from functools import lru_cache
from http.cookiejar import CookieJar
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from netspy import setting
from netspy.exceptions import ConfigError, RequestError
from netspy.network.downloader._common import (
    check_size,
    pick_proxy,
    report_bad_proxy,
    report_good_proxy,
    report_proxy_failure,
)
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


def _import_sync_playwright(engine: str) -> Any:
    """按 `engine` 取 `sync_playwright`——playwright（默认）或 patchright。

    patchright 是打了 CDP 层探测点补丁的 Playwright fork（避免用
    `Runtime.enable`、去掉 `--enable-automation` 等默认参数），API 跟
    Playwright 完全一致，只是换了顶层包名。只支持 chromium 这一点在
    `PlaywrightDownloader.__init__` 里已经校验过，这里不用重复判断。
    """
    if engine == "patchright":
        try:
            from patchright.sync_api import sync_playwright as patchright_sync_playwright
        except ImportError as exc:  # pragma: no cover - 可选依赖
            raise ImportError(
                'engine="patchright" 需要 pip install "netspy[render-patchright]"'
            ) from exc
        return patchright_sync_playwright
    from playwright.sync_api import sync_playwright

    return sync_playwright


class _Job:
    __slots__ = ("error", "event", "proxy", "request", "response")

    def __init__(self, request: Request, proxy: str | None = None) -> None:
        self.request = request
        #: 调用方线程里用 `pick_proxy()` 定好的出口；None = 直连（只在没开代理池、
        #: 或显式允许直连时才可能是 None）
        self.proxy = proxy
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
        #: persistent profile 模式下用来拼每个线程独立的子目录——
        #: Chrome 不允许多进程 / 多实例共享同一份 profile
        self._index = index
        self._jobs = jobs
        self._config = config
        self._stop_event = threading.Event()
        self._pw: Any = None
        self._browser: Any = None
        self._context: Any = None
        #: 默认 context 用的是哪个出口（launch 时就定死的 `WEBDRIVER["proxy"]`，或 None）
        self._default_proxy: str | None = config.get("proxy") or None
        #: 其它出口各一个 context。Chromium 的代理是 context 级的，一个 context 只能
        #: 走一个出口，换代理就得换 context。有界 LRU，跟 HTTP 下载器的连接池缓存一个口径
        self._proxy_contexts: OrderedDict[str, Any] = OrderedDict()

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
                    job.response = self._render(job.request, job.proxy)
                except Exception as exc:
                    job.error = exc
                finally:
                    job.event.set()
        finally:
            self._teardown()

    def _ensure_browser(self) -> None:
        # ⚠️ 判「已经初始化过」不能看 self._browser：persistent profile 模式下
        # 根本不会有独立的 browser 对象（launch_persistent_context 直接给
        # context），self._browser 永远是 None——用它当判据的话，
        # _ensure_browser 会在每个任务上都重新跑一遍，等于每个请求重开一次浏览器
        if self._context is not None:
            return
        sync_playwright = _import_sync_playwright(self._config.get("engine", "playwright"))

        cfg = self._config
        self._pw = sync_playwright().start()
        browser_type = getattr(self._pw, cfg.get("browser", "chromium"))
        launch_kwargs: dict[str, Any] = {
            "headless": cfg["headless"],
            # 关掉「这是自动化」这条最直接的信号（Blink 的 AutomationControlled
            # 特性会让 navigator.webdriver 之类的属性走跟正常启动不同的代码路径）。
            # 无条件加：对正常渲染没有任何副作用，纯收益
            "args": ["--disable-blink-features=AutomationControlled"],
        }
        if cfg.get("proxy"):
            launch_kwargs["proxy"] = {"server": cfg["proxy"]}
        channel = cfg.get("channel")
        if channel:
            # 用系统里真实安装的 Chrome 而不是 Playwright 自带的 Chromium——
            # UA 版本号、TLS/HTTP2 指纹更贴近目标站预期的「真实 Chrome」画像
            launch_kwargs["channel"] = channel

        ua = cfg.get("user_agent")
        if not ua and setting.RANDOM_USER_AGENT:
            ua = get_random_user_agent()
        ctx_kwargs: dict[str, Any] = {}
        if ua:
            ctx_kwargs["user_agent"] = ua
        viewport = cfg.get("viewport")
        if viewport:
            ctx_kwargs["viewport"] = {"width": viewport[0], "height": viewport[1]}
        locale = cfg.get("locale")
        if locale:
            ctx_kwargs["locale"] = locale
        timezone_id = cfg.get("timezone_id")
        if timezone_id:
            ctx_kwargs["timezone_id"] = timezone_id

        user_data_dir = cfg.get("user_data_dir")
        if user_data_dir:
            # 持久化 profile：带着上次的 cookies / localStorage / 历史重新启动，
            # 而不是每次全新指纹。每个渲染线程独立一份子目录——见 __init__ 里
            # self._index 的注释。launch_persistent_context 直接返回 context，
            # 没有独立的 browser 句柄，self._browser 保持 None（_teardown 本来
            # 就会跳过它）
            profile_dir = str(Path(user_data_dir) / str(self._index))
            self._context = browser_type.launch_persistent_context(
                profile_dir, **launch_kwargs, **ctx_kwargs
            )
        else:
            self._browser = browser_type.launch(**launch_kwargs)
            self._context = self._browser.new_context(**ctx_kwargs)

        # 额外的代理 context 要和默认 context 是同一份画像（UA / 视口 / 时区……），
        # 只换出口——否则换个代理，指纹也跟着变了
        self._ctx_kwargs = ctx_kwargs
        self._prepare_context(self._context)
        log.debug("渲染线程 {} 已启动 {}", self.name, cfg.get("browser", "chromium"))

    def _prepare_context(self, context: Any) -> None:
        if not self._config.get("load_images", False):
            context.route("**/*", _maybe_block)
        if self._config.get("stealth", True):
            context.add_init_script(_stealth_script())

    def _context_for(self, proxy: str | None) -> Any:
        """这个出口对应的 context。

        **不能静默退回默认 context**：默认 context 要么直连、要么走 launch 时
        定死的那个代理——拿它去渲染一个本该走别的代理的请求，等于把请求从
        错误的出口（往往就是源 IP）打出去。拿不到对的 context 就抛错。
        """
        # None 走默认 context：配了固定代理时 pick_proxy() 永远会返回它，
        # 走到这里的 None 只可能是「没开代理池、本来就直连」
        if proxy is None or proxy == self._default_proxy:
            return self._context
        if self._browser is None:
            # persistent profile：只有 launch 时那一个 context，出口改不了
            raise RequestError(
                f"持久化 profile（WEBDRIVER['user_data_dir']）模式下浏览器的出口在启动时"
                f"就定死了（{self._default_proxy or '直连'}），没法按请求换成 {proxy}。"
                "要配合代理用，把固定代理写进 WEBDRIVER['proxy']"
            )
        context = self._proxy_contexts.get(proxy)
        if context is not None:
            self._proxy_contexts.move_to_end(proxy)
            return context
        context = self._new_context(proxy)
        self._proxy_contexts[proxy] = context
        limit = max(setting.SESSION_CACHE_SIZE, 1)
        while len(self._proxy_contexts) > limit:
            _, old = self._proxy_contexts.popitem(last=False)
            try:
                old.close()
            except Exception:
                log.debug("关闭换出的代理 context 失败", exc_info=True)
        return context

    def _new_context(self, proxy: str | None) -> Any:
        """跟默认 context 同一份画像的新 context。出口和默认 context 相同时不传
        proxy——Chromium 的新 context 会沿用 launch 时的浏览器级代理。"""
        kwargs = dict(self._ctx_kwargs)
        if proxy is not None and proxy != self._default_proxy:
            kwargs["proxy"] = {"server": proxy}
        context = self._browser.new_context(**kwargs)
        self._prepare_context(context)
        return context

    def _render(self, request: Request, proxy: str | None = None) -> Response:
        cfg = self._config
        timeout_ms = float(cfg["timeout"]) * 1000
        url, headers, cookies = _render_inputs(request)
        if cookies and self._browser is not None:
            # 带 cookie 的请求（账号池挂上来的登录态最典型）单独开一个 context，用完即关：
            # 共享 context 是同一个渲染线程上所有请求共用的 cookie jar，往里塞 A 账号的
            # cookie，下一个 B 账号的请求就带着 A 的登录态出去了；站点回写的 cookie 也一样
            context, owned = self._new_context(proxy), True
        else:
            # persistent profile 只有一个 context（它本来就是「一个固定身份」）
            context, owned = self._context_for(proxy), False
        try:
            if cookies:
                context.add_cookies(
                    [{"name": k, "value": str(v), "url": url} for k, v in cookies.items()]
                )
            page = context.new_page()
        except BaseException:
            if owned:
                context.close()
            raise
        try:
            if headers:
                page.set_extra_http_headers(headers)
            nav = page.goto(url, timeout=timeout_ms, wait_until=cfg.get("wait_until", "load"))
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
            resp_cookies = {c["name"]: c["value"] for c in context.cookies()}
            status = nav.status if nav is not None else 200
            resp_headers = dict(nav.headers) if nav is not None else {}
            resp_headers.setdefault("content-type", "text/html; charset=utf-8")
            return Response(
                url=page.url,
                status_code=status,
                content=body,
                headers=resp_headers,
                cookies=resp_cookies,
                request=request,
                encoding="utf-8",
            )
        finally:
            page.close()
            if owned:
                context.close()

    def _teardown(self) -> None:
        extra = [(ctx, "close") for ctx in self._proxy_contexts.values()]
        self._proxy_contexts.clear()
        for obj, method in (
            *extra,
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


#: 渲染没法照办的请求参数：浏览器导航只会发 GET，没有请求体
_BODY_KEYS = ("data", "json", "content", "files")


def _render_inputs(request: Request) -> tuple[str, dict[str, str], dict[str, Any]]:
    """把 `requests_kwargs` 翻成浏览器能用的 (url, 额外请求头, cookie)。

    原来渲染这条路**整个忽略 requests_kwargs**：账号池挂上来的登录 cookie 不发，
    页面回来是未登录态，`check_login` 判失败，健康的账号被当成失效拉黑 30 分钟——
    实测一个 render 请求就把整个账号池拉黑了。`params` 不拼进 URL，渲染的是另一个页面；
    POST 被静默当成 GET。照办不了的就明说，不能悄悄换一种请求发出去。
    """
    rk = request.requests_kwargs
    body = [k for k in _BODY_KEYS if rk.get(k) is not None]
    if request.method != "GET" or body:
        got = "、".join([request.method, *body])
        raise RequestError(
            f"渲染只能发不带请求体的 GET（收到 {got}）：{request.url}。"
            "需要 POST 的页面请去掉 render=True，或在 render_script 里自己提交表单"
        )
    url = request.url
    params = rk.get("params")
    if params:
        items = params.items() if isinstance(params, Mapping) else params
        parts = urlsplit(url)
        query = parse_qsl(parts.query, keep_blank_values=True) + [
            (str(k), str(v)) for k, v in items
        ]
        url = urlunsplit(parts._replace(query=urlencode(query)))
    # User-Agent 归 WEBDRIVER["user_agent"] 管：请求头里另塞一个，跟 navigator.userAgent
    # 对不上，比不伪装更容易被识别
    headers = {
        str(k): str(v)
        for k, v in (rk.get("headers") or {}).items()
        if str(k).lower() != "user-agent"
    }
    raw = rk.get("cookies") or {}
    if isinstance(raw, CookieJar):
        cookies: dict[str, Any] = {c.name: c.value for c in raw if c.value is not None}
    else:
        cookies = dict(raw)
    return url, headers, cookies


#: Chromium 网络错误里「确实是代理自己的错」的那几种：连不上代理、隧道建不起来
_PROXY_FAULT_MARKERS = (
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_AUTH",
    "ERR_NO_SUPPORTED_PROXIES",
)


def _report_render_proxy_failure(proxy: str, exc: BaseException) -> None:
    """渲染失败时按谁的错分流，跟 HTTP 下载器同一个口径：确定是代理坏了就拉黑，
    说不清的（页面超时、目标站慢）只记一次可疑。"""
    if any(marker in str(exc) for marker in _PROXY_FAULT_MARKERS):
        report_bad_proxy(proxy)
    else:
        report_proxy_failure(proxy, exc)


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
        #: 一旦关闭就永远是 True——见 `_ensure_started` 的说明
        self._closed = False

    def _ensure_started(self) -> None:
        """懒启动。**关闭过的池不会被重新点亮**：`PlaywrightDownloader.download()`
        允许在 `close()` 进行中读到旧的池对象（同一类「读完之后线程被切走」的
        并发窗口），那个调用最终会走到这里。`close()` 停 worker、join 它们
        可能耗时数秒，期间这里会卡在同一把锁上；`close()` 结束后若只看
        `self._started`（这时已经被置回 False），这里会误判成"还没启动过"，
        重新拉起一整套 worker 线程——而 `PlaywrightDownloader._pool` 早就
        指向别的（或空的）对象了，这批新起的浏览器进程从此没有任何代码路径
        会再管它们，泄漏到进程退出为止。实测复现：`close()` 完成后被这样
        重新启动，日志里"渲染池启动"打印了两遍。
        """
        with self._lock:
            if self._closed or self._started:
                return
            size = max(1, int(self._config.get("pool_size", 1)))
            for i in range(size):
                worker = _RenderWorker(i, self._jobs, self._config)
                worker.start()
                self._workers.append(worker)
            self._started = True
            log.info("渲染池启动，{} 个浏览器", size)

    def submit(self, request: Request, proxy: str | None = None) -> Response:
        self._ensure_started()
        job = _Job(request, proxy)
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
            if self._closed:
                return
            self._closed = True
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
        # 在构造期就校验，而不是等第一个渲染任务才发现——patchright 只打了
        # chromium 的补丁，配 firefox/webkit 会在第一次真正启动浏览器时才
        # 冒出一个跟"引擎选错了"完全无关的错误，不如现在直接说清楚
        engine = self._config.get("engine", "playwright")
        browser = self._config.get("browser", "chromium")
        if engine == "patchright" and browser != "chromium":
            raise ConfigError(
                f'engine="patchright" 只支持 chromium，当前 browser={browser!r}。'
                "把 browser 改成 chromium，或者把 engine 改回 playwright"
            )
        self._pool: _RenderPool | None = None
        self._lock = threading.Lock()

    def download(self, request: Request) -> Response:
        # 全程只读一次 self._pool 到局部变量再用它，不在函数末尾重新读一遍
        # 实例属性——调度器停工作线程用的是**有超时的** join，一个慢渲染
        # 请求的 worker 完全可能在下载器 close() 时还在跑。原来的写法是
        # `return self._pool.submit(request)`，在锁外重新读了一次 self._pool：
        # close() 恰好在这中间把它置 None 的话，会直接
        # AttributeError: 'NoneType' object has no attribute 'submit'。
        # 跟 proxy_pool.get_proxy_pool() 是同一类坑，实测复现过。
        pool = self._pool
        if pool is None:
            with self._lock:
                pool = self._pool
                if pool is None:
                    pool = _RenderPool(self._config)
                    self._pool = pool
        # 出口跟另外三个下载器走同一个判定：请求显式指定 > WEBDRIVER["proxy"] > 代理池。
        # 原来渲染这条路**根本不问代理池**——`PROXY_ENABLE=True` 时普通请求都走代理，
        # render=True 的请求却从源 IP 直连目标站，正是代理池要防的那件事，而且一声不吭。
        # 在调用方线程里取：池空时 pick_proxy 会等 PROXY_WAIT_TIMEOUT，不能让渲染线程陪着等
        rk = request.requests_kwargs
        explicit = rk.get("proxy") or rk.get("proxies") or self._config.get("proxy") or None
        proxy = pick_proxy(request, explicit)
        from_pool = proxy is not None and explicit is None
        try:
            response = pool.submit(request, proxy)
        except RequestError as exc:
            if from_pool:
                _report_render_proxy_failure(proxy, exc.__cause__ or exc)  # type: ignore[arg-type]
            raise
        if from_pool:
            report_good_proxy(proxy)  # type: ignore[arg-type]
        return response

    def close(self) -> None:
        # 置空和读取必须和 download() 共用同一把锁，理由同上
        with self._lock:
            pool = self._pool
            self._pool = None
        if pool is not None:
            pool.close()
