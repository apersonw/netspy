from __future__ import annotations

import re
import threading
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from netspy import AirSpider, Request, setting
from netspy.exceptions import RequestError
from netspy.network.downloader import close_default_downloaders
from netspy.network.downloader._playwright import PlaywrightDownloader

pytestmark = pytest.mark.render

JS_PAGE = """<html><body>
<div id="app">loading</div>
<script>
setTimeout(function () {
  document.getElementById('app').textContent = 'rendered';
  var ul = document.createElement('ul');
  ['A', 'B', 'C'].forEach(function (t) {
    var li = document.createElement('li');
    li.className = 'row';
    li.textContent = t;
    ul.appendChild(li);
  });
  document.body.appendChild(ul);
}, 60);
</script></body></html>"""


@pytest.fixture(autouse=True)
def _cleanup_registry() -> Iterator[None]:
    yield
    close_default_downloaders()


@pytest.fixture(scope="session")
def chromium_ok() -> bool:
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            browser.close()
    except Exception:
        return False
    return True


@pytest.fixture(autouse=True)
def _require_chromium(chromium_ok: bool) -> None:
    if not chromium_ok:
        pytest.skip("chromium 不可用（playwright install chromium）")


@pytest.fixture(scope="module")
def pool(chromium_ok: bool) -> Iterator[PlaywrightDownloader]:
    if not chromium_ok:
        pytest.skip("chromium 不可用")
    downloader = PlaywrightDownloader({"pool_size": 2})
    yield downloader
    downloader.close()


@pytest.fixture
def js_url(httpserver: HTTPServer) -> str:
    httpserver.expect_request("/js").respond_with_data(
        JS_PAGE, content_type="text/html; charset=utf-8"
    )
    return httpserver.url_for("/js")


# ----------------------------------------------------------------------
def test_render_executes_js(pool: PlaywrightDownloader, js_url: str) -> None:
    resp = pool.download(Request(js_url, render=True, wait_for="ul li.row"))
    assert resp.status_code == 200
    assert "rendered" in resp.text
    assert resp.css("li.row::text").getall() == ["A", "B", "C"]
    assert resp.request is not None


def test_without_render_js_is_not_executed(js_url: str) -> None:
    resp = Request(js_url).download()  # httpx，不渲染
    assert "loading" in resp.text
    assert resp.css("li.row::text").getall() == []


def test_render_time_waits_for_late_mutation(
    pool: PlaywrightDownloader, httpserver: HTTPServer
) -> None:
    httpserver.expect_request("/late").respond_with_data(
        "<html><body><p id=x>early</p><script>"
        "setTimeout(function(){document.getElementById('x').textContent='late';}, 250);"
        "</script></body></html>",
        content_type="text/html",
    )
    url = httpserver.url_for("/late")
    assert "early" in pool.download(Request(url, render=True, render_time=0)).text
    assert "late" in pool.download(Request(url, render=True, render_time=0.6)).text


def test_render_script_runs_in_browser(pool: PlaywrightDownloader, httpserver: HTTPServer) -> None:
    httpserver.expect_request("/btn").respond_with_data(
        '<html><body><button id="b" '
        "onclick=\"document.body.setAttribute('data-clicked','yes')\">go</button>"
        "</body></html>",
        content_type="text/html",
    )

    def script(page: Any) -> None:
        page.click("#b")

    resp = pool.download(Request(httpserver.url_for("/btn"), render=True, render_script=script))
    assert 'data-clicked="yes"' in resp.text


def test_error_becomes_request_error(pool: PlaywrightDownloader) -> None:
    with pytest.raises(RequestError, match="渲染失败"):
        pool.download(Request("http://127.0.0.1:1/nope", render=True))


def test_pool_close_stops_render_threads(js_url: str) -> None:
    baseline = threading.active_count()
    downloader = PlaywrightDownloader({"pool_size": 2})
    downloader.download(Request(js_url, render=True))
    assert threading.active_count() >= baseline + 2

    downloader.close()
    deadline = time.monotonic() + 5
    while threading.active_count() > baseline and time.monotonic() < deadline:
        time.sleep(0.05)
    assert threading.active_count() == baseline


def test_stealth_script_covers_known_evasions() -> None:
    """不需要真浏览器的快速检查——防止升级 playwright-stealth 或改配置时
    悄悄漏掉这几个关键 evasion（webdriver 隐藏、WebGL 伪装、语言覆盖）。"""
    from netspy.network.downloader._playwright import _stealth_script

    script = _stealth_script()
    assert "webdriver" in script
    assert "zh-CN" in script
    # 默认的 WebGL 伪装目标——防止这块 evasion 被悄悄关掉或改错
    assert "Intel" in script


def test_stealth_hides_automation_and_webgl_signals(
    pool: PlaywrightDownloader, httpserver: HTTPServer
) -> None:
    """真实浏览器回归测试：验证反检测补丁真的生效，不是只测脚本字符串里有没有
    这几个词。

    实测过没打补丁的裸 headless Chromium 是什么样：``navigator.webdriver``
    是 ``True``、WebGL vendor/renderer 会暴露 ``SwiftShader``（软件渲染，
    没有真实 GPU 的容器里跑 headless Chrome 的典型指纹）——这两个信号是这次
    从 stealth.min.js 换成完整版 ``playwright-stealth`` 的直接理由，不是理论
    风险，是在小红书上真实撞到过的拦截原因。
    """
    httpserver.expect_request("/probe").respond_with_data(
        "<html><body></body></html>", content_type="text/html"
    )

    def script(page: Any) -> None:
        page.evaluate("""() => {
            const canvas = document.createElement('canvas');
            const gl = canvas.getContext('webgl');
            const dbg = gl && gl.getExtension('WEBGL_debug_renderer_info');
            const renderer = dbg ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : 'no-webgl';
            document.body.innerHTML =
                '<div id="webdriver">' + navigator.webdriver + '</div>' +
                '<div id="renderer">' + renderer + '</div>' +
                '<div id="lang">' + navigator.languages.join(',') + '</div>';
        }""")

    resp = pool.download(Request(httpserver.url_for("/probe"), render=True, render_script=script))
    assert '<div id="webdriver">false</div>' in resp.text
    assert "SwiftShader" not in resp.text
    assert "Intel" in resp.text
    assert '<div id="lang">zh-CN,zh</div>' in resp.text


def test_locale_and_timezone_take_effect_in_real_browser(
    chromium_ok: bool, httpserver: HTTPServer
) -> None:
    """真实浏览器回归测试：`locale` / `timezone_id` 配置项真的生效。

    这两个是 Playwright `new_context()` 原生支持的参数，不是靠注入 JS 伪造——
    但「原生支持」不代表「配置项传对了地方」，值得跟其它反检测参数一样
    用真浏览器验一遍，而不是只信任 mock 断言的调用参数。

    ⚠️ 选的时区必须**跟本机系统时区不一样**：第一版这里用了 Asia/Shanghai，
    在时区本来就是 CST 的机器上，就算完全不传 `timezone_id`，headless
    Chromium 默认继承的也是宿主系统时区——断言照样通过，测的其实是宿主机
    而不是这个参数。改用 Pacific/Kiritimati（UTC+14，全世界几乎不会有开发
    / CI 机器把系统时区设成这个）并显式断言它确实跟宿主时区不同，把这条
    测试前提钉死，不依赖"运气好机器时区不是这个"。
    """
    if not chromium_ok:
        pytest.skip("chromium 不可用")
    target_tz = "Pacific/Kiritimati"
    host_tz = datetime.now().astimezone().tzinfo
    assert host_tz is not None and str(host_tz) != target_tz, (
        f"宿主机时区恰好也是 {target_tz}，这条测试测不出参数有没有真的生效，换一个更不常见的时区"
    )
    httpserver.expect_request("/tz").respond_with_data(
        "<html><body></body></html>", content_type="text/html"
    )

    def script(page: Any) -> None:
        page.evaluate("""() => {
            document.body.innerHTML =
                '<div id="tz">' + Intl.DateTimeFormat().resolvedOptions().timeZone + '</div>' +
                '<div id="lang">' + navigator.language + '</div>';
        }""")

    # stealth=False 是故意的：默认的 stealth 脚本会把 navigator.languages 硬编码
    # 覆盖成 zh-CN/zh（见上面的 test_stealth_hides_automation_and_webgl_signals），
    # 如果这里也是 zh-CN，测试通过可能是那份硬编码生效了，跟这里真正要验证的
    # locale 参数无关——关掉 stealth 才能确认断言真的来自 new_context(locale=...)
    downloader = PlaywrightDownloader(
        {"locale": "zh-CN", "timezone_id": target_tz, "stealth": False}
    )
    try:
        resp = downloader.download(
            Request(httpserver.url_for("/tz"), render=True, render_script=script)
        )
    finally:
        downloader.close()
    assert f'<div id="tz">{target_tz}</div>' in resp.text
    assert '<div id="lang">zh-CN</div>' in resp.text


def test_user_data_dir_persists_cookies_across_restarts(
    chromium_ok: bool, httpserver: HTTPServer, tmp_path: Any
) -> None:
    """真实浏览器回归测试：`user_data_dir` 配置项真的让 cookie 跨重启存活——
    这是 persistent profile 唯一真正要验证的行为，其余（channel/locale 之类
    有没有一起传对）已经在 mock 级测试里验过参数拼接。

    第一个下载器种一个 cookie 然后关闭（模拟一次爬虫运行结束）；
    第二个下载器指向**同一个** `user_data_dir` 重新起来，读同一个页面——
    如果 cookie 还在，说明 profile 真的被复用了，不是每次都是全新指纹。

    ⚠️ cookie 必须带 `Max-Age`：第一版这里种的是没有 `Max-Age` / `Expires` 的
    会话 cookie，实测哪怕 `user_data_dir` 完全配对，会话 cookie 依然不会跨
    浏览器重启存活——这是**真实浏览器自己的行为**（真 Chrome 完全退出重开
    也一样清会话 cookie），不是 persistent profile 没生效，只是选错了要
    验证的信号。换成带 `Max-Age` 的持久 cookie 才测得出 profile 有没有真的
    被复用。
    """
    if not chromium_ok:
        pytest.skip("chromium 不可用")
    httpserver.expect_request("/set-cookie").respond_with_data(
        "<html><body></body></html>",
        content_type="text/html",
        headers={"Set-Cookie": "session=persisted-value; Path=/; Max-Age=86400"},
    )
    httpserver.expect_request("/read-cookie").respond_with_data(
        "<html><body></body></html>", content_type="text/html"
    )

    def read_cookie_script(page: Any) -> None:
        page.evaluate(
            "() => { document.body.innerHTML = "
            "'<div id=\"cookie\">' + document.cookie + '</div>'; }"
        )

    profile_dir = str(tmp_path / "profile")

    first = PlaywrightDownloader({"user_data_dir": profile_dir, "pool_size": 1})
    try:
        first.download(Request(httpserver.url_for("/set-cookie"), render=True))
    finally:
        first.close()

    second = PlaywrightDownloader({"user_data_dir": profile_dir, "pool_size": 1})
    try:
        resp = second.download(
            Request(
                httpserver.url_for("/read-cookie"), render=True, render_script=read_cookie_script
            )
        )
    finally:
        second.close()

    assert "session=persisted-value" in resp.text


def test_humanize_helpers_produce_spread_out_real_events(
    pool: PlaywrightDownloader, httpserver: HTTPServer
) -> None:
    """真实浏览器回归测试：`human_move` / `human_type` 在真页面上真的触发了
    分散的 mousemove / keydown 事件，不是纯 mock 断言调用了几次那么简单——
    这里用页面自己记录的事件时间戳来证明「确实不是零间隔的单帧动作」。
    """
    httpserver.expect_request("/human").respond_with_data(
        '<html><body><input id="q" /><script>'
        "window.__events = [];"
        "document.addEventListener('mousemove', () => window.__events.push(performance.now()));"
        "document.getElementById('q').addEventListener('keydown', "
        "() => window.__keydowns = (window.__keydowns || 0) + 1);"
        "</script></body></html>",
        content_type="text/html",
    )

    def script(page: Any) -> None:
        from netspy.network.downloader._humanize import human_move, human_type

        human_move(page, 200, 200, steps=10)
        human_type(page, "#q", "hi", delay_range=(0.02, 0.05))
        move_count = page.evaluate("window.__events.length")
        span_ms = page.evaluate(
            "window.__events.length > 1 ? "
            "window.__events[window.__events.length-1] - window.__events[0] : 0"
        )
        keydowns = page.evaluate("window.__keydowns || 0")
        page.evaluate(
            f"document.body.setAttribute('data-moves', '{move_count}');"
            f"document.body.setAttribute('data-span', '{span_ms}');"
            f"document.body.setAttribute('data-keydowns', '{keydowns}');"
        )

    resp = pool.download(Request(httpserver.url_for("/human"), render=True, render_script=script))
    # 数量下限用 >=10 而不是卡死等于 11——真实浏览器可能因为悬停 / 焦点变化
    # 多触发一两次合成的 mousemove，不是 human_move 自己的行为，不该被卡死的
    # 精确值判成失败；但至少要有 10 步 + 收尾那一次，证明真的是分步走过去的
    moves = int(re.search(r'data-moves="(\d+)"', resp.text).group(1))  # type: ignore[union-attr]
    assert moves >= 11, f"只触发了 {moves} 次 mousemove，应该至少有 11 次（10 步+收尾）"
    assert 'data-keydowns="2"' in resp.text  # "hi" 两个字符
    # 真实事件之间确实拉开了时间——不是全部在同一帧内瞬间发生
    assert 'data-span="0"' not in resp.text


def test_spider_with_render_end_to_end(
    chromium_ok: bool, httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not chromium_ok:
        pytest.skip("chromium 不可用")
    monkeypatch.setattr(setting, "DONE_CHECK_INTERVAL", 0.05)
    monkeypatch.setattr(setting, "DONE_CHECK_TIMES", 2)
    monkeypatch.setattr(setting, "BUFFER_FLUSH_INTERVAL", 0.02)
    monkeypatch.setattr(setting, "WEBDRIVER", {**setting.WEBDRIVER, "pool_size": 2})
    httpserver.expect_request("/js").respond_with_data(
        JS_PAGE, content_type="text/html; charset=utf-8"
    )
    base = httpserver.url_for("/js")

    class S(AirSpider):
        def start_requests(self) -> Iterator[Request]:
            for i in range(3):
                yield Request(f"{base}?i={i}", render=True, wait_for="li.row", callback=self.parse)

        def parse(self, request: Request, response: Any) -> Iterator[Any]:
            for text in response.css("li.row::text").getall():
                yield {"v": text}

    items: list[Any] = []
    S(item_handler=items.extend, thread_count=3).start()
    assert sorted(d["v"] for d in items) == ["A", "A", "A", "B", "B", "B", "C", "C", "C"]
