"""渲染请求要照办 `requests_kwargs`：cookie、请求头、params。

实测复现的真 bug：渲染下载器原来**整个忽略 requests_kwargs**。

- 账号池把登录 cookie 挂进 `requests_kwargs["cookies"]`，渲染根本不发 → 页面回来是
  未登录态 → `check_login` 判失败 → 健康账号被拉黑 30 分钟、换下一个、再被拉黑……
  实测**一个** render 请求就把账号池里两个号全拉黑了；
- `params` 不拼进 URL，渲染的是另一个页面，不报错；
- POST 被静默当成 GET 发出去。

真浏览器的用例验「服务端到底收到了什么」——参数拼对了不等于浏览器真发了。
"""

from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator
from http.cookiejar import Cookie, CookieJar
from typing import Any

import pytest

import netspy as mw
from netspy import setting
from netspy.exceptions import RequestError
from netspy.network.downloader._playwright import PlaywrightDownloader, _render_inputs
from netspy.network.request import Request


# ---------------------------------------------------------------- 翻译（不起浏览器）
def test_params_are_merged_into_the_url() -> None:
    url, _, _ = _render_inputs(Request("https://x.test/s?a=1", params={"q": "中 文", "p": 2}))
    assert url == "https://x.test/s?a=1&q=%E4%B8%AD+%E6%96%87&p=2"


def test_user_agent_header_is_left_to_the_browser_profile() -> None:
    """请求头里另塞一个 UA，会跟 navigator.userAgent 对不上。"""
    _, headers, _ = _render_inputs(
        Request("https://x.test/", headers={"Referer": "https://r/", "user-agent": "X"})
    )
    assert headers == {"Referer": "https://r/"}


@pytest.mark.parametrize(
    "kwargs",
    [{"method": "POST"}, {"data": {"a": 1}}, {"json": {"a": 1}}],
    ids=["post", "data", "json"],
)
def test_requests_a_browser_cannot_make_are_refused(kwargs: dict[str, Any]) -> None:
    """原来是静默当成 GET 发出去——换一种请求悄悄发出去比报错更糟。"""
    method = kwargs.pop("method", "GET")
    with pytest.raises(RequestError, match="GET"):
        _render_inputs(Request("https://x.test/", method, **kwargs))


def test_cookie_jar_is_accepted() -> None:
    jar = CookieJar()
    jar.set_cookie(
        Cookie(
            version=0,
            name="sid",
            value="v1",
            port=None,
            port_specified=False,
            domain="x.test",
            domain_specified=True,
            domain_initial_dot=False,
            path="/",
            path_specified=True,
            secure=False,
            expires=None,
            discard=False,
            comment=None,
            comment_url=None,
            rest={},
        )
    )
    _, _, cookies = _render_inputs(Request("https://x.test/", cookies=jar))
    assert cookies == {"sid": "v1"}


# ---------------------------------------------------------------- 真浏览器：服务端收到了什么
@pytest.fixture
def site() -> Iterator[tuple[str, dict[str, Any]]]:
    seen: dict[str, Any] = {}

    class Site(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            seen[self.path.split("?")[0]] = {
                "cookie": self.headers.get("Cookie") or "",
                "x-test": self.headers.get("X-Test"),
                "path": self.path,
            }
            logged_in = "sid=SECRET" in (self.headers.get("Cookie") or "")
            body = b"welcome back" if logged_in else b"please login"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Site)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}", seen
    server.shutdown()


@pytest.fixture
def renderer(monkeypatch: pytest.MonkeyPatch) -> Iterator[PlaywrightDownloader]:
    pytest.importorskip("playwright")
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    dl = PlaywrightDownloader({"headless": True, "timeout": 20, "pool_size": 1})
    yield dl
    dl.close()


@pytest.mark.render
def test_real_browser_sends_cookies_headers_and_params(
    site: tuple[str, dict[str, Any]], renderer: PlaywrightDownloader
) -> None:
    base, seen = site
    renderer.download(
        Request(
            f"{base}/a",
            render=True,
            cookies={"sid": "SECRET"},
            headers={"X-Test": "yes"},
            params={"q": "1"},
        )
    )
    assert "sid=SECRET" in seen["/a"]["cookie"]
    assert seen["/a"]["x-test"] == "yes"
    assert seen["/a"]["path"] == "/a?q=1"


@pytest.mark.render
def test_real_browser_does_not_leak_one_requests_cookies_into_the_next(
    site: tuple[str, dict[str, Any]], renderer: PlaywrightDownloader
) -> None:
    """账号池里 A 号的 cookie 不能出现在下一个请求上——那等于拿 A 号的登录态去发 B 号的请求。

    同一个渲染线程上的请求原来共用一个 context（也就共用一个 cookie jar），
    往里塞 cookie 就会跟着后面的请求一起出去。
    """
    base, seen = site
    renderer.download(Request(f"{base}/alice", render=True, cookies={"sid": "ALICE"}))
    renderer.download(Request(f"{base}/anonymous", render=True))
    renderer.download(Request(f"{base}/bob", render=True, cookies={"uid": "BOB"}))
    assert "ALICE" not in seen["/anonymous"]["cookie"]
    assert "ALICE" not in seen["/bob"]["cookie"] and "uid=BOB" in seen["/bob"]["cookie"]


@pytest.mark.render
def test_user_pool_accounts_survive_a_rendered_request(
    site: tuple[str, dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    """端到端：原来一个 render 请求就把账号池里的号全部拉黑了。"""
    pytest.importorskip("playwright")
    for name, value in {
        "DONE_CHECK_INTERVAL": 0.1,
        "DONE_CHECK_TIMES": 2,
        "RANDOM_USER_AGENT": False,
        "SPIDER_MAX_RETRY_TIMES": 1,
        "USER_POOL_WAIT_TIMEOUT": 0.3,
    }.items():
        monkeypatch.setattr(setting, name, value)
    base, _ = site
    pool = mw.LocalUserPool(
        [
            {"username": "alice", "cookies": {"sid": "SECRET"}},
            {"username": "bob", "cookies": {"sid": "SECRET"}},
        ]
    )
    pages: list[str] = []

    class S(mw.AirSpider):
        def user_pool(self) -> mw.LocalUserPool:
            return pool

        def check_login(self, response: mw.Response) -> bool:
            return "welcome" in response.text

        def start_requests(self) -> Iterator[mw.Request]:
            yield mw.Request(f"{base}/r", render=True, callback=self.parse)

        def parse(self, request: mw.Request, response: mw.Response) -> None:
            pages.append(response.text)

    S(thread_count=1).start()
    assert pages and "welcome" in pages[0], "渲染请求没带上账号的 cookie"
    assert not pool._blocked, f"健康账号被当成失效拉黑了：{sorted(pool._blocked)}"
