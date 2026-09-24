"""`engine="patchright"` 的真实浏览器验证——跑在独立子进程里，不跟其它用例共用进程。

⚠️ **为什么不能跟 test_render.py 放一起**：实测撞到过的真冲突——patchright 和
vanilla playwright 各自内部都用 asyncio 管理一条到浏览器驱动进程的连接，
`test_render.py` 里 `pool` 这个 module 级 fixture 会让一个 vanilla Chromium
在整个测试模块生命周期内保持存活；这时候在**同一个进程**里再启动一个
patchright 浏览器，`page.goto()` 会稳定卡满 30 秒超时（3/3 次复现，不是偶发）。
这不是 Netspy 的 bug——两边都是按文档在用官方 API，冲突在两个独立维护的
三方库各自的 asyncio/驱动进程管理内部，Netspy 管不到也不该试图去修。

真实部署不会撞上这个问题：一个进程的 `WEBDRIVER["engine"]` 只会是一个值，
不会同时跑两种引擎的浏览器。这里用子进程隔离纯粹是为了让测试环境干净，
不代表生产环境需要类似的隔离。
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.render

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _run_in_subprocess(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=str(_REPO_ROOT),
    )


def test_patchright_engine_is_really_used_not_silently_falling_back() -> None:
    """`engine="patchright"` 真的走的是 patchright，不是配置传对了却在某个
    分支悄悄退回 vanilla playwright。

    ⚠️ 一开始这里断言的是 `navigator.webdriver === false`，但那测不出
    patchright 有没有真的生效——Phase A 无条件加的
    `--disable-blink-features=AutomationControlled` launch 参数，**单靠
    vanilla playwright** 就已经能让 `navigator.webdriver` 变成 `false`
    （实测验证过），跟走不走 patchright 无关，这样写等于什么都没测出来。

    改用 patchright 自己文档写明的副作用：它为了避免 `Console.enable`
    这个 CDP 探测点，**页面的 console 消息不会再冒泡到 `page.on("console")`**
    （实测对比过：同一段 `console.log`，vanilla playwright 能收到，
    patchright 收不到）。这是 vanilla playwright + 那个 launch 参数复现不出来的
    行为，才是真正能证明「这次调用确实经过了 patchright 那条代码路径」的信号。
    """
    try:
        import patchright  # noqa: F401
    except ImportError:
        pytest.skip('未安装 patchright（pip install "netspy[render-patchright]"）')

    script = textwrap.dedent(
        """
        from pytest_httpserver import HTTPServer
        from netspy import Request
        from netspy.network.downloader._playwright import PlaywrightDownloader

        httpserver = HTTPServer()
        httpserver.start()
        try:
            httpserver.expect_request("/pr").respond_with_data(
                "<html><body><script>console.log('hello-from-page')</script></body></html>",
                content_type="text/html",
            )

            console_messages = []

            def script(page):
                page.on("console", lambda msg: console_messages.append(msg.text))
                page.reload()
                page.wait_for_timeout(200)

            downloader = PlaywrightDownloader({"engine": "patchright"})
            try:
                downloader.download(
                    Request(httpserver.url_for("/pr"), render=True, render_script=script)
                )
            finally:
                downloader.close()
        finally:
            httpserver.stop()

        assert console_messages == [], f"收到了不该收到的 console 消息：{console_messages}"
        print("PATCHRIGHT_OK")
        """
    )
    result = _run_in_subprocess(script)
    assert result.returncode == 0, f"子进程失败：\nstdout={result.stdout}\nstderr={result.stderr}"
    assert "PATCHRIGHT_OK" in result.stdout
