"""`_RenderWorker._ensure_browser()` 传给 launch() / new_context() 的参数。

不需要真浏览器：桩一个假的 `sync_playwright()`，只验证参数怎么被拼出来的——
真实浏览器下这些参数是否真的生效，由 test_render.py 里带 @pytest.mark.render
的用例另外验证（起真 chromium，读 navigator.webdriver / 时区等）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from netspy.network.downloader import _playwright as pw


class _FakeContext:
    def __init__(self) -> None:
        self.route = mock.Mock()
        self.add_init_script = mock.Mock()


class _FakeBrowser:
    def __init__(self) -> None:
        self.new_context_calls: list[dict[str, Any]] = []
        self.context = _FakeContext()

    def new_context(self, **kwargs: Any) -> _FakeContext:
        self.new_context_calls.append(kwargs)
        return self.context


class _FakeBrowserType:
    def __init__(self) -> None:
        self.launch_calls: list[dict[str, Any]] = []
        self.persistent_context_calls: list[tuple[str, dict[str, Any]]] = []
        self.browser = _FakeBrowser()
        self.persistent_context = _FakeContext()

    def launch(self, **kwargs: Any) -> _FakeBrowser:
        self.launch_calls.append(kwargs)
        return self.browser

    def launch_persistent_context(self, user_data_dir: str, **kwargs: Any) -> _FakeContext:
        self.persistent_context_calls.append((user_data_dir, kwargs))
        return self.persistent_context


class _FakePlaywright:
    def __init__(self) -> None:
        self.chromium = _FakeBrowserType()

    def stop(self) -> None:
        pass


@pytest.fixture
def fake_pw(monkeypatch: pytest.MonkeyPatch) -> _FakePlaywright:
    instance = _FakePlaywright()

    class _FakeSyncPlaywrightCtx:
        def start(self) -> _FakePlaywright:
            return instance

    monkeypatch.setattr("playwright.sync_api.sync_playwright", lambda: _FakeSyncPlaywrightCtx())
    return instance


def _worker(config: dict[str, Any]) -> pw._RenderWorker:
    import queue

    return pw._RenderWorker(0, queue.Queue(), config)


def test_automation_controlled_flag_always_disabled(fake_pw: _FakePlaywright) -> None:
    """无条件关掉 AutomationControlled——对正常渲染没有副作用，纯收益。"""
    worker = _worker({"headless": True})
    worker._ensure_browser()
    launch_kwargs = fake_pw.chromium.launch_calls[0]
    assert launch_kwargs["args"] == ["--disable-blink-features=AutomationControlled"]


def test_channel_not_passed_by_default(fake_pw: _FakePlaywright) -> None:
    """默认不设 channel：保持用 Playwright 自带的 Chromium，不影响现有用户。"""
    worker = _worker({"headless": True})
    worker._ensure_browser()
    assert "channel" not in fake_pw.chromium.launch_calls[0]


def test_channel_is_passed_when_configured(fake_pw: _FakePlaywright) -> None:
    worker = _worker({"headless": True, "channel": "chrome"})
    worker._ensure_browser()
    assert fake_pw.chromium.launch_calls[0]["channel"] == "chrome"


def test_locale_and_timezone_not_passed_by_default(fake_pw: _FakePlaywright) -> None:
    worker = _worker({"headless": True})
    worker._ensure_browser()
    ctx_kwargs = fake_pw.chromium.browser.new_context_calls[0]
    assert "locale" not in ctx_kwargs
    assert "timezone_id" not in ctx_kwargs


def test_locale_and_timezone_are_passed_when_configured(fake_pw: _FakePlaywright) -> None:
    worker = _worker({"headless": True, "locale": "zh-CN", "timezone_id": "Asia/Shanghai"})
    worker._ensure_browser()
    ctx_kwargs = fake_pw.chromium.browser.new_context_calls[0]
    assert ctx_kwargs["locale"] == "zh-CN"
    assert ctx_kwargs["timezone_id"] == "Asia/Shanghai"


# ======================================================================
# engine="patchright"
# ======================================================================
def test_patchright_engine_rejects_non_chromium_at_construction() -> None:
    """在构造期就报错，而不是等第一个渲染任务才发现——patchright 只打了
    chromium 的补丁，晚报错会冒出一个跟「引擎选错了」完全无关的错误。"""
    from netspy.exceptions import ConfigError
    from netspy.network.downloader._playwright import PlaywrightDownloader

    with pytest.raises(ConfigError, match="patchright"):
        PlaywrightDownloader({"engine": "patchright", "browser": "firefox"})


def test_patchright_engine_accepts_chromium() -> None:
    from netspy.network.downloader._playwright import PlaywrightDownloader

    PlaywrightDownloader({"engine": "patchright", "browser": "chromium"})  # 不该抛


def test_default_engine_uses_playwright_sync_api(fake_pw: _FakePlaywright) -> None:
    worker = _worker({"headless": True})
    worker._ensure_browser()
    # fake_pw 桩的是 playwright.sync_api.sync_playwright；能走到这里、
    # 且 self._pw 就是桩出来的实例，说明默认引擎确实用的是 playwright
    assert worker._pw is fake_pw


def test_patchright_engine_uses_patchright_sync_api(monkeypatch: pytest.MonkeyPatch) -> None:
    """`engine="patchright"` 时必须走 `patchright.sync_api`，不能悄悄退回 playwright。"""
    patchright_instance = _FakePlaywright()

    class _FakeSyncPlaywrightCtx:
        def start(self) -> _FakePlaywright:
            return patchright_instance

    # 故意不桩 playwright.sync_api：如果实现悄悄走了 playwright 分支，
    # 这里会因为用的是真 playwright 而不是这个桩对象，断言失败
    monkeypatch.setattr("patchright.sync_api.sync_playwright", lambda: _FakeSyncPlaywrightCtx())
    worker = _worker({"headless": True, "engine": "patchright"})
    worker._ensure_browser()
    assert worker._pw is patchright_instance


def test_patchright_engine_missing_package_gives_actionable_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """没装 patchright 时要给出「装哪个 extra」的提示，不是裸 ModuleNotFoundError。"""
    import sys

    monkeypatch.setitem(sys.modules, "patchright", None)  # 强制 import 失败
    monkeypatch.setitem(sys.modules, "patchright.sync_api", None)
    worker = _worker({"headless": True, "engine": "patchright"})
    with pytest.raises(ImportError, match="render-patchright"):
        worker._ensure_browser()


# ======================================================================
# user_data_dir（persistent profile）
# ======================================================================
def test_user_data_dir_not_used_by_default(fake_pw: _FakePlaywright) -> None:
    """默认不设 user_data_dir：走原来的临时 profile，不影响现有用户。"""
    worker = _worker({"headless": True})
    worker._ensure_browser()
    assert fake_pw.chromium.persistent_context_calls == []
    assert len(fake_pw.chromium.launch_calls) == 1


def test_user_data_dir_uses_persistent_context_with_per_worker_subdir(
    fake_pw: _FakePlaywright,
) -> None:
    """设了 user_data_dir 就该走 launch_persistent_context，且路径按
    worker 的 index 拼子目录——Chrome 不允许多实例共享同一份 profile，
    pool_size > 1 时几个渲染线程要各用各的。"""
    import queue

    worker = pw._RenderWorker(3, queue.Queue(), {"headless": True, "user_data_dir": "/tmp/prof"})
    worker._ensure_browser()

    assert fake_pw.chromium.launch_calls == [], "走了 persistent context 就不该再调 launch()"
    assert len(fake_pw.chromium.persistent_context_calls) == 1
    profile_dir, kwargs = fake_pw.chromium.persistent_context_calls[0]
    assert profile_dir == str(Path("/tmp/prof") / "3")
    assert kwargs["headless"] is True
    assert worker._browser is None, "persistent context 模式下不该有独立的 browser 对象"
    assert worker._context is fake_pw.chromium.persistent_context


def test_user_data_dir_merges_launch_and_context_kwargs(fake_pw: _FakePlaywright) -> None:
    """channel / locale / timezone_id 这些参数在 persistent 模式下也要生效，
    不能因为换了条初始化路径就漏掉。"""
    worker = _worker(
        {
            "headless": True,
            "user_data_dir": "/tmp/prof",
            "channel": "chrome",
            "locale": "zh-CN",
            "timezone_id": "Asia/Shanghai",
        }
    )
    worker._ensure_browser()
    _, kwargs = fake_pw.chromium.persistent_context_calls[0]
    assert kwargs["channel"] == "chrome"
    assert kwargs["locale"] == "zh-CN"
    assert kwargs["timezone_id"] == "Asia/Shanghai"


def test_ensure_browser_is_idempotent_in_persistent_mode(fake_pw: _FakePlaywright) -> None:
    """回归测试：判断「已经初始化过」不能看 self._browser。

    这是实测复现出来的真 bug——persistent profile 模式下 launch_persistent_context
    直接给 context，根本不会有独立的 browser 对象，self._browser 永远是 None。
    旧的守卫写的是 `if self._browser is not None: return`，在这个模式下这个条件
    永远是 False，_ensure_browser 会在**每一个渲染任务**上都重新跑一遍——
    等于每个请求都重开一次浏览器，而不是复用同一个持久化 profile。
    """
    worker = _worker({"headless": True, "user_data_dir": "/tmp/prof"})
    worker._ensure_browser()
    worker._ensure_browser()  # 模拟第二个任务
    worker._ensure_browser()  # 模拟第三个任务
    assert len(fake_pw.chromium.persistent_context_calls) == 1, (
        f"launch_persistent_context 被调了 {len(fake_pw.chromium.persistent_context_calls)} 次，"
        "应该只在第一次任务时初始化一次"
    )
