"""examples/ 的防腐测试。

例子会腐烂 —— 站点改版、API 变更都会让它悄悄失效，而没人会发现。
这里做两层：结构检查（不联网，进 CI）+ 真跑一遍（联网，标记为 network 不进 CI）。
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def test_example_file_exists() -> None:
    assert (EXAMPLES / "books_toscrape.py").is_file()
    assert (EXAMPLES / "README.md").is_file()


def test_example_imports_and_defines_spider() -> None:
    """不联网：确认例子能 import、类结构没被改坏。"""
    sys.path.insert(0, str(EXAMPLES))
    try:
        import books_toscrape as ex
    finally:
        sys.path.remove(str(EXAMPLES))

    import netspy as ns

    assert issubclass(ex.BookSpider, ns.AirSpider)
    assert issubclass(ex.BookItem, ns.Item)
    assert ex.BookItem.__table_name__ == "books"
    assert ex.BookItem.__unique_key__ == ["url"]
    for hook in ("start_requests", "parse_list", "parse_book"):
        assert callable(getattr(ex.BookSpider, hook)), hook


@pytest.mark.network
def test_example_actually_crawls() -> None:
    """联网真跑一遍 —— 站点改版导致选择器失效时，只有这条能发现。"""
    sys.path.insert(0, str(EXAMPLES))
    try:
        import books_toscrape as ex
    finally:
        sys.path.remove(str(EXAMPLES))

    from netspy import setting
    from netspy.utils import log

    setting.ITEM_PIPELINES = []
    setting.LOG_LEVEL = "CRITICAL"
    setting.DOWNLOAD_DELAY = 0.2
    log.configure()

    got: list[object] = []
    original = ex.BookSpider.parse_book

    def capture(self, request, response):  # type: ignore[no-untyped-def]
        for item in original(self, request, response) or ():
            got.append(item)
            yield item

    monkey = type("S", (ex.BookSpider,), {"parse_book": capture})
    monkey.MAX_LIST_PAGES = 1
    ex.MAX_LIST_PAGES = 1
    monkey().start()

    assert got, "例子没抓到任何条目 —— 站点改版了？"
    first = got[0]
    assert first.title and first.price, f"字段抽取失效：{first.to_dict()}"
