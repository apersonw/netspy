from __future__ import annotations

import ast
import importlib
import json
import sys
from pathlib import Path
from typing import Any

import pytest
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner

import netspy as ns
from netspy import setting
from netspy.commands import create as gen
from netspy.commands.cmdline import app
from netspy.commands.retry import retry_items, retry_requests
from netspy.pipelines.base import BasePipeline

runner = CliRunner()


class CapturePipeline(BasePipeline):
    rows: list[tuple[str, list[dict[str, Any]]]] = []
    fail: bool = False

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        if CapturePipeline.fail:
            return False
        CapturePipeline.rows.append((table, items))
        return True


_PIPE = f"{__name__}.CapturePipeline"


@pytest.fixture(autouse=True)
def _reset_capture() -> None:
    CapturePipeline.rows.clear()
    CapturePipeline.fail = False


# ---------------------------------------------------------------- name helpers
def test_name_helpers() -> None:
    assert gen._to_camel("news_list") == "NewsList"
    assert gen._to_camel("NewsSpider") == "NewsSpider"
    assert gen._to_camel("my-cool-thing") == "MyCoolThing"
    assert gen._to_snake("NewsSpider") == "news_spider"
    assert gen._to_snake("my-cool-thing") == "my_cool_thing"


# ---------------------------------------------------------------- create
def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "netspy" in result.stdout


def test_create_without_flags_fails() -> None:
    assert runner.invoke(app, ["create"]).exit_code == 1


def test_create_project_scaffold_compiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["create", "-p", "my_shop"])
    assert result.exit_code == 0

    root = tmp_path / "my_shop"
    for name in ("main.py", "setting.py", "README.md", "spiders/my_shop_spider.py"):
        assert (root / name).exists(), name
    for py in root.rglob("*.py"):
        ast.parse(py.read_text(encoding="utf-8"), str(py))
    assert "MyShopSpider" in (root / "main.py").read_text(encoding="utf-8")


def _generate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["create", "-p", "my_shop"]).exit_code == 0
    return tmp_path / "my_shop"


def test_scaffolded_spider_actually_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`ast.parse` 只证明语法合法 —— 模板引用一个不存在的 API 它照样通过。

    实测：把模板里的 `ns.Request` 改成 `ns.RequestXXX`，既有用例仍然 5 passed，
    而每个新用户生成的项目都跑不起来。脚手架是新用户碰到的第一样东西。

    这里真的把爬虫模块 import 进来并实例化。不真跑抓取 ——
    那要联网、会让用例变慢变脆，而 import + 实例化已经能抓住 API 改名这类故障。
    """
    root = _generate(tmp_path, monkeypatch)
    monkeypatch.syspath_prepend(str(root))
    for name in list(sys.modules):
        if name.startswith("spiders"):
            del sys.modules[name]

    module = importlib.import_module("spiders.my_shop_spider")
    spider_cls = module.MyShopSpider
    spider = spider_cls()  # 实例化也要能过：构造签名变了同样是破坏

    # 必须**迭代** start_requests：模板里的调用写在生成器体内，
    # 光 import + 实例化执行不到它 —— 第一版守卫就漏在这里，
    # 把 ns.Request 改成 ns.RequestXXX 之后用例照样绿。
    # 迭代只构造 Request 对象、不发请求，所以既不联网也不慢。
    seeds = list(spider.start_requests())
    assert seeds, "start_requests 一个种子都没产出"
    assert all(isinstance(r, ns.Request) for r in seeds)


def test_scaffolded_settings_all_exist(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """模板里声明的配置项，框架必须都认识。

    实测：往模板加一行框架不存在的配置，既有用例仍然 5 passed ——
    而那会让每个新项目带着一条永远不生效的配置。
    只看赋值语句：注释掉的示例是给人看的，不算。
    """
    root = _generate(tmp_path, monkeypatch)
    tree = ast.parse((root / "setting.py").read_text(encoding="utf-8"))
    declared = [
        node.targets[0].id
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
    ]
    assert declared, "模板一项配置都没声明，这个用例什么都没验到"
    unknown = [name for name in declared if not hasattr(setting, name)]
    assert not unknown, f"模板声明了框架不认识的配置项：{unknown}"


def test_create_project_refuses_nonempty_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "keep").write_text("x")
    assert runner.invoke(app, ["create", "-p", "taken"]).exit_code != 0


def test_create_spider_and_item(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert runner.invoke(app, ["create", "-s", "news-list"]).exit_code == 0
    assert runner.invoke(app, ["create", "-i", "NewsDetail"]).exit_code == 0

    spider = (tmp_path / "news_list_spider.py").read_text(encoding="utf-8")
    item = (tmp_path / "news_detail_item.py").read_text(encoding="utf-8")
    assert "class NewsListSpider(ns.AirSpider)" in spider
    assert "class NewsDetailItem(ns.Item)" in item
    assert '__table_name__ = "news_detail"' in item


def test_create_force_overwrites(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    gen.create_spider("Dupe")
    with pytest.raises(FileExistsError):
        gen.create_spider("Dupe")
    gen.create_spider("Dupe", force=True)  # 不抛


# ---------------------------------------------------------------- shell
def test_shell_namespace(httpserver: HTTPServer) -> None:
    from netspy.commands.shell import build_namespace

    httpserver.expect_request("/").respond_with_data("<h1>hi</h1>", content_type="text/html")
    ns = build_namespace(httpserver.url_for("/"))
    assert ns["response"].xpath("//h1/text()").get() == "hi"
    assert ns["request"].url == httpserver.url_for("/")
    assert "ns" in ns


# ---------------------------------------------------------------- retry
def test_retry_items_replays_and_clears(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_PIPE])
    dump = tmp_path / "failed_items.jsonl"
    dump.write_text(
        '{"table": "news", "data": {"url": "u1"}}\n{"table": "news", "data": {"url": "u2"}}\n',
        encoding="utf-8",
    )

    assert retry_items() == (2, 0)
    assert not dump.exists()
    assert CapturePipeline.rows == [("news", [{"url": "u1"}, {"url": "u2"}])]


def test_retry_items_keeps_still_failing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_PIPE])
    CapturePipeline.fail = True
    dump = tmp_path / "failed_items.jsonl"
    dump.write_text('{"table": "t", "data": {"k": 1}}\n', encoding="utf-8")

    assert retry_items() == (0, 1)
    assert dump.exists()
    assert json.loads(dump.read_text(encoding="utf-8")) == {"table": "t", "data": {"k": 1}}


def test_retry_requests_probes_without_discarding(
    httpserver: HTTPServer, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`retry --requests` 是**探活**，不是恢复 —— 所以一条记录都不删。

    它只重新下载看状态码，不跑回调、不产出 item、不入库。早先的版本会把探到可达的
    记录删掉，于是「报告恢复 1、库里 0 行、唯一副本没了」。
    真要把数据抓回来，用 `RETRY_FAILED_ON_START`。
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setting, "RANDOM_USER_AGENT", False)
    httpserver.expect_request("/ok").respond_with_data("ok")
    dump = tmp_path / "failed_requests.jsonl"
    dump.write_text(
        json.dumps({"url": httpserver.url_for("/ok"), "method": "GET"})
        + "\n"
        + json.dumps({"url": "http://127.0.0.1:1/down", "method": "GET"})
        + "\n",
        encoding="utf-8",
    )

    assert retry_requests() == (1, 1)  # 1 条可达，1 条仍不可达
    text = dump.read_text(encoding="utf-8")
    assert "down" in text
    assert "/ok" in text, "探到可达就把记录删了 —— 那是唯一副本，而数据并没有回来"


def test_retry_noop_when_no_dump(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert retry_items() == (0, 0)
    assert retry_requests() == (0, 0)


def test_retry_cli_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_PIPE])
    (tmp_path / "failed_items.jsonl").write_text(
        '{"table": "t", "data": {"a": 1}}\n', encoding="utf-8"
    )
    result = runner.invoke(app, ["retry", "--items"])
    assert result.exit_code == 0
    assert "成功 1" in result.stdout
