from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from netspy import setting
from netspy.commands import create as gen
from netspy.db.mysqldb import MysqlDB
from netspy.pipelines.mysql import MysqlPipeline


class FakeDB:
    """替身 MysqlDB：记录每次调用，可让某类操作抛错。"""

    def __init__(self, columns: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.closed = False
        self.raise_on: str | None = None
        self._columns = columns or []

    def query(self, sql: str, args: Any = None) -> list[dict[str, Any]]:
        self.calls.append(("query", sql, args))
        if "SHOW FULL COLUMNS" in sql:
            return list(self._columns)
        return []

    def execute(self, sql: str, args: Any = None) -> int:
        self.calls.append(("execute", sql, args))
        if self.raise_on == "execute":
            raise RuntimeError("boom")
        return 1

    def executemany(self, sql: str, args_list: Any) -> int:
        rows = list(args_list)
        self.calls.append(("executemany", sql, rows))
        if self.raise_on == "executemany":
            raise RuntimeError("boom")
        return len(rows)

    def close(self) -> None:
        self.closed = True


_COLUMNS = [
    {"Field": "id", "Type": "int", "Key": "PRI", "Comment": "主键"},
    {"Field": "title", "Type": "varchar(200)", "Key": "", "Comment": "标题"},
    {"Field": "url", "Type": "varchar(500)", "Key": "UNI", "Comment": ""},
]


# ---------------------------------------------------------------- pipeline
def test_save_items_upsert_sql() -> None:
    db = FakeDB()
    assert MysqlPipeline(db=db).save_items(
        "news", [{"id": 1, "title": "a"}, {"id": 2, "title": "b"}]
    )
    kind, sql, args = db.calls[0]
    assert kind == "executemany"
    assert sql == (
        "INSERT INTO `news` (`id`, `title`) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE `id`=VALUES(`id`), `title`=VALUES(`title`)"
    )
    assert args == [(1, "a"), (2, "b")]


def test_save_items_plain_insert_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "MYSQL_UPDATE_ON_DUPLICATE", False)
    db = FakeDB()
    MysqlPipeline(db=db).save_items("t", [{"a": 1}])
    assert db.calls[0][1] == "INSERT INTO `t` (`a`) VALUES (%s)"


def test_save_items_missing_field_becomes_none() -> None:
    db = FakeDB()
    MysqlPipeline(db=db).save_items("t", [{"a": 1, "b": 2}, {"a": 3}])
    assert db.calls[0][2] == [(1, 2), (3, None)]


def test_save_items_empty_is_noop() -> None:
    db = FakeDB()
    assert MysqlPipeline(db=db).save_items("t", []) is True
    assert db.calls == []


def test_save_items_failure_returns_false() -> None:
    db = FakeDB()
    db.raise_on = "executemany"
    assert MysqlPipeline(db=db).save_items("t", [{"a": 1}]) is False


def test_update_items_builds_set_and_where() -> None:
    db = FakeDB()
    assert MysqlPipeline(db=db).update_items("news", [{"id": 5, "title": "x", "hits": 9}], ["id"])
    kind, sql, args = db.calls[0]
    assert kind == "execute"
    assert sql == "UPDATE `news` SET `title`=%s, `hits`=%s WHERE `id`=%s"
    assert args == ("x", 9, 5)


def test_update_items_skips_row_without_set_cols() -> None:
    db = FakeDB()
    assert MysqlPipeline(db=db).update_items("t", [{"id": 1}], ["id"])
    assert db.calls == []


def test_update_items_failure_returns_false() -> None:
    db = FakeDB()
    db.raise_on = "execute"
    assert MysqlPipeline(db=db).update_items("t", [{"id": 1, "v": 2}], ["id"]) is False


def test_close_delegates_to_db() -> None:
    db = FakeDB()
    MysqlPipeline(db=db).close()
    assert db.closed


# ---------------------------------------------------------------- reflection
def test_reflect_table_returns_fields_and_pk() -> None:
    fields, unique = gen._reflect_table(FakeDB(_COLUMNS), "news")
    assert fields == [("id", "主键"), ("title", "标题"), ("url", "")]
    assert unique == ["id"]


def test_reflect_table_unknown_raises() -> None:
    with pytest.raises(ValueError, match="不存在"):
        gen._reflect_table(FakeDB([]), "nope")


def test_create_item_from_table(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    db = FakeDB(_COLUMNS)
    path = gen.create_item("news", table="news", mysql=db)

    text = path.read_text(encoding="utf-8")
    ast.parse(text)
    assert path.name == "news_item.py"
    assert "class NewsItem(ns.Item)" in text
    assert '__table_name__ = "news"' in text
    assert "__unique_key__ = ['id']" in text
    assert "id: object  # 主键" in text
    assert "url: object" in text
    assert db.closed is False  # 注入的 db 不该被关掉


def test_create_item_from_table_closes_url_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    db = FakeDB(_COLUMNS)
    monkeypatch.setattr(MysqlDB, "from_url", classmethod(lambda cls, url: db))

    gen.create_item("news", table="news", mysql="mysql://root@localhost/db")
    assert db.closed is True  # 由连接串创建的 db 用完关闭


# ---------------------------------------------------------------- MysqlDB.from_url
def test_from_url_parses_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(MysqlDB, "__init__", lambda self, **kw: captured.update(kw))

    MysqlDB.from_url("mysql://u:p%40ss@db.host:3307/shop")
    assert captured == {
        "host": "db.host",
        "port": 3307,
        "user": "u",
        "password": "p@ss",
        "database": "shop",
    }
