"""写库失败的数据，dump 一圈回放之后还是不是原来那条。

v4.14 把「dump 到 failed_items 也算落到了持久介质，可以销账」写成了保证。
这一组验的是那条退路本身。

实测（真 PG，默认 on_conflict="nothing"）：一条 UpdateItem 写库失败被 dump，
`netspy retry --items` 报告「成功 1，仍失败 0」并删掉文件，
而库里那行还是旧值 —— 静默、永久、还报告成功。

链条：dump 只记 table + data → retry 无条件 save_items（INSERT 而非 UPSERT）
→ PG 的 DO NOTHING 让这条 INSERT 什么都不做**并返回成功** → retry 据此报喜。
每一环都「按自己的契约正确工作」。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from netspy import UpdateItem, setting
from netspy.buffer.item_buffer import ItemBuffer
from netspy.commands.retry import _rewrite, retry_items
from netspy.dedup import Dedup
from netspy.pipelines.base import BasePipeline
from netspy.utils.stats import Stats


class RefusingPipeline(BasePipeline):
    """写库失败 —— 这批会被 dump。"""

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        return False

    def update_items(self, table: str, items: list[dict[str, Any]], keys: list[str]) -> bool:
        return False


class RecordingPipeline(BasePipeline):
    saved: list[tuple[str, list[dict[str, Any]]]] = []
    updated: list[tuple[str, list[dict[str, Any]], list[str]]] = []

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        RecordingPipeline.saved.append((table, items))
        return True

    def update_items(self, table: str, items: list[dict[str, Any]], keys: list[str]) -> bool:
        RecordingPipeline.updated.append((table, items, keys))
        return True


class InsertOnlyPipeline(BasePipeline):
    """没实现 update_items —— 基类会抛。retry 不能因此整个崩掉。"""

    def save_items(self, table: str, items: list[dict[str, Any]]) -> bool:
        return True


_REFUSE = f"{__name__}.RefusingPipeline"
_RECORD = f"{__name__}.RecordingPipeline"


class _Price(UpdateItem):
    __table_name__ = "prices"
    __update_key__ = ["url"]


@pytest.fixture(autouse=True)
def _reset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    RecordingPipeline.saved = []
    RecordingPipeline.updated = []
    monkeypatch.setattr(setting, "FAILED_ITEM_PATH", str(tmp_path / "failed.jsonl"))
    monkeypatch.setattr(setting, "ITEM_FILTER_ENABLE", False)


def _dump_one(item: Any) -> list[dict[str, Any]]:
    buf = ItemBuffer(Stats(), pipelines=[_REFUSE], dedup=Dedup(filter_type="lite"))
    buf.put(item)
    buf.flush()
    text = Path(setting.FAILED_ITEM_PATH).read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def test_dump_keeps_update_keys() -> None:
    """不记 update_keys 的话，回放时这条就从 UPSERT 退化成 INSERT。"""
    item = _Price()
    item.url, item.price = "https://x", 99
    (record,) = _dump_one(item)
    assert record["update_keys"] == ["url"], (
        f"dump 丢了 update_keys：{record} —— 回放会变成普通 INSERT"
    )


def test_dump_keeps_pipeline_routing() -> None:
    """逐条指定了管道的数据，回放时不该被灌进当前全部 ITEM_PIPELINES。"""
    item = _Price()
    item.url, item.price = "https://x", 1
    item.pipelines = [_REFUSE]  # 路由到会失败的那个，才走得到 dump
    (record,) = _dump_one(item)
    assert record["pipelines"] == [_REFUSE]


def test_plain_item_dump_stays_the_old_shape() -> None:
    """普通 Item 没有 update_keys，dump 出来就该和以前一模一样。"""
    (record,) = _dump_one({"url": "https://x"})
    assert record == {"table": setting.ITEM_DEFAULT_TABLE, "data": {"url": "https://x"}}


def test_replay_routes_updates_to_update_items(monkeypatch: pytest.MonkeyPatch) -> None:
    """带 update_keys 的记录必须走 update_items —— 这是整条链的关键一环。"""
    Path(setting.FAILED_ITEM_PATH).write_text(
        json.dumps({"table": "prices", "data": {"url": "u", "price": 9}, "update_keys": ["url"]})
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_RECORD])
    assert retry_items() == (1, 0)
    assert RecordingPipeline.updated == [("prices", [{"url": "u", "price": 9}], ["url"])]
    assert RecordingPipeline.saved == [], "UpdateItem 被当成普通插入回放了"


def test_replay_uses_the_recorded_pipelines(monkeypatch: pytest.MonkeyPatch) -> None:
    Path(setting.FAILED_ITEM_PATH).write_text(
        json.dumps({"table": "t", "data": {"k": 1}, "pipelines": [_RECORD]}) + "\n",
        encoding="utf-8",
    )
    # 当前配置是另一个管道；记录里指定了 RecordingPipeline，就该只走它
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_REFUSE])
    assert retry_items() == (1, 0)
    assert RecordingPipeline.saved == [("t", [{"k": 1}])]


def test_old_dump_files_still_replay(monkeypatch: pytest.MonkeyPatch) -> None:
    """老文件没有新字段，读的时候不能炸，也不能凭空当成 update。"""
    Path(setting.FAILED_ITEM_PATH).write_text(
        json.dumps({"table": "t", "data": {"k": 1}}) + "\n", encoding="utf-8"
    )
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_RECORD])
    assert retry_items() == (1, 0)
    assert RecordingPipeline.saved == [("t", [{"k": 1}])]
    assert RecordingPipeline.updated == []


def test_corrupted_trailing_line_does_not_block_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    """回归测试：一行解析不了，不该拖累文件里其它完好的记录。

    dump 文件是追加写的，进程如果正好在追加中途被杀，最后一行会是半截
    JSON——这条本来就救不回来，但旧实现一遇到它就让 `json.JSONDecodeError`
    直接穿出 `retry_items()`：前面两条完好的记录跟着一起读不出来，
    `netspy retry --items` 对着一个大部分完好的文件直接报错退出。
    `base_scheduler._replay_failed_requests()`（爬虫启动时的自动回放）早就
    对同一种损坏做了逐行跳过，这里该是同一个防御。
    """
    Path(setting.FAILED_ITEM_PATH).write_text(
        json.dumps({"table": "t", "data": {"k": 1}})
        + "\n"
        + json.dumps({"table": "t", "data": {"k": 2}})
        + "\n"
        + '{"table": "t", "data": {"k": "trunc',  # 半截 JSON，没有结尾
        encoding="utf-8",
    )
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [_RECORD])
    assert retry_items() == (2, 0), "两条完好的记录应该正常回放，不该被截断的那行拖累"
    saved_rows = [r for _, rows in RecordingPipeline.saved for r in rows]
    assert saved_rows == [{"k": 1}, {"k": 2}]


def test_rewrite_does_not_wipe_file_when_write_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """回归测试：写「仍失败」记录时半路被杀，原文件不能变空。

    `failed_items.jsonl` 是这些记录**唯一的副本**。旧实现直接
    `path.write_text(...)`——`open(path, "w")` 一上来就截断目标文件，
    进程这时候被 OOM Killer / SIGKILL / 断电，文件已经空了，新内容却一个
    字节都没落地：本该保留下来回放的记录就此永久消失。

    用一个只在临时文件（`.part`）上抛异常的 monkeypatch 模拟"写到一半被杀"：
    正确实现下原文件应该原封不动，因为改动只发生在临时文件上，
    真正的替换要等临时文件完整写完才会原子发生。
    """
    target = tmp_path / "failed_items.jsonl"
    target.write_text('{"table": "t", "data": {"k": "old"}}\n', encoding="utf-8")

    real_write_text = Path.write_text

    def _crash_on_temp_file(self: Path, data: str, **kw: Any) -> int:
        if self.name.endswith(".part"):
            raise KeyboardInterrupt("模拟写临时文件时进程被杀")
        return real_write_text(self, data, **kw)

    monkeypatch.setattr(Path, "write_text", _crash_on_temp_file)

    with pytest.raises(KeyboardInterrupt):
        _rewrite(target, [{"table": "t", "data": {"k": "new"}}])

    assert target.exists(), "原文件不该被删掉"
    assert '"old"' in target.read_text(encoding="utf-8"), (
        "原文件的内容被破坏了——写临时文件失败不该影响到它"
    )


def test_pipeline_without_update_items_does_not_kill_the_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """基类的 update_items 直接抛。这批该留在文件里，而不是让整个回放崩掉 ——
    那会连带丢掉本来能回放的其它记录。"""
    dump = Path(setting.FAILED_ITEM_PATH)
    dump.write_text(
        json.dumps({"table": "t", "data": {"k": 1}, "update_keys": ["k"]})
        + "\n"
        + json.dumps({"table": "t", "data": {"k": 2}})
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(setting, "ITEM_PIPELINES", [f"{__name__}.InsertOnlyPipeline"])
    ok, failed = retry_items()
    assert (ok, failed) == (1, 1), "普通那条本来能回放，不该被 update 那条连累"
    assert json.loads(dump.read_text(encoding="utf-8")) == {
        "table": "t",
        "data": {"k": 1},
        "update_keys": ["k"],
    }
