"""`RETRY_FAILED_ON_START` 在分布式下也要能把失败请求抓回来。

实测复现的真 bug：分布式下重试耗尽的请求 RPUSH 进 `<ns>:failed_requests`
之后**再也没有代码读它**。`RETRY_FAILED_ON_START` 只回放本地文件，而分布式节点
根本不往文件里写——文档让用户「要把数据真正抓回来，开 RETRY_FAILED_ON_START」，
对 `Spider` / `TaskSpider` 这句话是空的：站点恢复后重跑，0 个请求发出，失败列表
原封不动。
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import fakeredis
import pytest
from pytest_httpserver import HTTPServer

import netspy as mw
from netspy import setting
from netspy.core import redis_scheduler
from netspy.utils import tools

_FAILED = "netspy:replay_job:failed_requests"


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_scheduler, "get_redis", lambda url=None: client)
    monkeypatch.setattr("netspy.db.redisdb.get_redis", lambda url=None: client)
    yield client
    client.flushall()


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "DONE_CHECK_INTERVAL": 0.05,
        "DONE_CHECK_TIMES": 2,
        "BUFFER_FLUSH_INTERVAL": 0.02,
        "HEARTBEAT_INTERVAL": 0.05,
        "SPIDER_STARTUP_GRACE": 0.0,
        "RANDOM_USER_AGENT": False,
        "TASK_POLL_INTERVAL": 0.03,
        "TASK_EXHAUST_POLLS": 2,
        "ITEM_PIPELINES": [],
    }.items():
        monkeypatch.setattr(setting, name, value)


def _fail(redis: Any, url: str) -> None:
    """模拟上一轮：这个请求重试耗尽，被推进了 Redis 失败列表。"""
    redis.rpush(_FAILED, tools.dumps_json(mw.Request(url, callback="parse").to_dict()))


class _Spider(mw.Spider):
    parsed: list[str] = []

    def start_requests(self) -> Iterator[mw.Request]:
        return iter(())

    def parse(self, request: mw.Request, response: mw.Response) -> None:
        type(self).parsed.append(request.url)


def test_spider_replays_the_redis_failed_list(
    httpserver: HTTPServer, fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(setting, "RETRY_FAILED_ON_START", True)
    httpserver.expect_request("/recovered").respond_with_data("ok")
    url = httpserver.url_for("/recovered")
    _fail(fake_redis, url)
    _Spider.parsed = []

    _Spider(redis_key="replay_job").start()

    assert _Spider.parsed == [url], "失败列表里的请求没被回放"
    assert fake_redis.llen(_FAILED) == 0
    assert not fake_redis.keys(f"{_FAILED}:replaying:*"), "回放完的暂存 key 没删"


def test_nothing_is_replayed_unless_asked(httpserver: HTTPServer, fake_redis: Any) -> None:
    url = httpserver.url_for("/x")
    _fail(fake_redis, url)
    _Spider.parsed = []

    _Spider(redis_key="replay_job").start()

    assert _Spider.parsed == []
    assert fake_redis.llen(_FAILED) == 1, "没开 RETRY_FAILED_ON_START 时失败列表要原样保留"


def test_only_one_node_replays_when_several_start_together(
    fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """多个节点同时启动：改名是原子的，只有一个节点拿得到——否则每条失败请求会被灌 N 遍。"""
    monkeypatch.setattr(setting, "RETRY_FAILED_ON_START", True)
    for i in range(3):
        _fail(fake_redis, f"http://example.invalid/{i}")
    node_a = _Spider(redis_key="replay_job").scheduler
    node_b = _Spider(redis_key="replay_job").scheduler

    assert node_a._replay_failed_requests() == 3
    assert node_b._replay_failed_requests() == 0
    assert node_a._task_queue.qsize() == 3


def test_task_spider_replays_too(
    httpserver: HTTPServer, fake_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """TaskSpider 自己覆写了「种子」那一步（它没有 start_requests），回放不能跟着被跳过。"""
    monkeypatch.setattr(setting, "RETRY_FAILED_ON_START", True)
    httpserver.expect_request("/t").respond_with_data("ok")
    url = httpserver.url_for("/t")
    _fail(fake_redis, url)
    parsed: list[str] = []

    class T(mw.TaskSpider):
        def task_requests(self, task: Any) -> Iterator[mw.Request]:
            return iter(())

        def parse(self, request: mw.Request, response: mw.Response) -> None:
            parsed.append(request.url)

    T(redis_key="replay_job", task_key="replay_job:tasks").start()

    assert parsed == [url]
    assert fake_redis.llen(_FAILED) == 0


# ---------------------------------------------------------------- 心跳 / 租约的间隔约束
@pytest.mark.parametrize(
    ("overrides", "needle"),
    [
        ({"HEARTBEAT_INTERVAL": 2.0, "HEARTBEAT_STALE": 1.0}, "HEARTBEAT_STALE"),
        ({"HEARTBEAT_INTERVAL": 2.0, "HEARTBEAT_STALE": 9.0, "SPIDER_TASK_LEASE": 1.0}, "LEASE"),
    ],
    ids=["heartbeat-slower-than-stale", "heartbeat-slower-than-lease"],
)
def test_unsafe_heartbeat_timing_is_called_out_at_startup(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch, overrides: dict[str, float], needle: str
) -> None:
    """配反了不报错、只会悄悄出事：活节点被当成死节点、处理中的任务被别人回收重抓。

    照 test_distributed_full_stack 的教训：loguru 用 caplog 抓不到，要写文件再读；
    而且要**真的 start()**——只调私有方法的话，把调用从 _on_start 里删掉照样全绿。
    """
    from netspy.utils import log

    logfile = tmp_path / "run.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    for name, value in overrides.items():
        monkeypatch.setattr(setting, name, value)
    log.configure()
    _Spider(redis_key="timing_job").start()
    warnings = [
        line for line in logfile.read_text(encoding="utf-8").splitlines() if "WARNING" in line
    ]
    assert any(needle in line for line in warnings), f"没提醒 {needle} 的约束：{warnings}"


def test_safe_heartbeat_timing_stays_quiet(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from netspy.utils import log

    logfile = tmp_path / "run.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    log.configure()
    _Spider(redis_key="timing_job").start()
    body = logfile.read_text(encoding="utf-8")
    assert "HEARTBEAT_INTERVAL" not in body, "默认配置不该被误报"
