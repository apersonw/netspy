from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Any
from unittest import mock

import fakeredis
import pytest

from netspy import Request, setting
from netspy.core.task_queue import RedisTaskQueue
from netspy.db import redisdb
from netspy.dedup import Dedup, get_request_filter
from netspy.dedup.redis_filter import RedisBloomFilter, RedisSetFilter


@pytest.fixture
def rds(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redisdb, "get_redis", lambda url=None: client)
    yield client
    client.flushall()


# ---------------------------------------------------------------- redisdb
def test_get_redis_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    made: list[str] = []

    def fake_from_url(url: str, **_: Any) -> Any:
        made.append(url)
        return fakeredis.FakeRedis(decode_responses=True)

    monkeypatch.setattr(redisdb.redis.Redis, "from_url", staticmethod(fake_from_url))
    redisdb.close_redis()
    a = redisdb.get_redis("redis://x/0")
    b = redisdb.get_redis("redis://x/0")
    assert a is b
    assert made == ["redis://x/0"]
    redisdb.close_redis()


def test_close_redis_does_not_race_with_concurrent_get_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回归测试：`close_redis()` 遍历 `_clients` 时不能被并发的 `get_redis()`
    插入新 key 打断。

    这是实测复现出来的真 bug，不是假设性的——`get_redis()` 的写入
    （`_clients[url] = client`）一直是靠 `_lock` 保护的，但 `close_redis()`
    的遍历 + `.clear()` 原来完全没加锁。只要遍历到一半，另一个线程的
    `get_redis()` 插入了一个新 URL（字典大小变了），Python 会直接在
    for 循环自己的迭代步骤上抛 ``RuntimeError: dictionary changed size
    during iteration``——这个异常不在 `contextlib.suppress` 的保护范围内
    （那层 suppress 只包住了 `client.close()` 这一句，包不住 for 循环本身），
    会整个从 `close_redis()` 里炸出去；崩溃之前排在后面、还没关的连接
    也就没能力再关了。

    用一个在 `close()` 里暂停的 mock client，强制让另一个线程的
    `get_redis()` 插入恰好落在遍历中途。
    """
    redisdb._clients.clear()
    try:
        paused = threading.Event()
        proceed = threading.Event()

        pausing_client = mock.Mock()

        def pausing_close() -> None:
            paused.set()
            proceed.wait(timeout=2)

        pausing_client.close.side_effect = pausing_close
        redisdb._clients["url0"] = pausing_client
        for i in range(1, 5):
            redisdb._clients[f"url{i}"] = mock.Mock()

        errors: list[BaseException] = []

        def closer() -> None:
            try:
                redisdb.close_redis()
            except BaseException as exc:
                errors.append(exc)

        t = threading.Thread(target=closer)
        t.start()
        assert paused.wait(timeout=2), "没能让 close_redis 卡在迭代中途，测试前提不成立"

        monkeypatch.setattr(
            redisdb.redis.Redis, "from_url", staticmethod(lambda url, **_: mock.Mock())
        )
        redisdb.get_redis("brand-new-url-mid-iteration")

        proceed.set()
        t.join(timeout=2)

        assert not errors, f"close_redis() 不该抛异常：{errors}"
        pausing_client.close.assert_called_once()
    finally:
        redisdb._clients.clear()


def test_key_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "REDIS_KEY_PREFIX", "proj")
    assert redisdb.key("z_requests") == "proj:z_requests"


def test_acquire_once(rds: Any) -> None:
    assert redisdb.acquire_once(rds, "lock:seed") is True
    assert redisdb.acquire_once(rds, "lock:seed") is False


# ---------------------------------------------------------------- RedisTaskQueue
def test_redis_queue_priority_order(rds: Any) -> None:
    q = RedisTaskQueue("t")
    q.put(Request("https://a", priority=300))
    q.put(Request("https://b", priority=100))
    q.put(Request("https://c", priority=200))
    assert q.qsize() == 3
    assert [q.get().url for _ in range(3)] == ["https://b", "https://c", "https://a"]
    assert q.get() is None
    assert q.empty()


def test_redis_queue_roundtrips_request_fields(rds: Any) -> None:
    q = RedisTaskQueue("t")
    q.put(Request("https://x", "POST", callback="parse_x", render=True, cb_kwargs={"p": 2}))
    got = q.get()
    assert got is not None
    assert got.method == "POST"
    assert got.callback == "parse_x"
    assert got.render is True
    assert got.cb_kwargs == {"p": 2}


def test_redis_queue_get_batch(rds: Any) -> None:
    q = RedisTaskQueue("t")
    for i in range(5):
        q.put(Request(f"https://{i}", priority=i))
    batch = q.get_batch(3)
    assert [r.url for r in batch] == ["https://0", "https://1", "https://2"]
    assert q.qsize() == 2


def test_redis_queue_survives_new_instance(rds: Any) -> None:
    RedisTaskQueue("t").put(Request("https://persisted"))
    assert RedisTaskQueue("t").get().url == "https://persisted"  # 断点续爬


def test_redis_queue_blocking_get(rds: Any) -> None:
    q = RedisTaskQueue("t")
    q.put(Request("https://ready"))
    assert q.get(timeout=1).url == "https://ready"  # 有数据立即返回
    assert q.get(timeout=0.1) is None  # 空队列超时返回 None


# ---------------------------------------------------------------- redis dedup
def test_redis_set_filter(rds: Any) -> None:
    f = RedisSetFilter("ns")
    assert f.add("a") is True
    assert f.add("a") is False
    assert "a" in f
    assert "b" not in f
    assert len(f) == 1


def test_redis_bloom_filter(rds: Any) -> None:
    f = RedisBloomFilter("ns", capacity=10_000, error_rate=1e-4)
    assert f.add("x") is True
    assert f.add("x") is False
    assert "x" in f
    assert "y" not in f


def test_redis_bloom_no_false_negatives(rds: Any) -> None:
    f = RedisBloomFilter("ns", capacity=5_000, error_rate=1e-3)
    keys = [f"k{i}" for i in range(1_000)]
    for k in keys:
        f.add(k)
    assert all(k in f for k in keys)


def test_redis_dedup_shared_across_instances(rds: Any) -> None:
    RedisSetFilter("ns").add("dup")
    assert RedisSetFilter("ns").add("dup") is False


def test_dedup_facade_redis(rds: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DEDUP_FILTER", "redis")
    d = Dedup(name="run1")
    assert d.add("hello") is True
    assert d.add("hello") is False
    assert Dedup(name="run1").get("hello") is True  # 跨实例共享
    assert Dedup(name="run2").get("hello") is False  # 命名空间隔离


def test_dedup_facade_redis_set(rds: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "DEDUP_FILTER", "redis-set")
    d = get_request_filter(name="reqs")
    fp = Request("https://x?a=1").fingerprint
    assert d.add(fp) is True
    assert get_request_filter(name="reqs").get(fp) is True


def test_dedup_unknown_still_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    from netspy.exceptions import ConfigError

    monkeypatch.setattr(setting, "DEDUP_FILTER", "bogus")
    with pytest.raises(ConfigError):
        Dedup()


def test_queue_without_lease_does_not_need_lua(monkeypatch) -> None:
    """`SPIDER_TASK_LEASE = 0` 必须是一条**真的**退路。

    租约要用 Lua 保证「取走 + 记账」原子，而有些托管 / 代理型 Redis 会禁用脚本。
    关掉租约却仍然走脚本的话，这条退路等于不存在 —— 那些部署会直接崩在
    `unknown command 'evalsha'`。
    """
    import fakeredis  # 这个夹具里的 fakeredis 没装 lupa，正好当「不支持 Lua」用

    from netspy import setting
    from netspy.core.task_queue import RedisTaskQueue
    from netspy.network.request import Request

    monkeypatch.setattr(setting, "SPIDER_TASK_LEASE", 0.0)
    client = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(client, "register_script", _boom)
    queue = RedisTaskQueue("nolua", client)
    for i in range(3):
        queue.put(Request(f"http://a/{i}"))

    assert len(queue.get_batch(5)) == 3
    assert queue.reclaim_expired() == 0, "关掉租约后不该再去回收"


def _boom(*_a, **_k):
    raise AssertionError("关掉租约后不该再调用 Lua 脚本")
