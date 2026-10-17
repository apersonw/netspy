"""去重的规模边界（v4.5 阶段 A：先让它不再静默）。

布隆是**固定容量**，超容后误判率急剧升高 —— 而在去重里「误判」意味着一个从没
抓过的 URL 被当成「已抓过」**静默丢掉**。实测（容量 10 万、目标误判率 1e-6）：

    1× 容量 → 0%      3× → 7.2%（每 13 个丢 1 个）
    5× 容量 → 53.7%   8× → 92.8%

而在这之前**没有任何信号**：`count` 有记录但没人读，统计里只显示「去重 N 条」，
爬虫只是「提前结束了」或「少抓了很多」，查不出原因。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import fakeredis
import pytest

from netspy import setting
from netspy.dedup import Dedup
from netspy.dedup.bloom_filter import MemoryBloomFilter, ScalableBloomFilter
from netspy.dedup.redis_filter import RedisBloomFilter, RedisSetFilter
from netspy.utils import log
from netspy.utils.alert import AlertManager
from netspy.utils.stats import Stats


@pytest.fixture
def redis_client() -> Any:
    return fakeredis.FakeRedis(decode_responses=True)


# ---- 超容必须出声 ----------------------------------------------------
def test_memory_bloom_warns_once_when_over_capacity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # 项目用的是 loguru，caplog 抓不到 —— 照 test_log.py 的做法写文件再读
    logfile = tmp_path / "ns.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    monkeypatch.setattr(setting, "LOG_LEVEL", "WARNING")
    log.configure()

    bf = MemoryBloomFilter(capacity=50, error_rate=1e-3)
    for i in range(50):
        bf.add(f"k{i}")
    assert "超出容量" not in logfile.read_text(encoding="utf-8"), "还没超容就报警是在制造噪音"

    for i in range(50, 200):
        bf.add(f"k{i}")

    text = logfile.read_text(encoding="utf-8")
    assert text.count("超出容量") == 1, "超容要报、且只报一次（每插一条吵一次等于没吵）"
    assert "静默丢掉" in text, "告警要说清后果，不然用户不知道这有多严重"


def test_redis_bloom_counts_across_clients(redis_client: Any) -> None:
    """计数必须是**共享**的 —— 分布式下每个节点各数各的等于没数。

    这里用两个独立的过滤器对象连同一个 Redis，模拟两个节点。
    """
    a = RedisBloomFilter("ns", redis_client, capacity=1000, error_rate=1e-3)
    b = RedisBloomFilter("ns", redis_client, capacity=1000, error_rate=1e-3)

    for i in range(30):
        a.add(f"k{i}")
    for i in range(30, 50):
        b.add(f"k{i}")

    assert a.count == 50, f"节点 A 只看到 {a.count}，跨节点计数没共享"
    assert b.count == 50


def test_redis_bloom_add_is_atomic_about_newness(redis_client: Any) -> None:
    """同一个 key 只能被判定为「新」一次。

    原来是 pipeline 的 SETBIT —— pipeline 只打包发送、并不保证原子，
    两个节点可能同时判定为新。改成 Lua 之后这个竞态消失，计数也才准。
    """
    f = RedisBloomFilter("ns", redis_client, capacity=1000, error_rate=1e-3)
    assert f.add("same") is True
    assert f.add("same") is False
    assert f.count == 1, "重复的 key 不该把计数推高"


# ---- 告警通道 --------------------------------------------------------
def _alert_with(dedup: Any, sent: list[tuple[str, str]]) -> AlertManager:
    class _Spy:
        def send(self, title: str, message: str) -> None:
            sent.append((title, message))

    return AlertManager(Stats(), notifiers=[_Spy()], dedup=dedup)


def test_alert_fires_before_the_bloom_is_full(
    redis_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """要在**误判变糟之前**报 —— 等填满才报就已经晚了（3 倍容量时已丢 7%）。"""
    monkeypatch.setattr(setting, "WARNING_ENABLE", True)
    monkeypatch.setattr(setting, "DEDUP_WARN_FILL_RATE", 0.8)
    # 单层：这里测的是告警逻辑本身。分层时「容量」是各层之和，见下面的用例
    monkeypatch.setattr(setting, "DEDUP_MAX_LAYERS", 1)
    d = Dedup(filter_type="memory", capacity=100, error_rate=1e-3, to_md5=False)
    sent: list[tuple[str, str]] = []
    alert = _alert_with(d, sent)

    for i in range(70):
        d.add(f"k{i}")
    alert.check()
    assert sent == [], "七成满就报太吵了"

    for i in range(70, 85):
        d.add(f"k{i}")
    alert.check()
    assert len(sent) == 1, "过了 80% 该报了"
    assert "静默丢掉" in sent[0][1]


def test_exact_dedup_has_no_capacity_so_no_alert(
    redis_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """精确去重不会误判，没有容量概念 —— 不该对它报这个警。"""
    monkeypatch.setattr(setting, "WARNING_ENABLE", True)
    d = Dedup(filter_type="redis-set", name="ns", redis_client=redis_client, to_md5=False)
    sent: list[tuple[str, str]] = []
    alert = _alert_with(d, sent)

    for i in range(500):
        d.add(f"k{i}")
    alert.check()

    assert sent == []
    assert d.capacity is None
    assert isinstance(RedisSetFilter("ns", redis_client), RedisSetFilter)


# ---- 阶段 B：分层 ----------------------------------------------------
def test_layered_bloom_keeps_error_rate_low_past_base_capacity() -> None:
    """这是整个里程碑的核心断言：超过基础容量后，误判率不能塌掉。

    单层布隆在 8 倍容量时误判率 92.8%（一个从没抓过的 URL 有九成机会被当成
    抓过丢掉）。分层之后必须仍然接近 0。
    """
    cap = 5_000
    flat = MemoryBloomFilter(capacity=cap, error_rate=1e-6)
    layered = ScalableBloomFilter(capacity=cap, error_rate=1e-6, max_layers=4)
    for i in range(cap * 8):
        key = f"http://site/{i}"
        flat.add(key)
        layered.add(key)

    probes = 5_000
    flat_fp = sum(1 for i in range(probes) if f"http://never/{i}" in flat) / probes
    lay_fp = sum(1 for i in range(probes) if f"http://never/{i}" in layered) / probes

    assert flat_fp > 0.5, f"单层在 8 倍容量下应该已经塌了，实测 {flat_fp:.1%} —— 对照不成立"
    assert lay_fp < 0.01, f"分层的误判率 {lay_fp:.1%} 太高了"


def test_layered_bloom_never_forgets(monkeypatch: pytest.MonkeyPatch) -> None:
    """加层不能丢掉旧层的记忆 —— 否则「断点续爬不重复」就破了。"""
    layered = ScalableBloomFilter(capacity=100, error_rate=1e-4, max_layers=4)
    keys = [f"k{i}" for i in range(1000)]
    for k in keys:
        layered.add(k)
    assert len(layered._layers) > 1, "没触发加层，这个用例就没验到东西"
    assert all(k in layered for k in keys), "加层之后旧层的 key 找不到了"
    assert all(layered.add(k) is False for k in keys), "重复的 key 又被当成新的了"


def test_layer_count_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    """必须封顶：无限加层就是把内存变成无界资源。"""
    layered = ScalableBloomFilter(capacity=10, error_rate=1e-3, max_layers=3)
    for i in range(5_000):
        layered.add(f"k{i}")
    assert len(layered._layers) == 3
    assert layered.capacity == 10 + 20 + 40


def test_redis_bloom_layers_share_state_across_nodes(redis_client: Any) -> None:
    """两个「节点」连同一个 Redis：加层要一致，且互相认得对方写的 key。

    层数由共享计数器确定性推导 —— 各节点不协调也必须算出同一个顶层。
    """
    a = RedisBloomFilter("ns", redis_client, capacity=50, error_rate=1e-3, max_layers=4)
    b = RedisBloomFilter("ns", redis_client, capacity=50, error_rate=1e-3, max_layers=4)

    mine = [f"a{i}" for i in range(200)]
    yours = [f"b{i}" for i in range(200)]
    for k in mine:
        a.add(k)
    for k in yours:
        b.add(k)

    assert a.count == 400, f"共享计数不对：{a.count}"
    # 跨节点互认：A 写的 B 要认得，反之亦然（这才是分布式去重的意义）
    assert all(k in b for k in mine), "B 认不出 A 写的 key"
    assert all(k in a for k in yours), "A 认不出 B 写的 key"
    assert all(b.add(k) is False for k in mine), "B 又把 A 抓过的当成新的"


def test_redis_layer_zero_reuses_legacy_key(redis_client: Any) -> None:
    """第 0 层必须沿用老 key —— 否则升级上来的人所有去重状态失效，全站重抓一遍。"""
    old = RedisBloomFilter("ns", redis_client, capacity=1000, error_rate=1e-3, max_layers=1)
    old.add("seen-before-upgrade")

    upgraded = RedisBloomFilter("ns", redis_client, capacity=1000, error_rate=1e-3, max_layers=4)
    assert "seen-before-upgrade" in upgraded, "升级后认不出升级前抓过的 URL"
    assert upgraded.add("seen-before-upgrade") is False
