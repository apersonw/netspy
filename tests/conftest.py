from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

from netspy import setting
from netspy.network.downloader._common import set_effective_concurrency
from netspy.utils import log


@pytest.fixture(autouse=True)
def _isolated_cwd(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """每个用例在自己的临时目录里跑。

    `FAILED_REQUEST_PATH` / `FAILED_ITEM_PATH` / `RESPONSE_CACHE_PATH` / `CSV_OUTPUT_DIR`
    默认都是相对 cwd 的，项目 `setting.py` 也按 cwd 找。原来用例直接在仓库根跑：
    每跑一遍全量测试，仓库根的 `failed_requests.jsonl` 就多出约 2000 条指向早已关掉的
    临时 localhost 端口的请求——攒到 8.6 万条之后，一次开了 `RETRY_FAILED_ON_START`
    的运行就把它们全当成真失败请求回放了。用例要的话照样可以自己 chdir 到 tmp_path。
    """
    monkeypatch.chdir(tmp_path_factory.mktemp("cwd"))


@pytest.fixture(autouse=True)
def _reset_state(_isolated_cwd: None) -> Iterator[None]:
    """每个测试前后都把配置与日志恢复到默认，隔离用例间的环境 / 配置改动。

    `set_effective_concurrency(None)` 也在这里：那个全局是「调度器告知的真实线程数」，
    **取最大值且永不自降**，任何起过 Spider 的用例都会把它留给后面的用例。
    `test_session_sharding` 之前一直只是**碰巧**绿的 —— 按字母序排在它前面的
    `test_effective_concurrency` 自己收尾时把它清了；插进一个名字在两者之间、
    又起了 Spider 的测试文件，它就红了（test_run_scope.py 就是这么撞出来的）。
    """
    setting.reload()
    set_effective_concurrency(None)
    log.configure()
    yield
    setting.reload()
    set_effective_concurrency(None)
    log.configure()


# ----------------------------------------------------------------------
# 真实数据库集成测试用的夹具。没配环境变量就 skip —— 本地默认不拖慢，CI 里必跑。
#
#   NETSPY_TEST_POSTGRES_URL=postgresql://postgres:x@127.0.0.1:5432/postgres
#   NETSPY_TEST_MYSQL_URL=mysql://root:x@127.0.0.1:3306/netspy
# ----------------------------------------------------------------------
def _db_url(env: str) -> str:
    url = os.environ.get(env, "").strip()
    if not url:
        pytest.skip(f"未设置 {env}，跳过真库集成测试")
    return url


@pytest.fixture
def postgres_db() -> Iterator[Any]:
    from netspy.db.postgresdb import PostgresDB

    db = PostgresDB.from_url(_db_url("NETSPY_TEST_POSTGRES_URL"))
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def redis_url() -> str:
    """真 Redis 的连接串。

    分布式能力此前只用 fakeredis + 单进程测过 —— 那既不含真正的并发竞争，
    也不跨进程。这个夹具让集成测试连真实例。
    """
    return _db_url("NETSPY_TEST_REDIS_URL")


@pytest.fixture
def mysql_url() -> str:
    """真 MySQL 的连接串。

    并发用例需要**每个 worker 一条独立连接** —— 共用一个 `MysqlDB` 实例
    就没有真正的并发可言，也就测不出认领的竞态。
    """
    return _db_url("NETSPY_TEST_MYSQL_URL")


@pytest.fixture
def mysql_db() -> Iterator[Any]:
    from netspy.db.mysqldb import MysqlDB

    db = MysqlDB.from_url(_db_url("NETSPY_TEST_MYSQL_URL"))
    try:
        yield db
    finally:
        db.close()
