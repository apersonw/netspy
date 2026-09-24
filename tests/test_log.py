from __future__ import annotations

import subprocess
import sys
import threading
from pathlib import Path

import pytest

from netspy import setting
from netspy.utils import log

_REPO_ROOT = Path(__file__).resolve().parent.parent


def test_get_logger_is_usable() -> None:
    lg = log.get_logger("test")
    lg.info("hello")  # 不应抛异常


def test_get_logger_first_call_is_not_racy_across_threads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """回归测试：多个线程同时第一次调 `get_logger()`，只能真正 configure 一次。

    这是实测复现出来的真 bug——懒初始化检查（`if not _state["configured"]`）
    原来没加锁，而 `configure()` 本身不是幂等安全的：它先 `logger.remove()`
    清空全部 sink，再重新 `add()`。两个线程同时看到「还没配置」各自跑一遍的话，
    remove/add 会交错执行，实测会留下重复的 stderr sink——每行日志被打印两遍。

    `get_logger()` 几乎在每个模块顶层都会被调到，第一次调用完全可能撞上
    多线程（比如同一进程里跑了不止一个 Spider）。用 monkeypatch 在
    `logger.add` 里插一个暂停点，强制两个线程的第一次调用真正并发。
    """
    monkeypatch.setattr(log, "_state", {"configured": False})
    from loguru import logger

    logger.remove()

    paused = threading.Event()
    proceed = threading.Event()
    orig_add = logger.add
    call_count = {"n": 0}

    def patched_add(*args: object, **kwargs: object) -> int:
        call_count["n"] += 1
        if call_count["n"] == 1:
            paused.set()
            proceed.wait(timeout=2)
        return orig_add(*args, **kwargs)

    monkeypatch.setattr(logger, "add", patched_add)

    t1 = threading.Thread(target=lambda: log.get_logger("t1"))
    t1.start()
    assert paused.wait(timeout=2), "没能让第一次 configure() 卡在 add() 里，测试前提不成立"

    t2_done = threading.Event()

    def call_second() -> None:
        log.get_logger("t2")
        t2_done.set()

    t2 = threading.Thread(target=call_second)
    t2.start()
    # t1 还攥着锁在 configure 中途暂停——t2 这时候必须被真正挡住，
    # 不能因为看到 _state["configured"] 还是 False 就自己也跑一遍 configure()
    assert not t2_done.wait(timeout=0.3), (
        "第二个线程在第一个线程还没配置完时就完成了 get_logger()——"
        "两次 configure() 之间没有互斥，sink 随时可能重复"
    )

    proceed.set()
    t1.join(timeout=2)
    assert t2_done.wait(timeout=2), "第一个线程释放锁后，第二个线程应该能顺利完成"

    # 不按 repr 里有没有 "stderr" 过滤——pytest 默认会接管 sys.stderr 做输出捕获，
    # add() 加进去的 sink 绑的是捕获对象，repr 里不一定还看得到 "stderr" 字样。
    # 这里从「remove() 之后是空的」这个已知基线数，加了几个 sink 就是几个
    sinks = list(logger._core.handlers.values())
    assert len(sinks) == 1, (
        f"应该只有一个 sink，实际 {len(sinks)} 个——"
        "并发的第一次 configure() 没被锁住，留下了重复的 sink"
    )


def test_setting_reload_reconfigures_already_configured_logger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """回归测试：日志已经配置过一次之后，`setting.reload()` 也得让它跟着重建。

    `get_logger()` 只在**第一次**调用时按当时的 setting 配置好 sink，之后
    靠 `_state["configured"]` 短路。真实场景里这个「第一次」几乎总发生在
    `import netspy` 的过程中——一堆模块在顶层 `log = get_logger("xxx")`，
    而那时项目 `setting.py` / 环境变量还没加载。旧实现里 `reload()` 单纯
    改 `setting` 模块的全局变量，不会让已经建好的 sink 跟着重建——
    `LOG_LEVEL`/`LOG_FILE` 因此被静默忽略：`setting.LOG_LEVEL` 读出来是
    改过的值，loguru 里实际生效的还是配置时的旧值。

    先手动 configure 一次（模拟「已经被某个模块顶层的 get_logger() 配置过
    了」），再通过环境变量改配置并调 `reload()`，断言新配置真的生效了。
    用环境变量而不是 `monkeypatch.setattr(setting, ...)`：`reload()` 会先
    把所有配置重置回框架默认值再重新加载项目文件 / 环境变量，直接
    monkeypatch 模块属性的话，这一步会把改动原地冲掉。
    """
    log.configure()  # 模拟「已经配置过一次」
    logfile = tmp_path / "after_reload.log"
    monkeypatch.setenv("NETSPY_LOG_FILE", str(logfile))
    monkeypatch.setenv("NETSPY_LOG_LEVEL", "ERROR")
    setting.reload()
    log.get_logger("t").warning("不该出现")
    log.get_logger("t").error("该出现")
    assert logfile.exists(), "reload() 之后 LOG_FILE 该生效，文件应该被建出来"
    body = logfile.read_text(encoding="utf-8")
    assert "不该出现" not in body, "LOG_LEVEL=ERROR 没生效，WARNING 还是被打印了"
    assert "该出现" in body


def test_setting_apply_reconfigures_already_configured_logger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """同上，但走 Spider 的 `__custom_setting__` 用的 `setting.apply()` 路径。

    `AirSpider.__init__` 里 `setting.apply(self.__custom_setting__)` 原来
    也不会重建日志——只有 `debug=True` 那个分支才手动调了
    `_configure_log()`。于是写 `__custom_setting__ = {"LOG_LEVEL": "ERROR"}`
    的爬虫，日志级别实际上纹丝不动。
    """
    log.configure()  # 模拟「已经配置过一次」
    logfile = tmp_path / "after_apply.log"
    setting.apply({"LOG_FILE": str(logfile), "LOG_LEVEL": "ERROR"})
    log.get_logger("t").warning("不该出现")
    log.get_logger("t").error("该出现")
    body = logfile.read_text(encoding="utf-8")
    assert "不该出现" not in body
    assert "该出现" in body


def test_env_log_config_takes_effect_through_a_fresh_import(tmp_path: Path) -> None:
    """端到端回归测试：必须在**全新子进程**里跑。

    同进程里之前任何一次 `import netspy` 都已经把 `_state["configured"]`
    置成 True，会掩盖掉这个 bug——它的本质就是「第一次配置发生在
    `setting.reload()` 应用环境变量之前」，这个时序只在进程第一次
    `import netspy` 时才成立。
    """
    logfile = tmp_path / "fresh_import.log"
    script = f"""
import os
os.environ["NETSPY_LOG_LEVEL"] = "WARNING"
os.environ["NETSPY_LOG_FILE"] = {str(logfile)!r}
import netspy  # noqa: F401
from netspy.utils import log
log.get_logger("t").info("不该出现")
log.get_logger("t").warning("该出现")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
        cwd=str(_REPO_ROOT),
    )
    assert result.returncode == 0, f"子进程失败：{result.stderr}"
    assert logfile.exists(), f"NETSPY_LOG_FILE 没生效，日志文件没被建出来。stderr={result.stderr}"
    body = logfile.read_text(encoding="utf-8")
    assert "不该出现" not in body, "NETSPY_LOG_LEVEL=WARNING 没生效，INFO 还是被打印了"
    assert "该出现" in body


def test_configure_writes_to_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    logfile = tmp_path / "mw.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    log.configure()
    log.get_logger("t").warning("写到文件")
    assert logfile.exists()
    assert "写到文件" in logfile.read_text(encoding="utf-8")


def test_level_filters_lower_messages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    logfile = tmp_path / "mw.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    monkeypatch.setattr(setting, "LOG_LEVEL", "WARNING")
    log.configure()
    log.get_logger("t").info("看不见")
    log.get_logger("t").error("看得见")
    body = logfile.read_text(encoding="utf-8")
    assert "看不见" not in body
    assert "看得见" in body


# ======================================================================
# LoggerMixin —— 混入它就有 self.logger，不用自己 import / get_logger。
#
# 没有它之前，写一个爬虫想打日志得自己 `from netspy.utils.log import
# get_logger` 再手动 bind 一个名字，还常常图省事直接开在模块级（一个全局
# 变量，和具体类没绑定关系）。这里既测 mixin 本身，也逐个测四个真正混入了
# 它的基类——用户实际写的是这四个的子类，只在 mixin 层面测不出「忘了往
# 某个基类身上加」这种漏网。
# ======================================================================
def test_logger_mixin_binds_the_subclass_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    logfile = tmp_path / "mw.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    log.configure()

    class BookSpider(log.LoggerMixin):
        pass

    BookSpider().logger.info("抓到一本书")
    body = logfile.read_text(encoding="utf-8")
    assert "抓到一本书" in body
    assert "BookSpider" in body, "日志里该看到类名，不是随便一个全局名字"


def test_logger_mixin_distinguishes_different_classes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """两个不同的子类各自打一行——日志里得分得清是谁打的。"""
    logfile = tmp_path / "mw.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(logfile))
    log.configure()

    class Alpha(log.LoggerMixin):
        pass

    class Beta(log.LoggerMixin):
        pass

    Alpha().logger.info("来自 Alpha")
    Beta().logger.info("来自 Beta")
    lines = logfile.read_text(encoding="utf-8").splitlines()
    alpha_line = next(line for line in lines if "来自 Alpha" in line)
    beta_line = next(line for line in lines if "来自 Beta" in line)
    assert "Alpha" in alpha_line and "Beta" not in alpha_line
    assert "Beta" in beta_line and "Alpha" not in beta_line


def test_base_parser_subclasses_get_logger_with_zero_setup() -> None:
    """写一个爬虫，`self.logger` 直接能用——不用覆写 `__init__`，不用 import。"""
    from netspy.core.base_parser import BaseParser

    class DemoSpider(BaseParser):
        pass

    DemoSpider().logger.info("spider 直接能打日志")  # 不该抛异常


def test_base_pipeline_subclasses_get_logger_with_zero_setup() -> None:
    from netspy.pipelines.base import BasePipeline

    class DemoPipeline(BasePipeline):
        def save_items(self, table: str, items: list[dict[str, object]]) -> bool:
            self.logger.info("写了 {} 条到 {}", len(items), table)
            return True

    assert DemoPipeline().save_items("t", [{"a": 1}]) is True


def test_downloader_middleware_subclasses_get_logger_with_zero_setup() -> None:
    from netspy.network.middleware import DownloaderMiddleware

    class DemoMiddleware(DownloaderMiddleware):
        pass

    DemoMiddleware().logger.info("middleware 直接能打日志")


def test_user_pool_subclasses_get_logger_with_zero_setup() -> None:
    from netspy.network.user_pool.base import User, UserPool

    class DemoPool(UserPool):
        def get(self) -> User | None:
            self.logger.info("借号")
            return None

    assert DemoPool().get() is None
