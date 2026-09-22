"""基于 loguru 的日志封装。

`get_logger()` 在首次调用时按当前 `netspy.setting` 配置初始化 sink；
配置变更后可调用 `configure()` 重新初始化。
"""

from __future__ import annotations

import sys
import threading
from typing import TYPE_CHECKING

from loguru import logger

from netspy import setting

if TYPE_CHECKING:
    from loguru import Logger

#: 未绑定 name 的全局 logger，等价于 loguru 的 logger
log = logger

_lock = threading.Lock()
_state: dict[str, bool] = {"configured": False}

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <7}</level> | "
    "<cyan>{extra[name]}</cyan> - <level>{message}</level>"
)


def configure() -> None:
    """按 `netspy.setting` 的当前值重建日志 sink。"""
    with _lock:
        _configure_locked()


def _configure_locked() -> None:
    logger.remove()
    logger.configure(extra={"name": setting.PROJECT_NAME})
    logger.add(
        sys.stderr,
        level=setting.LOG_LEVEL,
        colorize=setting.LOG_COLOR,
        format=_FORMAT,
    )
    log_file = setting.LOG_FILE
    if log_file:
        logger.add(
            log_file,
            level=setting.LOG_LEVEL,
            rotation=setting.LOG_ROTATION,
            retention=setting.LOG_RETENTION,
            encoding="utf-8",
            format=_FORMAT,
        )
    _state["configured"] = True


def get_logger(name: str | None = None) -> Logger:
    """返回 logger；`name` 会作为 `{extra[name]}` 显示在日志中。"""
    if not _state["configured"]:
        # 双重检查加锁：get_logger 在几乎每个模块顶层都会被调到，第一次调用
        # 完全可能撞上多线程（比如同进程里跑了不止一个 Spider）。configure()
        # 不是幂等安全的——它先 logger.remove() 清空全部 sink 再重新 add，
        # 两个线程同时"看到还没配置"各自跑一遍的话，remove/add 交错执行，
        # 实测会留下重复的 sink，日志每行打印两遍。
        with _lock:
            if not _state["configured"]:
                _configure_locked()
    return logger.bind(name=name) if name else logger


class LoggerMixin:
    """混入它就有 ``self.logger``——绑定了具体子类名的 logger。

    没有它之前，写一个爬虫 / 管道 / 中间件想打日志，得自己
    ``from netspy.utils.log import get_logger`` 再手动 bind 一个名字，
    还常常图省事直接开在模块级（一个全局变量，和这个类本身没绑定关系）。
    `BaseParser` / `BasePipeline` / `DownloaderMiddleware` / `UserPool`
    都混入了它——写子类时直接 ``self.logger.info(...)`` 就行。

    写成 `@property` 现取、不缓存成实例属性，是因为混入它的几个基类
    （`BaseParser` / `DownloaderMiddleware` / `UserPool`）互相之间构造方式
    不一致——有的没有 `__init__`，有的子类重写 `__init__` 时不调用
    `super().__init__()`——没有一个通用的时机能安全地把它写进 `self.__dict__`。
    `property` 不依赖构造过程，混进去就能用。`get_logger` 本身只是
    `logger.bind()`，loguru 里很轻，现取不是性能负担。
    """

    @property
    def logger(self) -> Logger:
        return get_logger(type(self).__name__)
