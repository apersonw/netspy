"""模拟真人交互的工具函数——鼠标移动 / 点击 / 输入 / 滚动。

**不在渲染流程里自动调用**，这是刻意的：自动应用会给所有人默认加上不确定的
延迟，且这几个动作本身就该由用户自己决定用在哪一步（哪个元素、什么时候点）。
在自己的 ``render_script`` 里按需调用::

    from netspy.network.downloader._humanize import human_click, human_scroll

    yield mw.Request(url, render=True, render_script=lambda page: (
        human_scroll(page),
        human_click(page, "button.load-more"),
    ), callback=self.parse)

⚠️ **诚实说明能做到什么、做不到什么**：Playwright 不提供「读取鼠标当前位置」
的 API，所以 `human_move` 没法真的知道指针此刻在哪，只能假设一个起点
（默认视口中心）。这不是逐像素复刻真人轨迹的方案——真正要做到那个程度需要
持续跟踪指针状态、甚至接入真实的人类轨迹采样数据。这里做的是把「单帧瞬移、
零间隔批量输入、一次性跳转滚动」这几个最基础、最容易被行为分析捕捉的信号
去掉：分步骤移动、字符间随机停顿、分段滚动。聊胜于无，不是万能药。
"""

from __future__ import annotations

import random
import time
from typing import Any


#: smoothstep 缓动：起点和终点附近变化慢，中间快——比纯线性更接近真实手部动作
def _ease(t: float) -> float:
    return t * t * (3 - 2 * t)


def human_move(
    page: Any,
    x: float,
    y: float,
    *,
    start: tuple[float, float] | None = None,
    steps: int | None = None,
) -> None:
    """把鼠标移动到 ``(x, y)``——分随机段数、带抖动地移动，而不是单次跳变。

    ``start`` 不传时默认从视口中心出发（见模块文档：Playwright 读不到当前指针
    位置）。``steps`` 不传时随机取 8~20 步。
    """
    n = steps or random.randint(8, 20)
    if start is None:
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        start = (viewport["width"] / 2, viewport["height"] / 2)
    sx, sy = start
    for i in range(1, n + 1):
        t = _ease(i / n)
        jitter_x = random.uniform(-3, 3) if i < n else 0.0
        jitter_y = random.uniform(-3, 3) if i < n else 0.0
        page.mouse.move(sx + (x - sx) * t + jitter_x, sy + (y - sy) * t + jitter_y)
        time.sleep(random.uniform(0.002, 0.012))
    page.mouse.move(x, y)  # 收尾：确保精确落在目标点，不被最后一步的抖动带偏


def human_click(page: Any, selector: str, *, start: tuple[float, float] | None = None) -> None:
    """移动到目标元素上（位置在元素内随机，不总是打在正中心）再点击。

    元素定位不到 / 不可见时退回普通 `page.click()`，不额外抛异常——
    这个函数的定位是「让点击更像真人」，不该比普通点击更容易失败。
    """
    box = page.locator(selector).bounding_box()
    if box is None:
        page.click(selector)
        return
    target_x = box["x"] + box["width"] * random.uniform(0.35, 0.65)
    target_y = box["y"] + box["height"] * random.uniform(0.35, 0.65)
    human_move(page, target_x, target_y, start=start)
    time.sleep(random.uniform(0.05, 0.15))  # 真人「看清楚再点」的停顿
    page.mouse.click(target_x, target_y)


def human_type(
    page: Any,
    selector: str,
    text: str,
    *,
    delay_range: tuple[float, float] = (0.05, 0.18),
) -> None:
    """逐字符输入，字符间随机延迟，而不是 `fill()` 那种瞬间灌入整段文本。"""
    locator = page.locator(selector)
    locator.click()
    for ch in text:
        locator.press_sequentially(ch, delay=0)
        time.sleep(random.uniform(*delay_range))


def human_scroll(page: Any, *, distance: float | None = None, steps: int | None = None) -> None:
    """分段、带停顿地滚动，而不是一次性 `scrollTo` 跳过去。

    走 `page.mouse.wheel()` 而不是 `page.evaluate("scrollTo(...)")`——
    后者是直接改 `scrollTop`，不会触发真实的 wheel 事件序列。
    """
    total = distance if distance is not None else random.uniform(300, 900)
    n = steps or random.randint(4, 10)
    per_step = total / n
    for _ in range(n):
        page.mouse.wheel(0, per_step * random.uniform(0.7, 1.3))
        time.sleep(random.uniform(0.03, 0.12))
