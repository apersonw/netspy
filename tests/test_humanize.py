"""模拟真人交互工具函数——mock 级验证「确实是分步骤/带停顿的」，
不是单帧瞬移。真实浏览器下这些操作产生的事件间隔是否真的不均匀，
由 test_render.py 里带 @pytest.mark.render 的用例另外验证。
"""

from __future__ import annotations

from typing import Any
from unittest import mock

from netspy.network.downloader._humanize import (
    human_click,
    human_move,
    human_scroll,
    human_type,
)


def _fake_page(**overrides: Any) -> Any:
    page = mock.Mock()
    page.viewport_size = {"width": 1280, "height": 720}
    for key, value in overrides.items():
        setattr(page, key, value)
    return page


def test_human_move_moves_in_multiple_steps_not_a_single_jump() -> None:
    """真人的移动不是单帧瞬移——至少要拆成好几次 mouse.move 调用。"""
    page = _fake_page()
    with mock.patch("time.sleep"):
        human_move(page, 500, 300, steps=10)
    # 中间的 10 步 + 收尾精确落点这一次
    assert page.mouse.move.call_count == 11


def test_human_move_intermediate_points_are_not_identical() -> None:
    """中间点必须是真的在移动，不是原地重复调用同一个坐标。"""
    page = _fake_page()
    with mock.patch("time.sleep"):
        human_move(page, 500, 300, start=(0, 0), steps=10)
    calls = [call.args for call in page.mouse.move.call_args_list]
    xs = [c[0] for c in calls]
    assert len(set(xs)) > 1, "所有中间点的 x 坐标都一样，不是真的在移动"


def test_human_move_ends_exactly_at_target() -> None:
    """抖动不能带偏最终落点——最后一次调用必须精确是目标坐标。"""
    page = _fake_page()
    with mock.patch("time.sleep"):
        human_move(page, 500, 300, steps=5)
    last_call = page.mouse.move.call_args_list[-1]
    assert last_call.args == (500, 300)


def test_human_move_sleeps_between_steps() -> None:
    """步骤之间要有停顿，不是所有 move 调用背靠背零间隔发出。"""
    page = _fake_page()
    with mock.patch("time.sleep") as sleep_mock:
        human_move(page, 500, 300, steps=8)
    assert sleep_mock.call_count == 8
    assert all(call.args[0] > 0 for call in sleep_mock.call_args_list)


def test_human_click_uses_human_move_then_clicks() -> None:
    """点击前要先移过去，不是隔空直接点。"""
    locator = mock.Mock()
    locator.bounding_box.return_value = {"x": 100, "y": 200, "width": 50, "height": 20}
    page = _fake_page()
    page.locator.return_value = locator

    with mock.patch("time.sleep"):
        human_click(page, "button.go")

    assert page.mouse.move.call_count >= 1, "点击前应该先移动鼠标"
    assert page.mouse.click.call_count == 1
    click_x, click_y = page.mouse.click.call_args.args
    assert 100 <= click_x <= 150
    assert 200 <= click_y <= 220


def test_human_click_falls_back_to_plain_click_when_element_not_found() -> None:
    """定位不到元素就退回普通 click，不额外抛异常——这个函数不该比原生更脆弱。"""
    locator = mock.Mock()
    locator.bounding_box.return_value = None
    page = _fake_page()
    page.locator.return_value = locator

    human_click(page, "button.missing")

    page.click.assert_called_once_with("button.missing")
    page.mouse.click.assert_not_called()


def test_human_type_presses_one_character_at_a_time() -> None:
    """逐字符输入，不是一整段文字瞬间灌进去。"""
    locator = mock.Mock()
    page = _fake_page()
    page.locator.return_value = locator

    with mock.patch("time.sleep") as sleep_mock:
        human_type(page, "input#q", "hi")

    assert locator.press_sequentially.call_count == 2
    assert [c.args[0] for c in locator.press_sequentially.call_args_list] == ["h", "i"]
    assert sleep_mock.call_count == 2


def test_human_type_delay_respects_configured_range() -> None:
    page = _fake_page()
    page.locator.return_value = mock.Mock()
    with mock.patch("time.sleep") as sleep_mock:
        human_type(page, "input#q", "a", delay_range=(0.2, 0.2))
    assert sleep_mock.call_args.args[0] == 0.2


def test_human_scroll_happens_in_multiple_segments() -> None:
    """一次性滚动到底本身就是个信号——要分几段走。"""
    page = _fake_page()
    with mock.patch("time.sleep"):
        human_scroll(page, distance=500, steps=6)
    assert page.mouse.wheel.call_count == 6
    # 每段都是往下滚（y 方向为正），不是原地不动
    assert all(call.args[1] > 0 for call in page.mouse.wheel.call_args_list)


def test_human_scroll_sleeps_between_segments() -> None:
    page = _fake_page()
    with mock.patch("time.sleep") as sleep_mock:
        human_scroll(page, distance=400, steps=5)
    assert sleep_mock.call_count == 5
    assert all(call.args[0] > 0 for call in sleep_mock.call_args_list)
