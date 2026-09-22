from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from netspy import setting


def test_defaults_present() -> None:
    assert setting.SPIDER_THREAD_COUNT >= 1
    assert setting.DEDUP_FILTER in {"memory", "lite"}
    assert isinstance(setting.ITEM_PIPELINES, list)
    assert isinstance(setting.WEBDRIVER, dict)


def test_as_dict_snapshot_covers_all_keys() -> None:
    snap = setting.as_dict()
    assert "SPIDER_THREAD_COUNT" in snap
    assert "_DEFAULTS" not in snap
    assert all(k.isupper() for k in snap)


def test_env_override_int(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_SPIDER_THREAD_COUNT", "9")
    setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 9


def test_env_override_bool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_LOG_COLOR", "false")
    setting.reload()
    assert setting.LOG_COLOR is False


def test_env_override_float(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_REQUEST_TIMEOUT", "5.5")
    setting.reload()
    assert pytest.approx(5.5) == setting.REQUEST_TIMEOUT


def test_env_override_dict_via_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_WEBDRIVER", '{"pool_size": 5, "headless": false}')
    setting.reload()
    assert setting.WEBDRIVER["pool_size"] == 5
    assert setting.WEBDRIVER["headless"] is False


def test_bad_env_value_warns_and_keeps_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_SPIDER_THREAD_COUNT", "not-an-int")
    with pytest.warns(UserWarning, match="解析失败"):
        setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 4


def test_project_setting_file_is_discovered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "setting.py").write_text(
        textwrap.dedent(
            """
            SPIDER_THREAD_COUNT = 7
            MONGO_DB = "custom_db"
            _private = "ignored"
            lowercase = "ignored"
            """
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 7
    assert setting.MONGO_DB == "custom_db"


def test_broken_project_file_warns_and_keeps_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "setting.py").write_text("SPIDER_THREAD_COUNT = \n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with pytest.warns(UserWarning, match="加载项目配置"):
        setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 4


def test_env_beats_project_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "setting.py").write_text("SPIDER_THREAD_COUNT = 7\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("NETSPY_SPIDER_THREAD_COUNT", "11")
    setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 11


def test_explicit_setting_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = tmp_path / "my_conf.py"
    cfg.write_text("COLLECTOR_TASK_COUNT = 42\n", encoding="utf-8")
    monkeypatch.setenv("NETSPY_SETTING", str(cfg))
    setting.reload()
    assert setting.COLLECTOR_TASK_COUNT == 42


def test_missing_explicit_setting_path_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`NETSPY_SETTING` 指定的路径不存在，跟「压根没配」不能是同一种静默。

    不设 `NETSPY_SETTING` 时，默认候选路径（setting.py / settings.py）找不到
    是常态——把 netspy 当库用没有项目配置文件完全合法，不该吵。但
    `NETSPY_SETTING` 是用户自己明确指定的路径，找不到十有八九是打错路径 /
    部署时文件没放对位置：配置整个没生效，却和「压根没配」长得一模一样、
    零提示——这正是这个项目反复栽过的静默失效的形状。
    """
    monkeypatch.setenv("NETSPY_SETTING", str(tmp_path / "does_not_exist.py"))
    with pytest.warns(UserWarning, match="NETSPY_SETTING 指定的配置文件不存在"):
        setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 4  # 保持默认值，不是崩溃


def test_reload_restores_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NETSPY_SPIDER_THREAD_COUNT", "99")
    setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 99
    monkeypatch.delenv("NETSPY_SPIDER_THREAD_COUNT")
    setting.reload()
    assert setting.SPIDER_THREAD_COUNT == 4


def test_apply_merges_custom_setting() -> None:
    setting.apply({"SPIDER_THREAD_COUNT": 3, "ignored_lower": 1})
    assert setting.SPIDER_THREAD_COUNT == 3
    assert not hasattr(setting, "ignored_lower")
