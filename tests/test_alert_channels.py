"""告警渠道：钉钉 / 企业微信，以及「发送失败要被查出来」（v4.11）。

三家群机器人在 webhook 失效、关键词不匹配、需要加签这些情况下**都返回 HTTP 200**，
真正的结果在 body 的错误码里；而 `httpx.post` 对 4xx/5xx 也不抛异常。
原来的写法只捕获 `HTTPError`，等于什么都没检查 —— 你以为告警发出去了，其实没到。

**告警系统静默失效是最坏的一种**：出事那天才发现它自己早就哑了。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Request as WRequest
from werkzeug.wrappers import Response as WResponse

from netspy import setting
from netspy.utils import log as logmod
from netspy.utils.alert import (
    DingTalkNotifier,
    FeishuNotifier,
    WeChatWorkNotifier,
    build_notifiers,
)


@pytest.fixture
def logfile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """项目用 loguru，caplog 抓不到 —— 照 test_log.py 的做法写文件再读。"""
    path = tmp_path / "ns.log"
    monkeypatch.setattr(setting, "LOG_FILE", str(path))
    monkeypatch.setattr(setting, "LOG_LEVEL", "WARNING")
    logmod.configure()
    yield path


def _endpoint(server: HTTPServer, path: str, body: dict[str, Any], status: int = 200) -> list[Any]:
    """靶子：记下收到的请求体，按需返回各家的错误码。"""
    got: list[Any] = []

    def handler(request: WRequest) -> WResponse:
        got.append({"json": request.get_json(silent=True), "query": dict(request.args)})
        return WResponse(
            __import__("json").dumps(body), status=status, content_type="application/json"
        )

    server.expect_request(path, method="POST").respond_with_handler(handler)
    return got


# ---- 新渠道 ----------------------------------------------------------
def test_dingtalk_sends_text(httpserver: HTTPServer) -> None:
    got = _endpoint(httpserver, "/ding", {"errcode": 0, "errmsg": "ok"})

    DingTalkNotifier(httpserver.url_for("/ding")).send("卡死", "10 分钟没有新请求")

    assert len(got) == 1
    content = got[0]["json"]["text"]["content"]
    assert "卡死" in content and "10 分钟没有新请求" in content
    # 钉钉的「自定义关键词」安全模式要求消息里含关键词，固定带上省得用户踩坑
    assert "Netspy" in content


def test_dingtalk_signs_when_a_secret_is_given(httpserver: HTTPServer) -> None:
    """加签模式：要带 timestamp 和 HMAC-SHA256 签名，签错了钉钉会拒收。"""
    secret = "SECabc123"
    got = _endpoint(httpserver, "/ding", {"errcode": 0})

    DingTalkNotifier(httpserver.url_for("/ding"), secret).send("标题", "正文")

    q = got[0]["query"]
    assert "timestamp" in q and "sign" in q
    expected = base64.b64encode(
        hmac.new(secret.encode(), f"{q['timestamp']}\n{secret}".encode(), hashlib.sha256).digest()
    ).decode()
    # werkzeug 的 request.args 已经解码过一次，直接比对即可。
    # 这里**不能**再 unquote_plus：签名是 base64，里面的 `+` 会被再解码成空格 ——
    # 而 base64 有没有 `+` 看运气（44 个字符约一半概率），那样写的用例是偶发红的。
    assert q["sign"] == expected


def test_dingtalk_without_secret_sends_no_signature(httpserver: HTTPServer) -> None:
    """用「自定义关键词」模式的用户没有 secret，不该被塞一个空签名。"""
    got = _endpoint(httpserver, "/ding", {"errcode": 0})

    DingTalkNotifier(httpserver.url_for("/ding")).send("标题", "正文")

    assert "sign" not in got[0]["query"]


def test_wechat_work_sends_text(httpserver: HTTPServer) -> None:
    got = _endpoint(httpserver, "/wx", {"errcode": 0, "errmsg": "ok"})

    WeChatWorkNotifier(httpserver.url_for("/wx")).send("失败率过高", "失败 60 / 总计 100")

    assert got[0]["json"]["msgtype"] == "text"
    assert "失败 60 / 总计 100" in got[0]["json"]["text"]["content"]


# ---- 核心：失败不能被吞掉 --------------------------------------------
@pytest.mark.parametrize(
    ("name", "body"),
    [
        ("钉钉", {"errcode": 310000, "errmsg": "keywords not in content"}),
        ("企业微信", {"errcode": 93000, "errmsg": "invalid webhook url"}),
        ("飞书", {"code": 9499, "msg": "bad request"}),
    ],
)
def test_business_error_in_a_200_response_is_logged(
    httpserver: HTTPServer, logfile: Path, name: str, body: dict[str, Any]
) -> None:
    """HTTP 200 但 body 里带错误码 —— 这正是 webhook 失效时的真实样子。

    不检查 body 的话，这条告警就是**静默丢失**的。
    """
    _endpoint(httpserver, "/hook", body)
    url = httpserver.url_for("/hook")
    notifier: Any = {
        "钉钉": DingTalkNotifier(url),
        "企业微信": WeChatWorkNotifier(url),
        "飞书": FeishuNotifier(url),
    }[name]

    notifier.send("标题", "正文")

    text = logfile.read_text(encoding="utf-8")
    assert "被拒绝" in text, f"{name}：200 + 错误码被当成发送成功了"
    assert str(body.get("errcode") or body.get("code")) in text


def test_http_error_status_is_logged(httpserver: HTTPServer, logfile: Path) -> None:
    """httpx.post 对 4xx/5xx 不抛异常 —— 必须显式 raise_for_status。"""
    _endpoint(httpserver, "/hook", {"errcode": 0}, status=500)

    DingTalkNotifier(httpserver.url_for("/hook")).send("标题", "正文")

    assert "发送失败" in logfile.read_text(encoding="utf-8")


def test_unreachable_webhook_is_logged(logfile: Path) -> None:
    WeChatWorkNotifier("http://127.0.0.1:9/nope").send("标题", "正文")
    assert "发送失败" in logfile.read_text(encoding="utf-8")


def test_success_logs_nothing(httpserver: HTTPServer, logfile: Path) -> None:
    """发成功了就别刷屏 —— 告警本身不该制造噪音。"""
    _endpoint(httpserver, "/hook", {"errcode": 0, "errmsg": "ok"})

    DingTalkNotifier(httpserver.url_for("/hook")).send("标题", "正文")

    text = logfile.read_text(encoding="utf-8") if logfile.exists() else ""
    assert "发送失败" not in text and "被拒绝" not in text


# ---- 装配 ------------------------------------------------------------
def test_channels_are_wired_by_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(setting, "WARNING_DINGTALK_WEBHOOK", "https://example.com/ding")
    monkeypatch.setattr(setting, "WARNING_DINGTALK_SECRET", "SECxxx")
    monkeypatch.setattr(setting, "WARNING_WECHAT_WEBHOOK", "https://example.com/wx")
    monkeypatch.setattr(setting, "WARNING_FEISHU_WEBHOOK", "")
    monkeypatch.setattr(setting, "WARNING_EMAIL", {})

    kinds = {type(n).__name__ for n in build_notifiers()}

    assert "DingTalkNotifier" in kinds
    assert "WeChatWorkNotifier" in kinds


def test_unconfigured_channels_are_not_wired(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("WARNING_DINGTALK_WEBHOOK", "WARNING_WECHAT_WEBHOOK", "WARNING_FEISHU_WEBHOOK"):
        monkeypatch.setattr(setting, name, "")
    monkeypatch.setattr(setting, "WARNING_EMAIL", {})

    kinds = {type(n).__name__ for n in build_notifiers()}

    assert kinds == {"LogNotifier"}
