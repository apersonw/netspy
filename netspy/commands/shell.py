"""`netspy shell <url>` —— 抓一个页面，进交互式 shell 调试选择器。"""

from __future__ import annotations

import contextlib
from typing import Any


def build_namespace(url: str, *, render: bool = False) -> dict[str, Any]:
    import netspy as ns

    request = ns.Request(url, render=render)
    response = request.download()
    return {"ns": ns, "request": request, "response": response}


def run_shell(url: str, *, render: bool = False) -> None:
    ns = build_namespace(url, render=render)
    banner = (
        f"Netspy shell — {url}\n"
        f"  request  = {ns['request']!r}\n"
        f"  response = {ns['response']!r}\n"
        "  试试 response.xpath(...) / .css(...) / .re(...) / .text / .json()"
    )
    try:
        import IPython
    except ImportError:
        import code

        with contextlib.suppress(ImportError):
            import readline
            import rlcompleter

            readline.set_completer(rlcompleter.Completer(ns).complete)
            readline.parse_and_bind("tab: complete")
        code.interact(banner=banner, local=ns)
    else:
        IPython.embed(banner1=banner, user_ns=ns, colors="neutral")
