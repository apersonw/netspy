"""`netspy retry` —— 回放 dump 文件里的失败请求 / 数据。

- ``--items``：把 ``failed_items.jsonl`` 里的记录重新过一遍当前 ``ITEM_PIPELINES``
- ``--requests``：把 ``failed_requests.jsonl`` 里的请求重新下载一遍（不重跑 parse），
  用于确认目标是否恢复

两者都会把仍失败的记录写回文件，全部成功则删除文件。
"""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

from netspy import setting
from netspy.exceptions import RequestError
from netspy.network.downloader import close_default_downloaders
from netspy.network.request import Request
from netspy.utils import tools
from netspy.utils.log import get_logger

log = get_logger("retry")


def _read_lines(path: Path) -> list[Any]:
    """逐行解析 dump 文件，**单条解不出来不该拖累整个文件**。

    dump 是追加写的（见 `base_scheduler._append_requests` /
    `item_buffer._dump_failed`），进程如果正好在追加中途被杀
    （OOM / SIGKILL / 断电），最后一行会是半截 JSON —— 这条本来就救不回来，
    但不能让它连累前面已经完整落盘的记录跟着读不出来。
    `base_scheduler._replay_failed_requests()`（爬虫启动时的自动回放）
    早就是这么处理的，CLI 这条路径原来漏了同一防御。
    """
    if not path.is_file():
        return []
    records: list[Any] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            records.append(tools.loads_json(line))
        except Exception:
            log.warning("有一行记录解析不了（可能是写到一半被中断），跳过：{}", line[:120])
    return records


def _rewrite(path: Path, remaining: list[Any]) -> None:
    """回写仍失败的记录。**先写临时文件再改名**：这份文件是它们唯一的副本。

    `Path.write_text()` 直接写目标文件的话，`open(path, "w")` 一上来就截断 ——
    进程这时候被杀（OOM / SIGKILL / 断电），文件已经空了，新内容却一个字节
    都没落地：本该保留的「仍失败」记录直接消失，无法回放。跟 `network/cache.py`
    的 `store()` 是同一个坑、同一个修法。
    """
    if not remaining:
        if path.exists():
            path.unlink()
        return
    tmp = path.with_suffix(path.suffix + ".part")
    tmp.write_text("\n".join(tools.dumps_json(r) for r in remaining) + "\n", encoding="utf-8")
    tmp.replace(path)


def retry_items(path: str | None = None) -> tuple[int, int]:
    """返回 (成功条数, 仍失败条数)。"""
    file = Path(path or setting.FAILED_ITEM_PATH)
    records = _read_lines(file)
    if not records:
        return (0, 0)

    # 按 (表, update_keys, 管道路由) 分组 —— 只按表分组的话，UpdateItem 会退化成
    # 普通 INSERT：PG 默认 ON CONFLICT DO NOTHING 会让它什么都不做**并返回成功**，
    # 于是这里报告成功、删掉文件，那次更新永久消失
    groups: dict[tuple[str, tuple[str, ...], tuple[str, ...] | None], list[Any]] = defaultdict(list)
    for record in records:
        keys = tuple(record.get("update_keys") or ())
        paths = record.get("pipelines")
        groups[(record["table"], keys, tuple(paths) if paths else None)].append(record)

    cache: dict[tuple[str, ...] | None, list[Any]] = {}

    def _pipelines(paths: tuple[str, ...] | None) -> list[Any]:
        if paths not in cache:
            cache[paths] = [tools.load_object(p)() for p in (paths or setting.ITEM_PIPELINES)]
        return cache[paths]

    ok = 0
    remaining: list[Any] = []
    try:
        for (table, keys, paths), rows in groups.items():
            datas = [r["data"] for r in rows]
            try:
                if keys:
                    done = all(p.update_items(table, datas, list(keys)) for p in _pipelines(paths))
                else:
                    done = all(p.save_items(table, datas) for p in _pipelines(paths))
            except Exception:
                # 管道可能压根没实现 update_items（基类直接抛）。这批算失败留在文件里，
                # 但**不能让整个回放崩掉** —— 那会连带丢掉本来能回放的其它记录
                log.exception("[{}] {} 条回放异常，留在文件里", table, len(datas))
                done = False
            if done:
                ok += len(datas)
            else:
                remaining.extend(rows)
    finally:
        for pipes in cache.values():
            for p in pipes:
                p.close()

    _rewrite(file, remaining)
    log.info("failed_items 回放：成功 {}，仍失败 {}", ok, len(remaining))
    return (ok, len(remaining))


def retry_requests(path: str | None = None) -> tuple[int, int]:
    """**探活**：把失败请求重新下载一遍，看目标是否已经可达。返回 (可达条数, 仍失败条数)。

    这不是数据恢复 —— **不跑回调、不产出 item、不入库**。所以记录一条都不删：
    可达只说明站点回来了，那些页面仍然需要真正重跑一遍。

    真要把数据抓回来，用 ``RETRY_FAILED_ON_START``（爬虫启动时把这个文件
    重新灌回队列，走完整的下载 → 回调 → 落库）。
    """
    file = Path(path or setting.FAILED_REQUEST_PATH)
    records = _read_lines(file)
    if not records:
        return (0, 0)

    ok = 0
    unreachable = 0
    try:
        for record in records:
            request = Request.from_dict(record)
            request.filter_repeat = False
            try:
                response = request.download()
            except RequestError:
                unreachable += 1
                continue
            if response.ok:
                ok += 1
            else:
                unreachable += 1
    finally:
        close_default_downloaders()

    # 一条都不删：可达 ≠ 数据回来了。删掉的话，这些请求的唯一副本就没了
    log.info(
        "failed_requests 探活：{} 条已可达，{} 条仍不可达；记录全部保留 —— "
        "这只是探活，数据要靠 RETRY_FAILED_ON_START 重跑才回得来",
        ok,
        unreachable,
    )
    return (ok, unreachable)
