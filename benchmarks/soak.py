"""长跑内存画像：区分「线程栈开销」与「真泄漏」。

两个问题分开测，别混为一谈：

1. **RSS vs 线程数**（跑同样多的请求）—— 一次性开销，随线程数线性增长是正常的
2. **RSS vs 时间**（线程数固定）—— **只有这个持续上涨才是泄漏**

用法：
    python benchmarks/soak.py --mode threads      # 问题 1
    python benchmarks/soak.py --mode time --seconds 120   # 问题 2
    python benchmarks/soak.py --mode time --seconds 120 --trace   # 附 tracemalloc 归因
"""

from __future__ import annotations

import argparse
import gc
import os
import subprocess
import sys
import threading
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from server import BenchServer

import netspy as ns
from netspy import setting
from netspy.network.downloader import close_default_downloaders


def rss_mb() -> float:
    """当前 RSS（不是峰值），MB。

    `resource.getrusage` 给的是 ru_maxrss —— **峰值，只增不减**，同一进程内
    连跑多个配置会互相污染，所以不能用它。

    优先读 /proc（Linux，无需外部进程）；macOS 没有 /proc，回落到 ps。
    **不能只用 ps**：python:3.12-slim 之类的精简镜像根本没装它（procps 包），
    直接 FileNotFoundError —— 这是在 Linux 容器里实测撞出来的。
    """
    statm = Path("/proc/self/statm")
    if statm.exists():
        try:
            resident_pages = int(statm.read_text().split()[1])
            return resident_pages * os.sysconf("SC_PAGE_SIZE") / (1024 * 1024)
        except (OSError, ValueError, IndexError):
            pass
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
    except (FileNotFoundError, OSError):
        return float("nan")
    return int(out) / 1024 if out.isdigit() else float("nan")


def _configure(threads: int) -> None:
    setting.reload()
    setting.SPIDER_THREAD_COUNT = threads
    setting.ITEM_PIPELINES = []
    setting.LOG_LEVEL = "ERROR"
    setting.CONCURRENT_REQUESTS_PER_DOMAIN = 0
    setting.ROBOTS_OBEY = False
    setting.RANDOM_USER_AGENT = False
    # 基准工具**自己占用 stdout** 打表格；框架结束时那行 NETSPY_RUN_SUMMARY
    # 是给编排层读容器日志用的，在这里只会混进表里（test_benchmarks_smoke 解析
    # 的就是这段 stdout，0.22.0 发出去后 CI 四版齐红）—— 关掉
    setting.RUN_SUMMARY_ENABLE = False
    from netspy.utils import log

    log.configure()


def _spider(url: str, n: int, session: bool) -> ns.AirSpider:
    class SoakSpider(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(n):
                yield ns.Request(
                    f"{url}/?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                    use_session=session,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            # 真解析一遍，让对象分配贴近实际
            response.css("li.item a::text").getall()
            return None

    return SoakSpider()


# ----------------------------------------------------------------------
def mode_threads(counts: list[int], n: int) -> None:
    """RSS vs 线程数。**每个配置一个干净子进程**，避免峰值残留互相污染。"""
    print("## RSS vs 线程数\n")
    print("| 线程数 | 跑前 RSS | 跑后 RSS | 增量 | 每线程 |")
    print("|---:|---:|---:|---:|---:|")
    for th in counts:
        code = (
            f"import sys; sys.path.insert(0, {str(Path(__file__).parent)!r});"
            f"from soak import _one_shot; _one_shot({th}, {n})"
        )
        out = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, check=False
        ).stdout.strip()
        print(out or f"| {th} | — | — | 子进程失败 | — |")


def _one_shot(threads: int, n: int) -> None:
    """在干净子进程里跑一轮，打印一行 markdown。"""
    _configure(threads)
    gc.collect()
    before = rss_mb()
    with BenchServer(latency=0.005, n_items=40) as srv:
        _spider(srv.url, n, session=True).start()
    close_default_downloaders()
    gc.collect()
    after = rss_mb()
    delta = after - before
    print(
        f"| {threads} | {before:.0f}MB | {after:.0f}MB | "
        f"{delta:+.0f}MB | {delta / threads:+.2f}MB |"
    )


# ----------------------------------------------------------------------
def mode_time(seconds: float, threads: int, trace: bool) -> None:
    """RSS vs 时间，线程数固定。**只有这条持续上涨才是泄漏。**"""
    print(f"## RSS vs 时间（{threads} 线程，{seconds:.0f}s）\n")
    _configure(threads)
    if trace:
        tracemalloc.start(10)

    samples: list[tuple[float, float]] = []
    stop = threading.Event()

    def sampler() -> None:
        t0 = time.monotonic()
        while not stop.is_set():
            samples.append((time.monotonic() - t0, rss_mb()))
            stop.wait(2.0)

    with BenchServer(latency=0.005, n_items=40) as srv:
        watcher = threading.Thread(target=sampler, daemon=True)
        watcher.start()
        gc.collect()
        snap_before = tracemalloc.take_snapshot() if trace else None
        deadline = time.monotonic() + seconds
        rounds = 0
        while time.monotonic() < deadline:
            _spider(srv.url, 400, session=True).start()
            rounds += 1
        stop.set()
        watcher.join(timeout=3)

    close_default_downloaders()
    gc.collect()

    print(f"跑了 {rounds} 轮 × 400 请求\n")
    print("| 时刻 | RSS |")
    print("|---:|---:|")
    for at, rss in samples[:: max(len(samples) // 12, 1)]:
        print(f"| {at:5.0f}s | {rss:6.1f}MB |")

    if len(samples) >= 8:
        # 只看总斜率会误判：分配器 arena 增长、缓存预热都会让曲线前重后轻，
        # 那是**收敛**不是泄漏。比较前后半程的斜率才能分开这两种。
        mid = len(samples) // 2

        def _slope(chunk: list[tuple[float, float]]) -> float:
            span_min = (chunk[-1][0] - chunk[0][0]) / 60 or 1e-9
            return (chunk[-1][1] - chunk[0][1]) / span_min

        first, second = _slope(samples[:mid]), _slope(samples[mid:])
        # 中段的台阶会把后半程斜率拉高，最后四分之一才是最能说明问题的一段：
        # 泄漏在那里仍会稳定上涨，收敛则会压平
        last_q = _slope(samples[-max(len(samples) // 4, 2) :])
        print(f"\n- RSS：{samples[0][1]:.1f}MB → {samples[-1][1]:.1f}MB")
        print(f"- 前半程斜率：{first:+.2f} MB/分钟")
        print(f"- 后半程斜率：{second:+.2f} MB/分钟")
        print(f"- **最后 1/4 斜率：{last_q:+.2f} MB/分钟** ← 判据看这个")
        if last_q < 1.0:
            verdict = "✅ 已收敛 —— 前期增长是分配器 arena / 缓存预热，不是泄漏"
        elif last_q < first * 0.3:
            verdict = "🟡 仍在减速，像收敛中；再跑久一点确认"
        else:
            verdict = "⚠ 末段仍稳定上涨 —— 这才是泄漏的形状"
        print(f"- 判断：{verdict}")

    if trace and snap_before is not None:
        snap_after = tracemalloc.take_snapshot()
        print("\n### tracemalloc 增长 Top 10\n")
        print("```")
        for stat in snap_after.compare_to(snap_before, "lineno")[:10]:
            print(f"  {stat}")
        print("```")
        tracemalloc.stop()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["threads", "time"], default="threads")
    ap.add_argument("--seconds", type=float, default=120.0)
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--requests", type=int, default=800)
    ap.add_argument("--trace", action="store_true", help="附 tracemalloc 归因")
    args = ap.parse_args()

    if args.mode == "threads":
        mode_threads([4, 16, 32, 64, 128], args.requests)
    else:
        mode_time(args.seconds, args.threads, args.trace)


if __name__ == "__main__":
    main()
