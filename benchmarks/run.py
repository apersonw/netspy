"""跑吞吐矩阵，回答「框架在哪里触顶、为什么触顶」。

用法：
    python benchmarks/run.py --quick     # 小矩阵，几十秒
    python benchmarks/run.py             # 完整矩阵

最关键的一列是 **峰值并发**（服务端观测）：它直接回答
「在途连接数是不是真的卡在 SPIDER_THREAD_COUNT 上」。
"""

from __future__ import annotations

import argparse
import gc
import os
import statistics
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from server import BenchServer

import netspy as ns
from netspy import setting
from netspy.network.downloader import close_default_downloaders


def _rss_mb() -> float:
    try:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux 是 KB，macOS 是 byte
        return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024
    except Exception:
        return 0.0


@dataclass
class Result:
    threads: int
    downloader: str
    session: bool
    latency: float
    parse: str
    qps: float
    peak_inflight: int
    avg_inflight: float
    wall: float
    threads_peak: int
    rss_mb: float
    ideal_qps: float = 0.0

    @property
    def efficiency(self) -> float:
        """实测 QPS / 理论 QPS（并发 ÷ 延迟）。1.0 = 每个线程除了等响应什么都没干。"""
        return self.qps / self.ideal_qps if self.ideal_qps else 0.0

    @property
    def utilization(self) -> float:
        """**平均**在途 / 线程数。接近 1 说明线程确实一直在途上；小于 1 说明大部分时间花在别处。"""
        return self.avg_inflight / self.threads if self.threads else 0.0


def _make_spider(url: str, n: int, parse_mode: str, session: bool) -> type[ns.AirSpider]:
    class BenchSpider(ns.AirSpider):
        def start_requests(self):  # type: ignore[no-untyped-def]
            for i in range(n):
                # 必须传 Request(use_session=)：setting.USE_SESSION 是个死配置，
                # 框架从没读过它（benchmark 最早就是因为两行数字一模一样才暴露出来的）
                yield ns.Request(
                    f"{url}/?i={i}",
                    callback=self.parse,
                    filter_repeat=False,
                    use_session=session,
                )

        def parse(self, request, response):  # type: ignore[no-untyped-def]
            if parse_mode == "heavy":
                # 真走一遍 lxml：选择器 + 属性提取，模拟真实解析成本
                titles = response.css("li.item a::text").getall()
                prices = response.xpath('//span[@class="price"]/text()').getall()
                if not titles or not prices:
                    raise RuntimeError("解析拿不到数据，靶子 body 变了？")
            return None

    return BenchSpider


def _run_once(
    threads: int,
    downloader: str,
    session: bool,
    latency: float,
    parse: str,
    n: int,
    domain_cap: int = 0,
) -> Result:
    setting.reload()
    # **必须显式设**：压测所有请求都打同一个域，而 CONCURRENT_REQUESTS_PER_DOMAIN
    # 默认是 8 —— 不控制它的话，量到的是这个上限而不是框架的天花板。
    # 实测差别（50ms、sync、开 session）：
    #   上限 8   4→128 线程 QPS：75 → 146 → 146 → 146（在途卡死在 7.4）
    #   上限关   4→128 线程 QPS：74 → 240 → 542 → 608
    # 结论完全相反，而这个变量以前没人管。默认关掉 = 测框架自身；
    # 想看真实部署的样子就把它设回 8。
    setting.CONCURRENT_REQUESTS_PER_DOMAIN = domain_cap
    setting.SPIDER_THREAD_COUNT = threads
    setting.DOWNLOADER_ASYNC = downloader == "async"
    setting.DOWNLOADER_ASYNC_CONCURRENCY = max(threads * 2, 200)
    setting.ITEM_PIPELINES = []
    setting.RANDOM_USER_AGENT = False
    setting.ITEM_FILTER_ENABLE = False
    setting.LOG_LEVEL = "ERROR"
    # 基准工具**自己占用 stdout** 打表格；框架结束时那行 NETSPY_RUN_SUMMARY
    # 是给编排层读容器日志用的，在这里只会混进表里 —— 关掉
    setting.RUN_SUMMARY_ENABLE = False
    setting.METRICS_ENABLE = False
    setting.WARNING_ENABLE = False
    from netspy.utils import log

    log.configure()

    peak_threads = 0

    def watch() -> None:
        nonlocal peak_threads
        while not stop.is_set():
            peak_threads = max(peak_threads, threading.active_count())
            time.sleep(0.02)

    stop = threading.Event()
    with BenchServer(latency=latency) as srv:
        spider_cls = _make_spider(srv.url, n, parse, session)
        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        gc.collect()
        t0 = time.monotonic()
        try:
            spider_cls().start()
        finally:
            wall = time.monotonic() - t0
            stop.set()
            watcher.join(timeout=1)
            close_default_downloaders()
        s = srv.stats
        if s.total < n:
            print(f"    ⚠ 只完成 {s.total}/{n}，这一格数字不可信", file=sys.stderr)
        # 用服务端计时（首个请求到达 → 末个请求完成）而不是 wall clock：
        # 后者含爬虫启动与 DONE_CHECK_TIMES×DONE_CHECK_INTERVAL=1.5s 的结束检测轮询，
        # 小样本下这段固定开销会淹没真实吞吐
        return Result(
            threads=threads,
            downloader=downloader,
            session=session,
            latency=latency,
            parse=parse,
            qps=s.qps,
            ideal_qps=threads / latency if latency else 0.0,
            peak_inflight=s.max_inflight,
            avg_inflight=s.avg_inflight,
            wall=wall,
            threads_peak=peak_threads,
            rss_mb=_rss_mb(),
        )


def _median_of(rounds: int, **kw: object) -> Result:
    """多轮取中位数：单轮噪声足够大到能翻转结论。"""
    runs = [_run_once(**kw) for _ in range(rounds)]  # type: ignore[arg-type]
    best = sorted(runs, key=lambda r: r.qps)[len(runs) // 2]
    best.qps = statistics.median(r.qps for r in runs)
    best.peak_inflight = int(statistics.median(r.peak_inflight for r in runs))
    return best


def _table(results: list[Result], domain_cap: int = 0) -> str:
    cap_note = (
        "**单域并发上限已关**（测框架自身的天花板；真实部署默认是 8，届时会先撞上它）"
        if domain_cap <= 0
        else f"**单域并发上限 = {domain_cap}**（真实部署的样子）"
    )
    head = (
        f"{cap_note}\n\n"
        "| 线程数 | 下载器 | session | 延迟 | parse | QPS | 理论 QPS | 效率 | "
        "平均在途 | 峰值 | 在途/线程 | RSS(MB) |\n"
        "|---:|---|:-:|---:|---|---:|---:|---:|---:|---:|---:|---:|\n"
    )
    rows = "".join(
        f"| {r.threads} | {r.downloader} | {'✓' if r.session else '✗'} | "
        f"{r.latency * 1000:.0f}ms | {r.parse} | {r.qps:,.0f} | {r.ideal_qps:,.0f} | "
        f"{r.efficiency:.0%} | {r.avg_inflight:.1f} | {r.peak_inflight} | "
        f"{r.utilization:.2f} | {r.rss_mb:.0f} |\n"
        for r in results
    )
    return head + rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="小矩阵，用于冒烟")
    ap.add_argument("--rounds", type=int, default=3, help="每格跑几轮取中位数")
    ap.add_argument("--requests", type=int, default=600, help="每格发多少请求")
    ap.add_argument(
        "--domain-cap",
        type=int,
        default=0,
        help="单域并发上限（0=关，测框架天花板；8=框架默认，测真实部署的样子）",
    )
    args = ap.parse_args()

    if args.quick:
        grid = [
            (t, d, sess, 0.05, "light")
            for t in (4, 32)
            for d in ("sync", "async")
            for sess in (False, True)
        ]
        rounds, n = 1, 200
    else:
        grid = []
        for lat in (0.05, 0.2):
            for parse in ("light", "heavy"):
                for dl in ("sync", "async"):
                    for sess in (False, True):
                        for th in (4, 16, 64, 128, 256):
                            grid.append((th, dl, sess, lat, parse))
        rounds, n = args.rounds, args.requests

    print(
        f"# Netspy 吞吐画像\n\n共 {len(grid)} 格 × {rounds} 轮 × {n} 请求"
        f"　（Python {sys.version_info.major}.{sys.version_info.minor}, {sys.platform}, "
        f"CPU {os.cpu_count()}）\n"
    )

    results: list[Result] = []
    for i, (th, dl, sess, lat, parse) in enumerate(grid, 1):
        print(
            f"  [{i}/{len(grid)}] threads={th} {dl} session={sess} {lat * 1000:.0f}ms {parse} …",
            file=sys.stderr,
            flush=True,
        )
        results.append(
            _median_of(
                rounds,
                threads=th,
                downloader=dl,
                session=sess,
                latency=lat,
                parse=parse,
                n=n,
                domain_cap=args.domain_cap,
            )
        )
    print(_table(results, args.domain_cap))

    # 触顶点：QPS 不再随线程数上涨的地方
    print("\n## 读法\n")
    print("- **在途/线程 ≈ 1** → 线程确实都在途上，「1 线程 1 在途」成立")
    print(
        "- **在途/线程 明显 < 1** → 瓶颈在别处（锁争用 / 解析 / 缓冲区），"
        "此时上 async 批量分发解决不了问题"
    )
    print("- **QPS 随线程数增长到某点就平** → 那个点就是框架的实际天花板")
    print(
        "- **效率** = 实测 ÷ 理论（并发 ÷ 延迟）。远低于 100% 说明每个请求在"
        "「等响应」之外还花了大量时间（建连、锁、解析）"
    )


if __name__ == "__main__":
    main()
