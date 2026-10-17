# Netspy

[![PyPI](https://img.shields.io/pypi/v/netspy)](https://pypi.org/project/netspy/)
[![Python](https://img.shields.io/pypi/pyversions/netspy)](https://pypi.org/project/netspy/)
[![CI](https://github.com/apersonw/netspy/actions/workflows/ci.yml/badge.svg)](https://github.com/apersonw/netspy/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-apersonw.github.io-teal)](https://apersonw.github.io/netspy/)

一个上手简单、结构清晰的 Python 爬虫框架：
你只写 `start_requests` 和 `parse`，框架负责调度、下载、重试、去重、批量落库。

> 单机（`AirSpider`）到分布式（`Spider` / `TaskSpider` / `BatchSpider`）全部可用，
> 并支持[浏览器 TLS 指纹伪装](https://apersonw.github.io/netspy/anti-bot/)。
> 变更见 [CHANGELOG](CHANGELOG.md)，后续规划见 [Roadmap](https://apersonw.github.io/netspy/roadmap/)。

## 安装

```bash
pip install netspy            # 核心
pip install "netspy[all]"     # 含渲染 / 各类存储 / Redis / CLI / 指标 / 指纹伪装
```

也可以按需装单项：`render` · `mongo` · `mysql` · `postgres` · `elasticsearch` · `kafka` ·
`redis` · `cli` · `metrics` · `curl`。

## 快速开始

```bash
pip install "netspy[cli]"
netspy create -p news_crawler
cd news_crawler && python main.py
```

或直接写：

```python
import netspy as ns


class NewsSpider(ns.AirSpider):
    def start_requests(self):
        yield ns.Request("https://news.ycombinator.com/", callback=self.parse)

    def parse(self, request, response):
        for a in response.css("span.titleline > a"):
            yield {"title": a.css("::text").get(), "url": a.css("::attr(href)").get()}


NewsSpider().start()
```

## 示例

[`examples/`](examples/) 里有可以直接跑的完整例子：

```bash
python examples/books_toscrape.py   # 两级抓取：列表页翻页 → 详情页
```

## 文档

完整文档：**<https://apersonw.github.io/netspy/>**
（本地预览：`pip install "netspy[docs]" && mkdocs serve`）

## 能力一览

| | |
|---|---|
| 运行时 | 单进程多线程、优先级队列、结束检测、`Ctrl-C` 优雅排空、崩溃 dump + `retry` 回放 |
| 网络 | `Request`/`Response`（httpx + parsel）、自动重试、`validate`/`failed_request` 钩子、随机 UA、会话复用 |
| 礼貌性 | `robots.txt`（含 `Crawl-delay`）、per-domain 限速 + 单域并发上限、**跨节点全局限速**（Redis）、429/503 读 `Retry-After` 整域降速 |
| 稳健性 | 状态码策略（错误页不当数据入库）、指数退避 + 抖动、同域连续失败熔断、运行时长上限、响应体大小上限 + Content-Type 白名单、`SIGINT`/`SIGTERM` 优雅停止、**任务租约 + 落库后才销账**（节点被硬杀，任务能回收、数据不丢） |
| 分布式 | `Spider`：Redis 队列 + 布隆去重 + 断点续爬 + 种子一次性锁 + 多节点心跳结束检测 |
| 任务驱动 | `TaskSpider` 从任务源持续消费；`BatchSpider` 周期批次采集（任务表状态机 + 进度追踪 + 防丢） |
| 数据 | `Item`/`UpdateItem`、`Pipeline`（Console/CSV/MongoDB/MySQL/PostgreSQL/Elasticsearch/Kafka）、请求级 + Item 级去重（布隆/精确） |
| 渲染 | `Request(render=True)` —— Playwright 渲染池、`wait_for`/`render_time`/`render_script` |
| 反爬 | TLS / HTTP2 指纹伪装（`impersonate` 真实浏览器）、Cloudflare / Akamai 挑战页识别 |
| 扩展 | 下载中间件链、**代理池 / 账号池**（耗尽时不会悄悄降级成直连或匿名）、掉登录自动换号 |
| 观测 | Prometheus exporter、卡死/失败率告警（**飞书 / 钉钉 / 企业微信 / 邮件**，发送失败会记录）、`debug=True` |
| 开发体验 | **响应缓存**（重跑读本地文件，不再反复打目标站） |
| 工具 | `netspy create/shell/retry/cache`，`create -i --table` 读表结构反射生成 Item |

## 开发

```bash
conda env create -f environment.yml && conda activate netspy
pre-commit install
pytest && ruff check . && mypy
```

CI（GitHub Actions）：ruff + mypy(strict) + pytest（Python 3.10–3.13）+ mkdocs build。

## License

[MIT](LICENSE)
