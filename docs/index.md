# Netspy

一个上手简单、结构清晰的 Python 爬虫框架：
你只写 `start_requests` 和 `parse`，框架负责调度、下载、重试、去重、批量落库。

## 安装

```bash
pip install netspy            # 核心
pip install "netspy[all]"     # 含浏览器渲染 / MongoDB / MySQL / Redis / CLI / 指标
```

按需选择：`netspy[render]`（Playwright）、`[mongo]`、`[mysql]`、`[redis]`、`[cli]`、`[metrics]`。

## 30 秒示例

```python
import netspy as ns


class NewsSpider(ns.AirSpider):
    def start_requests(self):
        for page in range(1, 6):
            yield ns.Request(f"https://example.com/news?p={page}", callback=self.parse)

    def parse(self, request, response):
        for a in response.xpath('//a[@class="title"]'):
            item = ns.Item()
            item.table_name = "news"
            item.title = a.xpath("./text()").extract_first()
            item.url = response.urljoin(a.xpath("./@href").extract_first())
            yield item


if __name__ == "__main__":
    NewsSpider().start()
```

或用脚手架：

```bash
netspy create -p news_crawler
cd news_crawler && python main.py
```

## 当前能力

- **AirSpider** —— 单进程、多线程、内存队列，跑通「请求 → 解析 → 落库」闭环并优雅退出
- **Spider** —— [Redis 分布式](distributed.md)：多进程 / 多机共享队列与去重、断点续爬、多节点心跳
- **TaskSpider** —— [从任务源持续消费](distributed.md#taskspider)（Redis / DB），多节点分摊
- **BatchSpider** —— [周期性批次采集](batch-spider.md)：MySQL 任务表 + 批次记录 + 进度 / 防丢，master / worker 分离
- `Request` / `Response`（httpx + parsel）、自动重试、失败兜底钩子；可选[异步下载器](async-kernel.md)（共享连接池 / HTTP/2）
- `Item` / `UpdateItem`、`Pipeline`（Console / CSV / MongoDB / [MySQL](item-pipeline.md#mysql)）、请求级 + Item 级去重（内存 / Redis 布隆 / 精确）
- 浏览器渲染 `Request(render=True)`（Playwright 渲染池）
- 下载中间件链、代理池接口、[账号 / Cookie 池](user-pool.md)（掉登录自动换号）
- 指标（Prometheus exporter）、卡死 / 失败率告警（飞书 / 邮件）
- CLI 脚手架（`create -i --table` 读 MySQL 表反射 Item）、`shell` 调试、`retry` 回放

下一步见 [Roadmap](roadmap.md)。
