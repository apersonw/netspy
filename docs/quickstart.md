# 快速开始

## 1. 生成项目

```bash
pip install "netspy[cli]"
netspy create -p news_crawler
```

生成：

```
news_crawler/
├── main.py                入口
├── setting.py             项目配置（运行时自动加载）
├── spiders/
│   └── news_crawler_spider.py
└── items/
```

## 2. 改爬虫

`spiders/news_crawler_spider.py`：

```python
import netspy as ns


class NewsCrawlerSpider(ns.AirSpider):
    def start_requests(self):
        yield ns.Request("https://news.ycombinator.com/", callback=self.parse)

    def parse(self, request, response):
        for row in response.css("span.titleline > a"):
            yield {
                "title": row.css("::text").get(),
                "url": row.css("::attr(href)").get(),
            }
```

## 3. 运行

```bash
cd news_crawler && python main.py
```

```
scheduler - 爬虫启动（4 个工作线程）
scheduler - 种子请求 1 条
pipeline.console - [items] {"title": "...", "url": "..."}
scheduler - 爬虫结束 | 请求成功 1 | 入库 30 条（去重 0，失败 0）
```

默认走 `ConsolePipeline`（打日志）。要落库，改 `setting.py` 的 `ITEM_PIPELINES`。

## 4. 调试选择器

```bash
netspy shell https://news.ycombinator.com/
>>> response.css("span.titleline > a::text").getall()
```

## 常见调整

`setting.py` 里：

```python
SPIDER_THREAD_COUNT = 8
SPIDER_MAX_RETRY_TIMES = 5
ITEM_PIPELINES = [
    "netspy.pipelines.mongo.MongoPipeline",
]
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "news"
```

或用环境变量：`NETSPY_SPIDER_THREAD_COUNT=8 python main.py`。

## 完整示例

仓库的 [`examples/`](https://github.com/apersonw/netspy/tree/main/examples)
里有可以直接跑的例子：

```bash
python examples/books_toscrape.py
```

演示两级抓取（列表页翻页 → 详情页）、`cb_kwargs` 传状态、`Item` 去重，
以及真实站点上该配的礼貌性设置。
