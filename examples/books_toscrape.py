"""两级抓取：列表页翻页 → 详情页。

目标是 [books.toscrape.com](https://books.toscrape.com/) —— Zyte 专门为爬虫练习
搭建的站点，可以放心跑。

演示的东西：

- ``cb_kwargs`` 在回调之间传状态（这里传页码）
- ``response.urljoin`` 处理相对链接
- ``Item`` + ``__unique_key__``，配合去重实现「重跑不重复入库」
- 礼貌性设置：robots.txt、单域并发上限、请求间隔

跑起来：

    pip install netspy
    python examples/books_toscrape.py
"""

from __future__ import annotations

import netspy as ns
from netspy import setting

BASE = "https://books.toscrape.com/"
MAX_LIST_PAGES = 3  # 只抓前几页，示例不必贪多


class BookItem(ns.Item):
    __table_name__ = "books"
    __unique_key__ = ["url"]  # 按 url 去重：重跑不会产生重复条目

    url: str
    title: str
    price: str
    stock: str
    rating: str


class BookSpider(ns.AirSpider):
    def start_requests(self):  # type: ignore[no-untyped-def]
        yield ns.Request(BASE, callback=self.parse_list, cb_kwargs={"page": 1})

    def parse_list(self, request, response, page):  # type: ignore[no-untyped-def]
        # 详情页
        for href in response.css("h3 > a::attr(href)").getall():
            yield ns.Request(response.urljoin(href), callback=self.parse_book)

        # 翻页：cb_kwargs 把页码带到下一次回调，用来控制深度
        next_href = response.css("li.next > a::attr(href)").get()
        if next_href and page < MAX_LIST_PAGES:
            yield ns.Request(
                response.urljoin(next_href),
                callback=self.parse_list,
                cb_kwargs={"page": page + 1},
            )

    def parse_book(self, request, response):  # type: ignore[no-untyped-def]
        item = BookItem()
        item.url = response.url
        item.title = response.css("div.product_main h1::text").get()
        item.price = response.css("div.product_main p.price_color::text").get()
        item.stock = "".join(response.css("div.product_main p.availability::text").getall()).strip()
        # 星级藏在 class 里：<p class="star-rating Three">
        item.rating = (
            (response.css("p.star-rating::attr(class)").get() or "")
            .replace("star-rating", "")
            .strip()
        )
        yield item


if __name__ == "__main__":
    # 礼貌性：真实站点上这几项比抓得快更重要
    setting.ROBOTS_OBEY = True  # 遵守 robots.txt
    setting.CONCURRENT_REQUESTS_PER_DOMAIN = 4  # 单域最多 4 个在途
    setting.DOWNLOAD_DELAY = 0.3  # 同域两次请求至少隔 0.3s
    # 默认管道把 Item 打到日志；换成 CsvPipeline / MysqlPipeline 即可落库
    setting.ITEM_PIPELINES = ["netspy.pipelines.console.ConsolePipeline"]

    BookSpider().start()
