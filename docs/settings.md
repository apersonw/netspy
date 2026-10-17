# 配置项

优先级（后者覆盖前者）：

1. 框架默认值（`netspy/setting.py`）
2. 运行目录下的 `setting.py` / `settings.py`（或 `NETSPY_SETTING` 指定的文件）
3. 环境变量 `NETSPY_<KEY>`（按默认值类型自动转换，dict / list 传 JSON）

```bash
NETSPY_SPIDER_THREAD_COUNT=8 NETSPY_LOG_LEVEL=DEBUG python main.py
```

爬虫内 `__custom_setting__` 会在实例化时合并进全局配置。

## 调度 / 运行时

| 配置 | 默认 | 说明 |
|---|---|---|
| `SPIDER_THREAD_COUNT` | `4` | 工作线程数。`Spider(thread_count=N)` 会覆盖它，**分片与连接池上限跟着真实值走**，见下 |
| `SPIDER_MAX_RETRY_TIMES` | `3` | 单请求最大重试次数 |
| `SESSION_CACHE_SIZE` | `16` | 开 `USE_SESSION` 且用代理池时，最多缓存多少个代理的连接池（分片后上限按片数自动放大） |
| `SESSION_SHARD_THREADS` | `32` | 每个代理连接池最多服务多少线程，超出再开一片；`0` = 不分片。见下方说明 |
| `CURL_SESSION_SHARD_THREADS` | `16` | 同上，但用于 curl 下载器 —— **拐点是分别量出来的，不是同一个数** |
| `SPIDER_RETRY_INTERVAL` | `0.0` | 重试前等待秒数 |
| `COLLECTOR_TASK_COUNT` | `100` | collector 单次从队列取多少任务 |
| `DONE_CHECK_TIMES` / `DONE_CHECK_INTERVAL` | `3` / `0.5` | 结束检测的复查次数与间隔 |
| `DUMP_UNFINISHED_ON_EXIT` | `True` | 中断时 dump 未完成请求 |
| `FAILED_REQUEST_PATH` | `"failed_requests.jsonl"` | 未完成 / 重试耗尽的请求落盘位置（相对 cwd）。分布式下重试耗尽的进 Redis `<ns>:failed_requests` |
| `RETRY_FAILED_ON_START` | `False` | 启动时把失败请求重新灌回队列，走完整的下载 → 回调 → 落库：单机读 `FAILED_REQUEST_PATH`，分布式另外读 Redis 失败列表（多节点同时启动只有一个节点回放）。`netspy retry --requests` 只是探活，这个才会把数据抓回来 |
| `REQUEST_BUFFER_MAX_CACHED` | `1000` | 请求缓冲攒到这么多立刻推进队列 |
| `BUFFER_FLUSH_INTERVAL` | `0.1` | 请求 / 数据缓冲的定时 flush 间隔（秒） |
| `RUN_SUMMARY_ENABLE` | `True` | 结束时在 stdout 打一行机器可读摘要 `NETSPY_RUN_SUMMARY {...}`，供编排层读容器日志区分「跑了」和「抓了」，见[运行摘要](observability.md#运行摘要机器可读) |

## 请求

| 配置 | 默认 | 说明 |
|---|---|---|
| `REQUEST_TIMEOUT` | `22.0` | 秒 |
| `RANDOM_USER_AGENT` | `True` | 自动注入随机 UA |
| `USE_SESSION` | `False` | 复用 httpx 连接（连同 cookie jar）。收益随线程数变化，见下表；注意开启后 cookie 会跨请求共享 |
| `DOWNLOADER_MIDDLEWARES` | `[]` | 下载中间件点号路径 |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | `8` | 单域最大在途；`0` = 不限。**进程内生效**，见[限速](spider.md#限速) |
| `DOWNLOAD_DELAY` | `0.0` | 同域两次请求最小间隔（秒）；`0` = 不限。默认进程内生效，见 `GLOBAL_THROTTLE` |
| `RANDOMIZE_DOWNLOAD_DELAY` | `True` | 给上面的间隔加 ±50% 抖动 |
| `GLOBAL_THROTTLE` | `False` | 让 `DOWNLOAD_DELAY` 跨节点全局生效（需 Redis）。关闭时 N 个节点就是 N 倍速率，见[全局限速](spider.md#跨节点全局限速) |
| `RESPONSE_CACHE_ENABLE` | `False` | 缓存响应，重跑读本地文件。**只在开发期开**，见[响应缓存](spider.md#响应缓存开发调试用) |
| `RESPONSE_CACHE_PATH` | `".netspy_cache"` | 缓存目录 |
| `RESPONSE_CACHE_EXPIRE` | `3600.0` | 缓存有效期（秒）；`0` = 不过期 |
| `MAX_RESPONSE_SIZE` | `33554432` | 响应体上限（字节，32MB）；`0` = 不限。**行为变更**，见[资源边界](spider.md#资源边界) |
| `ALLOWED_CONTENT_TYPES` | `[]` | Content-Type 前缀白名单；空 = 不过滤。命中不了的响应不读 body 直接断开 |
| `ROBOTS_OBEY` | `False` | 遵守 robots.txt。**脚手架生成的项目里默认 `True`**，见 [robots.txt](spider.md#robotstxt) |

> 结束行里的「robots 拦截」是「丢弃」的**子集**而非并列项 —— 被 robots 拦下的请求同时计入两者，求和会重复计。
| `ROBOTS_USER_AGENT` | `"*"` | 按哪个 UA 组匹配（随机 UA 下按具体 UA 匹配没有意义） |
| `ROBOTS_CACHE_TTL` | `3600.0` | robots.txt 缓存时长（秒），`0` = 永不过期 |
| `CIRCUIT_FAILURE_THRESHOLD` | `10` | 同域连续失败多少次跳闸；`0` = 关闭。**404 等 4xx 不计**，见[熔断](spider.md#熔断) |
| `CIRCUIT_COOLDOWN` | `60.0` | 跳闸后该域冷却多久（秒） |
| `SPIDER_MAX_RUNTIME` | `0.0` | 运行时长上限（秒），到点优雅停止；`0` = 不限 |
| `CHECK_STATUS_CODE` | `True` | **0.7.0 起默认开启**：非 2xx/3xx 不再进 `parse()`。设 `False` 回到旧行为 |
| `RETRY_STATUS_CODES` | `[429,500,502,503,504]` | 这些码触发重试 |
| `ACCEPT_STATUS_CODES` | `[]` | 除 2xx/3xx 外还当成功的码，如 `[404]` 让 `parse` 自己处理 |
| `RETRY_AFTER_MAX` | `60.0` | 429/503 的 `Retry-After` 最多认多久（秒）；超过判失败。`0` = 不读该头 |
| `RETRY_BACKOFF` | `0.0` | 指数退避基数（秒），`0` = 关，沿用 `SPIDER_RETRY_INTERVAL` |
| `DOWNLOADER_ASYNC` | `False` | 普通请求走 [`AsyncHttpxDownloader`](async-kernel.md)（共享连接池 / HTTP/2） |
| `DOWNLOADER_ASYNC_CONCURRENCY` | `200` | async 下载器的信号量与连接池上限。**不是实际在途数** —— 实际在途由 `SPIDER_THREAD_COUNT` 决定，见下 |
| `ASYNC_THREADS_PER_LOOP` | `16` | 每个事件循环最多服务多少工作线程，超出再开一个；`0` = 不分片。见下 |
| `HTTPX_HTTP2` | `False` | httpx 开 HTTP/2（需 `pip install "httpx[http2]"`） |
| `DOWNLOADER_IMPERSONATE` | `""` | 伪装浏览器 TLS / HTTP2 指纹，填 `"chrome"` 等即启用（需 `pip install "netspy[curl]"`），见[反爬对抗](anti-bot.md) |
| `ANTIBOT_DETECT` | `True` | 识别 Cloudflare / Akamai 挑战页，命中抛 `AntiBotError`（走既有重试 + 换代理） |

## Item / 管道 / 去重

| 配置 | 默认 | 说明 |
|---|---|---|
| `ITEM_PIPELINES` | `["...ConsolePipeline"]` | 管道列表 |
| `ITEM_MAX_CACHED_COUNT` | `5000` | 达到即 flush |
| `ITEM_DEFAULT_TABLE` | `"items"` | 裸 dict 落库的表名 |
| `ITEM_FILTER_ENABLE` | `True` | Item 级去重开关 |
| `CSV_OUTPUT_DIR` | `"."` | CsvPipeline 输出目录 |
| `FAILED_ITEM_PATH` | `"failed_items.jsonl"` | 写库失败的数据落盘位置（相对 cwd），`netspy retry --items` 回放 |
| `DEDUP_FILTER` | `"memory"` | `memory`（布隆）\| `lite`（精确）\| `redis` \| `redis-set` |
| `DEDUP_TO_MD5` | `True` | 直接用 `Dedup` 传原始值时先 md5 再存（请求 / Item 指纹本身已是 md5，不受影响） |
| `DEDUP_ERROR_RATE` | `1e-6` | 布隆单层误判率（分层后总上界为其 2 倍） |
| `SPIDER_TASK_LEASE` | `600.0` | 任务租约（秒）。节点被硬杀时靠它回收，代价是「至少一次」语义；`0` = 关闭。见[节点被硬杀之后](distributed.md#节点被硬杀之后) |
| `SPIDER_STARTUP_GRACE` | `10.0` | 启动宽限（秒）：没拿到过任务的节点在此期间不判定结束。防止多节点同启时 N-1 个立刻退出，见[多节点同时启动](distributed.md#多节点同时启动) |
| `DEDUP_MAX_LAYERS` | `4` | 布隆最多几层。默认容量 ×15、内存 57MB，见[去重的容量](distributed.md#去重的容量) |
| `DEDUP_WARN_FILL_RATE` | `0.8` | 填到这个比例就告警。**超容会静默丢 URL**，宁可早报 |
| `RUN_ID` | `""` | 一次「运行」的标识。设了之后队列 / 种子锁 / 在途 / 心跳 / 失败列表落到 `<prefix>:<redis_key>:run:<id>` 下，每次运行互不相干；同一个 id 再起 = 续那次运行。**定时重跑的分布式任务必须设**，见[运行作用域](distributed.md#运行作用域定时重跑) |
| `DEDUP_SCOPE` | `"auto"` | 去重落在哪：`run`（本次运行下，重跑从头抓）\| `spider`（跨运行持久，增量爬）\| `auto`（有 `RUN_ID` 就是 run，否则 spider） |
| `RUN_TTL` | `604800` | 运行作用域下 key 的保留秒数（7 天）。跑着时由心跳续期，结束后到期自动清理 |
| `MONGO_URI` / `MONGO_DB` | `localhost` / `netspy` | |

## MySQL

`pip install "netspy[mysql]"`。用于 `MysqlPipeline` 与 `create -i --table`。

| 配置 | 默认 | 说明 |
|---|---|---|
| `MYSQL_HOST` / `MYSQL_PORT` | `localhost` / `3306` | |
| `MYSQL_USER` / `MYSQL_PASSWORD` | `root` / `""` | |
| `MYSQL_DB` | `"netspy"` | 库名 |
| `MYSQL_POOL_SIZE` | `5` | 连接池上限 |
| `MYSQL_UPDATE_ON_DUPLICATE` | `True` | `save_items` 用 `INSERT ... ON DUPLICATE KEY UPDATE` |

## PostgreSQL

`pip install "netspy[postgres]"`。用于 `PostgresPipeline`。psycopg 是 LGPL-3.0，
详见[数据与去重](item-pipeline.md#postgresql)。

| 配置 | 默认 | 说明 |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `postgres` / `""` | |
| `POSTGRES_DB` | `"netspy"` | 库名 |
| `POSTGRES_POOL_SIZE` | `5` | 连接池上限 |
| `POSTGRES_ON_CONFLICT` | `"nothing"` | `error` 冲突报错 / `nothing` 跳过 / `update` upsert |
| `POSTGRES_CONFLICT_TARGET` | `[]` | `update` 模式下的冲突列，通常是唯一索引的列 |

## Elasticsearch / Kafka

| 配置 | 默认 | 说明 |
|---|---|---|
| `ELASTICSEARCH_HOSTS` | `["http://localhost:9200"]` | 需 `pip install "netspy[elasticsearch]"` |
| `KAFKA_BOOTSTRAP_SERVERS` | `["localhost:9092"]` | 需 `pip install "netspy[kafka]"` |

## 代理 / 渲染 / 指标 / 告警

见 [中间件与代理](middleware-proxy.md)、[浏览器渲染](render.md)、[监控与调试](observability.md)。

## 日志

| 配置 | 默认 |
|---|---|
| `LOG_LEVEL` | `"INFO"` |
| `LOG_FILE` | `None`（只输出到 stderr） |
| `LOG_ROTATION` / `LOG_RETENTION` | `"50 MB"` / `"10 days"` |
| `LOG_COLOR` | `True`（stderr 着色；写进容器日志 / 文件给机器读时可以关掉） |
| `PROJECT_NAME` | `"netspy"`（没绑定名字的日志行显示成这个） |

写爬虫 / 管道 / 中间件 / 账号池时直接用 `self.logger`，不用自己
`from netspy.utils.log import get_logger` 再手动 bind 一个名字：

```python
class BookSpider(ns.AirSpider):
    def parse(self, request, response):
        self.logger.info("抓到 {} 条", len(items))
```

`AirSpider` / `Spider` / `BatchSpider` / `TaskSpider`（继承自 `BaseParser`）、
`BasePipeline`、`DownloaderMiddleware`、`UserPool` 都已经带了它——日志里的
名字就是这个子类自己的类名（比如上面这行会显示 `BookSpider`，不是一个
和具体类没关系的固定名字）。模块级的 `ns.log` / `ns.get_logger(name)`
还在，写在类外面（脚本级代码、独立函数）时用那两个。

## `USE_SESSION` 与分片

`USE_SESSION` 的收益**不是一个固定倍数**。一个 `httpx.Client` 被太多线程共用时，
连接池自己会成为争用点。实测（https 目标、走本机代理、靶子天花板已用裸 asyncio
客户端确认远高于被测）：

| 线程 | 16 | 32 | 48 | 64 | 96 |
|---|---:|---:|---:|---:|---:|
| 共用一个 client | 269 | 444 | **248** | **172** | **105** |
| 按 32 线程分片 | 269 | 451 | **563** | **492** | **434** |
| 每请求新建（不开 session） | 173 | 203 | 243 | 249 | 265 |

**拐点已在三种环境复核（v4.42，各 5 轮）**，共用一个 client 的曲线：

| 线程 | 16 | 32 | 48 | 64 | 96 |
|---|---:|---:|---:|---:|---:|
| 直连 | 287 | **486** | 282 | 185 | 106 |
| squid（会保活） | 284 | **474** | 288 | 186 | 109 |
| tinyproxy（不保活） | 280 | **475** | 282 | 184 | 108 |

三条几乎重合、峰都在 32 —— 默认值站得住。而且**保活与否不影响曲线**，
说明卡的是连接池的锁而不是连接本身。

共用一个 client 在 ~32 线程见顶后**掉头向下**，48 线程往上甚至比不开 session 还慢。
`SESSION_SHARD_THREADS`（默认 32，即实测拐点）让每个代理按需开多片连接池，
把这一段补回来。

- 线程数 ≤ `SESSION_SHARD_THREADS` 时只有一片，**与不分片完全一致**
  （框架默认 `SPIDER_THREAD_COUNT=4`，落在这一档）
- **分片不改变 cookie 语义**：同一个代理的所有分片共用一个 `CookieJar`
- 不同代理之间 cookie 仍然隔离，与分片前一致
- 设 `SESSION_SHARD_THREADS = 0` 回到「每代理一个 client」

### `pool_limits` 的依据（v4.41 复量）

`max_keepalive_connections` 低于并发数时，多出来的连接每轮用完就被关掉、下轮重建。
实测（**直连**、16 线程、96 个请求，靶子自己数接受了几条连接）：

| keepalive | 靶子接受连接 | QPS |
|---:|---:|---:|
| 20 | 16 | 581 |
| 100 | **5** | **1075** |

连接数就是证据。（早先文档引用过一组「281 → 499」，那是在一个**不保活**的代理
后面量的，已证伪 —— 那种环境下这个参数没有效果。）

### 这个收益**就是**连接复用

!!! warning "这段话在本项目里翻过三次面，这次有剂量反应曲线"
    ~~早先 v4.40 写过「收益不是连接复用」，依据是 squid 与 tinyproxy 下
    收益几乎一样（1.23/1.87/2.22× vs 1.21/1.87/2.13×）。~~
    下面这条曲线与那个结论矛盾，**所以那个结论不成立**。

    但**那两组数字为什么相等，本轮没有解释出来**。最顺理成章的解释是
    「两种代理对这个连接池等价」，可实测 tinyproxy 连客户端→代理这条腿
    也基本不保活（不开 session 时 6 个请求开 6 条连接），这个解释站不住。
    走代理时的机制可能与直连不同 —— 这里如实留白，不拿新猜想替换旧猜想。

把「每条连接值多少钱」当旋钮拧（32 线程、直连、靶子对每条新连接收一次费）：

| 每条连接的价钱 | 0 ms | 2 ms | 10 ms | 50 ms |
|---|---:|---:|---:|---:|
| `USE_SESSION=False` | 440 | 424 | 380 | **256** |
| `USE_SESSION=True` | 483 | 488 | 485 | **472** |
| 收益 | 1.10× | 1.15× | 1.28× | **1.84×** |

**形状本身就是机制**：开着的那一臂几乎不动 —— 它不在乎建连贵不贵，因为它基本
不建连；涨上去的全是不开的那一臂在掉。构造成本在这四档里完全相同，产生不出这个形状。

换 TLS 也一样（同样 32 线程、同一台靶子）：明文 loopback 建连几乎免费，收益 1.08×；
换成 https，每条连接多一次 TLS 握手，收益变成 1.58×。

**所以收益的大小取决于你的目标站建连有多贵**，不是一个固定倍数：
本机明文靶子上几乎白干，公网 https + 代理上是 2 倍。

（`ssl_context_for` 的缓存另算一笔账，而且早就付过了：裸 `httpx.Client()`
每次重建 `SSLContext`，构造要 41ms；走框架只要 0.44ms。）

只影响同步 httpx 下载器。异步下载器跑在单个事件循环里、不存在这种线程争用；
`curl_cffi` 用 libcurl 自己的连接池，机制不同，均未改动。

### `thread_count=` 会覆盖 `SPIDER_THREAD_COUNT`，分片跟着真实值走

每个 Spider 都接受 `thread_count=N`：

```python
MySpider(thread_count=64).start()      # 64 个工作线程，配置里写的 4 不算数
```

`SESSION_SHARD_THREADS` / `ASYNC_THREADS_PER_LOOP` / 连接池上限**都按这个真实值算**，
不是按配置里的数 —— 调度器启动时会把真实线程数告诉下载器层。

!!! warning "v4.46 之前不是这样，那是个静默失效"
    此前这三个函数直接读 `setting.SPIDER_THREAD_COUNT`，于是
    `AirSpider(thread_count=64)` 配默认的 `4` 会算出 **1 片、1 个事件循环** ——
    分片一直没生效，而日志、行为、返回值全都正常。代价（同样 64 个真实线程、
    50ms 靶子）：

    | | 分片 | 循环 | httpx 同步 | httpx 异步 |
    |---|---:|---:|---:|---:|
    | 按配置的 4 算（旧） | 1 | 1 | 546 | **198** |
    | 按真实的 64 算（新） | 2 | 4 | **899** | **775** |

一个进程里跑多个爬虫时**取最大值** —— 全局下载器是它们共用的，真实并发是各家之和；
爬虫收尾时这个值会跟着下载器一起重置。

## 每进程的开销天花板

上面那些旋钮都拧到最好之后，还有一堵墙：**httpx 自己的每请求 Python 开销**。

固定 32 线程逐层剥（零延迟本机靶子，每层只比上一层多加一样东西）：

| 层 | QPS | 相对上一层 |
|---|---:|---:|
| 裸 socket（复用连接） | 15,036 | — |
| 换成一个**共享**的 `httpx.Client` | **534** | **0.04×** |
| 再换成每请求新建 client | 486 | 0.91× |
| 再套上框架的下载器 | 487 | 1.00× |
| 再读一次 `.text` | 484 | 0.99× |

**一层就掉 28 倍，而框架在其上总共只加约 10%。** 这个平台期是 GIL 串行造成的：
httpx 的请求路径是纯 Python，每请求约 1.9ms 的串行 CPU，加线程摊不开。
（`sys.setswitchinterval` 在 1000 倍范围内扫过没有作用，0.93~1.01× —— 不是车队效应，
默认值已经是最优的。）

三个后端 × 线程数（同一台零延迟靶子；裸 socket 是对照，证明靶子远没到极限）：

| 后端 | 1 线程 | 8 | 32 | 64 |
|---|---:|---:|---:|---:|
| 裸 socket（对照） | 6,658 | 8,550 | 12,114 | 12,955 |
| httpx 同步 | 901 | 718 | 458 | 459 |
| `curl_cffi` | 635 | 849 | **978** | 948 |
| httpx 异步 | 584 | 393 | 171 | 141 |

**只有 `curl_cffi` 随线程数上涨** —— 它的解析在 C 里，不占 GIL。

### 那要不要换 curl？看你的目标站建连有多贵

| 靶子 | httpx 同步 | `curl_cffi` | curl/httpx |
|---|---:|---:|---:|
| http 明文（建连几乎免费） | 480 | 996 | **2.08×** |
| http + 每条连接 10ms | 453 | 577 | 1.27× |

优势随建连成本**收窄**，原因是 **curl 这条路径从不复用连接**（框架永远用
`stream=True`，而 curl_cffi 收尾时关掉整个句柄；见
`tests/test_connection_reuse_truth.py`）。

代价还不只是慢，**还烧端口**：

| 600 个请求 | 靶子接受的连接 | 事后 TIME_WAIT |
|---|---:|---:|
| httpx + `USE_SESSION` | 42 | 42 |
| `curl_cffi` | 616 | **616** |

一个请求一个端口。TIME_WAIT 默认要挂约 60 秒，而临时端口通常只有约 28,000 个 ——
**持续跑到几百 QPS 就会打光**。

**选型**：目标站响应快、建连便宜、QPS 不高 → curl 值得换；
目标站是 https 或走代理、或要长期高 QPS → 留在 httpx 并开 `USE_SESSION`。
无论哪种，**这堵墙靠加线程翻不过去，只能加进程。**

（一个没量干净的对照，如实记下：https 靶子上的 curl/httpx 没有得出可用的数字 ——
curl 一轮烧掉近千个端口，污染同进程里后跑的那个臂，httpx 的中位数 31 QPS 带着
±1350% 的离散度。要量这一格得把两个臂放进不同进程，本轮没做。）

## 异步下载器的事件循环分片

`DOWNLOADER_ASYNC_CONCURRENCY` 曾被本文档描述成「最大在途请求数」，**那是错的**：
工作线程是**同步阻塞**地调 `download()` 的，一个线程同时只有一个在途请求，
所以实际在途由 `SPIDER_THREAD_COUNT` 决定。实测默认 200 时均在途只有 3~23，
这个值从没成为过约束。

更要紧的是：「一个事件循环线程 + N 个线程阻塞提交」这个模式在 N 超过 ~24 时会**坍塌**。
实测（50ms 目标、每格 5 轮）：

| 线程 | 8 | 16 | 20 | 24 | 32 | 48 | 200 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 单个循环 | 145 | 287 | 350 | 286 | **82** | **62** | **73** |
| 按 16 线程分片 | 145 | 287 | 354 | 417 | **452** | **318** | **421** |
| 同步下载器（对照） | 118 | 194 | 223 | 246 | 287 | 344 | 463 |

坍塌时均在途从 18 掉到 4 —— 请求近乎串行。**这与本框架的逻辑无关**：
把框架整个拿掉、只留一个事件循环加 N 个线程反复
`run_coroutine_threadsafe(...).result()`，坍塌一模一样。所以只能多开几个循环。

- 线程数 ≤ `ASYNC_THREADS_PER_LOOP` 时只开一个循环，**与分片前完全一致**
  （框架默认 `SPIDER_THREAD_COUNT=4`，落在这一档）
- 每片是**独立**的事件循环、`AsyncClient`、信号量和代理连接池 ——
  `AsyncClient` 绑定在创建它的循环上，不能跨片用
- 设 `ASYNC_THREADS_PER_LOOP = 0` 回到单个事件循环

⚠️ **更正（v4.43）**：这里原来写「即便分片之后，`DOWNLOADER_ASYNC` 的优势也只在
中等线程数（≤32）成立」。**那句话是错的** —— 它出自一个不公平的对照
（同步侧没开 session，而异步下载器内部总是共享 client）。
两边都开 session 复量（5 轮）：

| 线程 | 8 | 16 | 32 | 64 | 128 |
|---|---:|---:|---:|---:|---:|
| async/sync | 0.97× | 0.99× | 0.84× | 0.48× | 0.49× |

**公平比较下异步在任何线程数都没有优势**：≤16 基本持平，32 线程同步快 1.19×，
64 线程往上同步快约 2 倍。分片的价值在于把异步从「>24 线程坍塌」救回到
「与同步同一量级」，不是让它反超。详见 [async 内核评估](async-kernel.md)。

原句（保留作对照）：~~即便分片之后，`DOWNLOADER_ASYNC` 相对同步下载器的优势
也只在**中等线程数**（≤32）成立；~~
48 线程往上两者接近，200 线程时同步略胜。它不是「线程越多越值」的开关。

## curl 下载器：`use_session` 不带来连接复用

框架的 curl 下载器**永远**用 `stream=True`（为了 `MAX_RESPONSE_SIZE` 边读边判），
而 curl_cffi 的 `Response._finalize_stream()` 收尾时执行 `self.curl.close()` ——
关掉的是**整个 Curl 句柄**，句柄的连接缓存随之消失，不是把连接归还池子。
实测（同线程 5 次串行、数保活 socket）：

| | stream=False | stream=True |
|---|---:|---:|
| `impersonate` 关 | 2 条 | **0 条** |
| `impersonate=chrome` | 2 条 | **0 条** |
| 对照 `httpx.Client` | 2 条 | — |

所以在 curl 这条路径上，**`use_session=True` 只带来 cookie 持久化，不带来连接复用**。
（早先的文档说「三个下载器都复用连接」，那句话对 curl 是错的。）
要拿回复用就得放弃边读边判的响应体上限，那是安全边界，不做这个交换。

**但共用一个 `Session` 仍然要付代价**，且拐点比 httpx 更低 —— curl 从 16 线程起
就不再增长，所以有独立的 `CURL_SESSION_SHARD_THREADS`（默认 16）：

| 线程 | 8 | 16 | 32 | 48 | 64 |
|---|---:|---:|---:|---:|---:|
| 分片前 | 131 | 233 | **235** | **234** | **233** |
| 分片后 | 133 | 233 | **371** | **466** | **525** |
| 不开 session（对照） | 127 | 214 | 338 | 405 | 459 |

分片前，32 线程往上开 `use_session` 比不开还慢；分片后全线反超。
线程数 ≤ `CURL_SESSION_SHARD_THREADS` 时只有一个 session，与分片前完全一致。
分片同样**不改变 cookie 语义**：同一代理的所有分片共用一个 `CookieJar`。
