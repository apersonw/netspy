# 编写爬虫

## AirSpider

继承 `ns.AirSpider`，实现 `start_requests`（产出种子请求）和 `parse`（解析）：

```python
class MySpider(ns.AirSpider):
    __custom_setting__ = dict(SPIDER_THREAD_COUNT=8)  # 合并进全局配置

    def start_requests(self):
        yield ns.Request("https://example.com", callback=self.parse)

    def parse(self, request, response):
        yield ns.Request(response.urljoin("/next"), callback=self.parse_detail)
        yield {"url": request.url}          # dict 或 Item 都行

    def parse_detail(self, request, response):
        ...

MySpider().start()          # 阻塞至爬完并优雅退出
```

`parse` / callback 可以 `yield`：

| yield 的东西 | 去向 |
|---|---|
| `ns.Request` | 回到调度队列（去重后） |
| `ns.Item` / `dict` | 进 ItemBuffer → 批量落库 |
| 可调用对象 | 在当前工作线程执行 |

打日志直接 `self.logger`，不用自己 `import logging` 或 `get_logger`——日志里
显示的名字就是这个爬虫类自己的类名：

```python
def parse(self, request, response):
    self.logger.info("抓到 {} 条", len(items))
```

## Request

```python
ns.Request(
    url,
    method="GET",
    callback=self.parse,          # 方法、函数或方法名字符串
    priority=300,                 # 越小越先出队
    retry_times=0,
    filter_repeat=True,           # 是否参与去重
    render=False,                 # 浏览器渲染，见「浏览器渲染」
    cb_kwargs={"page": 2},        # 传给 callback 的额外参数
    headers=..., params=..., data=..., json=..., cookies=..., timeout=...,  # 透传给 httpx
    batch_id=7,                   # 未知参数 -> request.batch_id
)
```

`response.xpath / .css / .re / .re_first / .text / .json() / .urljoin() / .status_code`。
`.xpath(...).extract_first()` 也可用（parsel 的 `.get()` 别名）。

## 钩子

```python
class MySpider(ns.AirSpider):
    def download_midware(self, request):
        request.headers = {"Referer": "https://example.com"}
        return request                       # 返回 None 用原请求

    def validate(self, request, response):
        if response.status_code != 200:
            raise ns.ValidationError("非 200")   # -> 重试
        if "验证码" in response.text:
            raise ns.NotRetryError("被封")       # -> 直接丢弃
        return True                              # False 也表示丢弃

    def failed_request(self, request, response):
        # 重试耗尽后调用；可再 yield Request / Item
        yield {"failed": request.url}

    def start_callback(self): ...   # 启动时一次
    def end_callback(self): ...     # 正常结束时一次
```

## 熔断

目标站挂了就别再死磕。同域**连续**失败到阈值时，该域进入冷却，所有工作线程一起避让
（复用[限速](#限速)的整域降速机制）。

```python
CIRCUIT_FAILURE_THRESHOLD = 10   # 连续失败多少次跳闸；0 = 关闭
CIRCUIT_COOLDOWN = 60.0          # 跳闸后该域冷却多久（秒）
```

### 只数「站点不健康」的信号

| 失败类型 | 计入熔断 |
|---|:-:|
| 网络错误（超时 / 连不上 / TLS 失败） | ✅ |
| 429 / 500 / 502 / 503 / 504 | ✅ |
| **404 / 403 等 4xx** | ❌ |
| 解析异常 / `validate` 失败 | ❌ |

!!! warning "为什么 404 不算"
    「按 ID 顺序探测」是常见爬法，连续几十个 404 完全正常。
    拿它跳闸会让爬虫每探十几个就停 60 秒，把正常爬取搞瘫。
    **404 是「这个 URL 不行」，不是「这个站不行」。**
    同理，解析异常是爬虫自己的问题，不该让目标站背锅。

### 计数发生在重试耗尽之后

不是每次失败都数，而是等到重试用完、请求真正失败时才记一笔。这样
[代理池](middleware-proxy.md)有机会先轮换出口 —— **「代理坏了」会被重试吸收，
只有站点真的挂了才会连续走到这一步**。

跳闸后计数清零：冷却结束就重新给站点机会，不必等它「证明自己恢复了」。

## 运行时长上限

```python
SPIDER_MAX_RUNTIME = 3600.0   # 秒；0 = 不限
```

到点走**优雅停止**：flush 缓冲区、把未完成请求 dump 到 `failed_requests.jsonl`
（`netspy retry --requests` 可回放），然后**正常返回，不抛异常** ——
定时任务「跑够一小时就停」不该被当成错误，否则监控会一直告警。

## robots.txt

```python
ROBOTS_OBEY = True          # 库默认 False；脚手架生成的项目里默认 True
ROBOTS_USER_AGENT = "*"     # 按哪个 UA 组匹配规则
ROBOTS_CACHE_TTL = 3600.0   # 缓存多久；0 = 永不过期
```

开启后，每个请求发出前会按域检查 robots.txt（按域缓存，多线程首访也只抓一次）。
被禁止的 URL **不会产生请求**，计入结束行的「robots 拦截」，且**不算失败** ——
那是有意跳过，不该污染失败率、也不该进 `failed_requests.jsonl` 等着被回放。

robots.txt 里的 `Crawl-delay` 会自动接管该域的[限速](#限速)，取
`max(DOWNLOAD_DELAY, Crawl-delay)` —— 站点自己声明的节奏不该被全局默认值放宽。

!!! note "为什么默认值是 `False`"
    库默认关闭，但 `netspy create -p` 生成的项目配置里写的是 `ROBOTS_OBEY = True`。
    这样**新项目开箱合规**，而把 Netspy 当库嵌入、或抓自己站点 / 内网服务的人
    不会被意外拦住。

!!! note "为什么按 `*` 匹配"
    框架默认 `RANDOM_USER_AGENT = True`，每个请求的 UA 都不一样 ——
    按具体 UA 匹配 robots 规则是没有意义的，所以默认匹配通配组。
    有固定 UA 时把 `ROBOTS_USER_AGENT` 设成它即可。

### 抓不到 robots.txt 时会放行

| robots.txt 响应 | 处理 |
|---|---|
| 2xx | 按规则判定 |
| 404 / 其他 4xx | 放行全部（没有 robots.txt = 没有限制） |
| 5xx / 超时 / 解析失败 | **放行全部 + warning 日志** |

最后一行是刻意的：`/robots.txt` 一次瞬时 500 不该让整个爬虫停摆，
而且「什么都不抓也不报错」是最难排查的故障形态。

## 限速

按**域名**分账，两个独立的旋钮：

```python
CONCURRENT_REQUESTS_PER_DOMAIN = 8   # 单域最大在途请求数；0 = 不限
DOWNLOAD_DELAY = 0.0                 # 同域两次请求的最小间隔（秒）；0 = 不限
RANDOMIZE_DOWNLOAD_DELAY = True      # 给上面的间隔加 ±50% 抖动
```

默认只限并发、不限间隔。而且默认上限 `8` 大于默认线程数 `4`，所以
**对默认配置完全无感** —— 它是你把 `SPIDER_THREAD_COUNT` 调大时的一张安全网：
想调快可以，但不会不小心把单个域名打爆。

域名键是小写 `netloc`（含端口），不做子域归并：`www.a.com` 与 `a.com` 各算各的。

`RANDOMIZE_DOWNLOAD_DELAY` 存在的理由和[反爬](anti-bot.md)同源 ——
分毫不差的请求节奏本身就是机器人特征。

!!! warning "默认是**进程内**限速"
    分布式 `Spider` 起 N 个节点，目标站承受的就是 **N 倍** —— 除非打开下面的
    `GLOBAL_THROTTLE`。并发上限（`CONCURRENT_REQUESTS_PER_DOMAIN`）目前**仍是进程内的**：
    配 `8` 起 10 个节点 = 该域实际最多 80 个并发，这部分仍需自己按节点数折算。

### 跨节点全局限速

```python
GLOBAL_THROTTLE = True   # 需要 REDIS_URL
DOWNLOAD_DELAY = 0.5     # N 个节点**合起来**每 0.5 秒一个请求
```

打开后 `DOWNLOAD_DELAY` 改由 Redis 记账：所有节点共用一份「该域下次可请求的时刻」，
取号用 Lua 保证原子。一次往返就算出准确的等待时长，不需要「没令牌就重试」的轮询。

时钟取自 **Redis 服务端**而不是各节点自己的 —— 节点间的时钟偏移会一比一变成限速误差。
（因此需要 **Redis 5+**：脚本里在写入前调了 `TIME`，更老的服务端会拒绝，
届时会退回进程内限速并告警。）

代价是**每个请求多一次 Redis 往返**（哪怕 `DOWNLOAD_DELAY = 0`——429 的整域冷却
存在同一个 key 里，跳过这次往返会让冷却失效）。所以它默认关闭：单机跑用不上。

三节点实测（`DOWNLOAD_DELAY=0.3`，判据取自靶子记录的到达时刻）：

| | 站点承受的峰值 |
|---|---|
| 关闭 | **10 请求/秒**（正好是配置值的 3 倍） |
| 打开 | **4 请求/秒**（配置允许 3.3/秒） |

429 的整域冷却同样会全局生效：一个节点撞上限速，**所有节点**一起避开。

!!! note "Redis 连不上时会退回进程内限速"
    退回去的是**进程内限速**，不是「不限速」—— 限速器一挂就放开手脚打目标站，
    是这里最不该有的失败模式。日志里会警告一次。

### 429 / 503 会让整个域降速

收到 `Retry-After` 时，冷却会作用在**整个域名**上，所有工作线程一起避开 ——
只让撞上 429 的那一个线程等是没用的，其余线程会继续满速打同一个域，退避形同虚设。

冷却时长与 `RETRY_AFTER_MAX`（默认 60s）一起封顶：既然超过上限就放弃该请求，
也不该给整个域挂上更长的冷却，否则爬虫会在那个域上彻底停摆。

## 响应缓存（开发调试用）

写爬虫是来回试的过程：改一版选择器、重跑一遍看结果，一天下来同一批页面可能被抓几十遍。
这既慢，对目标站也不礼貌。

```python
RESPONSE_CACHE_ENABLE = True          # 默认 False
RESPONSE_CACHE_PATH = ".netspy_cache"
RESPONSE_CACHE_EXPIRE = 3600.0        # 秒；0 = 不过期
```

打开后第一遍照常抓，之后重跑直接读本地文件。**命中缓存时不发请求、也不占
[限速](#限速)名额** —— 否则「重跑不打扰目标站」这件事只做了一半。

```bash
netspy cache            # 看有多少条、占多大
netspy cache --clear    # 改了解析逻辑，想重抓真实页面
```

!!! danger "只在开发期开"
    跑生产时开着很容易出事：你以为在抓新数据，实际读的是几天前的副本。
    所以默认是关的，而且这个开关的定位就是开发工具。

### 什么不会被缓存

- **非 GET 请求** —— POST / PUT 不幂等，把它们的响应缓存下来重放，等于把一次
  写操作的结果当成了「这个请求现在会返回什么」，这两件事不是一回事。
- **状态码不正常的响应** —— 把 429 / 503 存下来，等于每次重跑都在读那张限速页，
  [状态码策略](#状态码策略)还会以为站点一直在拒绝。

缓存读写失败（文件坏了、目录写不进去）都只记 debug 日志然后照常走真实请求 ——
**加速手段不该变成新的故障点**。

## 资源边界

框架此前会把**任何**响应整个读进内存 —— 不看大小，也不看类型。
实测一个 200MB 的响应让进程 RSS 涨 **618MB**（`bytes` 一份、`.text` 解码又一份），
默认 4 个线程同时撞上就是 ~2.5GB。

容器有内存上限时这就是 OOM，而 **OOM Killer 发的是 `SIGKILL`** ——
它绕过[优雅退出](#优雅退出)，节点本地已从 Redis 领走的任务会永久丢失。
所以这不只是「内存占用高」，它会让优雅停止形同虚设。

```python
MAX_RESPONSE_SIZE = 32 * 1024 * 1024   # 默认 32MB；0 = 不限
ALLOWED_CONTENT_TYPES = []             # 默认空 = 不过滤
```

下载改成了**流式**：先拿响应头，再决定要不要读 body、读多少。

超限抛 `ResponseTooLargeError`，**不重试** —— 再抓一次还是一样大，
重试只是把同样的流量和内存再烧 `SPIDER_MAX_RETRY_TIMES` 遍。
它也**不计入熔断**：响应大是「这个 URL 太大」，不是「这个站挂了」，
否则一个站上放着几个大 PDF 就能把整域熔断。

实测（200MB 靶子，上限 32MB，「靶子发出」是服务端在被断开前真正写出去的字节）：

| 场景 | 进程 RSS 增长 | 靶子发出 |
|---|---|---|
| 改之前 | +618MB | 200MB |
| 报了 `Content-Length` | **+5MB** | **0MB** |
| 分块传输、不报长度 | +50MB | 33MB |

!!! warning "这是行为变更"
    确实需要下载大文件的爬虫会被拦下，报错里会点名 `MAX_RESPONSE_SIZE`。
    放开就是 `MAX_RESPONSE_SIZE = 0`。

### Content-Type 白名单

```python
ALLOWED_CONTENT_TYPES = ["text/", "application/json", "application/xml"]
```

按前缀匹配。命中不了的响应**一个字节的 body 都不读**就断开连接 ——
省的是带宽，不只是内存。被挡掉算[丢弃](#robotstxt)而不是失败，
不污染失败率，也不会进 `failed_requests.jsonl` 等着被回放。

默认是空列表（不过滤）：有人就是故意抓 PDF / 图片的，而上面的大小上限
已经保证了内存安全，白名单是优化不是兜底。

没有 `Content-Type` 头的响应一律放行 —— 不少站点根本不发这个头，
白名单是用来挡「明确说了自己是视频」的响应，不是用来挡沉默的。

!!! note "`render=True` 上限照样生效，但拦得晚"
    渲染这条路没有可以边读边停的字节流（内容是从浏览器里取的），所以上限只能在
    拿到整页 HTML **之后**判定。它挡的是「把一个巨大的字符串交给你的 `parse()`」，
    挡不住浏览器进程那边的内存。

!!! note "高压缩比响应挡不住瞬时分配"
    上限按**解压后**字节数计（`Content-Length` 报的是压缩后大小，只看它会被
    gzip 炸弹绕过）。但下载库是按网络分片解压的，压缩包一次到齐时解压器会
    一次性吐出全部内容，我们只能在那之后判定超限：实测 200KB→200MB 的
    gzip 炸弹仍会让进程瞬时涨 ~180MB（不设上限时是 618MB）。
    **上限让请求快速失败并挡住 `.text` 的二次放大，但挡不住那一次解压分配。**

## 状态码策略

!!! warning "0.7.0 破坏性变更"
    0.6.0 及以前**不检查状态码** —— `validate()` 默认返回 `True`，于是 429 / 503 / 404 的
    响应体会直接进 `parse()` 被当成数据。被限速时不但不退避，还会把限速提示页入库。
    0.7.0 起默认按下表处理。**要回到旧行为：`CHECK_STATUS_CODE = False`。**

| 状态码 | 处理 |
|---|---|
| 2xx | 正常进 `parse()` |
| **3xx** | 正常进 `parse()`。能走到回调说明你显式关了 `allow_redirects`，那就是你要的 |
| `RETRY_STATUS_CODES`（默认 429 / 500 / 502 / 503 / 504） | 重试；429 与 503 会读 `Retry-After` |
| `ACCEPT_STATUS_CODES`（默认空） | 正常进 `parse()` |
| 其余非 2xx（404 / 403 …） | 判失败，走 `failed_request()` 钩子 |

想让 `parse()` 自己处理 404：

```python
ACCEPT_STATUS_CODES = [404]
```

### 退避

重试前等多久，按固定优先级：

1. **`Retry-After` 头**（仅 429 / 503）—— 服务端明说了等多久就听它的，秒数与 HTTP-date 都支持。
   超过 `RETRY_AFTER_MAX`（默认 60s）则不再等待、直接判失败 —— 等十分钟不值得占着一个工作线程
2. **`RETRY_BACKOFF > 0`** → 指数退避 `base × 2^(重试次数-1)` + 抖动，封顶 `RETRY_AFTER_MAX`。
   抖动是为了避免多个 worker 同步重试
3. 否则 `SPIDER_RETRY_INTERVAL`（默认 `0.0`）

!!! note "等待发生在工作线程内"
    这意味着退避期间该线程不处理别的请求。被限速时这反而是好事 —— 天然降低了对目标站的压力。

## 重试

下载失败、状态码不被接受、`validate` 抛 `ValidationError`、`parse` 抛非 `NotRetryError` 异常 →
`retry_times += 1` 重新入队（跳过去重）；超过 `SPIDER_MAX_RETRY_TIMES` →
调 `failed_request`，并把请求 dump 到 `failed_requests.jsonl`（`netspy retry --requests` 可回放）。

## 优雅退出

`Ctrl-C` 一次：停止取新任务、排空在途请求、flush 缓冲、dump 未完成请求。再按一次：强制退出。
