# 反爬对抗

## 为什么换 UA 没用

传统反爬看 `User-Agent`，所以「UA 池」曾经有效。现在的主流方案（Cloudflare、Akamai、
DataDome、PerimeterX）看的是**你还没发出第一个字节前**就已经暴露的东西：

- **TLS 握手指纹（JA3 / JA4）** —— 密码套件、扩展、椭圆曲线的顺序组合。Python 的
  ssl 模块跟任何浏览器都不一样，且高度稳定，等于一个身份证号
- **HTTP/2 SETTINGS 帧指纹** —— 帧参数、优先级树、伪头顺序
- **头部顺序与大小写** —— 浏览器的头是固定顺序的

实测一下裸奔的样子（`https://tls.peet.ws/api/all`）：

| | JA3 | JA4 | HTTP | UA 自称 |
|---|---|---|---|---|
| httpx + UA 池 | `478843a4…` | `t13d1712**h1**_…` | HTTP/1.1 | Firefox 126 |
| curl_cffi 伪装 | `65cff61e…` | `t13d1516**h2**_…` | **h2** | Chrome / macOS |

第一行有三重矛盾：UA 说自己是 Firefox 126，TLS 指纹是 Python 的，而且还在用 HTTP/1.1
（真浏览器早就走 h2 了）。**换多少个 UA 都改不了这三行里的后两项。**

## 启用指纹伪装

```bash
pip install "netspy[curl]"
```

```python
# setting.py
DOWNLOADER_IMPERSONATE = "chrome"        # 空串 = 关闭（默认）
```

启用后普通请求自动改走 `CurlDownloader`（[curl_cffi](https://github.com/lexiforest/curl_cffi)，
底层是 libcurl-impersonate）。**爬虫代码一行都不用改**，`Request` / `Response` /
中间件 / 代理池 / 账号池全部照旧。

常用取值：`chrome`（跟随最新 Chrome）、`chrome131`、`safari17_0`、`firefox135`、`edge101`。
不写具体版本号（只写 `chrome`）的好处是升级 curl_cffi 会自动跟上新版本。

按请求覆盖：

```python
yield ns.Request(url, impersonate="safari17_0")
yield ns.Request(url, impersonate="")        # 这一条不伪装
```

!!! warning "别再叠加随机 UA"
    `impersonate` 会带一整套自洽的浏览器头。如果同时再塞一个 UA 池里的 UA，就会变成
    「TLS 握手说 Chrome、UA 头说 Firefox」—— **这种自相矛盾比不伪装更容易被识破**。
    框架已经自动处理：`impersonate` 生效时不注入随机 UA。你显式传的 `headers` 仍然优先，
    但请自己保证与伪装目标一致。

### 与其他能力的关系

| | 说明 |
|---|---|
| `render=True` | 走 Playwright，真浏览器自带真实指纹，不需要也不会用 impersonate |
| 代理池 | 完全通用，`CurlDownloader` 复用同一套 `pick_proxy` / `report_bad` |
| 账号 / Cookie 池 | 不受影响 |
| `DOWNLOADER_ASYNC` | 伪装优先级更高；两者都开时普通请求走 curl |
| 分布式 `Spider` | `impersonate` 会随 `Request` 序列化进 Redis，worker 节点不会退化成裸奔 |

## 拦截识别

挑战页的麻烦之处在于它**返回 200**：Cloudflare 给你一段 JS，框架当成抓取成功，
把这个没有内容的壳子入库。**静默产出脏数据比直接报错难排查得多。**

框架默认开启识别（`ANTIBOT_DETECT = True`），命中就抛 `AntiBotError`：

```python
from netspy.exceptions import AntiBotError
```

`AntiBotError` 继承 `RequestError`，所以走的是既有的下载失败路径 —— **自动重试，
重试时代理池自然会换一个出口 IP**，不需要你写任何额外代码。

识别规则刻意保守（宁可漏报不可误伤），只认强特征：

| 类型 | 触发条件 |
|---|---|
| `cloudflare` | `cf-mitigated` 响应头；或 403/503/429 且 body 里有 `__cf_chl_` / `challenge-platform` |
| `akamai` | 403/428 且 body 里有 `_abck` / `bm-verify` / `cp_challenge` |
| `js_redirect` | 200、body < 2KB、没有任何正文标签、却带跳转脚本或 meta refresh |

正常页面不会被误伤：有正文标签（`<p>` `<h1>` `<table>` …）、或体积超过 2KB、
或只是普通的 403/404，都不会命中。

误伤了就关掉：

```python
ANTIBOT_DETECT = False
```

也可以只做判断不抛异常：

```python
from netspy.network import antibot

def parse(self, request, response):
    if antibot.detect(response):
        ...   # 自己决定怎么处理
```

## 还搞不定怎么办

按代价从低到高：

1. **换伪装目标** —— 有些站点对特定浏览器版本更宽容，试 `safari17_0` / `firefox135`
2. **加住宅代理** —— 机房 IP 段本身就是强特征，指纹再真也救不回来
3. **降速** —— 调大 `SPIDER_THREAD_COUNT` 的反面：请求节奏太整齐也是特征
4. **上 `render=True`** —— 真浏览器，代价是慢一到两个数量级
5. 需要过交互式验证码的，本框架不涉及

## 不做什么

框架不包含验证码打码平台接入、指纹浏览器（camoufox 等）、住宅代理商 SDK ——
这些都是「接一个外部付费服务」，用 `DOWNLOADER_MIDDLEWARES` 自己接即可。
