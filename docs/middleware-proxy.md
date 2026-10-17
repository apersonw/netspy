# 中间件与代理

## 下载中间件

在下载前后统一处理所有请求（加签名、换 Cookie、统计等）。

```python
from netspy.network.middleware import DownloaderMiddleware


class SignMiddleware(DownloaderMiddleware):
    def process_request(self, request):
        request.params = {**request.requests_kwargs.get("params", {}), "sign": sign(request.url)}
        return request                    # 返回 Response 则短路下载

    def process_response(self, request, response):
        if response.status_code == 202:
            return request                # 返回 Request 则丢回队列重新调度
        return response
```

`setting.py`：

```python
DOWNLOADER_MIDDLEWARES = ["myproj.middlewares.SignMiddleware"]
```

`process_request` 按列表顺序执行，`process_response` 逆序。爬虫自己的 `download_midware`
方法仍然有效，在全局中间件之后执行。

## 代理池

```python
PROXY_ENABLE = True
PROXY_EXTRACT_API = "http://proxy-provider.com/get?count=10"   # 返回每行一个代理，或 JSON 数组
PROXY_MAX_USE_TIMES = 100      # 单个代理用满这么多次后轮换
PROXY_WAIT_TIMEOUT = 30.0      # 池空时最多等多久，超时抛 ProxyUnavailableError
PROXY_ALLOW_DIRECT = False     # True 才允许「没代理就直连」
PROXY_BAN_SECONDS = 60.0       # 代理确定坏了之后冷却多久；连续失败按 2 倍退避，0 = 永久拉黑
PROXY_BAN_MAX_SECONDS = 900.0  # 退避封顶
```

所有下载器（httpx / async / curl，以及 `render=True` 的浏览器渲染）都会在请求没有显式代理时
从池里取一个；确定是代理的错时 `report_bad` 让它冷却。渲染请求按出口分 context，
见[浏览器渲染 · 代理](render.md#代理)。

!!! warning "拿不到代理时**不会**直连"
    代理用满 `PROXY_MAX_USE_TIMES`、被 `report_bad` 丢弃、或取号接口断供，
    都会让池空掉。这时框架**不会**退回直连 —— 那会把源 IP 暴露给目标站，
    而开代理池的全部意义就是别这么干。

    池空时先等 `PROXY_WAIT_TIMEOUT`（默认 30 秒）并按 `PROXY_MIN_INTERVAL`
    的节奏重新取号；仍拿不到才抛 `ProxyUnavailableError`。
    该请求走正常重试，重试用尽后落到 `failed_requests.jsonl`。
    `netspy retry --requests` 只是**探活**（重新下载看状态码，不跑回调、不入库）；要把数据真正抓回来，开 `RETRY_FAILED_ON_START` 重跑一次爬虫。

    这个错误**不计入熔断** —— 代理供应是自己这边的问题，
    算进去的话代理商断供五分钟就能把所有域全熔断一遍。

    确实想要「有代理就用、没有就直连」，把 `PROXY_ALLOW_DIRECT = True` 打开 ——
    显式配置就不算静默。

    早先的版本在这里返回 `None`，请求就那样直连出去了：实测一次断供后
    21 个请求里有 16 个从本机 IP 打到了靶子，没有任何提示。
池空时（且距上次拉取超过 `PROXY_MIN_INTERVAL`）重新拉取。

自定义代理池：继承 `netspy.network.proxy_pool.base.ProxyPool`，实现 `get_proxy()`，
然后 `PROXY_POOL = "myproj.MyProxyPool"`。另外三个是**可选**钩子，不实现也不会出错：

| 钩子 | 什么时候被调 |
|---|---|
| `report_bad(proxy)` | **确定是代理的错**：连不上代理、CONNECT 被拒 |
| `report_suspect(proxy)` | 说不清是谁的错：读超时、连接重置、响应畸形 |
| `report_good(proxy)` | 这个代理刚成功完成一次请求（**每个成功请求都调**，实现要便宜） |

## 代理用满次数之后会**轮换回来**

`PROXY_MAX_USE_TIMES` 是「用满这么多次后轮换」，不是终身配额：代理被丢出池、
下次拉取时补回来，**使用次数随之清零**，可以重新用满一轮。

!!! warning "v4.48 之前不是这样"
    计数从不清零，于是补回池里的代理**再用一次就又被丢**。后果只在第
    `PROXY_MAX_USE_TIMES × 池大小` 个请求之后才出现，短测试永远看不见 ——
    实测 3 个代理、`PROXY_MAX_USE_TIMES=5` 的池，第一轮用完后稳态退化成
    **每秒只取到 3 个代理**（其余全部空手而归），并且**每秒去拉一次供应商的
    列表接口，永远不停**。修好后同一配置是每秒 15 个。

    配合 `PROXY_ALLOW_DIRECT=False`（默认），那些空手而归的请求会各自等满
    `PROXY_WAIT_TIMEOUT`（默认 30 秒）再失败。

同一批修复还回收了按代理做键的记账。很多代理商的提取接口**每次返回全新 IP**，
而 `_use_count` / `_banned` / `_fails` / `_suspects` 原先只增不减 ——
实测 3000 次 `get_proxy` 之后 `_use_count` 里躺着 1500 条。现在每次拉取列表前
回收一次，只做**语义等价**的回收：过期的冷却、超过 `PROXY_BAN_MAX_SECONDS`
的失败记录（现有逻辑本来就当它们已归零）、以及既不在池中也不在冷却中的
使用次数。**退避阶梯不动** —— 有一条用例专门盯着这一点。

## 不是所有失败都算代理的错

下载器原先把任何下载异常都当成代理故障。后果实测过：三个**完全健康**的代理
（各自都成功连上并转发了请求）配一个 5 秒才回的目标站、1 秒超时 ——
**三个请求就把整池清空**，之后每个请求都要等满 `PROXY_WAIT_TIMEOUT`（默认 30 秒）
才失败。一个慢 URL 让整个爬虫停摆一分钟。

现在按阶段分流：

- **连接阶段失败**（`ConnectError` / `ConnectTimeout` / `ProxyError`，curl 的
  `CURLE_COULDNT_CONNECT` / `COULDNT_RESOLVE_PROXY` / `PROXY`）—— 配了代理时
  TCP 的对端就是代理本身，连不上只可能是它的问题：**立刻拉黑**，行为不变。
- **连上之后的失败**（读超时、连接重置、响应畸形）—— 说不清是谁的错，多半是
  目标站：记一次「可疑」，连续攒够 `PROXY_SUSPECT_BAN_AFTER`（默认 3）次才拉黑。
  **中间成功一次就清零** —— 真挂掉的代理会连续失败，照样被拉黑，只是晚几次。

!!! note "顺带修正了一句假话"
    退避日志里那句「第 N 次**连续**失败」此前是假的：代理池从来不知道
    「成功」这回事，`_fails` 只在距上次失败超过 `PROXY_BAN_MAX_SECONDS`（900 秒）
    时才清零。也就是说一个跑了一万次成功、15 分钟内偶尔失败三次的代理
    会被退避到 4 倍冷却。加上 `report_good` 之后，那句话才名副其实。

    **升级前写的自定义代理池不受影响**：三个新钩子都是可选的，没实现就跳过 ——
    对 `report_suspect` 而言「跳过」正好等于「目标站的锅不算代理头上」。

单个请求也可指定：`mw.Request(url, proxy="http://user:pass@host:port")`。
