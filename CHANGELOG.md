# Changelog

本文件格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [1.3.2] - 2026-09-24

### 修复

- `LOG_LEVEL` / `LOG_FILE` / `LOG_COLOR` / `LOG_ROTATION` / `LOG_RETENTION`
  —— 不管是写在项目 `setting.py`、环境变量 `NETSPY_LOG_*`，还是 Spider 的
  `__custom_setting__` 里，此前全部被静默忽略。根因：`get_logger()` 只在
  第一次调用时按当时的 setting 建 loguru sink，而这个「第一次」几乎总发生
  在 `import netspy` 的过程中——几十个模块在顶层 `log = get_logger("xxx")`，
  那时项目配置文件 / 环境变量还没加载；之后 `reload()`（应用配置文件与
  环境变量）和 `apply()`（`__custom_setting__`）只改了 `setting` 模块的
  全局变量，不会让已经建好的 sink 跟着重建。`setting.LOG_LEVEL` 读出来是
  配置对了的值，实际生效的还是默认值，且没有任何提示。现在
  `reload()` / `apply()` 末尾都会重建日志 sink，三条配置路径一次修好。

## [1.3.1] - 2026-09-24

### 修复

- `LocalUserPool.get()` 原来把 `login()`（通常是一次网络请求）整段包在
  账号池的全局锁里——任何线程调 `get()` 都会被卡住等别的账号登录完，
  哪怕两边要的是完全不同的账号。`SPIDER_THREAD_COUNT` 开得再高，账号池
  这一环也会静默退化成串行。改成按用户名拆分登录锁：不同账号的登录能
  真正并行，同一个账号被并发拿到时仍靠内层双重检查只登录一次。
- `ApiProxyPool._fetch()` 把拉取代理列表的 `httpx.get()` 整段包在主锁里，
  池空时一个线程触发拉取，另一个线程哪怕只是想 `report_bad()` 一个跟这次
  拉取毫不相干的代理，也得先陪着干等整个网络往返。新增专门的拉取锁，
  `httpx.get()` 挪到主锁之外，不再挡住无关操作。
- `ItemBuffer._resolve_pipelines()` 是无锁的「查 → 建 → 存」，`flush()`
  能从多个 worker 线程并发触发；缓存冷启动时两个线程会各建一份管道
  实例，输的那份既不会被用来写数据，也不在最终缓存里，`close()` 找不到
  它，资源（数据库连接、文件句柄）就那样泄漏——对 `CsvPipeline` 这类
  还会让两个各自独立加锁的实例同时写同一个文件。补上跟
  `ProxyClientCache.get_or_create()` 一样的锁保护。
- `netspy retry --items` 回写「仍失败」记录时原来直接
  `path.write_text()` 覆盖目标文件——`open(path, "w")` 一上来就截断，
  进程这时候被 OOM Killer / SIGKILL / 断电，文件已经空了，新内容却一个
  字节都没落地，本该保留下来回放的记录永久消失。改成先写临时文件再
  原子改名，跟 `network/cache.py` 的 `store()` 是同一个修法。
- `netspy retry` 读 dump 文件时，一行解析不了（比如上一条问题描述的、
  进程在追加写中途被杀留下的半截 JSON）会让 `JSONDecodeError` 直接穿出
  整个命令，前面已经完整落盘的记录跟着一起读不出来。改成逐行跳过解析
  失败的记录并记警告，不再拖累整个文件——爬虫启动时的自动回放早就是
  这么处理的，这次补上 CLI 这条路径缺的同一防御。

## [1.3.0] - 2026-09-24

### 新增

- Playwright 渲染下载器新增四项可选的反检测能力，全部默认关闭、不改变现有用法：
  - `WEBDRIVER["channel"]`（如 `"chrome"`）：用系统里真实安装的 Chrome 而不是
    Playwright 自带的 Chromium，UA 版本号、TLS/HTTP2 指纹更贴近目标站预期的
    「真实 Chrome」画像；启动时也无条件加上 `--disable-blink-features=
    AutomationControlled`，关掉最直接的自动化标记
  - `WEBDRIVER["locale"]` / `["timezone_id"]`：Playwright `new_context()`
    原生支持的参数
  - `WEBDRIVER["engine"] = "patchright"`：切换到打了 CDP 层探测点补丁的
    Playwright fork（避免 `Runtime.enable` 等探测点），需要
    `pip install netspy[render-patchright]`；只支持 chromium，配
    firefox/webkit 会在构造期直接报错，不会等到真正渲染时才发现配错了
  - `WEBDRIVER["user_data_dir"]`：持久化浏览器 profile，跨次运行保留
    cookies / localStorage，`pool_size` 个渲染线程各用一个子目录
  - `netspy.network.downloader._humanize`：`human_move` / `human_click` /
    `human_type` / `human_scroll` 四个工具函数，模拟真人的鼠标移动 / 点击 /
    逐字符输入 / 分段滚动，供 `render_script` 里按需调用（不自动生效）

### 修复

- 补上多处「创建 / 获取资源时用了锁，对应的关闭 / 清理函数却没有」的并发
  缺陷：`MemoryBatchStore`（`mark_task` / `reset_lost_tasks` 等方法）、
  `db.close_redis()`、下载器的 `ProxyClientCache.drain()` /
  `close_default_downloaders()`、`proxy_pool.close_proxy_pool()`、
  `PlaywrightDownloader.close()`。调度器停工作线程用的是**有超时的**
  `join()`，慢任务的 worker 完全可能在对应资源关闭时还在跑——不加锁轻则
  崩溃（`RuntimeError: dictionary changed size during iteration`），重则
  静默返回错误状态（`proxy_pool` 这处最隐蔽：`PROXY_ENABLE=True` 时可能
  悄悄返回 `None`，调用方误判成「没开代理池」直接暴露源 IP，比崩溃更难查）。
  全部修复都用强制交叉调度复现 + 变异测试验证过，不是理论推测。
- `BatchSpider.update_task()` 判断「数据还在不在缓冲里」和「登记落库后回调」
  原来是两次独立加锁的调用，中间有个没锁保护的窗口——`ItemBuffer.flush()`
  恰好插在这个窗口里跑完的话，任务永远标不上「完成」，卡在「处理中」直到
  租约超时才被重新捞回来重跑一遍。`ItemBuffer` 新增
  `after_persist_if_pending()`，把两步锁进同一个临界区。
- `get_logger()` 首次懒初始化没加锁，多线程同时触发第一次调用（比如同一
  进程跑了不止一个 Spider）会导致日志 sink 重复注册，每行日志打印两遍。
- `NETSPY_SETTING` 环境变量指定的配置文件路径不存在时原来完全静默——跟
  「压根没设置这个变量」是同一种沉默，配置整个没生效却毫无提示。现在区分
  两种情况：默认候选路径找不到保持沉默（合法用法），显式指定的路径找不到
  会警告一次。

## [1.2.0] - 2026-09-22

### 修复

- Playwright 渲染下载器的反检测补丁从手写 4 行升级成完整版：新增可选依赖
  `playwright-stealth`（`netspy[render]` 自动带上），默认把 WebGL
  vendor/renderer 伪装成常见 Intel 核显。原来那份补丁只处理了
  `navigator.webdriver`/`plugins`/`languages`，没有 GPU 的容器里跑
  headless Chrome 时 WebGL 会暴露 `SwiftShader`（软件渲染）这个明确的
  自动化信号，是实测撞到过的真实拦截原因。行为向后兼容——
  `WEBDRIVER["stealth"]` 默认仍是开启，未显式关闭的现有用法不受影响。

## [1.1.0] - 2026-09-16

### 新增

- `RedisUserPool` 新增 `require_cookies` 参数（默认 `False`，不影响现有行为）。
  给 `login=None`、Cookie 完全靠外部服务写进 `<name>:cookie:<username>` 的用法
  补一个缺口：外部还没写进来，或者写的 TTL 已经到期时，不再发一个没有 Cookie
  的 `User` 出去（调用方拿着它去请求十有八九认证失败，白打一次）——改成当作
  「暂不可用」放回冷却队列，`not_ready_retry_seconds`（默认 30 秒）控制多久后
  再让别的调用者试一次。

## [1.0.1] - 2026-09-15

文档修正，无代码变更。README 里刷新了一下措辞。

## [1.0.0] - 2026-09-15

首个正式版本。

### 新增

- **运行时**：`AirSpider`（单机单进程）、`Spider`（Redis 分布式，队列 + 去重 +
  断点续爬）、`TaskSpider`（从 Redis / DB 任务源持续拉任务）、`BatchSpider`
  （周期批次采集，master/worker 分离）
- **分布式运行作用域**（`RUN_ID`）：一次触发的一组 worker 共享运行 id，种子锁 /
  队列 / 去重按运行隔离，定时重跑互不干扰
- **`self.logger`**：`AirSpider` / `Spider` / `BatchSpider` / `TaskSpider`
  （经 `BaseParser`）、`BasePipeline`、`DownloaderMiddleware`、`UserPool`
  都混入了绑定子类名的 logger，`self.logger.info(...)` 直接可用
- **机器可读运行摘要**：结束时向 stdout 吐一行 `NETSPY_RUN_SUMMARY {...}`，
  管理平台据此区分「进程退出」与「真的抓到了东西」
- **三种下载器**：`httpx`（同步 + 连接池，按线程分片）、`AsyncHttpxDownloader`
  （专属事件循环 + 共享 `AsyncClient`）、`CurlDownloader`（curl_cffi TLS/HTTP2
  指纹伪装）+ Playwright 浏览器渲染
- **反爬对抗**：Cloudflare / Akamai 挑战页识别、TLS 指纹伪装、抑制矛盾的随机 UA
- **代理池 / 账号池**：失败冷却退避、按阶段分流、定期回收；`LocalUserPool` /
  `GuestUserPool` / `RedisUserPool`，掉登录自动换号重试
- **礼貌性与失败处理**：非 2xx 状态码处理、`Retry-After` 退避、per-domain 限速、
  跨节点全局限速、熔断器、robots.txt、响应体大小上限、响应缓存（开发调试用）
- **七种存储管道**：Console / CSV / MongoDB / MySQL / PostgreSQL / Elasticsearch /
  Kafka，`SqlPipeline` 基类统一 upsert 语义
- **分层去重**：布隆过滤器按容量分层扩容，避免固定容量下的静默误判
- **可观测性**：Prometheus 指标导出、飞书 / 钉钉 / 企业微信 / 邮件告警
- **CLI**：`netspy create`（项目 / 爬虫 / Item，含读数据库表反射生成字段）、
  `netspy shell`（交互式调试选择器）、`netspy retry`（失败数据回放）
- **分层配置**：框架默认 ← 项目 `setting.py` ← 环境变量 `NETSPY_<KEY>`
