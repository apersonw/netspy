# Roadmap

当前版本变更明细见 [CHANGELOG](https://github.com/apersonw/netspy/blob/main/CHANGELOG.md)。

## 已具备的能力

**运行时**

- [`AirSpider`](spider.md)：单机单进程，内存队列 + 内存布隆去重，最简单的起点
- [`Spider`](distributed.md)：Redis 分布式——队列 + 去重都在 Redis，断点续爬，
  `start_requests` 一次性锁，多节点心跳结束检测，失败请求落 Redis
- [`TaskSpider`](distributed.md#taskspider)：从 Redis / DB 任务源持续拉任务，多节点分摊，
  `keep_alive` 常驻
- [`BatchSpider`](batch-spider.md)：周期批次采集，任务表状态机 + 批次记录表 +
  进度追踪 + 任务防丢；master/worker 分离，`BatchStore` 可换 MySQL / 内存实现
- 分布式**运行作用域**（`RUN_ID`）：每次触发的一组 worker 共享一个运行 id，
  种子锁 / 队列 / 去重按运行隔离——定时重跑不会互相踩

**网络层**

- 三种下载器：`httpx`（同步 + 连接池，按线程分片）、`AsyncHttpxDownloader`
  （专属事件循环 + 共享 `AsyncClient`）、`CurlDownloader`（curl_cffi TLS/HTTP2
  指纹伪装，`DOWNLOADER_IMPERSONATE`）
- [浏览器渲染](render.md)：Playwright，`RenderPool` 复用浏览器实例
- [反爬对抗](anti-bot.md)：Cloudflare / Akamai 挑战页识别（`AntiBotError`，
  走既有重试与换代理）、TLS 指纹伪装、自动抑制矛盾的随机 UA
- [中间件与代理池](middleware-proxy.md)：`DownloaderMiddleware` 链、代理池
  （失败冷却退避、按阶段分流、定期回收）
- [账号 / Cookie 池](user-pool.md)：`LocalUserPool` / `GuestUserPool` /
  `RedisUserPool`，`user_pool()` + `check_login()` 钩子，掉登录自动换号重试
- 非 2xx 状态码处理、`Retry-After` 退避、per-domain 限速、跨节点全局限速
  （`GLOBAL_THROTTLE`）、熔断器、robots.txt、响应体大小上限
  （`MAX_RESPONSE_SIZE`）、[响应缓存](spider.md#响应缓存开发调试用)（开发期重跑读本地文件）

**数据与存储**

- [多种落库管道](item-pipeline.md)：Console / CSV / MongoDB / MySQL / PostgreSQL /
  Elasticsearch / Kafka，`SqlPipeline` 基类统一 upsert 语义
- 分层去重：内存布隆按容量分层扩容，避免固定容量下的静默误判
- `UpdateItem` 精确更新 + 指纹去重

**可观测性**

- `self.logger`：`AirSpider` / `Spider` / `BatchSpider` / `TaskSpider`、
  `BasePipeline`、`DownloaderMiddleware`、`UserPool` 都混入了绑定子类名的 logger
- 结束时吐一行机器可读运行摘要（stdout `NETSPY_RUN_SUMMARY {...}`），
  管理平台据此分得清「进程退出」和「真的抓到了东西」
- [Prometheus 指标导出](observability.md)、[飞书 / 钉钉 / 企业微信 / 邮件告警](observability.md#告警)

**工程化**

- [CLI](cli.md)：`netspy create`（项目 / 爬虫 / Item，含读数据库表反射生成字段）、
  `netspy shell`（交互式调试选择器）、`netspy retry`（失败数据回放）
- 分层配置（框架默认 ← 项目 `setting.py` ← 环境变量 `NETSPY_*`）
- 完整类型标注（mypy strict）、CI 覆盖 Python 3.10–3.13 + 真实 Postgres/MySQL/Redis
  集成测试

**管理平台**

- [NetspyHub](https://github.com/apersonw/netspyhub)：独立的爬虫管理平台——
  项目托管（git / zip，含版本历史与回退）、任务调度（手动 / 定时 / 间隔 / cron）、
  Docker-per-task 隔离运行、SSE 实时日志、告警、懂 Netspy 的监控看板
  （直读 Redis 队列深度 / 节点心跳 / 批次进度，抓 Prometheus 端点）

## 后续方向

暂无已定的具体计划——功能面已经覆盖了单机到分布式、七种存储、反爬、可观测性的
主要场景。有新的方向会更新在这里。
