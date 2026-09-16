# Changelog

本文件格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

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
