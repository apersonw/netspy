# 批次采集（BatchSpider）

`BatchSpider` 用于**周期性全量 / 增量批次采集**：任务放在 MySQL 任务表里，每隔
`BATCH_INTERVAL` 天开一个新批次、重跑全部任务，进度写在批次记录表，卡死的任务会被
自动回收重跑。

```bash
pip install "netspy[redis,mysql]"
```

## 任务表

由你自己建，至少要有：主键列、状态列、更新时间列（防丢检测用）。

```sql
CREATE TABLE `crawl_task` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `url` VARCHAR(500) NOT NULL,
  `batch_status` TINYINT NOT NULL DEFAULT 0,   -- 0 待处理 / 1 完成 / 2 处理中 / -1 失败
  `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
);
```

列名可配（`BATCH_TASK_ID_FIELD` / `BATCH_TASK_STATE_FIELD` / `BATCH_TASK_TIME_FIELD`）。
批次记录表 `crawl_task_batch_record` 由框架自动建。

## 写爬虫

```python
import netspy as ns


class CrawlTask(ns.BatchSpider):
    __task_table__ = "crawl_task"

    def task_requests(self, task):                 # 一行任务 -> 请求（必须实现）
        yield ns.Request(task["url"], callback=self.parse)

    def parse(self, request, response, task):      # 框架自动带上 task
        yield {"url": task["url"], "title": response.css("h1::text").get()}
        self.update_task(task["id"], ok=True)      # 关键：回写任务状态


if __name__ == "__main__":
    import sys

    if sys.argv[1:] == ["monitor"]:
        CrawlTask().start_monitor()   # master
    else:
        CrawlTask(keep_alive=True).start()   # worker
```

`self.update_task(task_id, ok=True)` 标完成，`ok=False` 标失败（不再重试）。
不回写的任务会一直停在「处理中」，被防丢机制反复重跑。重试耗尽的请求，框架默认帮你
`update_task(..., ok=False)`（覆写 `failed_request` 时记得 `super()`）。

!!! note "标「已完成」会等数据真正落库"
    上面那句 `yield` 只是把数据放进内存缓冲，还没落库。所以 `update_task(ok=True)`
    **不会立刻写任务表** —— 它等这次请求产出的数据整批落到持久介质之后才写。

    否则任务在库里是「已完成」而数据只在内存里，节点一死就两头落空：
    防丢机制**只回收「处理中」**，标了完成的任务永远不会被重跑。
    真库实测过这个形态：5 个任务全标完成，实际落库 **0 行**。

    三种情况仍是立即写入：这次请求没产出任何 item、`ok=False`（标失败不取决于数据）、
    以及不在请求处理过程中调用（比如 master 自己的逻辑）。
    写库失败被 dump 到 `failed_items.jsonl` 的那批**算落库**
    （与[请求销账](item-pipeline.md#落库之后才给任务销账)同一条规则），
    任务照常标完成 —— 数据可用 `netspy retry --items` 回放。

## 两种角色

| | 启动 | 职责 |
|---|---|---|
| **master** | `spider.start_monitor()` | 批次生命周期：到点开新批次（重置任务表）、把待处理任务推进 Redis 待抓队列、按任务表刷新进度、回收卡死任务、批次跑完收尾 |
| **worker** | `spider.start()` | 消费 Redis 队列，对每个任务调 `task_requests`，解析完回写状态。可多进程 / 多机 |

同一命名空间只允许一个 master（Redis 锁），worker 随便起几个。

```bash
python main.py monitor     # 一台，常驻；或 cron 每天跑一次 start_monitor(once=True)
python main.py             # 多台 / 多进程，worker
```

- `start_monitor(once=True)`：把当前批次跑到完成即返回，适合塞进 crontab。
- `start_monitor()`：常驻，一个批次跑完后等到下个 `BATCH_INTERVAL` 再开下一批。
- worker 建议 `keep_alive=True` 常驻（等 master 派活）；`keep_alive=False` 则队列抽干即退出。

## 一次巡检做什么

master 每 `BATCH_MONITOR_INTERVAL` 秒：

1. 把卡在「处理中」超过 `BATCH_LOST_TASK_STALE` 秒的任务重置回「待处理」
2. 认领「待处理」任务（置为处理中），最多 `BATCH_PUSH_LIMIT` 个，推进 Redis 队列 `<ns>:batch_pending`
3. 按任务表统计刷新批次记录的 done / fail / total，打印「批次进度 X/Y」
4. 所有任务都已结算（完成 + 失败 == 总数）→ 批次记录 `is_done=1`，置 `<ns>:batch_done` 标志

## 换存储后端

默认落 MySQL（`MysqlBatchStore`）。测试或小规模内存跑批可传 `batch_store=`：

```python
from netspy.core.batch_store import MemoryBatchStore

store = MemoryBatchStore([{"id": 1, "url": "..."}, {"id": 2, "url": "..."}])
CrawlTask(batch_store=store).start_monitor(once=True)
```

自定义后端继承 `netspy.core.batch_store.BatchStore`。

## 配置

| 配置 | 默认 | 说明 |
|---|---|---|
| `BATCH_INTERVAL` / `BATCH_INTERVAL_UNIT` | `7` / `"day"` | 批次间隔（`day` \| `hour`） |
| `BATCH_MONITOR_INTERVAL` | `10.0` | master 巡检间隔（秒） |
| `BATCH_LOST_TASK_STALE` | `600.0` | 「处理中」超过这么久算丢，重置回待处理 |
| `BATCH_PUSH_LIMIT` | `5000` | master 单次最多认领 / 推送多少任务 |
| `BATCH_TASK_ID_FIELD` | `"id"` | 任务表主键列名 |
| `BATCH_TASK_STATE_FIELD` | `"batch_status"` | 任务表状态列名 |
| `BATCH_TASK_TIME_FIELD` | `"update_time"` | 任务表更新时间列名 |

队列 / 去重 / 命名空间沿用 [`Spider`](distributed.md) 那套（Redis）。
