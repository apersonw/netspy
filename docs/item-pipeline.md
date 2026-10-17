# 数据与去重

## Item

```python
class NewsItem(ns.Item):
    __table_name__ = "news"          # 不写则由类名推导：NewsItem -> news
    __unique_key__ = ["url"]         # 指纹只用这些字段（不写则用全部非空字段）

    def pre_to_db(self):             # 落库前钩子，可选
        self.title = self.title.strip()
```

```python
item = NewsItem()
item.url = "https://..."
item.title = "标题"
yield item
```

直接 `yield` 普通 `dict` 也行，落到 `ITEM_DEFAULT_TABLE`（默认 `items`），但不参与 Item 去重。

## Pipeline

`setting.py`：

```python
ITEM_PIPELINES = [
    "netspy.pipelines.console.ConsolePipeline",
    "netspy.pipelines.csv.CsvPipeline",
    "netspy.pipelines.mongo.MongoPipeline",
    "netspy.pipelines.mysql.MysqlPipeline",
]
```

| Pipeline | 说明 |
|---|---|
| `ConsolePipeline` | 打日志，调试用 |
| `CsvPipeline` | 按表写 `<CSV_OUTPUT_DIR>/<table>.csv`，首批数据决定表头 |
| `MongoPipeline` | `insert_many`；`UpdateItem` 按 `__update_key__` 逐条 `update_one` upsert |
| `MysqlPipeline` | `executemany` 批量写；`MYSQL_UPDATE_ON_DUPLICATE=True` 时用 `INSERT ... ON DUPLICATE KEY UPDATE` 按唯一键 upsert；`UpdateItem` 按 `__update_key__` 逐条 `UPDATE` |

自定义：继承 `netspy.pipelines.base.BasePipeline`，实现 `save_items(table, items) -> bool`
（返回 `False` 该批会被 dump 到 `failed_items.jsonl`）。

单个 Item 可覆盖管道：`item.pipelines = ["myproj.pipelines.SpecialPipeline"]`。

## MySQL

`pip install "netspy[mysql]"`，连接信息走配置（`MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_USER`
/ `MYSQL_PASSWORD` / `MYSQL_DB`），底层是 pymysql + DBUtils 连接池。

```python
ITEM_PIPELINES = ["netspy.pipelines.mysql.MysqlPipeline"]
```

`save_items` 把一批数据拼成一条 `executemany`，字段以每批第一条为准（和 `CsvPipeline` 一致）。
`MYSQL_UPDATE_ON_DUPLICATE=True`（默认）时带 `ON DUPLICATE KEY UPDATE`——表上有唯一键 /
主键就是 upsert，配合 `__unique_key__` + 去重即可「重跑不重复入库」。

### 用表结构反射生成 Item

```bash
netspy create -i news --table news
netspy create -i news --table news --mysql mysql://root:pwd@10.0.0.2:3306/spider
```

读 `SHOW FULL COLUMNS FROM news`，生成的 Item 带 `__table_name__`、按主键填好
`__unique_key__`，并把每个字段 + 注释列成提示（注解形式，不进 `__dict__`）。
不加 `--mysql` 时用 `setting` 里的 MySQL 配置。

## PostgreSQL

`pip install "netspy[postgres]"`，连接信息走 `POSTGRES_HOST` / `POSTGRES_PORT` /
`POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`，底层是 psycopg 3 + `psycopg_pool`。

```python
ITEM_PIPELINES = ["netspy.pipelines.postgres.PostgresPipeline"]
```

写入骨架和 MySQL 完全一样（同一个 `SqlPipeline` 基类），**差别只在冲突处理**：
MySQL 的 `ON DUPLICATE KEY UPDATE` 不用指明冲突键，Postgres 的 `ON CONFLICT` 必须给
冲突目标。所以拆成两个配置：

| `POSTGRES_ON_CONFLICT` | 行为 |
|---|---|
| `"nothing"`（默认） | `ON CONFLICT DO NOTHING`，主键 / 唯一键重复就跳过。对爬虫最安全 |
| `"update"` | `ON CONFLICT (...) DO UPDATE SET ...`，即 upsert。**需要** `POSTGRES_CONFLICT_TARGET` |
| `"error"` | 裸 `INSERT`，冲突则整批失败并 dump 到 `failed_items.jsonl` |

```python
POSTGRES_ON_CONFLICT = "update"
POSTGRES_CONFLICT_TARGET = ["url"]   # 通常是唯一索引的列
```

`update` 模式漏填 `POSTGRES_CONFLICT_TARGET` 会降级成 `DO NOTHING` 并告警一次 ——
宁可少写几条，也好过整批抛异常。冲突目标列本身不会出现在 `SET` 里（Postgres 会报错）。

!!! note "许可证"
    psycopg 是 **LGPL-3.0**，而 Netspy 是 MIT。它是**可选** extra、由你自行安装、
    未被打包进本项目，因此不影响 Netspy 的授权；但如果贵司对 LGPL 依赖有合规要求，
    这里提前知会一声。

## Elasticsearch

`pip install "netspy[elasticsearch]"`，地址走 `ELASTICSEARCH_HOSTS`。
`table_name` 当索引名，`save_items` 走官方 `helpers.bulk`。

```python
ITEM_PIPELINES = ["netspy.pipelines.elasticsearch.ElasticsearchPipeline"]
```

`UpdateItem` 会把 `__update_key__` 各字段的值拼成 `_id` 做 upsert（`doc_as_upsert`），
所以重跑不会产生重复文档。索引与 mapping 由你自己管，框架不替你建索引。

## Kafka

`pip install "netspy[kafka]"`，地址走 `KAFKA_BOOTSTRAP_SERVERS`。
`table_name` 当 topic，每条 Item 序列化成一条 JSON 消息。

```python
ITEM_PIPELINES = ["netspy.pipelines.kafka.KafkaPipeline"]
```

!!! warning "它是投递，不是存储"
    这个管道只保证消息发出去了（每批 `flush` 后才返回成功），下游怎么落库不归它管。
    因此**不支持 `UpdateItem`** —— 消息队列没有「按主键更新一条已发出的消息」这种语义。
    要既投递又落库，把 Kafka 和一个数据库管道一起写进 `ITEM_PIPELINES` 即可。

## 管道能力对照

| 管道 | extra | `save_items` | `UpdateItem` | 备注 |
|---|---|---|---|---|
| `ConsolePipeline` | — | ✅ | ✅ | 默认，打日志 |
| `CsvPipeline` | — | ✅ | ❌ | 按表名分文件 |
| `MongoPipeline` | `mongo` | ✅ | ✅ | `update_one(upsert=True)` |
| `MysqlPipeline` | `mysql` | ✅ | ✅ | `ON DUPLICATE KEY UPDATE` |
| `PostgresPipeline` | `postgres` | ✅ | ✅ | `ON CONFLICT`，见上 |
| `ElasticsearchPipeline` | `elasticsearch` | ✅ | ✅ | `_id` 由 `__update_key__` 拼出 |
| `KafkaPipeline` | `kafka` | ✅ | ❌ | 投递而非存储 |

## 去重

!!! note "去重服务不可用时会降级，而不是丢数据"
    Item 去重要查 Redis。查不通时框架按「没见过」放行并继续写库 ——
    **去重是优化，丢数据不是可选项**。这一批会记一条 warning，
    运行结束的汇总里也会带上「⚠️ 去重降级 N 批（可能重复入库）」。

    早先的版本让异常从落库循环里穿出去：后面的分组既没写库、也没 dump。
    实测 9 条数据分 3 组，查重抖一下 **整批 9 条全丢**，
    记指纹抖一下 **第一组写成功、剩下 6 条凭空消失**。


- **请求级**：`Request.filter_repeat=True` 时按 `fingerprint`（method + 规范化 URL + body）去重
- **Item 级**：`ITEM_FILTER_ENABLE=True` 时按 `Item.fingerprint` 去重，**写库成功后**才记指纹

```python
DEDUP_FILTER = "memory"   # 布隆过滤器，省内存，极小概率误判
DEDUP_FILTER = "lite"     # 精确 set，内存换准确
```

!!! note "关于「重跑不重复」"
    AirSpider 的去重是**每次运行新建的内存过滤器**，同进程重跑不会自动跳过。
    单机幂等的正确姿势是 `UpdateItem` + `__update_key__`（按键 upsert）。
    跨进程 / 断点续爬的持久化去重是 Redis 版 `Spider` 的能力（Roadmap）。

## 写库失败

某批 `save_items` 返回 `False` → dump 到 `failed_items.jsonl`，指纹不记。
恢复：`netspy retry --items`（仍失败的写回文件，全部成功则删除文件）。

dump 出来的每行**带着回放所需的全部信息**，而不只是表名和数据：

```json
{"table": "prices", "data": {"url": "...", "price": 99}, "update_keys": ["url"]}
```

`update_keys` 决定回放走 `update_items` 还是 `save_items`。少了它，
`UpdateItem` 会退化成普通 INSERT —— 而 PostgreSQL 默认的
[`ON CONFLICT DO NOTHING`](#postgresql) 会让这条 INSERT **什么都不做并返回成功**，
于是 `retry` 报告成功、删掉文件，那次更新永久消失。

逐条指定了 `item.pipelines` 的数据同样会记下路由，
回放时只走它自己那几个管道，不会被灌进当前全部 `ITEM_PIPELINES`。

!!! note "老的 dump 文件仍然能回放"
    没有这些字段的旧记录按普通插入处理。

管道要是没实现 `update_items`（基类直接抛），那一组算失败留在文件里，
**不会让整个回放崩掉** —— 否则会连带丢掉本来能回放的其它记录。

## 落库之后才给任务销账

分布式下（`Spider` + Redis），一条请求的任务**要等它产出的数据真正落到持久介质
之后**才销账 —— 入库成功，或者写库失败被 dump 进 `failed_items.jsonl`，两者都算。

反过来（销账早于落库）会**静默丢数据**，而且外部完全看不出来：

1. 数据只在内存缓冲里，节点被硬杀就没了
2. 任务已经销过账，不在在途表里，[租约回收](distributed.md#节点被硬杀之后)捡不回来
3. **重跑一遍也补不回来** —— 请求指纹是入队**前**就写的，新节点看一眼去重就跳过

实测（400 页、跑到一半 `SIGKILL`、第二个节点接手跑完，管道每批写 1.5 秒）：
靶子确实发出了全部 400 页、爬虫 `exitcode=0` 报告抓完，**只落库 340 行**。

代价是**重抓变多**：那些没落库的任务不销账，会在租约到期后被重抓一遍。
同一场景下重抓从 6 页涨到 66 页 —— 把「静默丢 60 条」换成「多抓 60 页」。
数据这边给的是**至少一次**，所以 [Item 去重](#去重) 该开着。

!!! note "AirSpider 不涉及"
    单机内存队列没有租约也没有回收，进程一死，队列本身就没了。

## UpdateItem

!!! note "`UpdateItem` 的指纹按全字段算，不看 `__unique_key__`"
    它的语义是「同一条记录、**新的值**」。指纹要是按 `__unique_key__` 算，
    第二次更新和第一次同指纹，会被 [Item 去重](#去重)直接吃掉 ——
    而去重默认是开着的。

    所以 `UpdateItem` 的指纹**永远用全部非空字段**：值变了就写得进去，
    值没变的空转重复仍然被挡住。两件事同时成立，不是二选一。

    `__unique_key__` 在 `UpdateItem` 上仍然有用 —— 不写 `__update_key__` 时，
    upsert 的匹配键会回退到它。那是「**怎么写**」，和「**要不要写**」是两个问题。


!!! warning "SQL 管道是 UPDATE，不是 upsert"
    `MysqlPipeline` / `PostgresPipeline` 按 `__update_key__` 逐条 `UPDATE` ——
    **目标行不存在时什么都不会发生**。而 Mongo 是 `update_one(upsert=True)`、
    ES 是 `doc_as_upsert`，两者会把不存在的记录建出来。换库时要留意这个差异。

    没匹配到任何行的那条会**算作写入失败**：整批 dump 到 `failed_items.jsonl`，
    日志里点名具体的键，去重指纹不记（所以这条 URL 还会被重抓）。
    早先的版本忽略影响行数、报告成功，数据就那样静默消失了。

    想要「不存在就插入」，用 `MYSQL_UPDATE_ON_DUPLICATE=True`
    或 `POSTGRES_ON_CONFLICT="update"` 走 upsert，而不是指望 `UpdateItem`。


```python
class PriceItem(ns.UpdateItem):
    __table_name__ = "price"
    __update_key__ = ["sku"]        # 按 sku 查已有记录，$set 更新，没有则插入
```
