# 分布式（Spider）

`Spider` 把队列和去重都放到 Redis，多个进程 / 多台机器跑同一个爬虫、共享进度、断点续爬。

```bash
pip install "netspy[redis]"
```

## 用法

和 `AirSpider` 一模一样，只是换个基类：

```python
import netspy as mw


class NewsSpider(mw.Spider):
    __custom_setting__ = dict(REDIS_URL="redis://localhost:6379/0")

    def start_requests(self):
        for page in range(1, 100):
            yield mw.Request(f"https://example.com/news?p={page}", callback=self.parse)

    def parse(self, request, response):
        ...


if __name__ == "__main__":
    NewsSpider().start()
```

在多台机器 / 多个进程里跑同一份代码即可：

```bash
python main.py    # 机器 A
python main.py    # 机器 B —— 自动加入，一起消费队列
```

## 工作方式

| 机制 | 说明 |
|---|---|
| **队列** | Redis zset（`<prefix>:<redis_key>:z_requests`），score = priority |
| **去重** | Redis 布隆 / 精确 set（`DEDUP_FILTER=redis` / `redis-set`） |
| **种子一次性** | `start_requests` 靠 `SET NX EX` 锁，只有抢到锁的节点执行；其他节点直接消费队列 |
| **断点续爬** | 队列在 Redis，进程崩溃重启后队列还在，继续跑；崩溃节点采集器里没跑完的会在退出时推回队列 |
| **多节点结束检测** | 每个节点每 `HEARTBEAT_INTERVAL` 秒写一次心跳（含本节点在途数）。只有「队列空 + 所有活跃节点在途数都为 0」才判定结束 |
| **失败请求** | 重试耗尽的请求 RPUSH 到 `<prefix>:<redis_key>:failed_requests`；开 `RETRY_FAILED_ON_START` 后，下次启动时把它灌回队列（多节点同时启动只有一个节点回放） |

`redis_key` 默认是爬虫类名，决定 Redis 命名空间。不同爬虫用不同 `redis_key`，
同一个爬虫的多个进程用同一个。

## 运行作用域（定时重跑）

上面那套模型是「**一个 `redis_key` = 一个可续跑的作业**」：种子只注入一次，
抓过的 URL 永远不再抓。对「一次性大抓取，多机分摊，崩了续上」这是对的；
对**定时重跑**是错的 —— 而且错得没有声音。

!!! danger "生产上真发生过"
    一个 2 节点的分布式任务由平台每 5 分钟触发一次。49 小时里 1160 个实例，
    **1158 个「请求成功 0，入库 0 条」**，全部 exit 0，全部被记成 success。
    只有第一轮真干了活。

    机制：第一轮把 `lock:seed` 留在了 Redis 里（24 小时 TTL），之后每一轮
    的每个节点都打一句「另一节点已注入种子」，消费一个空队列，退出；
    每 24 小时抢到锁的那一个节点，种子 URL 又被没有 TTL 的布隆去重掉。

修法是给「运行」一个身份：

```bash
export NETSPY_RUN_ID=$(date +%s)     # 或任何每次都不同的字符串
python main.py
```

设了 `RUN_ID`，队列 / 种子锁 / 在途 / 心跳 / 失败列表都落到
`<prefix>:<redis_key>:run:<RUN_ID>` 下：

| | 没设 `RUN_ID`（默认） | 设了 `RUN_ID` |
|---|---|---|
| 再跑一次 | 续上次：种子不再注入，队列空就退出 | **从头抓**（新运行） |
| 同一个 id 再起 | — | 续那次运行（崩了续上的语义还在，只是挪到了运行这一层） |
| 去重 | 永久，跨运行 | 按 `DEDUP_SCOPE`：默认 `run`（每次重抓），`spider` = 增量爬 |
| key 的寿命 | 永久 | `RUN_TTL`（默认 7 天），跑着时由心跳续期 |
| 运行索引 | — | `<prefix>:<redis_key>:runs`（zset，score = 开始时刻） |

`DEDUP_SCOPE` 默认偏向 `run`，因为选错的代价不对称：`run` 选错了是多抓一遍
（看得见），`spider` 选错了是空转（exit 0，看不见 —— 上面那两天就是这么过的）。
真要增量爬，显式写 `DEDUP_SCOPE = "spider"`，启动时会打一行 INFO 说明
本轮不会重抓以前抓过的 URL。

[NetspyHub](https://github.com/apersonw/netspyhub) 给每个实例自动注入
`NETSPY_RUN_ID`，平台上的任务不用管这件事。

`BatchSpider` 不受 `RUN_ID` 影响：它的身份是批次作业，批次本身就是重跑单位。

!!! note "没设 `RUN_ID` 也不再是静默的"
    节点拿不到种子锁时会看一眼：锁是不是很早以前留下的、队列是不是空的、
    有没有活节点。三条都是 —— 那就是一个已完成的作业，会打一条 **WARNING**
    说明本轮无事可做、以及怎么改。同时启动的节点之间不会误报（锁太新）。

## 常驻模式

```python
NewsSpider(keep_alive=True).start()
```

爬完队列也不退出，继续轮询 —— 配合 `TaskSpider`（Roadmap）或一个长期运行的 worker 池。

## 配置

```python
REDIS_URL = "redis://:password@host:6379/0"
REDIS_KEY_PREFIX = "netspy"      # 所有 key 的前缀
DEDUP_FILTER = "redis"               # redis（布隆）| redis-set（精确）
# ⚠️ **别用默认的 memory / lite**：那是**进程内**去重，每个节点各有一份指纹、
# 互相不知道。实测两节点、两个入口页都链到同一页面：memory 下那页被抓 2 次、
# redis 下 1 次；而任务租约是「至少一次」语义，Item 去重不共享就会重复入库
# （SQL 管道有唯一键兜底，CSV / Mongo / 自定义管道没有）。
# 共享队列会让大部分现象看起来正常，所以这个错配不报错、只是悄悄多抓多写 ——
# 0.18.0 起启动时会为此告警。
SPIDER_SEED_LOCK_TTL = 86400         # 种子锁 TTL（秒）；过期后下次启动会重新注入种子
RUN_ID = ""                          # 设了 = 每次运行独立，见「运行作用域」；平台会注入
DEDUP_SCOPE = "auto"                 # run（每次重抓）| spider（增量爬）| auto（有 RUN_ID 就 run）
RUN_TTL = 7 * 86400                  # 运行作用域下 key 的保留秒数
HEARTBEAT_INTERVAL = 3.0
HEARTBEAT_STALE = 15.0               # 超过这个秒数没心跳的节点视为已死
SPIDER_KEEP_ALIVE = False
```

## 去重的容量

布隆过滤器是**固定容量**的，超容后误判率会急剧升高 —— 而在去重里，
「误判」意味着**一个从没抓过的 URL 被当成「已抓过」静默丢掉**。

实测（基础容量 10 万、目标误判率 `1e-6`，单层）：

| 已插入 | 倍数 | 误判率 | 实际后果 |
|---|---|---|---|
| 10 万 | 1× | 0% | 正常 |
| 30 万 | 3× | 7.2% | 每 13 个新 URL 丢 1 个 |
| 50 万 | 5× | **53.7%** | **一半的站点抓不到** |
| 80 万 | 8× | 92.8% | 基本不再发现新页面 |

所以布隆是**分层**的：一层填满就加一层，容量 ×2、误判率 ×0.5 ——
后者是关键，各层误判率成等比数列，**总误判率收敛到目标值的 2 倍以内**，
加多少层都不会失控。

```python
DEDUP_INITIAL_CAPACITY = 1_000_000   # 第一层的容量
DEDUP_MAX_LAYERS = 4                 # 最多几层
DEDUP_WARN_FILL_RATE = 0.8           # 填到八成就告警
```

代价是内存：

| 层数 | 累计容量 | 累计内存 | 总误判率上界 |
|---|---|---|---|
| 1 | 100 万 | 3MB | 1.0e-6 |
| **4（默认）** | **1500 万** | **57MB** | 1.88e-6 |
| 6 | 6300 万 | 260MB | 1.97e-6 |
| 8 | 2.55 亿 | 1.1GB | 1.99e-6 |

!!! warning "层数必须有顶"
    `DEDUP_MAX_LAYERS` 不是可选的保险 —— 让层数无限长下去就是把 Redis 内存
    变成一个**无界资源**，而那正是[资源边界](spider.md#资源边界)刚从下载路径上
    清掉的东西。到顶之后行为退化成单层布隆，并触发下面的告警。

    查询要遍历所有层，层数越多越慢，这是不宜开太大的另一个原因。

### 填满了会告警

填到 `DEDUP_WARN_FILL_RATE`（默认八成）就走[告警通道](observability.md)（飞书 / 邮件），
到顶后再写还会额外记一条日志。

这条告警是有来由的：在它之前**完全没有任何信号** —— 计数一直有记录但没人读，
统计里只显示「去重 N 条」，爬虫只是「提前结束了」或「少抓了很多」，查不出原因。

### 不想要误判就用精确去重

```python
DEDUP_FILTER = "redis-set"    # 一个 Redis SET，成员即指纹
```

不会误判，代价是内存随抓取量**线性**增长。这是取舍，不是「布隆坏了就换它」。

!!! note "升级不会让已有去重状态失效"
    第 0 层沿用原来的 key（`{name}:dedup:bloom`），所以从旧版本升上来的
    断点续爬不会从头再抓一遍。新增的层是 `:1`、`:2` ……

## 节点被硬杀之后

[优雅停止](spider.md#优雅退出)和退出落盘都要求**进程还活着**。而 OOM Killer、断电、
`docker kill` 不给这个机会 —— 队列用 `zpopmin`，**取走即删**，任务一旦进了某个节点的
内存，Redis 里就不存在了，进程一没它们就随之消失。

```python
SPIDER_TASK_LEASE = 600.0   # 秒；0 = 关闭（回到取走即删的老行为）
```

节点领走任务的同时会把它记进在途表（`{ns}:z_inflight`，score 是租约到期时刻），
处理完销账。超过租约还没销账的任务，任意节点都可以把它放回队列。

实测（24 个任务，节点跑 1.2 秒后被 `SIGKILL`，再起一个节点续爬）：

| | 结果 |
|---|---|
| 没有租约 | **只剩 5 / 24** |
| 有租约 | **24 / 24** |

!!! warning "这带来「至少一次」语义"
    节点只是**卡住**（长 GC、慢下载、网络挂起）而不是死了的话，租约照样会到期，
    任务被归还并再处理一遍 —— 而原节点可能随后又处理完了同一条。

    [请求去重](#去重的容量)能挡住大部分重复入库，但**回调仍可能跑两次**，
    有副作用的操作（扣款、发消息、写外部系统）要自己保证幂等。

    这是分布式队列绕不开的取舍：要么接受偶尔重复，要么接受硬杀丢数据。
    `SPIDER_TASK_LEASE` 就是这两者之间的旋钮 —— 调大更保守（丢得少、恢复慢），
    调小恢复快但更容易误判活着的慢节点。

### 活着的节点会自动续期

租约是**从领走那刻起算的**，而 collector 一次会领走 `COLLECTOR_TASK_COUNT`（默认 100）
个任务。要是每页处理得慢一点，排在后面的任务在被碰到之前租约就过期了 ——
节点全程健康，任务却被别人抢去重抓一遍。

所以[心跳线程](#多节点同时启动)每 `HEARTBEAT_INTERVAL` 会顺手把本节点仍持有的任务
（缓冲区里排队的 **+ worker 正在处理的**）租约往后推。心跳线程还在跑本身就代表
节点还活着 —— 两件事的判据天然是同一个。

实测（5 个任务、每个处理 0.6 秒、租约 2 秒）：

| | 健康节点被抢走 | 死节点被回收 |
|---|---|---|
| 没有续期 | **2 / 5** | 3 |
| 有续期 | **0** | 3 |

两条同时成立才算数：只做到「健康节点不被抢」很容易滑向「续期太激进、死节点也回收不了」。

!!! note "续期只更新已存在的租约"
    用的是 `ZADD XX`。任务如果已经处理完销账、或已被别的节点回收走了，
    续期就什么都不做 —— 否则会把它重新塞进在途表，**两个节点同时成为持有者**，
    那比不续期更糟。

### 数据落库之前不销账

上面两条保的是**任务**扛得住硬杀。数据是另一回事 ——
销账发生在解析完成那一刻，而那时 item 还只在内存缓冲里。

所以产出过数据的请求，销账权交给 [`ItemBuffer`](item-pipeline.md#落库之后才给任务销账)：
整批落到持久介质之后才销。在那之前它一直算作「本节点持有」，
既参与上面的租约续期，也参与下面的结束检测。

| 管道每批写入耗时 | 落库行数 | 静默丢失 | 重抓页数 |
|---|---|---|---|
| 0.3 秒（修复前 / 后） | 388 → **400** | 12 → **0** | 6 → 23 |
| 1.5 秒（修复前 / 后） | 340 → **400** | 60 → **0** | 6 → 66 |

重抓变多是这笔交易的**代价，不是副作用**：没落库的任务不销账，就会被重抓。
换来的是数据不再无声无息地少掉。

### 结束检测会等在途任务

队列空了但在途表里还有任务时，爬虫**不会**收工 —— 那些任务可能正在别的节点手里，
也可能属于一个已经死掉的节点，租约到期后还要再抓一遍。

## 多节点同时启动

节点起来时，只有一个能拿到[种子锁](#重新注入种子)去注入种子，其余节点直接消费队列。
这中间有个窗口：**其余节点会先看到一个空队列**。

```python
SPIDER_STARTUP_GRACE = 10.0   # 秒；0 = 关闭
```

在这个窗口里，一个任务都没拿到过的节点**不会**判定「抓完了」。没有它的话，
节点会在 `DONE_CHECK_TIMES × DONE_CHECK_INTERVAL`（默认 1.5 秒）内退出 ——
而播种节点那时往往还没把种子推进队列。

实测（两个 worker 容器同秒启动）：

| | 结果 |
|---|---|
| 没有宽限 | 一个节点抓完全部，另一个 **1 秒后退出、0 个请求** |
| 有宽限 | 两个节点分摊，35 + 28 = 63 请求，**交集 0** |

心跳结束检测挡不住这个：播种节点在那一刻的 `pending` 也是 0，它自己还没开始拉活。

!!! note "只有空跑的节点才等"
    拿到过任务之后就按原来的规则判定，不会拖慢正常结束。
    代价只在「真的没活干」时体现：多等 `SPIDER_STARTUP_GRACE` 秒才退出。

## 重新注入种子

想让爬虫重新从头跑（比如换了一批种子）：**首选设一个新的 `RUN_ID`**
（见[运行作用域](#运行作用域定时重跑)），什么都不用删。
没用 `RUN_ID` 的老部署，删掉种子锁和队列：

```bash
redis-cli DEL netspy:NewsSpider:lock:seed netspy:NewsSpider:z_requests
redis-cli DEL netspy:NewsSpider:dedup:bloom \
  netspy:NewsSpider:dedup:bloom:1 \
  netspy:NewsSpider:dedup:bloom:2 \
  netspy:NewsSpider:dedup:bloom:3 \
  netspy:NewsSpider:dedup:bloom:count      # 顺便清去重（含各分层与计数）
```

---

# TaskSpider

有一堆待抓的 id / url，想用一个或多个常驻进程慢慢消费 —— 用 `TaskSpider`。
它不走 `start_requests`，而是一个后台线程每 `TASK_POLL_INTERVAL` 秒从任务源
（默认 Redis list）拉一批任务，对每个任务调 `task_requests(task)` 生成请求。

```python
import netspy as mw


class ProductSpider(mw.TaskSpider):
    def task_requests(self, task):
        yield mw.Request(
            f"https://shop.com/p/{task['id']}",
            callback=self.parse,
            cb_kwargs={"task": task},
        )

    def parse(self, request, response, task):
        yield {"id": task["id"], "price": response.css(".price::text").get()}
```

生产任务（任意进程 / 脚本）：

```python
ProductSpider.push_tasks({"id": 1}, {"id": 2}, {"id": 3})
```

消费（一台或多台机器）：

```python
ProductSpider().start()               # 任务耗尽后退出
ProductSpider(keep_alive=True).start() # 常驻，队列空也不退出
```

多个节点 `lpop` 同一个 Redis list，任务天然分摊、不重复。运行中还能
`self.add_tasks(...)`（比如在 parse 里发现了新的待抓项）。

覆写 `fetch_tasks(self, limit)` 可换成从 MySQL / Mongo 查：

```python
class ProductSpider(mw.TaskSpider):
    def fetch_tasks(self, limit):
        rows = db.query("SELECT id FROM crawl_task WHERE status=0 LIMIT %s", limit)
        db.execute("UPDATE crawl_task SET status=1 WHERE id IN ...")   # 标记为处理中
        return [dict(r) for r in rows]
```

| 配置 | 默认 | 说明 |
|---|---|---|
| `TASK_POLL_INTERVAL` | `2.0` | 轮询任务源的间隔（秒） |
| `TASK_BATCH_SIZE` | `100` | 单次拉多少个 |
| `TASK_EXHAUST_POLLS` | `3` | 连续这么多次拉不到任务，视为耗尽（`keep_alive=False` 时据此退出） |

## 停止节点：用 SIGTERM 或 SIGINT，别用 SIGKILL

节点收到 `SIGTERM` / `SIGINT` 时会优雅停止：把 collector 与 buffer 里**已从 Redis
领走但还没跑完**的任务**推回队列**，交给其他节点或下次重启接管。

这一步不能省。`Collector` 一次会 `get_batch` 领走最多 `COLLECTOR_TASK_COUNT`（默认 100）
个任务，而 Redis 队列用的是 `zpopmin` —— **取走即删**。节点被 `SIGKILL`（`kill -9`）
硬杀时没有任何机会推回，这批任务就永久丢失了。

!!! note "实测"
    24 个任务的场景下，节点被硬杀会丢掉 20 个；优雅停止则全数恢复、且无重复。
    `docker stop`、Kubernetes 驱逐、`systemctl stop` 发的都是 `SIGTERM`，
    都会走优雅停止这条路。只有 `kill -9` 和 OOM Killer 绕不过去。

要缩小硬杀的损失面，可以调小 `COLLECTOR_TASK_COUNT`（代价是更频繁地访问 Redis）。
