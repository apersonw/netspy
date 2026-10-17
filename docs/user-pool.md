# 账号 / Cookie 池

需要登录才能抓、或者想用一批账号轮流抓、被封了自动换号 —— 用账号池。

爬虫里覆写两个方法即可，调度器会自动把池子挂到下载链上：

```python
import netspy as ns


class MallSpider(ns.Spider):
    def user_pool(self):
        return ns.LocalUserPool(
            users=[
                {"username": "u1", "password": "p1"},
                {"username": "u2", "password": "p2"},
            ],
            login=self.login,          # 账号没 cookie 时调用
        )

    def login(self, user):
        resp = ns.Request(
            "https://mall.com/api/login",
            method="POST",
            data={"user": user.username, "pwd": user.password},
        ).download()
        return dict(resp.cookies)      # 返回 cookies dict

    def check_login(self, response):
        return "请先登录" not in response.text   # False -> 拉黑当前账号，换号重试

    def start_requests(self):
        yield ns.Request("https://mall.com/orders", callback=self.parse)

    def parse(self, request, response):
        ...
```

每个请求下载前自动挂上某个账号的 cookie；下载后 `check_login` 判断是否掉登录，
掉了就 `report_bad`（默认拉黑 30 分钟、清掉缓存的 cookie）并换一个账号重试。

!!! warning "池子空了不会匿名发出去"
    账号全被拉黑（正是站点在封你的时候）会让池子取不到号。这时框架**不会**
    匿名把请求发出去 —— 需要登录的站会回一张登录墙，而那张页面会被当成数据入库。

    池空时先等 `USER_POOL_WAIT_TIMEOUT`（默认 10 秒，游客池能现登、拉黑也会到期），
    仍拿不到才抛 `UserUnavailableError`，走正常重试；重试耗尽后落进
    `failed_requests.jsonl`。`netspy retry --requests` 只是**探活**（重新下载看状态码，不跑回调、不入库）；要把数据真正抓回来，开 `RETRY_FAILED_ON_START` 重跑一次爬虫。
    这个错误**不计入熔断** —— 没号可用是自己这边的事，不是站点挂了。

    `check_login` 判断的是**响应**，和有没有挂账号无关，所以匿名发出的请求
    （允许匿名时）照样会被检查。早先的版本在这里先看有没有账号，
    没有就直接放行 —— 于是最需要检查的时候反而不检查了。

    实测：账号被封后 12 行落库里 **10 行是登录墙页面**，而用户写的 `check_login`
    一次都没被调用。修复后登录墙 **0** 行，那 10 个请求落进了 `failed_requests`。

    确实想要「有号更好、没号也能抓」，把 `USER_POOL_ALLOW_ANONYMOUS = True` 打开。

## 几种池子

| 类 | 场景 |
|---|---|
| `LocalUserPool` | 单机。给一批账号，轮流用，被封的拉黑一段时间 |
| `GuestUserPool` | 无账号，只要匿名 cookie。`login()` 拿一份游客 cookie，维护固定数量的游客会话 |
| `RedisUserPool` | 多进程 / 多机共享一批账号 —— **不会两个节点同时用同一个号**，cookie 缓存在 Redis |

```python
# 游客
def user_pool(self):
    return ns.GuestUserPool(login=self.get_guest_cookie, size=5)

# 分布式共享账号
def user_pool(self):
    return ns.RedisUserPool(
        "MallSpider",
        accounts=[{"username": "u1", "password": "p1"}, ...],
        login=self.login,
        cookie_ttl=3600,
    )
```

## 手动用

```python
pool = ns.LocalUserPool([{"username": "u1"}], login=my_login)
user = pool.get()
try:
    resp = ns.Request(url, cookies=user.cookies).download()
    pool.report_ok(user)
except SomethingBad:
    pool.report_bad(user, block_seconds=600)
```

单个请求也能直接指定 cookie：`ns.Request(url, cookies={"sid": "..."})`。
