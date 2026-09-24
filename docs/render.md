# 浏览器渲染

需要：

```bash
pip install "netspy[render]"
playwright install chromium
```

!!! note "停止时排队中的请求会失败，而不是挂住"
    渲染是串行的（`pool_size` 个浏览器轮流处理），停止爬虫时队列里往往还排着请求。
    这些请求会被当场收尾成失败，调用方拿到 `RequestError` ——
    重试用尽后落进 `failed_requests.jsonl`。`netspy retry --requests` 只是**探活**（重新下载看状态码，不跑回调、不入库）；要把数据真正抓回来，开 `RETRY_FAILED_ON_START` 重跑一次爬虫。

    早先的版本不收尾它们：`submit()` 的等待没有上限，而 `close()` 不排空队列，
    于是那些调用线程**永远醒不来**。实测 1 个在渲染、4 个排队时关闭，
    4 个线程永久挂起 —— `stop()` 它不看、`join()` 它不动，优雅停止就此失效。

## 用法

```python
yield mw.Request(
    "https://spa.example.com/list",
    render=True,
    wait_for="ul.items li",          # 等这个选择器出现再取内容
    render_time=1.0,                 # 之后再等 1 秒
    render_script=lambda page: page.click("button.load-more"),  # 在浏览器线程执行
    callback=self.parse,
)
```

`response.text` 是渲染后的完整 DOM，`.xpath / .css` 照常用。

## 配置

`setting.py`：

```python
WEBDRIVER = dict(
    pool_size=2,          # 并发浏览器数
    browser="chromium",   # chromium | firefox | webkit
    headless=True,
    load_images=False,    # 拦截图片 / 字体 / 媒体，加速
    timeout=30,           # 秒
    wait_until="domcontentloaded",
    proxy=None,
    stealth=True,         # 注入基础反检测脚本
    viewport=[1920, 1080],
    channel=None,         # 如 "chrome"：用系统里真实安装的 Chrome 而非 Playwright 自带的 Chromium
    locale=None,          # 如 "zh-CN"
    timezone_id=None,     # 如 "Asia/Shanghai"
    engine="playwright",  # playwright | patchright
    user_data_dir=None,   # 如 "./.netspy_profiles"：设置后启用持久化 profile
)
```

`channel` / `locale` / `timezone_id` 都是 Playwright `new_context()` 原生支持的参数，不需要额外依赖。
未设置时行为跟以前完全一样。`channel="chrome"` 需要本机装了对应版本的 Chrome（`playwright install
chrome` 或系统自带），UA 版本号、TLS/HTTP2 指纹会更贴近目标站预期的「真实 Chrome」画像，而不是
Playwright 打包的那份 Chromium；启动时也默认带上 `--disable-blink-features=AutomationControlled`，
关掉最直接的自动化标记（这条无条件生效，没有开关）。

### engine="patchright"

```bash
pip install "netspy[render-patchright]"
```

[patchright](https://github.com/Kaliiiiiiiiii-Vinyzu/patchright-python) 是打了 CDP 层探测点补丁的
Playwright fork：避免调用 `Runtime.enable`、去掉默认的 `--enable-automation` 等参数，堵的是比
`playwright-stealth`（JS 注入层）更底层的探测面。两者可以同时开——`stealth` 补的是 `navigator.*`
这类 JS 可读属性，`engine="patchright"` 补的是 CDP 协议层面的痕迹，不冲突。

只支持 `chromium`：配 `browser="firefox"` 或 `"webkit"` 会在 `PlaywrightDownloader` 构造期直接抛
`ConfigError`，不会等到真正渲染时才发现配错了。

!!! warning "不要在同一个进程里混用两种引擎"
    实测撞到的真冲突：如果同一个 Python 进程里，一个 vanilla Playwright 浏览器和一个 patchright
    浏览器同时存活（比如测试里前一个用例的浏览器还没关，下一个用例又用另一个引擎开了一个），
    `page.goto()` 会稳定卡满超时。两边各自维护一条到浏览器驱动进程的 asyncio 连接，冲突在两个
    独立维护的三方库内部，不是 Netspy 的问题，但会影响你自己写的测试或脚本——一个进程认定用哪个
    引擎，不要中途切换。生产部署本来就只会有一个 `WEBDRIVER["engine"]`，不会撞上这个问题。

### user_data_dir（长期登录态）

设置后浏览器带着上次的 cookies / localStorage / 历史重新启动，而不是每次都是全新指纹——
全新的自动化 profile 没有任何历史状态本身也是一种信号，长期跑同一批"账号"时尤其明显。

```python
WEBDRIVER = dict(user_data_dir="./.netspy_profiles")
```

`pool_size` 个渲染线程各用一个子目录（`user_data_dir/0`、`user_data_dir/1` ……）——Chrome
不允许多个实例共享同一份 profile，共用会互相打架。只有**带 `Max-Age` / `Expires` 的持久
cookie**才会跨重启存活，会话 cookie（不带这两个属性）在真实浏览器里本来就不会挺过一次完整
重启，`user_data_dir` 救不了它——这是浏览器自己的行为，跟有没有持久化 profile 无关。

## 模拟真人交互

```python
from netspy.network.downloader._humanize import human_click, human_scroll, human_type

def script(page):
    human_scroll(page)
    human_click(page, "button.load-more")
    human_type(page, "input#q", "关键词")

yield mw.Request(url, render=True, render_script=script, callback=self.parse)
```

`human_move` / `human_click` / `human_type` / `human_scroll` 四个函数：鼠标分步移动
（带抖动，不是单帧瞬移）、逐字符输入（字符间随机延迟，不是 `fill()` 瞬间灌入）、分段滚动
（带停顿，不是一次性跳到底）。**不会自动生效**——这是刻意的，自动应用会给所有请求默认加上
不确定的延迟，且用哪个动作、作用在哪个元素上，只有你自己的 `render_script` 知道。

这几个函数诚实地说不能做到什么：Playwright 读不到鼠标当前位置，`human_move` 只能假设一个
起点（默认视口中心），不是逐像素复刻真人轨迹的方案。做的是把"单帧瞬移、零间隔批量输入、
一次性跳转滚动"这几个最基础的信号去掉，聊胜于无，不是万能药。

## 架构

`pool_size` 个独立渲染线程，每个持有一个 chromium（Playwright 的 sync API 不能跨线程）。
工作线程把渲染任务投进队列并阻塞等结果 —— 所以 `pool_size` 就是真正的并发浏览器上限，
小于 `SPIDER_THREAD_COUNT` 时天然形成背压。

导航超时 / 连接失败 → `RequestError` → 走正常重试逻辑。爬虫结束时自动关闭所有浏览器。
