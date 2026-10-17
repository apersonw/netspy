# 命令行

需要 `pip install "netspy[cli]"`。

## create

```bash
netspy create -p news_crawler      # 项目脚手架
netspy create -s ProductSpider     # 一个 AirSpider（写到 ./product_spider.py）
netspy create -i ProductItem       # 一个 Item
netspy create -i news --table news  # 读 MySQL 表结构反射字段（需 [mysql]）
netspy create --setting            # 一份注释齐全的 setting.py
netspy create -s Foo --force       # 覆盖已存在文件
```

名字会自动转换：`product-list` / `product_list` / `ProductList` 都能识别，
生成 `class ProductListSpider` + 文件 `product_list_spider.py`。

`-i --table <表名>` 会连 MySQL 读 `SHOW FULL COLUMNS`，按主键填 `__unique_key__` 并把字段
+ 注释列进 Item。连接默认取 `setting` 的 `MYSQL_*`，也可 `--mysql mysql://user:pwd@host:3306/db` 覆盖。

## shell

```bash
netspy shell https://example.com
netspy shell https://spa.example.com --render
```

抓一个页面进交互式 shell，绑定 `request` / `response` / `ns`。装了 IPython 用 IPython，
否则用内置 REPL（带 tab 补全）。

## retry

```bash
netspy retry --items       # 重放 failed_items.jsonl 到当前 ITEM_PIPELINES
netspy retry --requests    # 探活：重新下载看目标是否可达（不跑回调、不入库）
netspy retry               # 两者都做
```

仍失败的记录写回文件，全部成功则删除文件。

## 版本

```bash
netspy --version
```
