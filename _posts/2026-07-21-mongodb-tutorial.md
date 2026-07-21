---
title: "MongoDB 从零到用起来:一份不讲黑话的实战教程"
date: 2026-07-21 22:30:00 +0800
description: "不假设你懂 CAS、幂等、乐观锁这些词。从'MongoDB 到底存的是什么'一步一步讲,类比工厂车间的账本、贴条、盖章,最后落到线上真实的排产系统代码。"
tags: [MongoDB, Python, Distributed Systems, DDD]
permalink: /2026/07/21/mongodb-tutorial/
---

> 这份教程写给"听说过 MongoDB、但没真正拿它当账本用过"的同学。你不用提前懂什么"CAS""幂等""乐观锁""SSOT",这些词我都会先用大白话解释。我们会从 MongoDB 最基础的操作讲起,一步一步搭出一个能扛住重复消息、崩溃重启、多个人同时抢活的账本系统,最后把每一段基础操作对应到线上真实生产代码(视频加工厂式的排产系统)。

> **心智模型的比喻**(先给你一个总的画面,后面所有操作都在这张画面里):
>
> 把 MongoDB 想象成一个**工厂的中央档案室**。档案室里有很多**文件柜**(数据库),每个文件柜里有很多**抽屉**(集合),每个抽屉里放很多**活页夹**(文档),每个活页夹上写着很多**表格项**(字段)。工人每天做的事就是三件:**翻活页夹**、**在活页夹上贴条/盖章/涂改**、**添新活页夹进去**。而"MongoDB 保证一份活页夹改的时候不会被人抢着改一半"—— 这就是它能当账本的核心。

> 学习目标:读完这份教程你应该能
>
> - 说清楚 MongoDB 到底在存什么、为什么它适合当"任务账本"
> - 熟练做增/删/查/改这四件事,不写错别字
> - 用 `$set / $inc / $push / $setOnInsert` 精细改一个活页夹里的某几项
> - 用 `find_one_and_update` 一句话干完"翻出来、盖章、送出去"三步,中间不被别人插队
> - 用 upsert + `$setOnInsert` 让"同一个通知被送来两次"不会把状态打回原形
> - 用 `$inc` 加一个计数字段,精确知道"12 个小任务都做完了,可以开下一道工序了"
> - 用 `revision` 字段避免两个工人同时改同一份档案时打架
> - 读懂线上账本"接单 → 分单 → 出活 → 收单"这一整套代码里每一行 MongoDB 命令在干嘛

> 示例用 Python 3.11+ 的 `pymongo` 官方库。异步版换成 `AsyncMongoClient`(pymongo 4.9+)或 `motor`,写法几乎一样,每个命令前面多写一个 `await` 就行。

## 目录

1. MongoDB 到底是什么、为什么用它当账本
2. 环境准备:装 MongoDB 和 Python 客户端
3. 心智模型:数据库 / 集合 / 文档 / 字段 到底是什么
4. 四件基本功:插入、查找、修改、删除
5. "改一个活页夹"的各种花式操作符
6. "翻一个活页夹"的各种查询条件
7. 一句话干完"翻+改+返回":`find_one_and_update`
8. 重复消息来了怎么办:upsert + `$setOnInsert`
9. 12 个小任务都做完了怎么知道:`$inc` 原子计数
10. 两个人同时改同一份档案:revision 版本号防打架
11. 索引:让"翻档案"不用一柜子一柜子翻
12. 聚合管道:一次搞完"过滤 + 分组 + 统计"
13. 事务:什么时候真的需要(其实大部分时候不需要)
14. 对应到线上代码:接单 handler 逐段拆
15. 对应到线上代码:抢单 + 交单 逐段拆
16. 常见坑与调试小技巧

---

## 1. MongoDB 到底是什么、为什么用它当账本

先说一句最粗的话:**MongoDB 是一个存 JSON 的数据库**。你写进去的东西长得就像 JSON,读出来也是 JSON。

它跟你可能熟悉的其他东西比一下:

| | MySQL | Redis | MongoDB |
|---|---|---|---|
| 存的东西长什么样 | 一张表,固定几列 | 各种小玩意儿(字符串/列表/集合) | 一大堆 JSON |
| 存哪 | 硬盘 | 内存 | 硬盘 + 有内存缓存 |
| 要不要提前"建表定字段" | 要,而且很严格 | 不要 | 可要可不要,很宽松 |
| 一条命令能改一堆字段而中途不被人打断吗 | 事务能保证 | 单命令能保证 | 单个 JSON 内部能保证 |
| 查询语法 | SQL | 自己的一套命令 | JSON 语法 |
| 典型场合 | 严格关系数据(订单、账户) | 缓存、队列、锁 | 半结构化业务状态(工单、任务) |

**如果一句话说 MongoDB 的定位**:它像一个**很懂业务、又很宽松的档案室**。你可以塞任意形状的 JSON 进去,以后想加字段直接加,不用停机改表结构。而且它保证:**你改一份 JSON 的时候,别人插不进来改一半**。

### 为什么用它当"账本"

先解释一下什么叫"账本"。在我们这种视频加工厂式的系统里,每一个直播房间进来,系统要记很多事:

- 这个房间现在处于哪一步(收到通知 / 已经分桶 / 正在切片 / 打标中 / 全部完成)
- 它有几个子任务、做完了几个
- 每个子任务的状态和错误信息
- 上一次谁在处理它、什么时候
- 优先级、重试次数、下次可以重试的时间...

这一堆状态**必须存下来**,而且**多个进程都要读写它**。这个"存状态、多进程读写"的东西,就是"账本"。

为什么 MongoDB 适合当账本:

1. **一整份任务状态可以塞进一个 JSON**:不用像 MySQL 那样拆成 5 张表 JOIN,读一次就够了
2. **改一份 JSON 时,MongoDB 保证不会被人插队改一半**(这个后面反复讲)
3. **`$inc` 这种"给某个数字加 1"是原子的**:1000 个人同时给同一个字段加 1,最后拿到的就是精确的 +1000 效果,一分不多一分不少
4. **加字段不用改表结构**:业务演进的时候,今天没这个字段,明天想加就加
5. **`find_one_and_update` 一句话干完"翻出来 + 盖章 + 送出去"**:这个是当账本用最重要的一句 API,后面单独一节讲

**它不擅长什么**(避免选型踩坑):

- 跨多个 JSON 的强一致(能做多文档事务,但代价大,能不用不用)
- 严格的关系型查询(有 JOIN 但很慢,别当 MySQL 用)
- 单一个 JSON 的高频热点写(同一份 JSON 被 1 万 QPS 打,写会互相排队,慢)

---

## 2. 环境准备

### 装 MongoDB 服务

Mac:

```bash
brew tap mongodb/brew
brew install mongodb-community
brew services start mongodb-community
```

装完默认监听 `localhost:27017`。这个 27017 就是 MongoDB 的默认端口,记住就行。

### 装 Python 客户端

```bash
pip install pymongo
```

这就是我们跟 MongoDB 说话的"翻译"。Python 的字典 → MongoDB 里的 JSON,来回自动翻。

### 第一次连接,写一份 JSON 进去,再读出来

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]              # 挑一个"文件柜"
col = db["rooms"]                # 挑一个"抽屉"

col.insert_one({"_id": "r1", "status": "pending"})
print(col.find_one({"_id": "r1"}))
# {'_id': 'r1', 'status': 'pending'}
```

**注意**:`mydb` 和 `rooms` 都**不用提前创建**,你第一次往里塞东西的时候 MongoDB 自动帮你建。这一点跟 MySQL 完全不一样(MySQL 要先 `CREATE DATABASE`、`CREATE TABLE`)。

### 异步版(线上生产用这个)

```python
from pymongo import AsyncMongoClient
import asyncio

async def main():
    client = AsyncMongoClient("mongodb://localhost:27017")
    col = client["mydb"]["rooms"]

    await col.insert_one({"_id": "r1", "status": "pending"})
    doc = await col.find_one({"_id": "r1"})
    print(doc)

asyncio.run(main())
```

跟同步版几乎一模一样,每个 MongoDB 命令前面加一个 `await` 而已。为什么线上用异步版?因为异步不会"卡住线程",一个进程可以同时干很多件事,吞吐上去了。

下面的示例默认用同步版,方便读。用的时候记得加 `await` 就行。

---

## 3. 心智模型:数据库 / 集合 / 文档 / 字段 到底是什么

一张图记住:

```
MongoDB 服务(一个中央档案室,监听 27017 端口)
└── database "orderprod"          ← 文件柜(比如"订单业务柜")
    ├── collection "rooms"         ← 抽屉(比如"房间抽屉")
    │   ├── document {_id: "r1", status: "pending", ...}   ← 活页夹一
    │   ├── document {_id: "r2", status: "done",    ...}   ← 活页夹二
    │   └── ...
    └── collection "fragments"     ← 另一个抽屉("视频切片抽屉")
        └── ...
```

四个词一句话解释:

- **数据库(database)**:一个"文件柜",里面装一堆抽屉。一个 MongoDB 服务可以有很多文件柜。
- **集合(collection)**:一个"抽屉",相当于 MySQL 的一张表。抽屉里装的是同一类活页夹。
- **文档(document)**:一份"活页夹",相当于 MySQL 的一行数据。它长得像 JSON。
- **字段(field)**:活页夹上的"一栏表格项",相当于 MySQL 的一列。

一份文档实际长这样(这就是我们线上一个房间的账本):

```json
{
  "_id": "room_12345",
  "status": "bucketed",
  "priority": 8,
  "expected_count": 12,
  "completed_count": 3,
  "targets": {
    "asr":     {"status": "done",    "at": 1721000000},
    "caption": {"status": "pending"}
  },
  "revision": 5,
  "created_at": 1720999000,
  "updated_at": 1721000123
}
```

看到没,一个活页夹里可以嵌套另一个"表格块"(比如 `targets.asr` 就是一个小的表格块)。这就是 JSON 的层级。

几个新手容易懵的点:

- **`_id` 是每份文档的主键(唯一编号)**。你不给,MongoDB 会自动生成一个像 `ObjectId('5f...')` 这样的长串。**如果你想让"同一个 room_id 只对应一份文档",就自己把 `_id` 设成 `room_id`**。这在后面讲幂等的时候会用到。
- **字段名区分大小写**:`Status` 和 `status` 是两个字段,别写错。
- **嵌套字段用点号**:想改 `targets` 里面的 `asr` 里面的 `status`,写 `"targets.asr.status"`。
- **集合不用提前建**,第一次写入就自动创建了。

---

## 4. 四件基本功

档案室的日常操作就四件:塞新的进去、翻出来看、涂改、扔掉。分别对应下面的 API。

### 4.1 塞新的进去:`insert`

```python
# 塞一份
col.insert_one({"_id": "r1", "status": "pending", "priority": 5})

# 塞一批
col.insert_many([
    {"_id": "r2", "status": "pending"},
    {"_id": "r3", "status": "pending"},
])

# 想知道 MongoDB 自动生成的 _id 是啥
res = col.insert_one({"name": "Alice"})
print(res.inserted_id)   # ObjectId('...')
```

**要注意的坑**:如果你手动指定了 `_id`,并且这个 `_id` 已经存在了,`insert_one` 会抛错(`DuplicateKeyError`)。想"存在就跳过、不存在才塞",那不是 insert 干的活,是 upsert 干的,第 8 节讲。

### 4.2 翻出来看:`find`

```python
# 翻一份出来(第一份匹配到的)
col.find_one({"_id": "r1"})

# 翻一批出来(返回一个"游标",可以遍历)
for doc in col.find({"status": "pending"}):
    print(doc)

# 只要某些栏,别的都不看(叫"投影")
col.find({"status": "pending"}, {"_id": 1, "priority": 1})
# {"_id": 1, "priority": 1} 表示"只返回 _id 和 priority 这两栏"
# {"priority": 0} 表示"除了 priority 都返回"

# 排序 + 分页
col.find({"status": "pending"}).sort("priority", -1).skip(0).limit(20)
# sort 里 -1 是降序,1 是升序
# skip 是"跳过前 N 条",limit 是"最多要几条"

# 数一下有多少份匹配
col.count_documents({"status": "done"})
```

**为什么"只要某些栏"很重要**:线上账本一份可能有几十栏,你要 100 份的话是几十×100 栏。如果只关心两栏,加上投影能省一大截网络传输和内存。

**"游标"是什么**:`find(...)` 不会立即把结果全捞回来,而是给你一个"指针",你每 `for` 一次它才从 MongoDB 拿一批。这样即使匹配到几百万条也不会一次撑爆内存。

### 4.3 涂改:`update`

```python
# 改一份(第一份匹配到的)
col.update_one(
    {"_id": "r1"},                      # 找哪一份
    {"$set": {"status": "bucketed"}},   # 怎么改
)

# 改一批
col.update_many(
    {"status": "pending"},
    {"$set": {"priority": 5}},
)

# 结果里能拿到统计
res = col.update_one({"_id": "r1"}, {"$set": {"status": "done"}})
print(res.matched_count)    # 匹配到几份
print(res.modified_count)   # 实际改了几份(如果值本来就是 done,就是 0)
```

**这里是新手最爱踩的一个大坑**:第二个参数**必须带 `$` 开头的操作符**,比如 `$set` / `$inc` / `$push`。

**如果你写成**:

```python
col.update_one({"_id": "r1"}, {"status": "done"})   # ❌
```

MongoDB 会理解成"把整份文档替换成 `{status: 'done'}`",结果这份文档里除了 `_id`,别的什么 priority、created_at、revision **全没了**。灾难。新版 pymongo 会直接报错拦下来,老版本会静默替换。反正记住:**改字段必须用 `$set`,别裸写**。

### 4.4 扔掉:`delete`

```python
col.delete_one({"_id": "r1"})
col.delete_many({"status": "expired"})
```

线上的账本**几乎不物理删**,一般改状态成 `archived` 或 `abandoned`,保留证据链,方便后面追查。

---

## 5. "改一个活页夹"的各种花式操作符

上一节说了改字段要用 `$set`。其实除了 `$set`,还有很多花式操作符,每个负责一种"改法"。全都是往 `update_one` 的第二个参数里塞。

### 5.1 `$set` — 改某几栏

```python
col.update_one({"_id": "r1"}, {"$set": {
    "status": "done",
    "targets.asr.status": "done",   # 嵌套字段用点号
    "updated_at": 1721000123,
}})
```

- 字段本来没有的话会自动创建
- 嵌套栏用 `"a.b.c"` 语法

### 5.2 `$unset` — 删某几栏

```python
col.update_one({"_id": "r1"}, {"$unset": {"tmp_flag": ""}})
# 右边写什么都行,反正是删掉这一栏
```

### 5.3 `$inc` — 给数字加减

```python
col.update_one({"_id": "r1"}, {"$inc": {
    "completed_count": 1,       # 加 1
    "retry_count": 1,
    "budget_remaining": -100,   # 减 100
}})
```

**重点来了**:`$inc` 是**原子**的。什么叫原子?

想象一下:你去银行取钱,你的账户是 1000 块。同一秒你老婆也在另一个 ATM 取钱。如果这俩取钱操作不是原子的,就可能发生:

- 你的 ATM 读到 1000,准备扣 100
- 同时你老婆的 ATM 也读到 1000,也扣 100
- 你的 ATM 写回 900,你老婆的 ATM 也写回 900
- 结果:两个人一共取了 200,但账户只扣了 100

这就是"没有原子性"的后果:钱少扣了 100。

原子性就是说:**"读 → 算 → 写回"这一整套过程,中间不会被别人插进来**。要么完整发生,要么完全没发生。

`$inc` 就是这么个东西。1000 个 worker 同时给同一份文档的 `completed_count` +1,最后精确变成 +1000。**这就是我们后面"12 个小任务都做完了才触发下一步"这个逻辑的地基**。

### 5.4 `$push` — 往数组里塞一个

```python
col.update_one({"_id": "r1"}, {"$push": {
    "events": {"kind": "asr_done", "at": 1721000000},
}})

# 一次塞好几个
col.update_one({"_id": "r1"}, {"$push": {
    "events": {"$each": [event1, event2, event3]},
}})

# 限制数组长度(只保留最新 100 个,老的自动挤出去)
col.update_one({"_id": "r1"}, {"$push": {
    "events": {"$each": [new_event], "$slice": -100},
}})
```

第三种写法特别实用:**"事件日志"这种东西容易越长越长**,用 `$slice: -100` 就能自动只保留最新 100 条。

### 5.5 `$pull` — 从数组里删

```python
col.update_one({"_id": "r1"}, {"$pull": {
    "tags": "expired",             # 删所有值为 "expired" 的元素
}})

col.update_one({"_id": "r1"}, {"$pull": {
    "tasks": {"status": "cancelled"},   # 删掉数组里满足条件的对象
}})
```

### 5.6 `$addToSet` — 往数组里塞,重复了就跳过

```python
col.update_one({"_id": "r1"}, {"$addToSet": {"tags": "vip"}})
# 数组里已经有 "vip" 就不动,没有才加
```

**用途**:给某个任务加"标签"、给某个用户加"权限",避免重复。

### 5.7 `$setOnInsert` — 只在"这是第一次塞进来"时生效

**这个是幂等入队的灵魂,第 8 节详细讲**。先记住它长这样:

```python
col.update_one(
    {"_id": "r1"},
    {
        "$set":         {"updated_at": now},
        "$setOnInsert": {"created_at": now, "status": "pending"},
    },
    upsert=True,     # 关键:找不到就插入,叫 upsert
)
```

上面这个命令做的事:

- 如果 `r1` 不存在:整份新塞进去,`$set` 和 `$setOnInsert` 全生效 → `status` 是 `pending`,`created_at` 和 `updated_at` 都是 now
- 如果 `r1` 已经在了:**只有 `$set` 生效**,`$setOnInsert` **不动**已有的 `created_at` 和 `status`

这个"只在首次生效"的能力,就是解决"同一条 MQ 消息被投递两次不会把状态打回原形"的关键。

### 5.8 `$min` / `$max` — 只往一个方向改

```python
col.update_one({"_id": "r1"}, {"$max": {"priority": 8}})
# 只有当传入的 8 比现有 priority 大时才更新;小于等于就不动
```

**用途**:优先级"只升不降"。业务上经常有这个需求:第一次通知说优先级是 3,重发通知说是 8,那就升到 8;后面又来一条说是 5,不能降回去。

### 5.9 `$currentDate` — 让 MongoDB 服务端填时间

```python
col.update_one({"_id": "r1"}, {"$currentDate": {"updated_at": True}})
```

**为什么要 MongoDB 填而不是自己写 `time.time()`**:因为不同机器的时钟不同步,自己写的时间可能差几秒。让 MongoDB 服务端填,所有机器写进去的时间都是同一个时钟源。

---

## 6. "翻一个活页夹"的各种查询条件

跟 update 的操作符类似,find 里也有一堆条件符,每种代表一种"匹配规则"。

### 6.1 比大小和等不等

```python
col.find({"priority": {"$eq": 5}})           # 等于 5
col.find({"priority": {"$ne": 5}})           # 不等于 5
col.find({"priority": {"$gt": 5}})           # 大于 5
col.find({"priority": {"$gte": 5}})          # 大于等于
col.find({"priority": {"$lt": 5}})           # 小于
col.find({"priority": {"$lte": 5}})          # 小于等于
col.find({"priority": {"$in": [3, 5, 8]}})   # 是这三个值之一
col.find({"priority": {"$nin": [3, 5]}})     # 不是这些值之一
```

**记住**:`{"priority": 5}` 是 `{"priority": {"$eq": 5}}` 的简写。80% 的查询都是这种简写。

### 6.2 且、或

```python
# 且(默认就是且,把条件写在一起)
col.find({"status": "pending", "priority": {"$gte": 5}})
# 意思:status 是 pending 而且 priority 大于等于 5

# 或
col.find({"$or": [
    {"status": "pending"},
    {"status": "transient_failed"},
]})
# 意思:status 是 pending 或者是 transient_failed

# 且和或混着用
col.find({
    "priority": {"$gte": 5},
    "$or": [
        {"status": "pending"},
        {"next_retry_at": {"$lte": now}},
    ],
})
# 意思:priority ≥ 5 而且 (status=pending 或者 到了重试时间)
```

### 6.3 有没有这个字段

```python
col.find({"lease_until": {"$exists": True}})   # 有 lease_until 这一栏的
col.find({"lease_until": {"$exists": False}})  # 没有这一栏的
```

### 6.4 数组的查询

```python
# tags 数组里包含 "vip"
col.find({"tags": "vip"})

# tags 里同时包含 vip 和 hot
col.find({"tags": {"$all": ["vip", "hot"]}})

# tags 长度为 3
col.find({"tags": {"$size": 3}})

# 数组里存的是对象,想同时满足几个条件
col.find({"tasks": {"$elemMatch": {
    "status": "pending",
    "priority": {"$gte": 5},
}}})
# 意思:tasks 数组里至少有一个对象 满足 status=pending 且 priority≥5
```

### 6.5 嵌套字段

```python
col.find({"targets.asr.status": "done"})
```

---

## 7. 一句话干完"翻+改+返回":`find_one_and_update`

**如果你只能记住 MongoDB 一个 API,记这个**。

它做的事情用大白话说就是:**从抽屉里翻一份出来 → 涂改一下 → 把改完的样子还给你**。整个过程 MongoDB 保证不被别人插队。

```python
doc = col.find_one_and_update(
    {"_id": "r1", "status": "pending"},          # 翻:满足这两个条件才动手
    {"$set": {"status": "processing",
              "lease_until": now + 30}},         # 改:盖章
    return_document=True,                        # 返回:True 是"改完的样子",False 是"改之前的样子"
)

if doc is None:
    print("没抢到:要么不存在,要么状态早就不是 pending 了")
else:
    print(f"抢到了:{doc['_id']}")
```

### 为什么重要?为什么不能拆成两步?

我们看一个反面教材。假设你想"从 pending 里挑一个出来处理",拆成两步:

```python
# ❌ 反面教材
doc = col.find_one({"status": "pending"})   # 第一步:翻
if doc:
    col.update_one({"_id": doc["_id"]},     # 第二步:改
                   {"$set": {"status": "processing"}})
```

问题来了:**第一步和第二步之间有时间缝隙**。多个 worker 同时跑这段代码:

- worker A 在第一步翻到了 doc,拿到 `_id="r1"`
- worker B 同一瞬间也翻到了 doc,拿到 `_id="r1"`
- 两个 worker 都跑第二步,都把 r1 的 status 改成 processing
- **结果:两个 worker 都以为自己抢到了 r1,并行处理同一个任务**。资源浪费、可能重复扣费。

用 `find_one_and_update` 就没这个问题:

```python
# ✅ 正确
doc = col.find_one_and_update(
    {"_id": "r1", "status": "pending"},         # 过滤条件里带上 status=pending
    {"$set": {"status": "processing"}},
    return_document=True,
)
```

**因为翻和改是一体的**:worker A 抢到了 r1(status 从 pending 变成 processing),worker B 再来找 status=pending 的 r1,已经找不到了(过滤条件不匹配),`find_one_and_update` 返回 `None`。B 就知道自己没抢到,该干嘛干嘛去。

### 常用参数

```python
col.find_one_and_update(
    filter,
    update,
    return_document=True,                    # True 或 ReturnDocument.AFTER = 返回改完的样子
    upsert=True,                             # 找不到就插入
    sort=[("priority", -1)],                 # 多份都匹配时按什么排序,取第一份
    projection={"_id": 1, "status": 1},      # 返回时只要哪几栏
)
```

### 抢任务的经典写法

```python
# 从所有 pending 里,挑一个优先级最高的任务,原子改成 processing
doc = col.find_one_and_update(
    {"status": "pending"},
    {
        "$set": {
            "status":       "processing",
            "worker_id":    my_id,
            "lease_until":  now + 30,
        },
        "$inc": {"revision": 1},
    },
    sort=[("priority", -1), ("created_at", 1)],   # 优先级高的先做,同优先级下老的先做
    return_document=True,
)
```

多个 worker 同时跑这个,同一份文档只会被一个人抢到,别人拿到 `None`,睡一小会儿再重试。

---

## 8. 重复消息来了怎么办:upsert + `$setOnInsert`

先解释两个词:

- **upsert**:update + insert 的合体。找到就 update,找不到就 insert(把过滤条件里的字段 + 更新里的字段一起塞进去)。
- **幂等**:这个词说得玄乎,大白话就是**"同一个操作做一次和做多次,效果一样"**。比如"电梯按钮按 5 次和按 1 次,结果都是电梯来一趟",这就是幂等。

### 场景

上游 MQ(消息队列)保证"消息至少投递一次",意思就是"有可能投递多次"。所以你的 handler **一定会**收到重复的消息。你不希望"第二次收到通知就把已经在处理的任务打回原形"。

### 错误写法

```python
# ❌ 每次都写 status=pending
col.update_one({"_id": room_id},
               {"$set": {"status": "pending"}},
               upsert=True)
```

第一次消息到了,r1 不存在,upsert 会插入 `{_id: "r1", status: "pending"}`。worker 抢到了,改成 `processing`,开始干活。

**第二次同样的消息到了,这段代码又跑一遍**:找到 r1,把 status 从 processing 打回 pending。worker 干到一半发现自己的任务莫名其妙不见了,或者被别的 worker 抢走了。**这就是不幂等的灾难**。

### 正确写法

```python
col.update_one(
    {"_id": room_id},
    {
        # 每次都刷新的
        "$set": {
            "updated_at":     now,
            "last_notify_at": now,
        },
        # 只在"这是第一次插入"时才写
        "$setOnInsert": {
            "status":         "pending",       # 首次才置 pending
            "created_at":     now,
            "priority":       5,
            "revision":       0,
        },
    },
    upsert=True,
)
```

行为:

- **第一次**:r1 不存在,全部字段都塞进去,`status=pending`
- **第二次**:r1 已经在了,**只有 `$set` 里的两个字段被刷新**,`$setOnInsert` 里的四项 **一动不动**。已经在 processing 的任务继续 processing。

**这就是"MQ 是邮差,Mongo 是账本"的完整实现**。邮差可能把同一封信送来两次,但账本上"这个任务的初始状态"只记一次。第二次送来只算"通知了一声,更新一下时间戳"。

---

## 9. 12 个小任务都做完了怎么知道:`$inc` 原子计数

### 场景

一个直播房间的处理流程是:先分桶 → 再切片(拆成 12 个小片段)→ 每个小片段做 ASR/画面提取等处理 → **全部 12 个片段都做完了**,进入下一步"整体打标"。

**难点**:12 个片段并行处理,分布在不同的 worker 上。谁来判断"12 个都做完了"?怎么判断得准?

### 错误写法(经典)

```python
# ❌ 反面教材
doc = col.find_one({"_id": room_id})
new_count = doc["completed_count"] + 1
col.update_one({"_id": room_id},
               {"$set": {"completed_count": new_count}})

if new_count == doc["expected_count"]:
    trigger_labeling(room_id)
```

问题:回到第 5.3 节的银行 ATM 的例子。两个片段同时完成:

- worker A 读到 `completed_count = 5`,准备写 6
- worker B 同时读到 5,也准备写 6
- 都写回 6,**实际应该是 7**。丢了一次

结果:`completed_count` 永远到不了 12。下一步"整体打标"永远不被触发。房间烂在那儿。

### 正确写法

```python
doc = col.find_one_and_update(
    {"_id": room_id},
    {"$inc": {"completed_count": 1}},
    return_document=True,           # 拿到 $inc 之后的最新值
)

if doc["completed_count"] == doc["expected_count"]:
    trigger_labeling(room_id)
```

**关键三点**:

1. `$inc` 是原子的(第 5.3 节讲过),12 个 worker 同时 +1 最后精确得到 12
2. `return_document=True` 让你拿到"我这一次 +1 之后的最新值"
3. 谁看到 `completed_count == expected_count`,谁就是"最后一个完成的",由他触发下一步

**为什么这样一定不会漏也不会重**?因为 MongoDB 保证 `$inc` 是原子的,精确等于 12 的那次操作**有且只有一次**。所以下一步一定被触发一次,不会零次也不会两次。

### 还要一个"Timer 兜底"

现实很骨感:总有一些片段会永远失败(比如原视频不见了),`completed_count` 永远到不了 12。这时候不能光等,得有个后台协程定期扫:

```python
# 后台每 30 秒扫一次
async def sealer_loop():
    while True:
        stuck_rooms = rooms_col.find({
            "status": "producing",
            "$expr": {"$lt": ["$completed_count", "$expected_count"]},   # 还没做完
            "updated_at": {"$lt": now - 30 * 60},                        # 但 30 分钟没进展
        })
        async for room in stuck_rooms:
            # 强制推进,带上"我是兜底推进的"标记
            await force_seal_room(room["_id"])

        await asyncio.sleep(30)
```

这就是**"事件驱动 + Timer 兜底"**的组合:正常时事件推动,异常时定时器捡漏。

---

## 10. 两个人同时改同一份档案:revision 版本号防打架

### 场景

worker A 抢到了任务 t1,状态改成 processing,开始干活。10 秒过去,worker A 干完了,准备把状态写成 done。

**同时**,leader 的巡检协程发现 t1 的 `lease_until` 过期了(以为 worker A 挂了或者太慢了),把 t1 从 processing 打回 pending,想让别的 worker 接手。

**同一瞬间**,worker A 也在写 done。**这俩不能都成功**,不然会一团糟(worker A 的结果覆盖了 pending,或者 pending 覆盖了 done,场面很难看)。

### 心法:给每份文档带一个"版本号"

在每份文档里加一栏 `revision`,初始为 0。**每次改任何东西,都做两件事**:

1. 过滤条件里带上"我期望的 revision"
2. 更新里加 `$inc: {revision: 1}`

这样:

- 如果别人先改了,`revision` 已经 +1,你的过滤条件 `revision == expected_rev` 匹配不上,`find_one_and_update` 返回 `None`
- 如果没人抢先改,你的更新就生效,`revision` +1

**这就叫"乐观锁"**。"乐观"是因为它假设冲突很少,不加锁,读的时候记下版本号,写的时候校验版本号没变才成功。冲突了就整段作废重来。CAS 就是这个"比较并写"的缩写。

### 代码

```python
doc = col.find_one_and_update(
    {
        "_id":      task_id,
        "revision": expected_rev,        # 关键:我期望的版本号
        "status":   "processing",        # 只有还在 processing 才能改
    },
    {
        "$set": {"status": "done", "updated_at": now},
        "$inc": {"revision": 1},
    },
    return_document=True,
)

if doc is None:
    # 有人抢先改了,或者状态已经不是 processing
    # 放弃自己的结果,不做任何写入
    log.warning(f"我慢了一步,{task_id} 被别人接管了,放弃本次结果")
else:
    log.info(f"改成 done 成功,新 revision = {doc['revision']}")
```

**用途**:

- worker 完成任务时,写回结果
- lease(租约)续期,只有 revision 没变才续
- 优先级提升,避免降级掉别人的高优先级

---

## 11. 索引:让"翻档案"不用一柜子一柜子翻

**索引是什么**:就像一本书的"目录"。没目录,你想找"第 5 章"得从头翻到第 5 章;有目录,直接跳过去。

MongoDB 的索引就是"某个字段的目录"。你查 `status = "pending"`,如果 `status` 有索引,MongoDB 直接跳到"pending 那一堆",不用翻整个抽屉。

### 建索引

```python
from pymongo import ASCENDING, DESCENDING

# 单字段索引
col.create_index([("status", ASCENDING)])

# 复合索引(多字段一起做目录,顺序重要)
col.create_index([("status", ASCENDING), ("priority", DESCENDING)])

# 唯一索引(这一字段的值全表唯一,重复的插不进去)
col.create_index([("business_key", ASCENDING)], unique=True)

# TTL 索引(自动过期删除)
col.create_index([("created_at", ASCENDING)], expireAfterSeconds=86400)
# 每份文档 created_at 之后 86400 秒(一天)被自动删掉
```

TTL 索引特别香:临时表的清理不用自己写,MongoDB 帮你删。

### 看有哪些索引

```python
list(col.list_indexes())
```

### 复合索引怎么选字段顺序

**规则**:"等值条件放前面,范围条件放后面。"

举例:你的常见查询是 `find({"status": "pending", "priority": {"$gte": 5}}).sort("created_at", 1)`。

好索引:`(status, priority, created_at)`。

**为什么**:MongoDB 沿着索引走,先找到 status=pending 的段(等值),再在这段里找 priority≥5 的(范围),已经排好序的 created_at 直接顺着走就行。

**最左前缀原则**:索引 `(a, b, c)` 能加速的查询:

- 只查 `a` 的
- 查 `a` 和 `b` 的
- 查 `a`、`b`、`c` 的

**不能加速** 只查 `b` 或者 `c` 的。因为目录是按 `a` 排的,查 `b` 得从头扫。

### 看到底走没走索引:`explain`

```python
col.find({"status": "pending"}).explain()
```

关注返回里的 `winningPlan.stage`:

- `COLLSCAN` = 全表扫(整个抽屉翻),慢
- `IXSCAN` = 索引扫(用目录跳),快

**上线前一定要 explain 每个高频查询**。没走索引的,加索引;加了还没走的,查一下过滤条件写法有没有问题(比如把 `$or` 换成 `$in` 有时能命中索引)。

---

## 12. 聚合管道:一次搞完"过滤 + 分组 + 统计"

聚合管道就是**流水线**:好几步串起来,每一步的输出是下一步的输入。你写一段聚合,MongoDB 一次跑完,不用来回和你交互。

```python
pipeline = [
    # 步骤 1:过滤(相当于 SQL 的 WHERE)
    {"$match": {"created_at": {"$gte": today_ts}}},

    # 步骤 2:选字段(相当于 SELECT)
    {"$project": {"status": 1, "priority": 1, "cost": 1}},

    # 步骤 3:分组统计
    {"$group": {
        "_id":     "$status",              # 按 status 分组
        "count":   {"$sum": 1},            # 每组多少条
        "avg_cost": {"$avg": "$cost"},     # 每组 cost 平均值
    }},

    # 步骤 4:排序
    {"$sort": {"count": -1}},

    # 步骤 5:只要前 10 条
    {"$limit": 10},
]

for row in col.aggregate(pipeline):
    print(row)
```

**常用 stage(步骤)一览**:

| stage | 干嘛的 | 类比 SQL |
|---|---|---|
| `$match` | 过滤 | WHERE |
| `$project` | 选字段 / 算派生字段 | SELECT |
| `$group` | 分组聚合 | GROUP BY |
| `$sort` | 排序 | ORDER BY |
| `$limit` / `$skip` | 分页 | LIMIT / OFFSET |
| `$unwind` | 数组展开成多条 | 无 |
| `$lookup` | join 别的集合 | JOIN(慢,慎用) |
| `$facet` | 一次做多个视图并列输出 | 无 |

**心法**:能用 `$match` 尽早过滤就早过滤。**过滤要放在最前面**,而且要能走索引。不然 MongoDB 得把全表拉出来做聚合,慢到你想哭。

---

## 13. 事务:什么时候真的需要(其实大部分时候不需要)

先解释"事务":一组操作绑在一起,要么全部成功,要么全部回滚(当作没做过)。银行转账是经典场景:从 A 账户扣 100,给 B 账户加 100,俩必须一起成功。

**MongoDB 的默认承诺**:改一份文档是原子的。但**改多份文档就不是**了。

只有当"必须多份一起改"时才需要事务:

```python
with client.start_session() as session:
    with session.start_transaction():
        col_a.update_one({"_id": "a1"}, {"$inc": {"balance": -100}}, session=session)
        col_b.update_one({"_id": "b1"}, {"$inc": {"balance":  100}}, session=session)
```

**但线上账本 90% 的场景不需要事务**。为什么?**好的领域建模会让"必须一起改的东西"落在同一份文档里**。

举例:一个房间的所有子任务状态,别拆成 12 份文档存,就存在这一份房间文档的 `targets.*` 里。这样"把这 12 个状态改一下"是一份文档的更新,天然原子。

**为什么能不用事务就不用**:事务性能代价大,延迟通常增加 10 倍以上。而且事务复杂了容易死锁,排查很累。

**如果真的跨文档一致性**:考虑用"幂等 + 补偿"(工程界叫 saga 模式)代替强事务。意思就是"每一步都可以重试,失败了走另一条修复路径"。这比事务灵活得多。

---

## 14. 对应到线上代码:接单 handler 逐段拆

现在你有了所有基础工具,我们看真实生产代码。

**场景**:上游 MQ 通知"这个房间有新料了",我们要把它记进账本。

```python
async def handle_room_notification(msg: RoomNotification) -> None:
    """接单 handler:只做幂等落库 + 立即 ACK,重活丢给后台 worker。"""
    now = int(time.time())

    await rooms_col.update_one(
        {"_id": msg.room_id},
        {
            # 每次都刷新的字段
            "$set": {
                "last_notify_at":  now,
                "updated_at":      now,
            },
            # 优先级只升不降
            "$max": {"priority": msg.priority},

            # 只在"这是第一次插入"时写的字段
            "$setOnInsert": {
                "status":         "pending",
                "created_at":     now,
                "expected_count": None,       # 分桶之后才知道要拆几片
                "completed_count": 0,
                "targets": {
                    "bucketing": {"status": "pending"},
                    "splitting": {"status": "pending"},
                    "labeling":  {"status": "pending"},
                },
                "revision":       0,
            },
        },
        upsert=True,
    )
    # 立即 ACK,不做任何重活
```

**逐段拆**:

- `upsert=True` + `$setOnInsert`:同一条 MQ 消息投递两次,第一次落库,第二次只刷新时间戳,状态不动。**这就是幂等**。
- `$max: {priority}`:重发的消息如果带更高优先级,升上去;更低,忽略。**这就是优先级只升不降**。
- **`_id` 用业务的 `room_id`**:所以"同一个房间"永远对应同一份文档,不会重复。
- **一句代码搞定"第一次消息"和"重复消息"两种情况**。零 if/else。
- **不启动业务处理**:后台 worker 通过定期扫 `status=pending` 接手真正的活。handler 只签收,立刻走人。

**背后的哲学**:MQ 是邮差,Mongo 是账本。邮差就负责送信、拿回执,信里的活由账本这边的工人慢慢做。

---

## 15. 对应到线上代码:抢单 + 交单 逐段拆

### 15.1 抢单:worker 从账本里挑一个任务干

```python
async def claim_next_task(worker_id: str, now: int) -> dict | None:
    """从 pending 里挑一个优先级最高的,原子改成 processing。"""
    doc = await tasks_col.find_one_and_update(
        {
            # 两种任务都能被抢:新任务,或者上次失败已经过冷却期的
            "$or": [
                {"status": "pending"},
                {
                    "status":         "transient_failed",
                    "next_retry_at":  {"$lte": now},
                },
            ],
        },
        {
            "$set": {
                "status":       "processing",
                "worker_id":    worker_id,
                "lease_until":  now + LEASE_SEC,
                "picked_at":    now,
            },
            "$inc": {"revision": 1},
        },
        sort=[("priority", -1), ("created_at", 1)],
        return_document=True,
    )
    return doc
```

**逐段拆**:

- `$or`:一次能挑两种任务 —— 新的 `pending`,或者失败后已经过冷却期的 `transient_failed`。省得写两次查询。
- `sort`:优先级高的先做,同优先级下老的先做。**这就是"公平 + 优先级"的简易实现**。
- `$set`:把任务从"可被抢"改成"我在做",并留下我的名字(`worker_id`)和 lease 到期时间。lease 是啥?就是"我承诺 30 秒内做完,做不完你可以来抢我"的意思。
- `$inc revision`:版本号 +1,后面写结果时用来做 CAS 校验。
- 返回 `None` = 没抢到(队列空了 or 都被别人拿走了),睡一小段再试。

### 15.2 交单:CAS 保证"我做完的时候,任务还是我的"

```python
async def commit_result(
    task_id: str,
    worker_id: str,
    expected_rev: int,
    result: dict,
    now: int,
) -> bool:
    """把结果写回,只在'仍是我的任务且 revision 没变'时才生效。"""
    doc = await tasks_col.find_one_and_update(
        {
            "_id":       task_id,
            "worker_id": worker_id,       # 还是我在做
            "revision":  expected_rev,    # 版本号没被别人推进
            "status":    "processing",
        },
        {
            "$set": {
                "status":     "done",
                "result":     result,
                "done_at":    now,
                "updated_at": now,
            },
            "$unset": {"worker_id": "", "lease_until": ""},
            "$inc":   {"revision": 1},
        },
        return_document=True,
    )
    return doc is not None
```

**返回 `False` 会在什么情况下发生**:

- 我干太久,lease 过期,巡检协程把我踢了(status 打回 pending,worker_id 清空),别人接手了
- 有别人接手并推进了 revision
- 状态已经被外部改成 `abandoned` / `archived`

**处理**:什么都别写,记日志走人。任务已经在别人手上或者已经作废,我强行写会覆盖别人的正确结果,那就毁了。

### 15.3 批次闭合:$inc + 精确等值 触发下一步

```python
async def mark_fragment_done(room_id: str, fragment_id: str) -> None:
    # 1. 先把这个小片段标 done
    await fragments_col.update_one(
        {"_id": fragment_id},
        {"$set": {"status": "done", "done_at": now}},
    )

    # 2. 原子递增房间的 completed_count,拿到最新值
    room = await rooms_col.find_one_and_update(
        {"_id": room_id},
        {"$inc": {"completed_count": 1}, "$set": {"updated_at": now}},
        return_document=True,
    )

    # 3. 如果我是最后一个完成的,触发下一步
    if room["expected_count"] and room["completed_count"] >= room["expected_count"]:
        await enqueue_labeling(room_id)
```

**这就是"原子批次闭合"**。1000 个片段并发完成,只有一个人会看到 `completed_count == expected_count`,由他触发下一步。**不会漏(必有一次触发),也不会重(只有一次触发)**。

**再配一个 Timer 兜底**:后台协程扫"预期该做完但一直没做完"的房间,超过 30 分钟就强制推进,带上补偿标记。这就是"事件驱动 + Timer 补偿"的组合拳。

---

## 16. 常见坑与调试小技巧

### 16.1 更新忘了 `$set`,整份文档被覆盖

```python
# ❌ 灾难:除了 _id,别的全没了
col.update_one({"_id": "r1"}, {"status": "done"})

# ✅ 正确
col.update_one({"_id": "r1"}, {"$set": {"status": "done"}})
```

老版 pymongo 会静默替换,新版会抛错。反正你眼里要有 `$`。

### 16.2 `find_one_and_update` 忘了 `return_document`

**默认返回"改之前"的文档**!这是历史遗留(源自 MongoDB 命令 `findAndModify`)。很多"$inc 之后取新值"的场景需要显式:

```python
col.find_one_and_update(..., return_document=True)   # 或者 ReturnDocument.AFTER
```

### 16.3 upsert 没配合 `$setOnInsert`,状态被打回原形

见第 8 节。任何有 upsert 的地方,先问自己:"这次是重复消息的话,哪些字段不该被覆盖?"

### 16.4 忘建索引,线上慢查询爆炸

**上线前**:每一个 find / find_one_and_update 的过滤器都要过一遍 `explain`,确保走索引(`IXSCAN` 而不是 `COLLSCAN`)。

### 16.5 时间戳来源不一致

不同机器时钟不同步,`now = int(time.time())` 得出来的值差几秒是常事。**跨机器比较时间必须用同一个源**:

- 各机器都用 NTP 校时(公司基础设施一般都配了)
- 或者用 `$currentDate` 让 MongoDB 服务端填

### 16.6 用异步版本(motor 或 AsyncMongoClient)时忘 `await`

```python
doc = col.find_one({"_id": "r1"})   # ← 忘 await,返回的是 coroutine 对象
```

**症状**:`doc["status"]` 抛 `TypeError: 'coroutine' object is not subscriptable`。看到这个报错第一反应就是找哪儿漏了 `await`。

### 16.7 大文档写入变慢

单个文档不要超过几十 KB。太大的话:

- 拆表:主体和大字段分开存
- 用 GridFS 或对象存储(TOS/S3)存大二进制
- 数组不要无限增长,用 `$slice: -N` 限长

### 16.8 调试:mongosh + explain

命令行工具 `mongosh` 是你的好朋友,直接在里面写 JSON 试查询:

```bash
mongosh
> use mydb
> db.rooms.find({"status": "pending"}).explain("executionStats")
```

看输出里的 `totalDocsExamined`(扫了多少份)和 `nReturned`(返回了多少份)的比例。10:1 以内算健康,100:1 就该加索引了。

---

## 收工

你现在应该能:

- 说清楚"MQ 邮差 + Mongo 账本"这套架构在干什么
- 用 upsert + `$setOnInsert` 让重复消息不打乱状态机
- 用 `find_one_and_update` + revision 让多个 worker 抢任务不打架
- 用 `$inc` + 原子返回值 精确知道"最后一个片段完成"
- 用索引 + explain 让查询不再全表扫
- 判断什么时候真的需要事务(多数时候不需要)

再往深走,推荐这几件事:

1. **把线上账本每一个 find / update 都过一遍 explain**,把慢查询干掉。这是回报最高的一件事。
2. 读一遍 pymongo 官方文档的 `find_one_and_update` 和 `bulk_write`(批量写,吞吐能上一个台阶)。
3. 学 change stream(`col.watch()`):让消费者被动订阅文档变化,替代定期扫描。省 CPU 又更实时。
4. 学 schema versioning(数据结构版本管理):业务演进时怎么滚动升级字段而不停服。

Redis(低延迟调度) + MongoDB(持久化账本) 组合起来,你就有了一整套排产系统的骨架。剩下的就是业务领域怎么切分 —— 那是另外一份教程的事了。
