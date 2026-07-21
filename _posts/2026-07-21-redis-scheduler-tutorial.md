---
title: "Redis 队列调度实战：从 API 小白到能读懂线上 leader/worker"
date: 2026-07-21 21:00:00 +0800
description: "从 Redis 五种核心数据结构讲起，一步步搭出分布式锁、优先级队列、visibility timeout、分布式信号量，最后把每一段基础 API 对应到线上真实的 leader/worker 代码。"
tags: [Redis, Python, Distributed Systems, Scheduling]
permalink: /2026/07/21/redis-scheduler-tutorial/
---

> 这份教程写给"知道 Redis 是个 KV 数据库、但没真正用它做过调度系统"的同学。你不需要提前懂分布式锁、visibility timeout、分布式信号量这些黑话。我们会从 Redis 最基础的 API 讲起,一步步搭出一个完整的多机调度框架,最后把每一段基础 API 对应到线上真实生产代码(fragment 生产系统的 leader/worker)。

> 学习目标:读完这份教程你应该能
>
> - 熟练使用 Redis 五种核心数据结构(String / Hash / List / Set / ZSET)
> - 理解 `NX / XX / EX / PX` 四个参数,它们是 Redis 分布式的灵魂
> - 从零手写一个健壮的分布式锁(带 fencing token + 自动续期)
> - 从零手写一个带优先级、可自愈的任务队列
> - 读懂真实线上系统的 `leader_loop` 和 `process_one` 每一行

> 示例默认用 Python 3.11+ 的 `redis` 官方库(不是 `aioredis`,那个已经合并进 `redis` 了)。

## 目录

1. Redis 是什么、为什么用它做调度
2. 环境准备:安装 Redis 和 Python 客户端
3. 五种核心数据结构:够用 95% 的场景
4. TTL(过期时间):Redis 自愈的魔法
5. 分布式锁:从最朴素到生产可用
6. Lua 脚本:原子性利器
7. 优先级队列:ZSET 的正确用法
8. Visibility timeout:任务不丢的秘密
9. 分布式信号量:全局并发限流
10. QPS 时间槽:下游红线保护
11. 把所有东西拼起来:一个最小可用调度器
12. 对应到线上代码:leader.py 逐段拆解
13. 对应到线上代码:worker.py 逐段拆解
14. 常见坑与调试技巧

---

## 1. Redis 是什么、为什么用它做调度

一句话:**一个跑在内存里的、支持复杂数据结构的键值数据库**。

对比一下:

| | MySQL/Mongo | Redis |
|---|---|---|
| 存哪 | 磁盘 | 内存 |
| 速度 | 毫秒级 | 微秒级(100 倍以上) |
| 数据模型 | 表/文档 | 8 种数据结构 |
| 用途 | 存"真相" | 缓存、队列、锁、计数器 |

**心智模型**:Redis 是一本"便签簿",每条便签是 `key → value`。value 不只是字符串,还可以是列表、集合、哈希表等复杂结构。

**为什么用它做调度**:

1. **原子操作**:很多 Redis 命令是原子的,多进程同时操作不会打架
2. **过期时间**:每个 key 都可以设自动过期,这让"崩溃自愈"变得极其简单
3. **数据结构丰富**:ZSET 天然支持"按优先级排序",队列直接用它就够
4. **速度快**:调度系统最怕慢,毫秒级的 Redis 是 sweet spot

---

## 2. 环境准备

### 装 Redis 服务

Mac:

```bash
brew install redis
brew services start redis
```

### 装 Python 客户端

```bash
pip install redis
```

### 第一次连接

```python
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
# decode_responses=True:让返回值自动解成 str,不然是 bytes

r.set("hello", "world")
print(r.get("hello"))  # 'world'
```

**异步版**(线上生产用这个):

```python
import redis.asyncio as redis
import asyncio

async def main():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    await r.set("hello", "world")
    print(await r.get("hello"))
    await r.aclose()

asyncio.run(main())
```

**记住**:异步 API 每个命令前加 `await`,其他跟同步一样。下面示例我都用同步版方便读。

---

## 3. 五种核心数据结构

### 3.1 String — 最基础

**能存**:字符串、数字、二进制。

```python
# 最基本
r.set("name", "Alice")
r.get("name")           # 'Alice'
r.delete("name")

# 计数器(原子递增,多进程安全)
r.set("counter", 0)
r.incr("counter")       # 1
r.incr("counter")       # 2
r.incrby("counter", 10) # 12
r.decr("counter")       # 11

# 带过期时间(TTL)
r.set("token", "xyz", ex=60)     # 60 秒后自动删
r.set("token", "xyz", px=60000)  # 60000 毫秒
r.ttl("token")                   # 查剩余秒数:60

# 只在不存在时设置(关键 API!)
r.set("lock", "me", nx=True)     # 存在则返回 None,不覆盖
```

`nx=True` 让 SET 变成**原子的"如果不存在才设置"**,是所有分布式锁的基石。

### 3.2 Hash — 存"对象"

**心智模型**:一个 key 下面挂一个字典。

```python
r.hset("user:1000", mapping={
    "name": "Alice",
    "age": "30",
    "email": "a@x.com",
})

r.hget("user:1000", "name")        # 'Alice'
r.hgetall("user:1000")             # {'name': 'Alice', ...}
r.hincrby("user:1000", "age", 1)   # age 从 30 → 31
r.hdel("user:1000", "email")
r.hlen("user:1000")                # 字段数
```

**什么时候用 Hash 而不是 JSON 字符串**:

- 只想改某个字段 → Hash(不用读整个反序列化再写回)
- 要按字段计数 → Hash + HINCRBY
- 整体读写 → JSON 也行

### 3.3 List — 队列/栈

**心智模型**:一个可以两头进出的双向链表。

```python
# 塞入
r.rpush("queue", "task1")   # 右边塞
r.rpush("queue", "task2")
r.lpush("queue", "task0")   # 左边塞

# 取出
r.lpop("queue")             # 左边弹出
r.rpop("queue")             # 右边弹出

# 阻塞式弹(消费者最爱)
r.blpop("queue", timeout=5) # 等 5 秒,来一个立即返回
```

**经典用法:简单任务队列**

```python
# 生产者
r.rpush("todo", "任务A")

# 消费者
while True:
    _, task = r.blpop("todo", timeout=10)
    if task:
        do_work(task)
```

**BLPOP 是原子的**。10 个消费者同时 BLPOP,同一个任务只会被一个人拿到。

**局限**:List 不支持"按优先级排序",只能 FIFO 或 LIFO。要优先级用 ZSET。

### 3.4 Set — 去重

**心智模型**:一个不重复的元素集合。

```python
r.sadd("tags:post_1", "python", "redis", "tutorial")
r.sadd("tags:post_1", "python")   # 已存在,无效果
r.smembers("tags:post_1")
r.sismember("tags:post_1", "python")  # True
r.scard("tags:post_1")             # 元素个数
```

**常用场景**:去重、标签、好友关系。

### 3.5 Sorted Set (ZSET) — 优先级队列的秘密武器

**心智模型**:Set + 每个元素带一个分数(score),**自动按分数排序**。

**这是调度系统里最重要的数据结构**。

```python
# 塞入
r.zadd("leaderboard", {"Alice": 100, "Bob": 85, "Carol": 92})

# 按分数从小到大取
r.zrange("leaderboard", 0, -1, withscores=True)
# [('Bob', 85.0), ('Carol', 92.0), ('Alice', 100.0)]

# 按分数从大到小取
r.zrevrange("leaderboard", 0, -1, withscores=True)

# 取分数在某个区间的
r.zrangebyscore("leaderboard", 90, 100)  # ['Carol', 'Alice']

# 弹出分数最小的(原子操作!)
r.zpopmin("leaderboard")

# 只在不存在时添加(幂等入队!)
r.zadd("leaderboard", {"Dave": 88}, nx=True)

# 只在已存在时更新(visibility 预占的关键!)
r.zadd("leaderboard", {"Alice": 200}, xx=True)

# 计数
r.zcard("leaderboard")                       # 总数
r.zcount("leaderboard", 90, 100)             # 分数区间内数量
```

**关键参数记住**:

- `NX`:不存在才加 → **幂等入队**
- `XX`:存在才更新 → **visibility 预占**
- `CH`:返回真的改动了几个 → **区分"新增"和"无变化"**

---

## 4. TTL — Redis 的自愈魔法

**任何 key 都可以设过期时间**。到期自动删除。这是分布式锁能崩溃自愈的基础。

```python
# 设置时带过期
r.set("session", "abc123", ex=3600)      # 1 小时后删(秒)
r.set("session", "abc123", px=60000)     # 60 秒后删(毫秒)

# 后加过期
r.expire("session", 3600)
r.pexpire("session", 60000)

# 查
r.ttl("session")        # 剩余秒数:3600, -1(永久), -2(不存在)
r.pttl("session")       # 剩余毫秒数

# 取消/续期
r.persist("session")
r.expire("session", 3600)   # 直接重设
```

**注意**:ZSET 里的成员没有独立 TTL,只能整个 key 过期。想让 ZSET 里单个成员"过期"?用 score 存过期时间戳,配合 `ZRANGEBYSCORE` 清理 — 这就是下面 visibility 的核心思路。

---

## 5. 分布式锁 — 从零实现

### 5.1 最简单版

```python
got = r.set("lock:resource", "me", nx=True, ex=60)
# nx=True: 只在不存在时设置 → 抢锁语义
# ex=60:   60 秒后过期 → 崩溃自愈

if got:
    try:
        do_work()
    finally:
        r.delete("lock:resource")
```

**这就是分布式锁的核心**。`SET key val NX EX 60` 是**一条原子的 Redis 命令**。

### 5.2 为什么要用 token(防误删)

反例:

- 进程 A 抢到锁,卡住不干活
- 60 秒 TTL 到期,锁自动释放
- 进程 B 抢到同一把锁,开干
- A 醒过来 `DEL lock:resource` → **把 B 的锁删了!**
- 进程 C 又能抢到 → A、B、C 同时干

修复:每个人生成唯一 token,删锁前先检查是不是自己的。

```python
import uuid

token = uuid.uuid4().hex
got = r.set("lock:resource", token, nx=True, ex=60)
if got:
    try:
        do_work()
    finally:
        # 只删属于自己的锁
        current = r.get("lock:resource")
        if current == token:
            r.delete("lock:resource")
```

**但这里有 bug**:`get` 和 `delete` 是两步,中间可能出问题。修复方法是用 Lua 脚本让两步变原子(下面讲)。

---

## 6. Lua 脚本 — 原子性利器

Redis 支持一次执行一段 Lua 脚本,**期间不允许别的命令插入**。这就是"复合原子操作"的秘密。

```python
FENCED_DEL = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""

r.eval(FENCED_DEL, 1, "lock:resource", token)
# 参数:脚本、KEYS 数量、KEYS..., ARGV...
```

**Lua 脚本约定**:

- `KEYS[i]`:涉及的 key
- `ARGV[i]`:其他参数
- 返回 0/1 表示是否真的删了

### 完整的健壮分布式锁

```python
import uuid

class RedisLock:
    RELEASE_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    else
        return 0
    end
    """
    RENEW_LUA = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('PEXPIRE', KEYS[1], ARGV[2])
    else
        return 0
    end
    """

    def __init__(self, r, key, ttl_ms=60_000):
        self.r = r
        self.key = key
        self.ttl_ms = ttl_ms
        self.token = uuid.uuid4().hex

    def acquire(self) -> bool:
        return bool(self.r.set(self.key, self.token, nx=True, px=self.ttl_ms))

    def release(self) -> bool:
        return bool(self.r.eval(self.RELEASE_LUA, 1, self.key, self.token))

    def renew(self) -> bool:
        return bool(self.r.eval(self.RENEW_LUA, 1, self.key, self.token, self.ttl_ms))
```

### 自动续期(做长任务)

```python
import asyncio

async def with_lock(r, key, ttl_ms, worker):
    lock = RedisLock(r, key, ttl_ms)
    if not lock.acquire():
        return
    async def renewer():
        while True:
            await asyncio.sleep(ttl_ms / 3 / 1000)  # 每 1/3 TTL 续一次
            if not lock.renew():
                return   # 锁被抢走了
    task = asyncio.create_task(renewer())
    try:
        await worker()
    finally:
        task.cancel()
        lock.release()
```

**为什么每 1/3 TTL 续一次**?留出 2 次续期失败的缓冲。如果每 1/2 续,失败一次就危险了。

---

## 7. 优先级队列 — ZSET 的正确用法

### 7.1 塞任务(生产者)

```python
def enqueue(task_id, priority=0, urgency_secs=0):
    # 分数编码:priority 权重最大,urgency 次之
    score = -(priority * 10**10 + urgency_secs)
    #         负号:让"高优先级"变成"score 小"
    #         Redis ZSET 默认升序,所以 score 小 = 排前面
    r.zadd("todo", {task_id: score}, nx=True)
    #                                ↑ 幂等
```

**分数编码技巧**:

用**位段编码**把多个维度打包成一个数字。

```
分数 = -(priority × 10^10 + urgency_secs)
       ↑              ↑
       高位段          低位段
```

高位段的 1 差异 > 低位段所有可能差异之和,所以**严格分层压制**:priority 永远压过 urgency。

如果你需要三个维度,继续叠:

```python
score = -(priority * 10**10 + completed_count * 10**8 + urgency_secs)
```

`completed_count * 10**8`:已完成工序数,让"快做完的单子优先做完"(收敛避免半成品)。

### 7.2 消费者 — 朴素版

```python
def consume():
    result = r.zpopmin("todo", count=1)   # 原子:弹出分数最小的
    if not result:
        return None
    return result[0][0]
```

**问题**:ZPOPMIN 之后任务从队列消失了。如果处理中崩溃 → **任务丢了**。

---

## 8. Visibility Timeout — 任务不丢的秘密

**核心思路**:不真弹出,而是把 score 改成"未来"(比如 `now + 5min`),让它临时隐身。

```python
import time

def fetch_one():
    now_ms = int(time.time() * 1000)

    # 只看分数 ≤ now_ms 的(可见任务)
    candidates = r.zrangebyscore("todo", "-inf", now_ms, start=0, num=10)

    for task_id in candidates:
        # 抢一下:用 XX 只在成员存在时更新
        changed = r.zadd(
            "todo",
            {task_id: now_ms + 5*60*1000},
            xx=True, ch=True,
        )
        if changed:
            return task_id   # 抢到了,score 已推到 5 分钟后
    return None
```

**为什么这个方案好**:

- 抓到就"隐身"(score 推到未来),别的 worker 看不到
- 处理中崩溃?**5 分钟后 score 回到可见区间,别人自动接手**
- 完全去中心化,无需协调
- 不用 Lua,单条 ZADD XX 就是原子的

**完成时删除**:

```python
def complete(task_id):
    r.zrem("todo", task_id)
```

### 双保险:再叠一层 lease 锁

visibility 已经防重复取,但生产系统会再叠一把 lease 锁(`SET NX PX`)确保绝对不重复:

```python
def process_one():
    task_id = fetch_one()
    if not task_id:
        return

    # 二次上锁
    lock = RedisLock(r, f"lease:{task_id}", ttl_ms=5*60*1000)
    if not lock.acquire():
        return

    try:
        do_work(task_id)
        r.zrem("todo", task_id)   # 完成
    finally:
        lock.release()
```

**为什么要两层**?

- ZSET visibility 是**队列侧**的隐身,防止其他 worker **取到**同一条
- lease 锁是**任务侧**的独占,防止两个 worker 都取到时都**执行**同一条

前者是"取的时候不打架",后者是"退一步说,万一取到了也不打架"。**双保险的目的是可组合的正确性**。

---

## 9. 分布式信号量 — 全局并发限流

需求:"whisper 任务全厂同时最多 6 个在跑"。

**为什么不用简单计数器**?

```python
# 反例
n = r.incr("sem:whisper")
if n <= 6:
    do_work()
    r.decr("sem:whisper")
```

问题:worker 崩溃 → 没 DECR → 计数永远漏,最终 sem = limit **永久卡死**。

**正确姿势:HASH + Lua**

```python
SEM_ACQUIRE_LUA = """
local n = tonumber(redis.call('HLEN', KEYS[1]))
if n < tonumber(ARGV[1]) then
    redis.call('HSET', KEYS[1], ARGV[2], ARGV[3])
    return 1
end
return 0
"""

def acquire_slot(field, limit):
    token = uuid.uuid4().hex
    while True:
        got = r.eval(SEM_ACQUIRE_LUA, 1, f"sem:{field}", limit, token, int(time.time()))
        if got:
            return token
        time.sleep(0.2)   # 抢不到,退避

def release_slot(field, token):
    r.hdel(f"sem:{field}", token)
```

**属性**:

- 每个持有者写自己的 token 到 HASH
- 崩溃 → 定期清理超时 token(可选:token 里带时间戳,leader 定期扫)
- HLEN 直接反映当前占用数

**为什么必须用 Lua**?"读 HLEN + 判断 + HSET"三步不能被其他连接切断,否则会超发。

---

## 10. QPS 时间槽 — 下游红线保护

**并发数 ≠ QPS**。

- 并发数 = 同时有几个人在干活
- QPS = 每秒发出去多少个下游请求

假设 6 个并发 worker 每个每秒发 5 个下游 RPC → 30 QPS,可能压爆下游。

**方案**:把时间切成毫秒级槽,每槽只放 1 个请求过。

```python
import math

def acquire_qps_slot(downstream, qps):
    slot_width_ms = math.ceil(1000 / qps)  # 例如 qps=10 → 100ms/槽

    while True:
        now_ms = int(time.time() * 1000)
        slot = now_ms // slot_width_ms
        key = f"rl:{downstream}:{slot}"

        # 抢这个时间槽,抢到才放行
        got = r.set(key, "1", nx=True, px=slot_width_ms * 2)
        if got:
            return

        # 抢不到 → 睡到下一槽边界
        sleep_ms = slot_width_ms - (now_ms % slot_width_ms)
        time.sleep(sleep_ms / 1000)
```

**属性**:

- 全局强制。开多少实例、多少并发都不会破 QPS 红线
- 无状态。每个时间槽都是独立 key,Redis 自动过期清理
- 精度到 1ms

---

## 11. 一个最小可用的调度器

把所有东西拼起来:

```python
import redis
import uuid
import time
import math

r = redis.Redis(decode_responses=True)

FENCED_DEL = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""

# ---------- 生产者 ----------
def enqueue(field, task_id, priority=0):
    """幂等入队"""
    score = -(priority * 10**10)
    r.zadd(f"ready:{field}", {task_id: score}, nx=True)

# ---------- 消费者 ----------
def fetch_one(field, lease_ttl_ms=5*60*1000):
    now_ms = int(time.time() * 1000)
    candidates = r.zrangebyscore(
        f"ready:{field}", "-inf", now_ms, start=0, num=10
    )
    for task_id in candidates:
        changed = r.zadd(
            f"ready:{field}",
            {task_id: now_ms + lease_ttl_ms},
            xx=True, ch=True,
        )
        if changed:
            return task_id
    return None

def try_lock(key, token, ttl_ms):
    return bool(r.set(key, token, nx=True, px=ttl_ms))

def unlock(key, token):
    return bool(r.eval(FENCED_DEL, 1, key, token))

def worker_loop(field):
    while True:
        task_id = fetch_one(field)
        if not task_id:
            time.sleep(1)
            continue

        token = uuid.uuid4().hex
        lease_key = f"lease:{field}:{task_id}"
        if not try_lock(lease_key, token, ttl_ms=5*60*1000):
            continue

        try:
            print(f"processing {task_id}")
            time.sleep(2)  # 模拟工作
            r.zrem(f"ready:{field}", task_id)
        finally:
            unlock(lease_key, token)

# ---------- 使用 ----------
if __name__ == "__main__":
    enqueue("whisper", "task_A", priority=1)
    enqueue("whisper", "task_B", priority=10)  # 更急
    enqueue("whisper", "task_C", priority=0)

    worker_loop("whisper")
```

跑起来观察:`task_B` 先被处理,然后 `task_A`、`task_C`。

---

## 12. 对应到线上代码:leader.py 逐段拆解

现在把上面所有 API 对应到我们线上的 `leader.py`(fragment 生产调度器)。

### 12.1 整体架构

```
                    ┌───────────────┐
                    │  Mongo 权威    │
                    │  materials_core│
                    └───┬───────▲───┘
                        │       │
                  ①扫描 │       │④写回
                        ▼       │
                    ┌───────────┴───┐
                    │ leader        │  ← 每工序 1 个
                    │ 每 5s 扫库    │
                    └───┬───────────┘
                        │
                  ②投队列
                        ▼
              ┌─────────────────┐
              │ Redis ready ZSET │
              │ 分数排序          │
              └───┬─────────────┘
                  │
                  ▼
           worker 消费(下节)
```

**核心思想**:Mongo 是权威,Redis 是提醒板。leader 负责把 Mongo 里"待做"的任务同步到 Redis ZSET。

### 12.2 leader 选主

多实例部署时,不能每台都扫库。所以 leader 用 Redis 分布式锁选主:

```python
# leader.py 简化后
async def leader_loop(field, *, stop_event):
    token = uuid.uuid4().hex
    while not stop_event.is_set():
        # 抢主锁
        got = await r.set(f"scan:leader:{field}", token, nx=True, px=30_000)
        if not got:
            await asyncio.sleep(15)   # 别的实例是 leader,退避
            continue

        try:
            while await still_leader(field, token):
                await scan_and_enqueue(field)
                await sleep(5)
        finally:
            # 释放主锁(fenced_del,确保还是自己的)
            await fenced_del(f"scan:leader:{field}", token)
```

对应到基础 API:

- **抢主**:`SET scan:leader:whisper <token> NX PX 30000` → 只有一个实例能拿到
- **续锁**:每轮末尾 `PEXPIRE`,防止扫描过程中锁过期
- **让位**:每轮开始前 `GET` 校验 token,如果被抢走(比如本实例卡顿导致锁过期)则退出
- **fenced 释放**:Lua 脚本,只删自己的

**为什么每工序一个 leader**?whisper / media / caption 各有独立的 `scan:leader:<field>` key。一个工序 leader 卡了不影响其他工序调度。

### 12.3 scan_and_enqueue 扫库入队

```python
async def scan_and_enqueue(field):
    async for doc in mongo.find(scan_filter(field)):
        score = compute_score(
            priority=doc["reconcile_priority"],
            completed_count=doc["completed_count"],
            urgency_secs=int(time.time()) - doc["last_touch"],
        )
        # NX:已经在队列里的不重投
        await r.zadd(f"ready:{field}", {doc["_id"]: score}, nx=True)
```

对应到基础 API:

- **优先级编码**:`compute_score = -(priority × 10^10 + completed_count × 10^8 + urgency_secs)`
- **幂等入队**:`ZADD ready:<field> {fid: score} NX` → 反复扫库不重复入队
- **动态优先级**:如果外部改了 `reconcile_priority`,下轮扫描重新算分,新加进来的以新分数入队(旧的已经在队列里的可以用 `ZADD GT` 特殊处理,这里略)

### 12.4 定期打扫卫生

leader 除了扫库入队,还负责几件低频维护工作:

```python
async def leader_session(field, token, *, stop_event):
    last_sweep = 0
    while not stop_event.is_set():
        # 每 5 秒
        await scan_and_enqueue(field)

        # 每 90 秒(低频)
        if time.time() - last_sweep > 90:
            await sweep_zombies(field)          # 回收崩溃 worker 的锁
            await sweep_exhausted(field)        # 超过重试上限的 → 终态
            await rescue_visibility_orphans(field)  # 被隐身但没人接手的
            last_sweep = time.time()

        await sleep(5)
```

**Visibility 孤儿救援**是个有意思的场景:

- 某个任务被 worker 抓走,score 推到未来
- worker 处理中重启 / 被 kill / 网络分区
- 任务的 score 会自然到期(再变可见)
- 但如果 leader 觉得"这个 score 已经过了,还在这里没被处理",就主动把 score 刷回优先级 score,加速接管

代码:

```python
async def rescue_visibility_orphans(field):
    now_ms = int(time.time() * 1000)
    # 分数在 (0, now_ms] 的成员:曾经被推到未来,现在已经到期还在队列里
    orphans = await r.zrangebyscore(f"ready:{field}", 0, now_ms)
    for fid in orphans:
        doc = await mongo.find_one({"_id": fid})
        new_score = compute_score(doc)
        await r.zadd(f"ready:{field}", {fid: new_score}, xx=True)
```

---

## 13. 对应到线上代码:worker.py 逐段拆解

worker 负责从 ZSET 消费任务、抢锁、干活、释放。核心函数是 `process_one`。

### 13.1 单次任务处理:5 条正确性规则

```python
async def process_one(field, fragment_id, config):
    token = uuid.uuid4().hex
    key = f"lease:{field}:{fragment_id}"
    lease_ttl_ms = config.lease_ttl_s * 1000

    # === 规则 1:抢 lease(SETNX) ===
    if not await r.set(key, token, nx=True, px=lease_ttl_ms):
        return   # 抢不到 = 别的 worker 在做

    try:
        # === 规则 2:visibility 预占 ===
        now_ms = int(time.time() * 1000)
        await r.zadd(
            f"ready:{field}",
            {str(fragment_id): float(now_ms + lease_ttl_ms)},
            xx=True,
        )

        # === 规则 3:二次校验(幂等闸)===
        doc = await mongo.find_one({"_id": fragment_id})
        if doc is None or not should_still_build(field, doc):
            await fenced_zrem(field, fragment_id, key, token)
            return

        # === 规则 4:干活时后台续锁 ===
        async with lease_renewer(key, token, lease_ttl_ms):
            result = await fetch_and_persist(
                FragmentFetchCommand(
                    id=fragment_id,
                    targets=[field],
                    ...
                )
            )

        # === 规则 5:fenced 出队 ===
        # 只有锁还是自己的才 zrem,防止误删接管者的成果
        await fenced_zrem(field, fragment_id, key, token)

    finally:
        await fenced_del_lease(key, token)
```

### 13.2 五条规则对应的基础 API

| 规则 | Redis 命令 | 对应 tutorial 章节 |
|---|---|---|
| 1. 抢独占 lease | `SET key token NX PX` | §5 分布式锁 |
| 2. Visibility 预占 | `ZADD ready XX` 推 future | §8 visibility |
| 3. 二次校验 | 读 Mongo 判断 | 幂等闸,防重复劳动 |
| 4. 后台续锁 | Lua `GET+PEXPIRE` 每 lease_ttl/3 秒 | §6 Lua + §5 续期 |
| 5. Fenced 出队 | Lua `GET+ZREM` 校验 token | §6 Lua 原子性 |

### 13.3 lease_renewer 后台续期

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lease_renewer(key, token, ttl_ms):
    async def renew_loop():
        while True:
            await asyncio.sleep(ttl_ms / 3 / 1000)
            renewed = await r.eval(RENEW_LUA, 1, key, token, ttl_ms)
            if not renewed:
                return   # 锁被抢走
    task = asyncio.create_task(renew_loop())
    try:
        yield
    finally:
        task.cancel()
```

**为什么用 `async with`**?

自动管理生命周期。业务代码 `async with lease_renewer(...): await do_work()`,退出 block 时(不管正常还是异常)自动 cancel 续期协程。

### 13.4 fenced_zrem — 为什么不能直接 zrem?

**反例**:

- worker A 抢到 lease,开始干活
- A 网络卡了,lease 过期
- worker B 抢到 lease,开始干活
- A 网络恢复,干完了,直接 `ZREM` 把任务从队列删掉
- **但 B 还在干这个任务!** ZREM 之后 B 的任务如果失败,再也不会被重试

**修复**:

```python
FENCED_ZREM_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('ZREM', KEYS[2], ARGV[2])
else
    return 0
end
"""

async def fenced_zrem(field, fragment_id, lease_key, token):
    await r.eval(
        FENCED_ZREM_LUA,
        2,
        lease_key, f"ready:{field}",
        token, str(fragment_id),
    )
```

**规则**:任何"决定性写操作"(把任务从队列删除、写库标 success)前,都要用 fencing token 校验锁还是自己的。

### 13.5 三层并发控制

上面的 `process_one` 处理**单个**任务。真实 worker 是这样跑的:

```python
async def worker_loop(field):
    while True:
        # 计算当前该有多少个并发
        limit = resolve_concurrency(field)   # 见下

        # 起一个 producer + N 个 consumer 的流式管道
        queue = asyncio.Queue(maxsize=limit)

        async def producer():
            while True:
                # 从 ZSET 抓一批
                candidates = await r.zrangebyscore(
                    f"ready:{field}", "-inf", int(time.time()*1000),
                    start=0, num=2048,
                )
                # 随机采样 N 个防止多实例哄抢
                picked = random.sample(candidates, min(limit, len(candidates)))
                for fid in picked:
                    # visibility 预占
                    changed = await r.zadd(
                        f"ready:{field}",
                        {fid: now_ms + lease_ttl_ms},
                        xx=True, ch=True,
                    )
                    if changed:
                        await queue.put(fid)

        async def consumer():
            while True:
                fid = await queue.get()
                # 抢全局并发信号量
                slot = await acquire_slot(field, limit)
                if not slot:
                    continue
                try:
                    await process_one(field, fid, config)
                finally:
                    await release_slot(field, slot)

        # 起 1 个 producer + N 个 consumer
        ...
```

**三层并发决策**:

| 层 | 作用 | Redis 命令 |
|---|---|---|
| 全局信号量 | 跨实例合计并发 ≤ 上限 | HASH + Lua HLEN 判断 |
| 本地并发 | 单实例最多起 N 个 consumer | Python asyncio.Queue |
| Producer 采样 | 从 2048 候选窗口随机抽 N 个 | `random.sample` |

**为什么要"随机采样 2048"**?

多实例场景。10 个实例的 producer 都 `ZRANGEBYSCORE ... LIMIT 0 N`,拿到的是同一批 N 条。10 个人抢同 N 条 → visibility XX 大部分抢失败 → 浪费。

改成:每人取 2048 条候选,从里面随机抽 N 条,**天然错开,减少抢锁失败率**。

### 13.6 QPS 硬红线

worker 干活时会调下游服务(转码 / LLM / ASR)。合计 QPS 由 SETNX 时间槽保护:

```python
# 在 ops/media_fetch.py 里
async def call_media_process(...):
    await acquire_qps_slot("mediaProcess", qps=50)   # §10
    await rpc_call(...)
```

**并发和 QPS 是两个正交的旋钮**:

- 6 个并发 worker,每个每秒发 3 个 RPC → 18 QPS
- 6 个并发 worker,每个每秒发 1 个 RPC → 6 QPS

**只加信号量不加时间槽**,同一批 worker 内部循环可能瞬间发很多请求打爆下游。

---

## 14. 常见坑与调试技巧

### 坑 1:`decode_responses` 忘开

```python
r = redis.Redis()
r.get("k")   # b'v' 是 bytes,不是 str

# 修复
r = redis.Redis(decode_responses=True)
```

### 坑 2:忘了 NX/XX

```python
# ❌ 每次扫库都覆盖 score,把已经在队列里的优先级刷掉
r.zadd("todo", {"task1": new_score})

# ✅ 已存在的不动
r.zadd("todo", {"task1": new_score}, nx=True)
```

### 坑 3:分布式锁不带 TTL

```python
# ❌ 崩溃就死锁
r.set("lock", "me", nx=True)

# ✅ 一定要有过期
r.set("lock", "me", nx=True, ex=60)
```

### 坑 4:删锁不校验 token

一定用 Lua fenced_del。

### 坑 5:用 KEYS 扫全库

`KEYS *` 会阻塞 Redis。**永远用 SCAN**:

```python
cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, match="lock:*", count=100)
    for k in keys:
        process(k)
    if cursor == 0:
        break
```

### 坑 6:内存不释放

Redis 所有 key 都在内存里。忘设 TTL 或用无界数据结构(List/ZSET 不断塞)会 OOM。

**建议**:

- 任何有生命周期的 key 都设 TTL
- 队列 ZSET 定期清理已完成的
- 用 `INFO memory` 监控

### 调试神器:redis-cli

```bash
redis-cli
> KEYS *                       # 列所有 key(生产慎用!)
> SCAN 0 MATCH "lease:*"       # 分批扫
> TYPE mykey                   # 看类型
> TTL mykey                    # 剩余过期时间
> ZRANGE ready:whisper 0 -1 WITHSCORES   # 看队列内容
> HGETALL sem:whisper          # 看信号量占用
> INFO memory                  # 内存使用
```

---

## 15. API 速查表(贴墙上)

| 目的 | 命令 | 章节 |
|---|---|---|
| 抢分布式锁 | `SET key val NX PX 60000` | §5 |
| 释放锁 | Lua `GET + DEL` (校验 token) | §6 |
| 续锁 | Lua `GET + PEXPIRE` | §6 |
| 幂等入队 | `ZADD key {member: score} NX` | §7 |
| Visibility 预占 | `ZADD key {member: future_ts} XX CH` | §8 |
| 取到期任务 | `ZRANGEBYSCORE key -inf now_ms LIMIT 0 N` | §8 |
| 优先级弹出 | `ZPOPMIN key` | §7 |
| 计数器 | `INCR / INCRBY / DECR` | §3.1 |
| 缓存带 TTL | `SET key val EX 3600` | §4 |
| 简单队列生产 | `RPUSH key val` | §3.3 |
| 简单队列消费 | `BLPOP key timeout` | §3.3 |
| 全局并发计数 | Lua `HLEN + HSET` | §9 |
| QPS 红线 | `SET rl:xxx:slot NX PX` | §10 |

---

## 16. 学习路径建议

1. **先玩通 5 种数据结构**,用 `redis-cli` 手动敲每个命令
2. **理解 `NX / XX / EX / PX` 四个参数**(Redis 分布式的灵魂)
3. **手写一个分布式锁**(`SET NX PX` + Lua fenced_del)
4. **手写一个优先级队列**(ZSET + visibility)
5. **回头看线上真实代码** — leader.py 和 worker.py 每一行都能对应到上面的模式

---

## 一句话总结

> **一张单子在 Mongo,一块白板在 Redis。leader 扫库喂白板,worker 抢白板干活。**
>
> **并发靠信号量,QPS 靠时间槽,正确靠版本号 CAS,自愈靠锁自动过期。**

Redis 提供的能力其实很少:原子操作、TTL、数据结构。但把它们组合起来,能搭出令人惊讶的复杂系统。**关键是理解每个 API 在解决什么问题**,而不是死记命令。

---

*如果觉得有用,可以对着你手边一份用 Redis 做队列的代码,把 tutorial 里的模式一一对号入座。多数"看起来很复杂"的调度系统,拆开都是这些基础模式的组合。*
