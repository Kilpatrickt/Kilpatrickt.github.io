---
title: "Redis 队列调度实战：从零到读懂线上代码"
date: 2026-07-21 21:00:00 +0800
description: "这不是官方文档的翻译。我会带你从生活场景出发理解 Redis 到底在解决什么问题，一步步把小玩具搭成能扛住 10 台机器同时跑的调度系统，最后把每一行操作对应到真实生产代码。"
tags: [Redis, Python, Distributed Systems, Scheduling]
permalink: /2026/07/21/redis-scheduler-tutorial/
---

> 这份教程写给这样的你：
> - **知道 Redis 存在，但没真正用它写过东西**
> - 看过一些"分布式锁"、"任务队列"的文章，但读完还是一脸懵
> - 手头正好有一个"多台机器一起处理任务、不能重复不能漏"的场景
>
> 我不打算堆黑话。每讲一个概念，我先讲**日常生活里同样的问题是怎么解决的**，再讲代码。你会发现分布式系统里那些吓人的名词，本质上都是"一群人一起干活时怎么不打架"的常识。

---

## 目录

1. 先讲个故事：Redis 到底解决什么问题
2. 装个 Redis，敲第一行命令
3. 五种数据结构：一句话记住每种是干嘛的
4. 过期时间：Redis 最神奇的一个特性
5. 分布式锁：三个版本演进，每一版都有 bug 等着你踩
6. Lua 脚本：为什么它是绕不开的
7. 优先级队列：怎么让 VIP 单永远排最前面
8. 隐身机制：任务取走后还能"回来"
9. 全局并发信号量：10 台机器加起来最多同时干 6 个
10. QPS 时间槽：保护脆弱的下游
11. 把所有零件拼起来：一个能跑的调度器
12. 对着看线上代码：leader.py 每一段在干嘛
13. 对着看线上代码：worker.py 每一段在干嘛
14. 你会踩的坑
15. 一句话总结

---

## 1. 先讲个故事：Redis 到底解决什么问题

想象你开了家餐厅，只有你一个人。客人点单直接告诉你，你写在小本子上，做完划掉。没有任何"并发"问题——因为只有一个人。

现在生意好了，你雇了 10 个厨师。**问题来了**：

- 客人的单子写哪？如果每个厨师有自己的小本子，同一桌菜可能做 10 遍
- 谁来分单？如果 10 个人一起抢单，可能抢到打起来
- VIP 客人的菜要先做，怎么办？
- 一个厨师做菜做一半晕倒了，他手上那单谁来接？

这就是"多台机器一起干活"要面对的所有问题。而 Redis，本质上是**厨房中间那块所有人都能看到的公共白板**。

- 客人的单子 → 写在白板上
- 谁抢到就在白板上盖个"我的"章
- VIP 单子写得靠前一点
- 厨师晕倒？白板上的"我的"章有个"5 分钟自动消失"的墨水

Redis 的所有牛逼之处，其实就这几个：**大家都能看到**、**改动是原子的（不会改一半）**、**能设过期时间**。

---

## 2. 装个 Redis，敲第一行命令

Mac 用户：

```bash
brew install redis
brew services start redis
```

装个 Python 客户端：

```bash
pip install redis
```

连上试试：

```python
import redis

# decode_responses=True 让返回值是字符串而不是 bytes，别忘了这个参数
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

r.set("hello", "world")
print(r.get("hello"))   # 'world'
```

线上生产用的是异步版：

```python
import redis.asyncio as redis
import asyncio

async def main():
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    await r.set("hello", "world")
    print(await r.get("hello"))

asyncio.run(main())
```

**唯一区别**：每个命令前加 `await`。下面为了好读我都用同步版写。

---

## 3. 五种数据结构：一句话记住每种是干嘛的

Redis 支持很多种数据结构，但你日常 95% 的活儿只需要这 5 种。我先给你一句话记忆：

| 结构 | 一句话 | 什么时候用 |
|---|---|---|
| **String** | 一个 key 存一个值 | 缓存、计数器、锁 |
| **Hash** | 一个 key 下面挂一个字典 | 存"对象"，改单字段方便 |
| **List** | 一根双向链表 | 简单的 FIFO 队列 |
| **Set** | 一个不重复的集合 | 标签、去重、朋友圈 |
| **ZSET** | Set 但每个元素带个分数，自动排序 | **优先级队列** |

下面一个个讲。你不用记 API，看懂在干嘛就行。真用的时候翻回来查。

### 3.1 String：最简单也最常用

```python
# 存和取
r.set("name", "Alice")
r.get("name")             # 'Alice'

# 计数（重点：原子的，10 台机器同时 incr 结果也对）
r.set("counter", 0)
r.incr("counter")         # 1
r.incr("counter")         # 2
r.incrby("counter", 100)  # 102
```

看到"原子的"这个词你可能没感觉。**举个例子**：

```python
# ❌ 错误示范
n = int(r.get("counter"))
r.set("counter", n + 1)
```

10 个进程都这么写，会发生什么？

- 进程 A 读到 counter = 5
- 进程 B 也读到 counter = 5
- A 写回 6
- B 写回 6

明明 +2 应该是 7，结果是 6。**这就叫"竞态条件"**。你在多机场景反复会遇到它。

`INCR` 一条命令，Redis 内部帮你搞定了这个问题。**这是 Redis 分布式能力的基础**：把"读+改+写"合并成一条原子命令。

### 3.2 String 里最重要的 4 个参数

```python
r.set("token", "xyz", ex=60)     # 60 秒后自动删（单位：秒）
r.set("token", "xyz", px=60000)  # 60000 毫秒后自动删
r.set("lock", "me", nx=True)     # 只在 key 不存在时才设置
r.set("lock", "me", xx=True)     # 只在 key 已存在时才设置
```

这 4 个参数（`ex / px / nx / xx`）是 Redis 分布式系统的灵魂。**你只要记住**：

- **`ex/px`**：让 key 过一段时间自己消失。**这就是崩溃自愈的基础**——进程死了，锁不用手动释放
- **`nx`**：让 SET 变成 "不存在才设置"。**这就是抢锁的基础**——第一个 SET 上的人赢
- **`xx`**：让 SET 变成 "存在才设置"。用来更新，不用来创建

而且它们可以组合：

```python
r.set("lock:job:123", "me", nx=True, ex=60)
```

这一条命令的意思是：**"如果这把锁没人拿，我拿走，60 秒后自动释放"**。这就是分布式锁的全部——一行代码。

### 3.3 Hash：存对象

想存一个用户？

```python
# 一次性写多个字段
r.hset("user:1000", mapping={
    "name": "Alice",
    "age": "30",
})

r.hget("user:1000", "name")        # 'Alice'
r.hgetall("user:1000")             # {'name': 'Alice', 'age': '30'}
r.hincrby("user:1000", "age", 1)   # 只改 age 字段，不影响 name
```

**Hash vs JSON 字符串**：

- 用 JSON：每次改一个字段都要读整个 JSON、反序列化、改、序列化、写回
- 用 Hash：只改要改的字段，其他字段不动

如果字段之间独立、经常单独更新 → Hash 完胜。

### 3.4 List：简单队列

```python
# 塞进去
r.rpush("queue", "任务A")   # 从右边塞
r.rpush("queue", "任务B")

# 取出来
r.lpop("queue")             # 从左边弹：'任务A'
r.lpop("queue")             # '任务B'

# 阻塞式弹（消费者最爱）
r.blpop("queue", timeout=5) # 空队列就等 5 秒，来任务立即返回
```

**BLPOP 是原子的**。10 个消费者同时 BLPOP，Redis 保证一个任务只会被一个人拿到。这是最简单的任务分发。

**List 的死穴**：不支持优先级。VIP 单子塞进去，也得排在普通单后面。

### 3.5 Set：去重

```python
r.sadd("tags:post_1", "python", "redis", "tutorial")
r.sadd("tags:post_1", "python")   # 重复，无效果
r.smembers("tags:post_1")          # {'python', 'redis', 'tutorial'}
r.sismember("tags:post_1", "python")  # True，快速判断存在性
```

用于打标签、判断"这个人是不是我好友"这类场景。跟队列关系不大，简单过一下。

### 3.6 ZSET：优先级队列的秘密武器

**这个是重头戏**。

ZSET = Set + 每个元素带一个分数（score）。**Redis 会按分数自动排好序**。

想象一个 VIP 排号系统：

```python
# 塞进去：{名字: 分数}
r.zadd("waitlist", {"Alice": 100, "Bob": 50, "VIP_Carol": 5})

# 按分数从小到大取所有
r.zrange("waitlist", 0, -1, withscores=True)
# [('VIP_Carol', 5.0), ('Bob', 50.0), ('Alice', 100.0)]
#     ↑ VIP 天然排在最前面
```

分数越小越靠前。所以你想让谁靠前，给他小分数（甚至负数）。

**ZSET 的核心 API**（这几个你以后会反复用）：

```python
# 塞入
r.zadd("q", {"task1": 100})

# 幂等塞入：已存在就不动
r.zadd("q", {"task1": 100}, nx=True)

# 只更新已存在的：不存在就不动
r.zadd("q", {"task1": 200}, xx=True)

# 按分数区间取
r.zrangebyscore("q", -100, 50)   # 取分数在 [-100, 50] 的成员

# 弹出分数最小的（原子）
r.zpopmin("q", count=1)

# 删除
r.zrem("q", "task1")
```

**你必须记住**这两组用法，后面会一直出现：

- **`ZADD ... NX`** → 幂等入队（反复塞不会重复）
- **`ZADD ... XX`** → "隐身"技巧的关键（后面讲）

---

## 4. 过期时间：Redis 最神奇的一个特性

Redis 每个 key 都可以设"多久之后自动消失"。

```python
r.set("code", "123456", ex=300)   # 5 分钟后自动删（比如短信验证码）
r.ttl("code")                     # 查剩余秒数：300、299、298 ...
```

**为什么这个特性这么重要？**

分布式系统最头疼的一件事：**进程死了怎么办？**

- 传统方案：中心化的"心跳"机制，每 5 秒汇报"我还活着"，超时被判定死亡
- Redis 方案：任何"表示我在忙"的 key 都带过期时间。**你活着，就每隔一会续期一下；你死了，key 到期自动消失，别人自然接手**

这个想法太优雅了。**不需要中央调度、不需要心跳协议**，全靠 Redis 帮你算时间。

举个例子。你抢了把锁：

```python
r.set("lock:job", "me", nx=True, ex=60)
```

正常干完活：

```python
r.delete("lock:job")   # 主动释放
```

进程崩溃了：

```
（什么都不用做）
60 秒后 → Redis 自己把这个 key 删掉 → 别人可以抢
```

**这就是"崩溃自愈"**。整个系统不需要监控器，不需要故障切换协议，Redis 的 TTL 帮你搞定。

---

## 5. 分布式锁：三个版本演进

现在做点真的东西。假设有一个任务 `job:123`，10 台机器都可能想处理它，我们要保证**同一时刻只有一台机器在处理**。

### 版本 1：最朴素

```python
got = r.set("lock:job:123", "me", nx=True, ex=60)
if got:
    try:
        process_job()
    finally:
        r.delete("lock:job:123")
```

**这个版本能工作**：

- `nx=True`：只有第一个 SET 成功
- `ex=60`：如果进程崩了，60 秒后锁自动释放
- 别的机器 `set` 返回 `None`，就跳过

**但有个坑**——想象这个场景：

1. 机器 A 抢到锁，开始处理
2. A 因为 GC / 网络卡顿 / 磁盘慢，卡了 65 秒
3. 60 秒时锁自动过期
4. 机器 B 抢到同一把锁，开始处理
5. A 缓过神来，跑到 `r.delete("lock:job:123")` → **把 B 的锁删了**
6. 机器 C 抢到锁，开始处理
7. **B 和 C 同时在处理 job:123**

这就是**误删锁**问题。

### 版本 2：给锁盖个"我的"章

给每次抢锁都生成一个唯一 token，删锁前先看看是不是自己的：

```python
import uuid

token = uuid.uuid4().hex
got = r.set("lock:job:123", token, nx=True, ex=60)
if got:
    try:
        process_job()
    finally:
        # 只删属于自己的
        if r.get("lock:job:123") == token:
            r.delete("lock:job:123")
```

看起来解决了？**还有 bug**。`get` 和 `delete` 是两步：

1. 机器 A 检查 `get` 返回自己的 token ✓
2. 就在 A 准备 `delete` 的这一瞬间，A 的锁过期了
3. 机器 B 抢到锁
4. 机器 A 执行 `delete` → **又把 B 的锁删了**

问题在于 "检查 + 删除" 不是原子的。需要让它变成原子的。

### 版本 3：Lua 脚本救场

这就要引出下一节的主角。

---

## 6. Lua 脚本：为什么绕不开

Redis 支持一次执行一段小脚本，脚本执行期间**Redis 完全不干别的**（Redis 是单线程的）。这就是让"多步操作变成原子操作"的秘密武器。

```python
FENCED_DEL = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""

# 用法
r.eval(FENCED_DEL, 1, "lock:job:123", token)
```

参数说明：

- `FENCED_DEL` → Lua 脚本字符串
- `1` → 后面的参数里有 1 个是 key
- `"lock:job:123"` → KEYS[1]
- `token` → ARGV[1]

**读起来是**："检查这把锁的持有者是不是我，是的话删掉，不是就啥都不干"。整段代码作为一个原子操作在 Redis 里执行，中间不会被打断。

### 完整的健壮分布式锁

现在把上面的都整合起来：

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

    def acquire(self):
        return bool(self.r.set(self.key, self.token, nx=True, px=self.ttl_ms))

    def release(self):
        return bool(self.r.eval(self.RELEASE_LUA, 1, self.key, self.token))

    def renew(self):
        return bool(self.r.eval(self.RENEW_LUA, 1, self.key, self.token, self.ttl_ms))
```

用起来：

```python
lock = RedisLock(r, "lock:job:123", ttl_ms=30_000)
if lock.acquire():
    try:
        process_job()
    finally:
        lock.release()
```

### 但是——如果任务真的要跑 30 分钟怎么办？

你不能把 TTL 设成 30 分钟——进程崩了要等 30 分钟才自愈，太慢。
也不能把 TTL 设成 30 秒——任务还没跑完锁就过期了。

**答案：自动续期**。设短 TTL，但业务代码一直在跑的时候，后台每隔一段时间偷偷续一次期。

```python
import asyncio
from contextlib import asynccontextmanager

@asynccontextmanager
async def with_renewer(lock):
    async def renewer():
        while True:
            # 每 1/3 TTL 续一次（留出容错空间）
            await asyncio.sleep(lock.ttl_ms / 3 / 1000)
            if not lock.renew():
                return   # 续期失败说明锁已经被别人抢走
    task = asyncio.create_task(renewer())
    try:
        yield
    finally:
        task.cancel()

# 使用
if lock.acquire():
    try:
        async with with_renewer(lock):
            await do_long_work()   # 30 分钟的活也没问题
    finally:
        lock.release()
```

**为什么是 1/3 而不是 1/2**？留出两次续期失败的容错。如果 1/2 就续，一次失败就危险了。1/3 允许连续失败两次仍安全。

---

## 7. 优先级队列：ZSET 的实战

回到主线。我们要做的是**任务队列**。10 台机器，谁快谁抢单，VIP 单先做。

### 第一版：直接用 ZSET

**生产者**（把任务塞进队列）：

```python
def enqueue(task_id, priority=0):
    # 分数越小越靠前，所以把 priority 取负
    score = -priority
    r.zadd("todo", {task_id: score}, nx=True)
    #                                ↑ 幂等：反复塞不会重复入队
```

**消费者**（把任务取出来干）：

```python
def consume():
    result = r.zpopmin("todo", count=1)
    if not result:
        return None
    task_id, score = result[0]
    return task_id
```

**问题**：任务被 pop 出来之后就从队列消失了。如果消费者拿走后崩了，**这个任务永远丢了**。

我们需要一种"取出来但不真消失"的机制。

### 第二版：多维度优先级

先讲优先级怎么做得更细。真实场景里"急"不是一个维度：

- **运维手动插队**：VIP 客户，必须最先
- **快做完的先做完**：一个任务做了 5/6 了，另一个刚开始，前者优先（避免半成品堆积）
- **太久没动的**：可怜的老单子，救救它

一个数字怎么装下三个维度？**位段编码**：

```python
def compute_score(priority, completed_count, urgency_secs):
    return -(priority * 10**10
             + completed_count * 10**8
             + urgency_secs)
```

想象一个 12 位的数字：`PP CC CCCC CCUU`。前 2 位是 priority，中间 4 位是 completed_count，后面是 urgency。**高位差 1，压过低位所有变化**。

举例：

| 任务 | priority | completed | urgency | 分数 |
|---|---|---|---|---|
| VIP 单 | 1 | 0 | 0 | -1e10 |
| 老半成品 | 0 | 5 | 3600 | -5e8-3600 ≈ -5e8 |
| 老单 | 0 | 0 | 7200 | -7200 |

严格分层：VIP < 老半成品 < 老单，就是我们想要的顺序。

**这个技巧非常常用**。你想让 ZSET 按几个维度排序，就用位段编码把它们塞进一个 score 里。

---

## 8. 隐身机制：Visibility Timeout

回到"任务取走后不能真丢"的问题。

### 核心思路

**不要真 pop 出来。而是把它的 score 改成"未来"**，让它临时看不到。

想象超市寄存柜：

- 你存东西 → 柜子上贴张纸条 "占用中，1 小时后自动解锁"
- 你正常拿走 → 撕掉纸条，柜子空了
- 你忘了 → 1 小时后柜子自动解锁，管理员可以清理

Redis 里同样的做法：

```python
import time

def fetch_one():
    now_ms = int(time.time() * 1000)

    # 只看分数 ≤ now_ms 的，也就是"现在可见"的
    candidates = r.zrangebyscore("todo", "-inf", now_ms, start=0, num=10)

    for task_id in candidates:
        # 尝试把它的 score 改成 5 分钟后（推向未来 = 临时隐身）
        # xx=True 关键：只在成员还在时更新
        # ch=True 让 Redis 告诉我"这次调用真的改动了吗"
        changed = r.zadd(
            "todo",
            {task_id: now_ms + 5 * 60 * 1000},
            xx=True, ch=True,
        )
        if changed:
            return task_id   # 抢到了！现在这个任务对别人隐身 5 分钟
    return None
```

**为什么这个方法好**：

- **不打架**：10 个 worker 都 zrangebyscore，可能拿到同一批候选。但 `ZADD XX CH` 是原子的，只有一个人真的改动 → 其他人 `changed=0` 跳过
- **崩溃自愈**：worker 拿走任务后死了 → 5 分钟后 score 回到"可见"区间 → 别人自然接手
- **不需要额外的"重试机制"**：Redis 帮你处理了

处理完就正常删除：

```python
def complete(task_id):
    r.zrem("todo", task_id)
```

### 完整的一次"抢-做-删"

```python
def process_one():
    task_id = fetch_one()   # 抢到，任务已隐身
    if not task_id:
        return

    # 再上一把独占锁（双保险）
    lock = RedisLock(r, f"lease:{task_id}", ttl_ms=5 * 60 * 1000)
    if not lock.acquire():
        return   # 极小概率抢不到，跳过

    try:
        do_work(task_id)          # 干活
        r.zrem("todo", task_id)    # 完成，正式出队
    finally:
        lock.release()
```

**"为什么要双保险？两层锁不冗余吗？"**

- **ZSET 隐身**：让别人**取不到**这个任务
- **lease 锁**：万一有人取到了（比如 ZSET 有短暂并发窗口），保证只有一个人**真的执行**

分层是"深度防御"的思想。**一个防线可能有洞，两个防线的洞不太可能对齐**。

---

## 9. 全局并发信号量：10 台机器加起来最多同时干 6 个

现在场景更复杂了。假设 whisper 语音识别是个重活，我们希望**全公司加起来同时最多 6 个 whisper 任务在跑**，多了下游 GPU 服务扛不住。

10 台机器，每台想跑就跑，怎么保证合计不超 6？

### 反例：简单计数器

```python
n = r.incr("sem:whisper")
if n <= 6:
    do_work()
    r.decr("sem:whisper")
```

**这个想法有致命 bug**：worker 崩溃 → 没执行 decr → 计数器永远漏一个 → **最终卡死在 6 无法释放**。

### 正确姿势：HASH + Lua

思路：每个正在干活的人往一个 HASH 里写自己的名字。想干活先看 HASH 里有几个名字，少于 6 就把自己加进去。

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
        got = r.eval(
            SEM_ACQUIRE_LUA, 1,
            f"sem:{field}", limit, token, int(time.time()),
        )
        if got:
            return token
        time.sleep(0.2)   # 抢不到就退避

def release_slot(field, token):
    r.hdel(f"sem:{field}", token)
```

**为什么用 HASH 而不是计数器**？崩溃时 HASH 里会留下一个"僵尸 token"，但你可以用 leader 定期扫描"HASH 里那些 token 的时间戳太老 = 崩溃了 = 清掉"。**HASH 可回溯、可清理，计数器不行**。

**为什么必须用 Lua**？"读 HLEN + 判断 + HSET" 是三步。如果不是原子的，可能出现：

- 机器 A 读到 HLEN = 5，判断 5 < 6，准备 HSET
- 机器 B 读到 HLEN = 5，判断 5 < 6，准备 HSET
- 两人都 HSET → 最终 HLEN = 7，**超发了**

Lua 让三步变一步，物理上不可能超发。

---

## 10. QPS 时间槽：保护下游

**并发数和 QPS 是两回事，一定要区分清楚**。

- **并发数** = 同时有几个人在干活
- **QPS** = 每秒发出去多少个下游请求

假设 6 个 whisper worker 各自并发在干活，但每个 worker 内部循环里每秒调 5 次下游 → 合计 30 QPS，可能压爆下游。

下游服务说"我最多每秒接 10 个请求"——你需要**独立的一把闸**来限住 QPS。

### 方案：把时间切成毫秒级槽

一个 QPS = 10 意味着"每 100ms 只放 1 个请求"。所以：

```python
import math

def acquire_qps_slot(downstream, qps):
    slot_width_ms = math.ceil(1000 / qps)   # qps=10 → 100 ms/槽

    while True:
        now_ms = int(time.time() * 1000)
        slot = now_ms // slot_width_ms       # 当前处于第几个槽

        key = f"rl:{downstream}:{slot}"
        # NX：只有第一个抢到这个槽的能通过
        got = r.set(key, "1", nx=True, px=slot_width_ms * 2)
        if got:
            return   # 放行

        # 没抢到 → 睡到下一槽
        sleep_ms = slot_width_ms - (now_ms % slot_width_ms)
        time.sleep(sleep_ms / 1000)
```

**每个下游一组槽**：`rl:caption:12345`、`rl:mediaProcess:12345`……互不干扰。

**跨实例强制**：10 台机器都在 `SET NX`，同一个 slot 只有一台能抢到。**开多少实例都不会破 QPS 红线**。

**跟并发限流的关系**：

- 并发限流（HASH 信号量）：**"同时"** 最多几个人在跑
- QPS 时间槽：**"每秒"** 最多几次调用

有时候你需要两个都上。比如"最多 6 个 worker 同时跑，但下游合计 QPS 不超 10"。

---

## 11. 一个能跑的最小调度器

把前面所有东西拼起来。**这 100 行代码你可以直接复制去玩**：

```python
import redis
import uuid
import time
import math

r = redis.Redis(decode_responses=True)

FENCED_DEL_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end
"""

# ---------- 生产者 ----------
def enqueue(field, task_id, priority=0):
    """幂等入队，priority 越大越靠前"""
    score = -priority * 10**10
    r.zadd(f"ready:{field}", {task_id: score}, nx=True)

# ---------- 消费侧 ----------
def fetch_one(field, lease_ttl_ms=5 * 60 * 1000):
    """抢一个可见任务，同时把它推向未来（隐身）"""
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
    return bool(r.eval(FENCED_DEL_LUA, 1, key, token))

def worker_loop(field):
    while True:
        task_id = fetch_one(field)
        if not task_id:
            time.sleep(1)
            continue

        token = uuid.uuid4().hex
        lease_key = f"lease:{field}:{task_id}"
        if not try_lock(lease_key, token, ttl_ms=5 * 60 * 1000):
            continue   # 别人抢到了，跳过

        try:
            print(f"[{field}] processing {task_id}")
            time.sleep(2)   # 模拟干活
            r.zrem(f"ready:{field}", task_id)   # 完成，出队
        finally:
            unlock(lease_key, token)

# ---------- 试一下 ----------
if __name__ == "__main__":
    enqueue("whisper", "task_A", priority=1)
    enqueue("whisper", "task_B", priority=10)   # 更急
    enqueue("whisper", "task_C", priority=0)

    worker_loop("whisper")
```

运行你会看到：`task_B` 先处理，然后 `task_A`，最后 `task_C`。

**可以做的实验**：
- 开两个终端跑 `worker_loop("whisper")`，看两个 worker 怎么分单
- 在 worker 干活到一半时 Ctrl+C 杀掉，5 分钟后看另一个终端会不会接手
- 用 `redis-cli` 敲 `ZRANGE ready:whisper 0 -1 WITHSCORES` 看队列状态变化

---

## 12. 对着看线上代码：leader.py 每一段在干嘛

现在把前面所有概念对应到我们真实生产的 fragment 生产调度器。

### 12.1 整体架构

```
┌───────────────────────────────┐
│  Mongo (materials_core)        │  ← 权威数据
│  记录每个 fragment 的 target 状态 │
└──────┬──────────────────────▲──┘
       │ ① 扫库                  │ ④ 写回结果
       ▼                        │
┌───────────────────┐            │
│ leader (每工序 1 个) │            │
│ 每 5s 扫一次库      │            │
└──────┬────────────┘            │
       │ ② 入队                   │
       ▼                        │
┌───────────────────┐            │
│ Redis ready ZSET   │            │
│ 按优先级排序        │            │
└──────┬────────────┘            │
       │ ③ 消费                   │
       ▼                        │
┌───────────────────┐            │
│ worker (多实例)    │────────────┘
│ 抢锁 → 干活 → 出队 │
└───────────────────┘
```

**核心哲学**：**Mongo 是权威真值，Redis 是低延迟提示板**。任何时候两边不一致，以 Mongo 为准（因为 leader 每 5 秒会重扫一次库自愈）。

### 12.2 leader 选主：只让一个进程扫库

多实例部署时，不能让每台机器都在扫 Mongo（浪费）。用 Redis 分布式锁选出唯一的 leader：

```python
async def leader_loop(field, *, stop_event):
    token = uuid.uuid4().hex
    while not stop_event.is_set():
        # 抢主锁（跟你在 §5 学的一模一样）
        got = await r.set(f"scan:leader:{field}", token, nx=True, px=30_000)
        if not got:
            # 别的实例是 leader，退避后再试
            await asyncio.sleep(15)
            continue

        try:
            # 抢到了！进入 leader 会话
            while await still_leader(field, token):
                await scan_and_enqueue(field)
                await asyncio.sleep(5)
        finally:
            # 释放主锁（记得用 fenced_del，见 §6）
            await fenced_del(f"scan:leader:{field}", token)
```

**每工序一个 leader** 意味着有 `scan:leader:whisper`、`scan:leader:media`、`scan:leader:caption` 好几把独立的主锁。一个工序的 leader 卡住不影响其他工序。

**leader 崩溃怎么办**？TTL 30 秒到期后锁自动消失，别的机器抢到成为新 leader。**这就是 §4 讲的自愈的实际应用**。

### 12.3 scan_and_enqueue：扫库入队

```python
async def scan_and_enqueue(field):
    # 从 Mongo 里查还没做完的 fragment
    async for doc in mongo.find(scan_filter(field)):
        score = compute_score(
            priority=doc["reconcile_priority"],
            completed_count=doc["completed_count"],
            urgency_secs=int(time.time()) - doc["last_touch"],
        )
        # 幂等入队（见 §7）
        await r.zadd(f"ready:{field}", {doc["_id"]: score}, nx=True)
```

- **`compute_score`**：§7 讲的位段编码，把三维打包成一个 score
- **`ZADD ... NX`**：反复扫库同一个 fragment 不会被重复投递
- **动态优先级**：如果 Mongo 里改了 `reconcile_priority`，下一轮扫描算出新分数（但因为 NX，只对新加进来的生效——对已经在队列里的，需要用 `ZADD ... GT` 或者主动 `ZREM` 后重投）

### 12.4 定期打扫卫生

leader 除了扫库入队，还负责几件低频维护工作。这些工作在同一个 leader 会话里做，因为**只需要一个进程做**：

```python
last_zombie_sweep = 0
last_orphan_rescue = 0

while await still_leader(field, token):
    # 高频：每 5 秒
    await scan_and_enqueue(field)

    # 低频：每 90 秒
    if time.time() - last_zombie_sweep > 90:
        await sweep_zombies(field)          # 清崩溃 worker 留下的 running 状态
        await sweep_exhausted(field)        # 超过重试上限的打成永久失败
        await rescue_visibility_orphans(field)   # 隐身后没人接手的
        last_zombie_sweep = time.time()

    await asyncio.sleep(5)
```

**Visibility 孤儿救援** 是个很有意思的场景，我详细讲讲：

正常流程：
- worker 抢到任务 → 把 ZSET score 推到 `now + 5min`（隐身）
- 干完 → `ZREM` 删除
- 崩溃 → 5 分钟后 score 到期回到"可见"区间

**异常场景**：worker 抢到任务后，把 score 推到了 `now + 5min`，然后进程被 kill 且**恢复很慢**（比如新实例还没起来）。这个任务就一直在 ZSET 里挂着，没人处理。

leader 定期扫一下：**"哪些成员的 score 在 (0, now] 区间？也就是本该可见但一直没被 pop 的？"** 这些就是孤儿：

```python
async def rescue_visibility_orphans(field):
    now_ms = int(time.time() * 1000)
    orphans = await r.zrangebyscore(f"ready:{field}", 0, now_ms)
    for fid in orphans:
        # 重新查一次 Mongo 算个真实分数
        doc = await mongo.find_one({"_id": fid})
        new_score = compute_score(doc)
        # XX：只更新（不新增，因为它本来就在）
        await r.zadd(f"ready:{field}", {fid: new_score}, xx=True)
```

这样孤儿从"隐身刚过期"回到"最高优先级"，很快被接手。

---

## 13. 对着看线上代码：worker.py 每一段在干嘛

worker 负责从 ZSET 消费任务、抢独占锁、干活、清理。核心函数叫 `process_one`。

### 13.1 五条正确性规则

我从生产代码里翻译一版清晰的：

```python
async def process_one(field, fragment_id, config):
    token = uuid.uuid4().hex
    lease_key = f"lease:{field}:{fragment_id}"
    lease_ttl_ms = config.lease_ttl_s * 1000

    # === 规则 1：抢独占 lease ===
    # SETNX 抢到才继续，抢不到说明别人在处理
    if not await r.set(lease_key, token, nx=True, px=lease_ttl_ms):
        return

    try:
        # === 规则 2：Visibility 预占 ===
        # 把 ZSET 里这条推到 5 分钟后，让别的 worker 看不到
        now_ms = int(time.time() * 1000)
        await r.zadd(
            f"ready:{field}",
            {str(fragment_id): float(now_ms + lease_ttl_ms)},
            xx=True,
        )

        # === 规则 3：二次校验 ===
        # 从 Mongo 重读，确认这条真的还需要做
        # (可能在我们抢锁前已经被别的路径做完了)
        doc = await mongo.find_one({"_id": fragment_id})
        if doc is None or not should_still_build(field, doc):
            # 不用做了，出队走人
            await fenced_zrem(field, fragment_id, lease_key, token)
            return

        # === 规则 4：干活时后台续锁 ===
        # 用 async with 起后台续锁协程，覆盖整个干活过程
        async with lease_renewer(lease_key, token, lease_ttl_ms):
            result = await fetch_and_persist(
                FragmentFetchCommand(id=fragment_id, targets=[field])
            )

        # === 规则 5：Fenced 出队 ===
        # 只有锁还是自己的才 zrem，防止误删别人的成果
        await fenced_zrem(field, fragment_id, lease_key, token)

    finally:
        await fenced_del_lease(lease_key, token)
```

### 13.2 每条规则对应我们讲过的哪个概念

| 规则 | Redis 操作 | 对应本文哪节 |
|---|---|---|
| 1. 抢独占 lease | `SET key token NX PX` | §5 分布式锁 |
| 2. Visibility 预占 | `ZADD ready XX` 推 future | §8 隐身机制 |
| 3. 二次校验 | 读 Mongo 判断 | 幂等闸，防重复劳动 |
| 4. 后台续锁 | Lua `GET+PEXPIRE` | §6 Lua + 自动续期 |
| 5. Fenced 出队 | Lua `GET+ZREM` | §6 Lua 原子性 |

**为什么规则 3 特别重要？** 想象这个场景：

- 用户在网页上点了"手动重试"按钮 → 服务直接调 `fetch_and_persist` → 任务完成了
- 与此同时 leader 的 ZSET 里还挂着这条任务
- worker 从 ZSET 拿到，抢锁成功，如果**直接开始干活**，就会重复处理

规则 3 的二次校验就是防这种情况：**"我抢到锁只代表我有权干，还得确认这活儿真的还需要干"**。

### 13.3 lease_renewer：后台续锁

对应到 §6 讲的自动续期，具体这样写：

```python
from contextlib import asynccontextmanager

@asynccontextmanager
async def lease_renewer(key, token, ttl_ms):
    async def renew_loop():
        while True:
            await asyncio.sleep(ttl_ms / 3 / 1000)   # 每 1/3 TTL 续一次
            renewed = await r.eval(RENEW_LUA, 1, key, token, ttl_ms)
            if not renewed:
                return   # 锁被别人抢走了，或者已过期

    task = asyncio.create_task(renew_loop())
    try:
        yield
    finally:
        task.cancel()

# 用起来
async with lease_renewer(lease_key, token, lease_ttl_ms):
    await do_actual_work()   # 任意长时间
```

用 `async with` 的原因：**自动生命周期管理**。业务代码正常退出、异常退出、被 cancel，续锁协程都会被正确清理。**这是 Python 里管理"后台伴生任务"的标准姿势**。

### 13.4 fenced_zrem：为什么不能直接 zrem

**反例场景**：

1. worker A 抢到 lease，开始干活
2. A 网络卡了 60 秒，lease 过期
3. worker B 抢到同一个 lease，开始干活
4. A 网络恢复，干完了，跑到 `r.zrem` 把任务从队列删除
5. **但 B 还没干完！** ZREM 之后如果 B 失败了，这个任务再也不会被重试

修复：删除前用 Lua 校验锁还是我的：

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
        2,                                    # 2 个 key
        lease_key, f"ready:{field}",         # KEYS[1], KEYS[2]
        token, str(fragment_id),             # ARGV[1], ARGV[2]
    )
```

**"任何决定性写操作前，都要校验锁还是自己的"** —— 这是分布式系统的重要原则。释放锁、写库标 success、从队列删除，都要 fencing。

### 13.5 三层并发管理

上面的 `process_one` 处理单条。真实 worker 是这样跑的：

```python
async def worker_loop(field):
    while True:
        # 计算这一轮该有多少个 consumer
        limit = resolve_concurrency(field)   # 后面讲

        queue = asyncio.Queue(maxsize=limit)

        async def producer():
            """从 ZSET 抓一批 → 隐身 → 塞进本地 Queue"""
            while True:
                now_ms = int(time.time() * 1000)
                # 从 2048 个候选里随机采样，减少多实例哄抢
                candidates = await r.zrangebyscore(
                    f"ready:{field}", "-inf", now_ms,
                    start=0, num=2048,
                )
                picked = random.sample(candidates, min(limit, len(candidates)))
                for fid in picked:
                    changed = await r.zadd(
                        f"ready:{field}",
                        {fid: now_ms + lease_ttl_ms},
                        xx=True, ch=True,
                    )
                    if changed:
                        await queue.put(fid)   # 塞进本地队列，等 consumer

        async def consumer():
            """从本地 Queue 取，抢全局信号量，处理"""
            while True:
                fid = await queue.get()
                slot = await acquire_slot(field, limit)   # §9 全局并发
                if not slot:
                    continue
                try:
                    await process_one(field, fid, config)
                finally:
                    await release_slot(field, slot)

        # 起 1 个 producer + N 个 consumer
        await asyncio.gather(producer(), *[consumer() for _ in range(limit)])
```

**这里有三层并发控制**：

| 层 | 干嘛 | 对应本文哪节 |
|---|---|---|
| Producer 采样 | 从 2048 候选里随机 N 个，避免多实例同时抢同一批 | §8 隐身机制的扩展 |
| 本地 Queue | 单实例内部生产-消费管道，控制单机并发 | Python asyncio 基础 |
| 全局信号量 | 跨实例合计并发不超 limit | §9 分布式信号量 |

**为什么随机采样 2048**？

想象 10 台机器，每台的 producer 都干 `ZRANGEBYSCORE ... LIMIT 0 8`（要 8 个）。10 个人拿到的是**完全相同的前 8 个**。然后 10 个人 `ZADD XX` 抢，只有 1 个人成功 → 剩下 9 个人白跑一趟。

改成"每人拿 2048 个候选，从里面随机抽 8 个"，10 个人各抽各的，**天然错开**。多数情况下每个任务只有一两个人抢，抢锁成功率大幅提升。

### 13.6 QPS 保护在哪一层

worker 干活时最终会调下游服务（转码、LLM、语音识别）。QPS 红线在调用点保护：

```python
# 类似于 ops/media_fetch.py 里
async def call_media_process(...):
    await acquire_qps_slot("mediaProcess", qps=50)   # §10 时间槽
    return await rpc_call(...)
```

**并发和 QPS 是两把独立的闸**：

- 6 个 worker 并发跑，每个每秒调 3 次下游 → 18 QPS
- 6 个 worker 并发跑，每个每秒调 10 次下游 → 60 QPS

**只加信号量不加时间槽**，worker 内部循环调用可能瞬间打爆下游。所以生产环境两个都要有。

---

## 14. 你会踩的坑

**坑 1：忘了 `decode_responses`**

```python
r = redis.Redis()
r.get("k")   # 返回 b'v' bytes，不是 str
# 修复
r = redis.Redis(decode_responses=True)
```

**坑 2：ZADD 忘了 NX/XX**

```python
# ❌ 每次扫库都覆盖 score，把已在队列里的优先级刷掉
r.zadd("todo", {"task1": new_score})

# ✅ 已存在的不动
r.zadd("todo", {"task1": new_score}, nx=True)
```

**坑 3：分布式锁不带 TTL**

```python
# ❌ 崩溃就死锁
r.set("lock", "me", nx=True)

# ✅ 一定要有过期
r.set("lock", "me", nx=True, ex=60)
```

**坑 4：删锁不校验 token**

一定用 Lua fenced_del。

**坑 5：用 KEYS 扫全库**

`KEYS *` 会阻塞整个 Redis。**永远用 SCAN**：

```python
cursor = 0
while True:
    cursor, keys = r.scan(cursor=cursor, match="lock:*", count=100)
    for k in keys:
        process(k)
    if cursor == 0:
        break
```

**坑 6：内存不释放**

Redis 所有 key 都在内存里。忘设 TTL 或用无界数据结构（List/ZSET 不断塞）会 OOM。

**建议**：
- 任何有生命周期的 key 都设 TTL
- 队列 ZSET 定期清理已完成的
- 用 `INFO memory` 监控

### 调试神器：redis-cli

```bash
redis-cli
> KEYS *                       # 列所有 key（生产慎用！）
> SCAN 0 MATCH "lease:*"       # 分批扫
> TYPE mykey                   # 看类型
> TTL mykey                    # 剩余过期时间
> ZRANGE ready:whisper 0 -1 WITHSCORES   # 看队列内容和分数
> HGETALL sem:whisper          # 看信号量占用
> INFO memory                  # 内存使用
> MONITOR                      # 实时看所有命令（生产不能用！）
```

### API 速查表

| 目的 | 命令 | 章节 |
|---|---|---|
| 抢分布式锁 | `SET key val NX PX 60000` | §5 |
| 释放锁 | Lua `GET + DEL`（校验 token） | §6 |
| 续锁 | Lua `GET + PEXPIRE` | §6 |
| 幂等入队 | `ZADD key {member: score} NX` | §7 |
| Visibility 预占 | `ZADD key {member: future_ts} XX CH` | §8 |
| 取到期任务 | `ZRANGEBYSCORE key -inf now_ms LIMIT 0 N` | §8 |
| 优先级弹出 | `ZPOPMIN key` | §7 |
| 计数器 | `INCR / INCRBY / DECR` | §3.1 |
| 缓存带 TTL | `SET key val EX 3600` | §4 |
| 简单队列 | `RPUSH` + `BLPOP` | §3.4 |
| 全局并发 | Lua `HLEN + HSET` | §9 |
| QPS 红线 | `SET rl:xxx:slot NX PX` | §10 |

---

## 15. 一句话总结

看到最后你应该体会到：**Redis 提供的能力其实不多——原子操作、TTL、几种数据结构**。但把它们组合起来能搭出让人惊讶的复杂系统。

我用四句话概括这整套东西：

> **数据的家在 Mongo，任务的告示板在 Redis。**
>
> **leader 一个人扫库贴告示，worker 大家抢告示干活。**
>
> **并发数靠信号量拦住，QPS 靠时间槽拦住，正确性靠版本号 CAS，崩溃靠锁自动过期。**
>
> **抓任务先隐身，干活时续锁，写结果前校验，删任务前 fencing。**

**你现在的行动**：

1. 打开 `redis-cli`，把 §3 的每种数据结构手动敲一遍
2. 复制 §11 的 100 行代码跑起来，开两个终端观察行为
3. 回头看你手头的项目，找一处用 Redis 的地方，把它对应到本教程的某一节
4. 有问题？回来看对应的章节

多数看起来复杂的调度系统，拆开都是这些基础模式的组合。**理解了每个 API 在解决什么问题，你就能看懂任何一份线上代码**。
