---
title: "Python 并发编程 Cookbook：从编程小白到能设计并发程序"
date: 2026-06-23 09:00:00 +0800
description: "一份从线程、进程、线程池到 asyncio、网络编程和并发工程化的 Python 并发学习笔记。"
tags: [Python, Concurrency, Threading, Asyncio]
permalink: /2026/06/23/python-concurrency-cookbook/
---

> 这份教程写给“会一点 Python 基础，但还没真正写过并发程序”的同学。你不需要一开始就懂线程、锁、协程、GIL。我们会先从生活和业务场景理解并发到底有什么用，再一点点写代码，最后完成几个紧贴教程内容的 lab。

> 学习目标：读完这份教程并完成实验后，你应该能判断一个任务该用线程、进程、线程池、进程池还是 `asyncio`；能写出带有超时、限流、背压、异常传播和优雅关闭的并发程序；能独立实现一个简化版线程池和协程池。

> 适用版本：示例默认兼容 Python 3.11+。Python 3.13+ 的新能力会在文中单独标注。

## 目录

1. 并发有什么用：先从问题出发
2. 同步基准：先写最笨但最清楚的版本
3. threading 从 0 到 1：创建、等待、传参、通信，含 Lab 1 和 Lab 2
4. 线程池：日常并发的第一选择，含 Big Lab
5. 进程模型：CPU 密集任务和多核并行
6. asyncio：事件循环和协作式调度，含 Lab 3 和 Bonus Lab
7. 网络编程：socket、HTTP、协议和并发服务端，含 Lab 4
8. 并发工程化：限流、背压、超时、取消、关闭
9. 并发选型速查表
10. 常见错误清单
11. 继续练习路线
12. Python 常用数据结构与 collections 速查

## 学习方式：每学一个概念，就做一个 lab

这份教程不是“先讲一堆概念，最后做题”。它更像一条升级路线：

| 学到的知识 | 马上检验的 lab | 你要获得的能力 |
| --- | --- | --- |
| `Thread.start()`、`join()`、共享状态 | Lab 1 的线程启动部分 | 能真的启动多个线程，并等待它们结束 |
| `Condition` 和共享变量 `turn` | Lab 1：顺序打印 ABC | 能让多个线程按规则协作，而不是靠运气调度 |
| `Barrier` | 章节内小练习：统一开跑 | 能让固定数量线程在同一阶段集合后继续 |
| `Condition`、有界队列、`task_done()`、`join()`、sentinel | Lab 2：手写 BlockingQueue | 能理解生产者消费者底层模型 |
| `asyncio.Queue`、async worker、`Semaphore`、timeout | Lab 3：async 流水线 | 能把线程队列模型迁移到 async 世界 |
| `Future`、worker 循环、shutdown | Big Lab：线程池 | 能理解标准库线程池背后的设计 |
| socket、TCP 消息边界、线程池服务端 | Lab 4：并发 TCP Echo 服务 | 能写出最小可用的网络服务端和客户端 |
| async `Future`、协程 worker、async shutdown | Bonus Lab：协程池 | 能设计自己的 async worker pool |

建议你阅读时不要跳过 lab。并发最怕“看懂了，但手一写就乱”。每个 lab 都是为了让你把刚学的模型敲进手里。

## 1. 并发有什么用：先从问题出发

### 1.1 小白先记住一句话

并发的本质是：**让程序在等待某件事的时候，不要傻站着，可以先去推进别的任务。**

比如你写了一个程序，要下载 100 个网页。最朴素的写法是：

1. 下载第 1 个网页，等它返回。
2. 下载第 2 个网页，等它返回。
3. 下载第 3 个网页，等它返回。
4. 一直重复到第 100 个。

这当然能跑，但很浪费时间。因为下载网页时，大部分时间不是 CPU 在计算，而是在等网络返回。既然第 1 个网页正在等网络，程序完全可以同时发起第 2 个、第 3 个、第 4 个请求。

并发就是为了解决这类问题。

### 1.2 并发在真实开发里有什么用

常见用途：

- **批量请求接口**：一次性查 100 个用户资料、100 个商品价格、100 条订单状态。
- **爬虫和数据采集**：同时抓多个页面，避免一个页面慢就拖住整个程序。
- **批量处理文件**：同时读取、压缩、上传多个文件。
- **后台任务系统**：主程序接收任务，worker 在后台慢慢处理。
- **Web 服务**：很多用户同时访问时，服务端需要同时处理多个请求。
- **GUI/命令行工具**：下载或计算时，不要让界面卡死。
- **数据流水线**：一边读取数据，一边清洗，一边写入数据库。

但并发不是万能药。它主要解决两类问题：

- **提高吞吐**：同一段时间内处理更多任务。
- **改善等待体验**：一个任务在等待时，别的任务还能继续推进。

如果你的程序只有一个很重的纯 Python 计算循环，并发不一定能帮你，甚至可能更慢。这个我们后面讲 GIL 和进程池时会解释。

### 1.3 并发和并行不是一回事

**并发 concurrency**：多个任务在一段时间内交替推进。重点是“同时管理多个进行中的任务”。

**并行 parallelism**：多个任务在同一时刻真的同时运行。重点是“同时使用多个执行资源”。

例子：

- 一个人煮饭时切菜、烧水、看锅，这是并发。
- 三个人分别切菜、烧水、看锅，这是并行。

很多 Python 初学者会把“开线程”理解成“一定更快”，这很危险。并发首先是组织任务的方式，其次才可能带来性能提升。

### 1.4 程序为什么会等待：阻塞、就绪、运行

初学并发时，可以先把程序想象成一个任务队伍。每个任务大概会在三种状态之间切换：

- **运行 running**：任务正在占用 CPU 执行 Python 代码。
- **等待 blocked/waiting**：任务在等网络、磁盘、锁、队列、sleep、数据库返回。
- **就绪 ready**：任务已经可以继续执行，只是在等调度器给它时间片。

同步程序的问题是：当唯一的执行路线进入等待状态时，整个程序就停住了。并发程序的思路是：当任务 A 在等网络时，让任务 B、C、D 先推进。

以下载 5 个网页为例：

```text
同步：请求1 -> 等 -> 请求2 -> 等 -> 请求3 -> 等 -> 请求4 -> 等 -> 请求5 -> 等
并发：请求1 -> 等
      请求2 -> 等
      请求3 -> 等
      请求4 -> 等
      请求5 -> 等
```

并发不是把等待变没了，而是把多个等待重叠起来。

### 1.5 延迟和吞吐：不要只问“快不快”

并发通常改善的是**吞吐**，不一定改善单个任务的**延迟**。

- **延迟 latency**：一个任务从开始到结束花多久。例如下载一个网页要 300ms。
- **吞吐 throughput**：一段时间内能完成多少任务。例如 1 秒能下载多少网页。

如果一个接口本身就要 300ms 才返回，并发不会神奇地让单个接口变成 30ms。但你可以同时发 10 个请求，让 10 个请求在差不多 300ms 内一起返回。单个请求的延迟没变，总吞吐提高了。

这也是为什么并发特别适合 IO 密集任务：任务大量时间都在等。

### 1.6 并发程序新增了哪些复杂度

并发有用，但它会带来新问题：

- **执行顺序不稳定**：线程谁先运行、谁后运行，不完全由代码行顺序决定。
- **共享状态容易出错**：两个线程同时改同一个变量，可能出现竞态。
- **异常容易被藏起来**：后台线程或 task 报错，如果没人收集结果，主流程可能不知道。
- **关闭更复杂**：程序退出时，worker 是否还在处理任务？队列里还有没有任务？资源有没有释放？
- **下游会被打爆**：并发数无限放大，数据库、HTTP 服务、文件句柄都可能扛不住。

所以一个成熟的并发程序不仅要“跑得起来”，还要有：限流、背压、超时、取消、异常传播、优雅关闭。

### 1.7 初学者最该先掌握哪条路线

建议学习顺序：

1. 先写同步代码，知道程序原本怎么跑。
2. 学 `threading`，理解线程、共享内存、锁、队列。
3. 学 `ThreadPoolExecutor`，掌握日常最常用的线程池写法。
4. 学 `ProcessPoolExecutor`，知道 CPU 密集任务为什么要用进程。
5. 学 `asyncio`，理解事件循环和 `await`。
6. 最后补工程化能力：超时、取消、限流、背压、优雅关闭。

本教程会严格按这个顺序走。

### 1.8 Python 中的四类主流并发工具

| 工具 | 核心模块 | 适合场景 | 主要风险 |
| --- | --- | --- | --- |
| 线程 | `threading` | 阻塞 IO、调用同步 SDK | 竞态、死锁、共享状态复杂 |
| 线程池 | `concurrent.futures.ThreadPoolExecutor` | 批量同步 IO 任务 | 提交过多任务、异常被忽略 |
| 进程池 | `concurrent.futures.ProcessPoolExecutor` | CPU 密集任务 | 序列化成本、启动成本、跨平台细节 |
| 协程 | `asyncio` | 高并发网络 IO | 阻塞事件循环、忘记 await、取消处理复杂 |

这张表先不用背。你现在只要先记住：

- **等 IO**，例如网络、文件、数据库：通常用线程或 `asyncio`。
- **算 CPU**，例如大量 Python 循环计算：通常用进程。
- **刚入门**：先从线程和队列学起，因为它们最直观。

### 1.9 三个必须先回答的问题

写并发代码之前，先回答：

1. **慢在哪里？** CPU、网络、磁盘、数据库、锁等待、第三方接口限流？
2. **任务之间怎么通信？** 共享变量、队列、Future、消息、文件、数据库？
3. **失败时怎么收场？** 超时、取消、重试、关闭、清理资源、保留部分结果？

没有这三个答案，并发代码通常会变成“平时能跑，出事难查”。

### 1.10 GIL 要怎么理解

传统 CPython 有 GIL，也就是全局解释器锁。它让同一个进程内的多个 Python 线程不能同时执行 Python 字节码。

这意味着：

- 线程对 IO 密集任务很有用，因为等待 IO 时线程可以让出执行机会。
- 线程对纯 Python CPU 密集任务通常帮助有限，因为多个线程不能真正同时执行 Python 字节码。
- 进程可以绕开这个限制，因为每个进程有自己的解释器和内存空间。

版本注记：Python 3.13 开始提供 free-threaded build 选项，可以在特定构建中关闭 GIL。但大多数实际环境仍会长期运行传统 CPython，所以学习并发时仍应掌握传统 GIL 模型。

## 2. 同步基准：先写最笨但最清楚的版本

并发优化第一课：**先写同步版本，测出基准。**

同步代码的好处是简单：一行执行完，再执行下一行。它通常不是最快的，但最容易理解、调试和验证正确性。

下面我们先模拟一个“查询用户信息”的任务。每次查询都要等 0.3 秒，代表网络、数据库或外部接口的等待时间。

```python
import time


def fetch_user(user_id: int) -> str:
    time.sleep(0.3)  # 模拟网络等待
    return f"user-{user_id}"


def main() -> None:
    start = time.perf_counter()
    results = [fetch_user(i) for i in range(5)]
    elapsed = time.perf_counter() - start
    print(results)
    print(f"elapsed={elapsed:.2f}s")


if __name__ == "__main__":
    main()
```

这个程序大约需要 1.5 秒，因为 5 次等待是串行发生的。

这段代码有什么特点？

- 优点：简单、稳定、容易看懂。
- 缺点：第 1 个用户在等待网络时，程序什么也不干，只能傻等。

如果慢在等待，线程池或 `asyncio` 往往能提升吞吐。如果慢在纯 Python 计算，进程池通常更合理。

### 2.1 第一次并发改造：先不用线程池，直接用 threading

接下来我们用最原始的 `threading.Thread` 改造它。先不要追求完美，目标只是理解“线程是怎么启动的”。

```python
import threading
import time


def fetch_user(user_id: int) -> None:
    time.sleep(0.3)
    print(f"user-{user_id}")


def main() -> None:
    start = time.perf_counter()

    threads = []
    for user_id in range(5):
        thread = threading.Thread(target=fetch_user, args=(user_id,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    elapsed = time.perf_counter() - start
    print(f"elapsed={elapsed:.2f}s")


if __name__ == "__main__":
    main()
```

这次耗时大约会接近 0.3 秒，而不是 1.5 秒。因为 5 个线程几乎同时开始等待，等待时间重叠了。

但这个版本还有问题：结果只是 `print` 出来了，没有收集成列表；如果线程里报错，主线程也不好处理。后面我们会一步步修。

## 3. threading 从 0 到 1：创建、等待、传参、通信

`threading` 是 Python 标准库里最基础的线程模块。你可以把一个线程理解成“同一个 Python 程序里的另一条执行路线”。

主线程在执行 `main()`，你也可以创建新的线程，让它们同时执行别的函数。

这一章要建立一张完整的 threading 地图：

| 概念 | 解决什么问题 | 初学优先级 |
| --- | --- | --- |
| `Thread` | 创建一条新的执行路线 | 必学 |
| `start()` / `join()` / `is_alive()` | 启动、等待、检查线程状态 | 必学 |
| `name` / `ident` / `native_id` | 让日志和调试能定位线程 | 常用 |
| `daemon` | 主程序退出时是否强行结束线程 | 知道但慎用 |
| 共享内存 | 线程之间天然能看到同一份对象 | 必学 |
| `Lock` | 保护共享状态，避免竞态 | 必学 |
| `RLock` | 同一线程需要重复拿同一把锁 | 知道用途 |
| `Semaphore` | 限制同时执行某段逻辑的线程数量 | 常用 |
| `Event` | 一个线程向一组线程发开始/停止信号 | 常用 |
| `Condition` | 围绕共享状态等待某个条件成立 | 进阶必学 |
| `Barrier` | 固定数量线程到齐后一起继续 | 知道用途 |
| `Queue` | 在线程之间安全传任务和结果 | 必学 |
| `threading.local()` | 每个线程各自保存一份变量 | 知道用途 |
| `Timer` | 延迟一段时间后执行函数 | 知道用途 |

学习时不要把它们当成一堆孤立 API。它们其实对应几类问题：

- **怎么启动线程**：`Thread`、`start()`、`join()`。
- **怎么保护共享状态**：`Lock`、`RLock`。
- **怎么限制并发数量**：`Semaphore`。
- **怎么通知别的线程**：`Event`。
- **怎么等待复杂条件**：`Condition`。
- **怎么传递任务**：`Queue`。
- **怎么调试和收尾**：线程名、异常处理、daemon、关闭信号。

### 3.1 一个线程对象到底包含什么

最小写法：

```python
import threading


def worker() -> None:
    print("hello from worker")


thread = threading.Thread(target=worker)
thread.start()
thread.join()
```

你要理解 3 个动作：

- `threading.Thread(target=worker)`：创建线程对象，但还没运行。
- `thread.start()`：启动线程，Python 会在新线程里调用 `worker()`。
- `thread.join()`：主线程等待这个线程结束。

如果不 `join()`，主线程可能先继续往下跑。很多初学者会看到输出顺序不固定，这就是并发的第一个信号：多个任务的执行顺序不再完全由代码行顺序决定。

#### Thread 的生命周期

一个 `Thread` 对象通常经历这些状态：

```text
创建 Thread 对象 -> start() 启动 -> 运行 target 函数 -> target 返回或抛异常 -> 线程结束 -> join() 回收等待
```

几个重要规则：

- 一个 `Thread` 对象只能 `start()` 一次，重复 `start()` 会报错。
- `join()` 可以调用多次，它只是等待线程结束。
- `is_alive()` 可以检查线程是否还活着。
- 线程结束后不能重新启动；要再跑一次，就创建新的 `Thread` 对象。

```python
import threading
import time


def worker() -> None:
    time.sleep(0.5)


thread = threading.Thread(target=worker, name="demo-worker")
print(thread.is_alive())  # False

thread.start()
print(thread.is_alive())  # True

thread.join()
print(thread.is_alive())  # False
```

#### 常用线程属性和调试函数

| API | 含义 |
| --- | --- |
| `thread.name` | 线程名，日志里很有用 |
| `thread.ident` | Python 层线程标识，线程启动后才有 |
| `thread.native_id` | 操作系统线程 id，Python 3.8+ 可用 |
| `threading.current_thread()` | 获取当前正在运行的线程对象 |
| `threading.main_thread()` | 获取主线程对象 |
| `threading.enumerate()` | 获取当前还活着的线程列表 |
| `threading.active_count()` | 当前活跃线程数量 |

调试并发问题时，先把线程名加到日志里，再用 `threading.enumerate()` 看有没有线程没退出，往往比盲猜有效。

### 3.2 给线程传参数

线程的 `target` 是函数，`args` 是位置参数，`kwargs` 是关键字参数。

```python
import threading
import time


def work(name: str) -> None:
    print(f"{name}: start")
    time.sleep(1)
    print(f"{name}: done")


threads = [threading.Thread(target=work, args=(f"t{i}",)) for i in range(3)]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
```

关键点：

- `Thread(...)` 只是创建线程对象。
- `start()` 才是真正启动。
- `join()` 等线程结束。
- 多线程共享同一个进程的内存，所以共享变量需要同步保护。

注意 `args=(f"t{i}",)` 后面的逗号。只有一个参数时，它必须是单元素 tuple。

#### 另一种写法：继承 Thread

你也会看到有人通过继承 `threading.Thread` 来写线程：

```python
import threading


class WorkerThread(threading.Thread):
    def __init__(self, worker_id: int) -> None:
        super().__init__(name=f"worker-{worker_id}")
        self.worker_id = worker_id

    def run(self) -> None:
        print(f"run worker {self.worker_id}")


thread = WorkerThread(1)
thread.start()
thread.join()
```

初学和大多数业务代码里，更推荐 `Thread(target=函数)`。它更简单，也更容易和线程池、测试代码配合。继承 `Thread` 通常用于你要封装一类长期运行的线程对象时。

### 3.3 线程的名字和日志

真实项目里，给线程起名字很有用，因为日志里能看出是哪一个 worker 出了问题。

```python
import logging
import threading


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(threadName)s %(message)s",
)


def worker(task_id: int) -> None:
    logging.info("start task %s", task_id)
    logging.info("finish task %s", task_id)


thread = threading.Thread(target=worker, args=(1,), name="user-fetcher-1")
thread.start()
thread.join()
```

初学时可以先用 `print`，但真正排查并发问题时，日志会比 print 更靠谱。

#### 线程里的异常会发生什么

如果 worker 线程里抛异常，主线程不会自动 `raise` 同一个异常。线程会结束，异常默认打印到 stderr。

```python
import threading


def bad_worker() -> None:
    raise RuntimeError("boom")


thread = threading.Thread(target=bad_worker, name="bad-worker")
thread.start()
thread.join()
print("main still running")
```

这也是为什么后面推荐用 `ThreadPoolExecutor`：它会把异常保存在 `Future` 里，调用 `future.result()` 时再抛给主线程。

如果你自己管理线程，至少要在 worker 里捕获异常并记录日志：

```python
import logging


def do_work() -> None:
    raise RuntimeError("boom")


def safe_worker() -> None:
    try:
        do_work()
    except Exception:
        logging.exception("worker failed")
```

更高级一点，可以自定义 `threading.excepthook` 统一处理线程未捕获异常。知道有这个能力即可，初学阶段优先用日志和线程池。

### 3.4 join 是什么：等待线程结束

`join()` 的意思是：当前线程等另一个线程执行完。

```python
import threading
import time


def slow() -> None:
    time.sleep(1)
    print("worker done")


thread = threading.Thread(target=slow)
thread.start()
print("main: waiting")
thread.join()
print("main: worker finished")
```

如果你需要最多等一段时间，可以传 timeout：

```python
thread.join(timeout=0.5)
if thread.is_alive():
    print("worker is still running")
```

注意：Python 线程不能被外部安全强杀。`join(timeout=...)` 只是主线程不再继续等，不是把 worker 杀掉。

### 3.5 daemon 线程：初学者先少用

`daemon=True` 的线程会在主程序退出时被强行结束。

```python
thread = threading.Thread(target=worker, daemon=True)
```

它适合某些后台监控任务，但不适合需要写文件、提交数据库、释放资源的任务。刚学并发时，优先使用非 daemon 线程，并设计清楚关闭流程。

### 3.6 线程之间怎么拿到结果

一个常见问题：线程函数没有返回值给主线程，怎么办？

最简单但不够安全的写法，是共享一个 list：

```python
import threading
import time


def fetch_user(user_id: int, results: list[str]) -> None:
    time.sleep(0.3)
    results.append(f"user-{user_id}")


results: list[str] = []
threads = [
    threading.Thread(target=fetch_user, args=(user_id, results))
    for user_id in range(5)
]

for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

print(results)
```

这个例子通常能跑，但它暴露了一个重要事实：**线程共享同一份内存。**

共享内存既方便，也危险。方便是因为所有线程都能看到 `results`；危险是因为多个线程可能同时修改它。简单 append 在 CPython 下通常不会把 list 结构弄坏，但你不能因此得出“共享状态都安全”的结论。只要逻辑涉及“先检查再修改”“多个变量保持一致”，就需要同步。

更稳的做法是让 worker 把结果放进 `Queue`，或者直接使用后面要讲的 `ThreadPoolExecutor`。手写线程时，共享 list 适合演示；工程代码里优先使用队列或 Future。

### 3.7 竞态条件：为什么一行加法也可能错

竞态条件 race condition 指的是：程序结果取决于多个线程抢执行顺序。你以为自己写的是确定逻辑，实际上线程切换顺序一变，结果就变了。

```python
import threading

count = 0


def unsafe_inc() -> None:
    global count
    for _ in range(100_000):
        count += 1


threads = [threading.Thread(target=unsafe_inc) for _ in range(4)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()

print(count)
```

`count += 1` 看起来是一行，但底层包含读取、计算、写回。线程可能在中途被切走，导致更新丢失。

修复：

```python
import threading

count = 0
lock = threading.Lock()


def safe_inc() -> None:
    global count
    for _ in range(100_000):
        with lock:
            count += 1
```

经验法则：锁保护的是“不变量”，不是某一行代码。你要知道哪些状态必须一起保持一致。

### 3.8 Lock、RLock、Semaphore：控制进入临界区的方式

这一节讲三种容易混的同步工具：

- `Lock`：同一时刻只允许一个线程进入。
- `RLock`：同一线程可以重复进入同一把锁。
- `Semaphore`：同一时刻允许 N 个线程进入。

#### Lock：最基础的互斥工具

锁的基本思想是：同一时刻只允许一个线程进入某段关键代码。

```python
import threading

balance = 0
lock = threading.Lock()


def deposit(amount: int) -> None:
    global balance
    with lock:
        old = balance
        new = old + amount
        balance = new
```

`with lock:` 进入时获取锁，离开时释放锁。即使中间抛异常，锁也会释放，所以优先使用 `with`，少手写 `acquire()` / `release()`。

不要把锁包得太大。锁内代码越多，并发程度越低，也越容易死锁。一般只把真正访问共享状态的部分放进锁里。

锁的常见坑：

- 忘记释放锁：优先用 `with lock:`。
- 锁粒度太大：所有线程排队，性能退化成串行。
- 多把锁顺序不一致：线程 A 拿锁 1 等锁 2，线程 B 拿锁 2 等锁 1，容易死锁。
- 以为有 GIL 就不需要锁：GIL 不保护你的业务不变量。

#### RLock：同一线程可重入的锁

普通 `Lock` 不能被同一个线程重复获取。下面这类代码会卡住：

```python
import threading


lock = threading.Lock()


def outer() -> None:
    with lock:
        inner()


def inner() -> None:
    with lock:
        print("inner")
```

`outer()` 已经拿了 `lock`，再调用 `inner()` 时又想拿同一把 `lock`，普通锁会等待自己释放自己，结果就卡死。

如果确实需要同一线程重复进入同一把锁，可以用 `RLock`：

```python
import threading


lock = threading.RLock()


def outer() -> None:
    with lock:
        inner()


def inner() -> None:
    with lock:
        print("inner")
```

但不要一上来就用 `RLock` 掩盖设计问题。很多时候，更好的做法是调整函数边界：让外层负责加锁，内层假设已经在锁内。

#### Semaphore：限制同时进入的线程数量

`Semaphore` 像一个有 N 张票的入口。每个线程进入前拿一张票，离开时还票。票拿完了，后来的线程就等。

它适合限制并发量：

- 最多 5 个线程同时请求某个接口。
- 最多 3 个线程同时处理大文件。
- 最多 10 个线程同时访问某个稀缺资源。

```python
import threading
import time


sem = threading.Semaphore(3)


def call_api(task_id: int) -> None:
    with sem:
        print(f"task-{task_id}: enter")
        time.sleep(0.5)
        print(f"task-{task_id}: leave")


threads = [threading.Thread(target=call_api, args=(i,)) for i in range(10)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
```

这段代码里最多只有 3 个任务同时处在 `with sem:` 内。

`BoundedSemaphore` 是更严格的版本：如果释放次数超过获取次数，它会报错。初学时你可以先用 `Semaphore`；如果你担心代码里多 release 了，可以用 `BoundedSemaphore` 帮你发现问题。

一句话区分：

| 工具 | 同时允许几个线程进入 | 典型用途 |
| --- | --- | --- |
| `Lock` | 1 个 | 保护共享变量 |
| `RLock` | 1 个，但同一线程可重复进入 | 嵌套调用需要同一把锁 |
| `Semaphore(n)` | n 个 | 限制并发访问数量 |

### 3.9 Event：一个线程向一组线程发信号

`Event` 是最容易理解的线程同步工具。你可以把它想象成一个**全局信号灯**：

```text
Event 没有 set：红灯，等待的人继续等
Event 已经 set：绿灯，等待的人可以继续走
```

它内部维护一个布尔标志位：

- 初始状态通常是 False。
- `event.set()` 把状态改成 True，并唤醒所有正在 `wait()` 的线程。
- `event.clear()` 把状态改回 False。
- `event.is_set()` 查看当前是否为 True。
- `event.wait()` 等到状态变成 True。
- `event.wait(timeout=...)` 最多等一段时间，返回值表示有没有等到 True。

注意：`Event` 只表达“某件事发生了没有”，它不携带数据，也不计数。如果你要传任务，用 `Queue`；如果你要等复杂条件，用 `Condition`。

#### Event 的原理模型

假设有 3 个 worker 都在等主线程发令：

```text
worker-1: event.wait()  -> 阻塞
worker-2: event.wait()  -> 阻塞
worker-3: event.wait()  -> 阻塞
main:     event.set()   -> 三个 worker 全部被唤醒
```

`Event` 很适合“广播式通知”：一个线程说“可以开始了”或“该停止了”，很多线程都能收到。

#### 用法 1：通知线程停止

线程不能被外部安全强杀，所以常见做法是让 worker 自己定期检查停止信号。

```python
import threading
import time


stop_event = threading.Event()


def worker() -> None:
    while not stop_event.is_set():
        print("working...")
        time.sleep(0.2)
    print("worker exit")


thread = threading.Thread(target=worker)
thread.start()

time.sleep(1)
stop_event.set()
thread.join()
```

这叫**协作式停止**：主线程只负责发信号，worker 负责在合适的位置退出。

这个模式适合：

- 后台轮询任务。
- 定时刷新任务。
- 需要优雅退出的 worker。
- 主线程收到 Ctrl+C 后通知所有线程收尾。

#### 用法 2：让多个线程等同一个开始信号

有时你希望先创建好所有线程，但不要让它们马上开跑。等主线程准备完资源后，再统一放行。

```python
import threading
import time


start_event = threading.Event()


def worker(worker_id: int) -> None:
    print(f"worker-{worker_id}: waiting")
    start_event.wait()
    print(f"worker-{worker_id}: start")


threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
for thread in threads:
    thread.start()

time.sleep(1)
print("main: release all workers")
start_event.set()

for thread in threads:
    thread.join()
```

这和 `Barrier` 有点像，但重点不同：

- `Event` 是一个线程发令，其他线程等令。
- `Barrier` 是固定数量线程互相等待，大家到齐后一起继续。

#### 用法 3：带 timeout 的等待

`wait(timeout=...)` 很重要，因为并发程序最怕永久等待。

```python
import threading


ready_event = threading.Event()


if ready_event.wait(timeout=2):
    print("ready")
else:
    print("not ready after 2 seconds")
```

`wait()` 的返回值是布尔值：

- 返回 True：等到了 `set()`。
- 返回 False：超时了。

#### Event 的常见坑

坑 1：以为 `set()` 只能唤醒一个线程。

实际上 `set()` 会唤醒所有正在 `wait()` 的线程，并且之后再调用 `wait()` 的线程也会直接通过，直到你调用 `clear()`。

坑 2：忘记 `clear()`。

如果你想复用同一个 Event 做多轮开始/停止信号，就要在合适时机 `clear()`。否则它一直是 True，后续 `wait()` 不会阻塞。

坑 3：用 Event 传递多个任务。

Event 只表示“发生过”。如果主线程连续 `set()` 两次，worker 不会知道发生了两次。需要计数或传任务时，用 `Queue`。

坑 4：worker 长时间不检查 `is_set()`。

如果 worker 在一个大循环里 10 分钟才检查一次停止信号，那主线程发出 `set()` 后也要等很久。协作式停止的关键是：worker 要在合理频率检查事件。

### 3.10 Condition：锁 + 条件 + 等待队列

`Condition` 比 `Event` 难一点，但它非常重要。它解决的问题是：**线程要等某个共享状态满足条件，才能继续执行。**

先看一句话模型：

```text
Condition = 一把锁 + 一个等待队列 + 围绕共享状态的条件判断
```

它不是单独使用的。`Condition` 一定要和“共享状态”一起理解。

从源码角度看，`Condition` 的核心结构也差不多就是这样：

```python
class Condition:
    def __init__(self, lock=None):
        if lock is None:
            lock = RLock()
        self._lock = lock
        self._waiters = deque()
```

也就是说，`Condition` 并不是一种完全独立于锁的东西。它内部本来就有一把锁，另外还有一个等待队列 `_waiters`。

你可以把它想象成：

```text
Lock：一扇门，一次只允许一个线程进屋修改共享状态
Condition：一扇门 + 一个等候室

条件不满足的线程：先去等候室睡觉
条件发生变化的线程：通知等候室里的人醒来
醒来的人：重新排队拿门锁，再检查条件
```

所以 `Lock` 和 `Condition` 的区别不是“谁更高级”，而是解决的问题不同：

| 工具 | 解决的问题 | 典型说法 |
| --- | --- | --- |
| `Lock` | 我能不能独占地修改共享状态 | 一次只让一个线程进来 |
| `Condition` | 条件不满足时怎么睡，条件变化后怎么醒 | 先等，别人改了状态再叫我 |

从新手视角看，`Lock` 只解决“别人在改的时候我不能同时改”。它不解决“现在条件不满足，我该如何高效地等到条件变好”。

如果只用 `Lock` 等队列非空，你通常会写出两种糟糕方案。

第一种是拿着锁等待：

```python
with lock:
    while not items:
        pass
```

这会死锁或接近死锁，因为 consumer 一直拿着锁，producer 根本进不来放数据，`items` 永远不会变成非空。

第二种是反复拿锁、检查、释放、sleep：

```python
while True:
    with lock:
        if items:
            item = items.pop(0)
            break
    time.sleep(0.01)
```

这叫忙等或轮询。它能跑，但不优雅：反复醒来检查会浪费 CPU，而且 `sleep` 时间很难调。调短了浪费资源，调长了响应变慢。

`Condition` 真正补上的能力是：**在同一个原子语义里完成“释放锁 + 睡眠等待”，再在被通知后“醒来 + 重新拿锁 + 重新检查条件”。**

如果只是保护 `count += 1`，用 `Lock`。如果要表达“队列空了就等，队列有数据再取”，用 `Condition` 或封装好的 `Queue`。

比如顺序打印 ABC 时，共享状态是：

```python
turn = 0  # 0 表示 A，1 表示 B，2 表示 C
```

线程 A 的条件是 `turn == 0`，线程 B 的条件是 `turn == 1`，线程 C 的条件是 `turn == 2`。条件不满足就等，条件满足就执行，然后修改 `turn`，通知其他线程。

#### Condition 的原理：wait 会释放锁，再重新拿锁

这是理解 `Condition` 最关键的一点。

当线程执行：

```python
with condition:
    condition.wait()
```

它不是“拿着锁睡觉”。它做的是：

1. 当前线程已经持有 condition 里的锁。
2. 调用 `wait()` 后，线程进入等待队列。
3. `wait()` 会临时释放锁，让别的线程有机会进入临界区修改共享状态。
4. 某个线程调用 `notify()` 或 `notify_all()` 后，等待线程被唤醒。
5. 被唤醒的线程不会立刻继续执行，它必须先重新拿到锁。
6. 重新拿到锁后，`wait()` 才返回，线程继续检查条件或执行后续代码。

为什么必须释放锁？因为如果等待线程一直拿着锁，其他线程就无法修改 `turn`、队列状态、缓冲区状态，条件永远不会变成 True，程序就死锁了。

把 `wait()` 翻译成伪代码，大概是：

```python
def wait(self):
    if current_thread_does_not_hold_lock:
        raise RuntimeError

    waiter = allocate_lock()
    waiter.acquire()          # 先把这个 waiter 锁住
    self._waiters.append(waiter)

    self._lock.release()      # 关键：释放 Condition 的主锁

    waiter.acquire()          # 卡在这里，直到 notify release 这个 waiter

    self._lock.acquire()      # 醒来后重新拿回主锁
```

真实源码还要处理 `RLock` 的重入层数、timeout、异常恢复，但核心思想就是这几步。

#### notify 的原理：释放等待队列里的 waiter

`notify()` 不是直接“运行某个线程”，它做的是释放等待队列里的 waiter。

简化伪代码：

```python
def notify(self, n=1):
    if current_thread_does_not_hold_lock:
        raise RuntimeError

    while self._waiters and n > 0:
        waiter = self._waiters.popleft()
        waiter.release()      # 被卡在 waiter.acquire() 的线程醒来
        n -= 1
```

`notify_all()` 本质上就是：

```python
def notify_all(self):
    self.notify(len(self._waiters))
```

所以源码级心智模型可以总结成：

```text
wait():
  进入等待队列
  释放主锁
  睡在 waiter 上
  被唤醒后重新拿主锁

notify():
  从等待队列里挑 waiter
  release waiter
  让对应线程有机会醒来
```

注意这里说的是“有机会醒来”。被唤醒的线程还要重新竞争主锁，拿到锁后才会从 `wait()` 返回。

#### Condition 的标准模板

初学时记住这个模板：

```python
condition = threading.Condition()
state = ...


def condition_is_met() -> bool:
    ...


def update_state() -> None:
    ...


def worker() -> None:
    with condition:
        condition.wait_for(condition_is_met)
        # 条件满足后，安全地读写共享状态
        update_state()
        condition.notify_all()
```

更底层的写法是 `while + wait()`：

```python
with condition:
    while not condition_is_met():
        condition.wait()
    update_state()
    condition.notify_all()
```

`wait_for(predicate)` 本质上就是帮你写了这个 while 循环。推荐初学者优先用 `wait_for()`，因为它更不容易写错。

#### 为什么必须用 while 或 wait_for，而不是 if

错误写法：

```python
with condition:
    if turn != index:
        condition.wait()
    output.append(letters[index])
```

这个写法不安全。原因有两个：

- 线程可能被“误唤醒”，醒来后条件仍然不满足。
- `notify_all()` 会叫醒很多线程，但只有一个线程真的满足条件。

所以醒来之后必须重新检查条件。正确写法：

```python
with condition:
    while turn != index:
        condition.wait()
    output.append(letters[index])
```

或者：

```python
with condition:
    condition.wait_for(lambda: turn == index)
    output.append(letters[index])
```

#### notify 和 notify_all 的区别

`notify()`：随机唤醒一个等待线程。

`notify_all()`：唤醒所有等待线程，让它们重新竞争锁并检查条件。

顺序打印 ABC 时，更推荐 `notify_all()`，因为当前线程不知道下一个该醒的是谁。唤醒所有线程后，只有 `turn == index` 的那个线程能通过 `wait_for()`，其他线程会继续睡。

如果你能确定只需要叫醒一个线程，例如简单的单消费者模型，可以用 `notify()`。但初学阶段，为了正确性，涉及多个不同条件时优先用 `notify_all()`。

#### notify 不会立刻释放锁

很多人第一次学 `Condition` 会误解：调用 `notify_all()` 后，被唤醒的线程是不是马上运行？不是。

```python
with condition:
    turn = next_turn
    condition.notify_all()
    # 这里还在锁里面
```

被唤醒的线程必须等当前线程退出 `with condition:`、释放锁之后，才能重新拿锁并继续执行。

这也解释了为什么“修改共享状态”和“通知别人”要放在同一个锁里：别人醒来后看到的一定是已经更新好的状态。

#### 例子 1：顺序打印的核心逻辑

```python
with condition:
    condition.wait_for(lambda: turn == index)
    output.append(letters[index])
    turn = (turn + 1) % len(letters)
    condition.notify_all()
```

含义：

- `wait_for(lambda: turn == index)`：不是我的回合就等。
- `output.append(...)`：轮到我了，执行动作。
- `turn = ...`：把回合交给下一个线程。
- `notify_all()`：叫醒其他线程，让它们重新检查条件。

#### 例子 2：一个容量为 1 的手写缓冲区

这个例子能帮你理解 `Condition` 为什么经常用在生产者消费者里。我们手写一个只能放 1 个元素的缓冲区：满了 producer 要等，空了 consumer 要等。

```python
import threading


condition = threading.Condition()
slot: int | None = None


def put(item: int) -> None:
    global slot
    with condition:
        condition.wait_for(lambda: slot is None)
        slot = item
        condition.notify_all()


def get() -> int:
    global slot
    with condition:
        condition.wait_for(lambda: slot is not None)
        item = slot
        slot = None
        condition.notify_all()
        return item
```

这里有两个条件：

- producer 等 `slot is None`，表示缓冲区空了，可以放。
- consumer 等 `slot is not None`，表示缓冲区有东西，可以取。

这就是 `queue.Queue` 背后的核心思想之一。实际开发里你不需要自己写这个缓冲区，直接用 `Queue` 更好；但理解这个例子，会让你真正懂 `Condition`。

#### Condition 的常见坑

坑 1：不在 `with condition:` 里调用 `wait()` / `notify()`。

`wait()` 和 `notify()` 必须在持有 condition 锁时调用，否则会报错，或者逻辑不安全。

坑 2：只 wait，不改变共享状态。

Condition 等的是共享状态变化。如果没有线程负责修改状态，所有线程都会一直等。

坑 3：修改状态后忘记 notify。

如果你把 `turn` 改了，但没有 `notify_all()`，其他线程可能还在睡，不知道条件已经变化。

坑 4：用 Condition 替代 Queue。

如果你的目标只是在线程之间传任务，优先用 `Queue`。`Condition` 更适合你需要自己定义复杂等待条件时使用。

#### Event 和 Condition 怎么选

| 问题 | 用 Event | 用 Condition |
| --- | --- | --- |
| 是否只需要表达“发生了/没发生” | 是 | 否 |
| 是否需要围绕共享变量判断复杂条件 | 否 | 是 |
| 是否需要广播停止信号 | 很适合 | 通常不需要 |
| 是否需要按顺序轮流执行 | 不适合 | 很适合 |
| 是否需要传任务数据 | 不适合，用 Queue | 可以但不首选 |

一句话：`Event` 是信号灯，`Condition` 是“带锁的条件等待”。

这正好对应后面的 Lab 1。做 Lab 1 时，你不是在背 API，而是在练习“多个线程通过条件变量协作”。

### Lab 1：三线程顺序打印 ABC

目标：启动 3 个线程，严格输出 `ABCABCABC...`。

这个 lab 对应第 3.10 节的 `Condition`。你要练的不是“打印 ABC”本身，而是理解：多个线程如何围绕同一个共享条件协作。

#### 1 先把问题拆开

我们需要 3 个 worker：

- A worker：只负责打印 A。
- B worker：只负责打印 B。
- C worker：只负责打印 C。

但是它们不能想什么时候打印就什么时候打印。它们必须遵守一个共享规则：

```text
turn = 0 时，只能 A 打印
turn = 1 时，只能 B 打印
turn = 2 时，只能 C 打印
打印完成后，turn 交给下一个线程
```

所以这个 lab 的核心状态只有一个：`turn`。

#### 2 为什么不能用 sleep

很多初学者会想：A 先打印，B sleep 一下，C 再 sleep 久一点。这个方法不可靠，因为线程调度由操作系统决定。机器变慢、CPU 忙、某次调度顺序变化，输出就乱了。

正确方法是：让线程等待一个明确条件，而不是等待一个猜出来的时间。

#### 3 你需要写出的核心逻辑

每个 worker 都执行同一套逻辑，只是自己的 `index` 不同：

```python
with condition:
    condition.wait_for(lambda: turn == index)
    output.append(letters[index])
    turn = (turn + 1) % len(letters)
    condition.notify_all()
```

这段代码刚好对应 `Condition` 的四件事：等待条件、执行动作、改变条件、通知别人。

要求：

- 输入 `rounds=5` 时输出必须是 `ABCABCABCABCABC`。
- 必须真的启动 3 个线程。
- 不能用 `sleep()` 猜时间。
- 推荐使用 `threading.Condition`。

#### 4 练习前的骨架

你可以先照这个骨架写，卡住再看参考解法：

```python
import threading


def run(rounds: int = 5) -> str:
    letters = "ABC"
    output: list[str] = []
    condition = threading.Condition()
    turn = 0

    def worker(index: int) -> None:
        nonlocal turn
        for _ in range(rounds):
            # TODO: 等到 turn == index
            # TODO: 追加当前字母
            # TODO: 把 turn 交给下一个线程
            # TODO: 唤醒其他等待线程
            pass

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return "".join(output)
```

#### 5 参考解法

```python
from __future__ import annotations

import threading


def run(rounds: int = 5) -> str:
    letters = "ABC"
    output: list[str] = []
    condition = threading.Condition()
    turn = 0

    def worker(index: int) -> None:
        nonlocal turn
        for _ in range(rounds):
            with condition:
                condition.wait_for(lambda: turn == index)
                output.append(letters[index])
                turn = (turn + 1) % len(letters)
                condition.notify_all()

    threads = [
        threading.Thread(target=worker, args=(i,), name=f"printer-{letters[i]}")
        for i in range(len(letters))
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return "".join(output)


if __name__ == "__main__":
    result = run(5)
    print(result)
    assert result == "ABCABCABCABCABC"
```

思考题：

- `turn` 为什么必须在 `condition` 的锁里面读写？
- 为什么用 `wait_for()` 而不是只 `wait()` 一次？
- 如果改成打印 `ABCD`，代码需要改哪里？



### 3.11 常用线程同步和辅助工具

| 工具 | 用途 | 典型例子 |
| --- | --- | --- |
| `Lock` | 互斥访问临界区 | 修改共享计数器、字典、列表 |
| `RLock` | 同一线程可以重复获取的锁 | 递归调用或嵌套调用里需要同一把锁 |
| `Semaphore` | 限制同时进入的数量 | 最多 10 个请求同时访问接口 |
| `Event` | 广播某个状态 | 通知所有 worker 停止 |
| `Condition` | 等待某个条件成立 | 有界缓冲区、顺序打印 |
| `Barrier` | 等一组线程都到达某个阶段 | 多阶段并行任务、统一开跑 |
| `Queue` | 线程安全消息队列 | 生产者消费者 |
| `threading.local()` | 每个线程一份自己的变量 | 线程级上下文 |
| `Timer` | 延迟执行函数 | 简单提醒、简单超时动作 |

### 3.12 Barrier：让一组线程在同一个关卡集合

`Barrier` 的中文可以理解成“栅栏”或“集合点”。它解决的问题是：**必须等 N 个线程都到达某个位置，大家才能一起继续往下走。**

生活例子：几个人约好一起跑步。每个人从不同地方来到操场入口，先到的人不能直接开跑，要等所有人都到了，大家一起开始。

代码里的典型场景：

- 多个 worker 都完成初始化后，再同时开始压测。
- 多个线程完成第 1 阶段计算后，再一起进入第 2 阶段。
- 测试并发代码时，让多个线程尽量同时冲进某段逻辑，暴露竞态问题。

最小例子：

```python
import random
import threading
import time


def worker(worker_id: int, barrier: threading.Barrier) -> None:
    print(f"worker-{worker_id}: preparing")
    time.sleep(random.uniform(0.1, 0.5))
    print(f"worker-{worker_id}: ready")

    barrier.wait()

    print(f"worker-{worker_id}: start together")


barrier = threading.Barrier(3)
threads = [threading.Thread(target=worker, args=(i, barrier)) for i in range(3)]

for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
```

运行时你会看到：每个线程准备完成的时间不同，但 `start together` 会等到 3 个线程都 `ready` 之后才出现。

`Barrier(3)` 里的 `3` 叫 parties，表示需要 3 个参与者都调用 `barrier.wait()`。如果只有 2 个线程调用了 `wait()`，它们会一直等第 3 个。

可以给 `wait()` 加 timeout，避免永远卡住：

```python
try:
    barrier.wait(timeout=2)
except threading.BrokenBarrierError:
    print("有人没有按时到达，barrier 已经 broken")
```

什么时候不要用 `Barrier`？

- 生产者消费者模型通常不用 `Barrier`，用 `Queue` 更自然。
- 如果线程数量不固定，不适合用 `Barrier`。
- 如果某个线程可能失败或提前退出，一定要考虑 timeout 和 `BrokenBarrierError`。

一句话记忆：`Event` 是“一个开关通知大家”，`Condition` 是“等某个条件成立”，`Barrier` 是“等固定数量的人到齐”。

小练习：把上面的 `Barrier(3)` 改成 `Barrier(5)`，但只启动 4 个线程。你会看到程序卡住。然后给 `wait(timeout=2)` 加上超时，观察 `BrokenBarrierError`。这个练习能帮你记住：`Barrier` 的参与者数量必须和实际到达数量匹配。

### 3.13 threading.local：每个线程一份自己的变量

普通全局变量是所有线程共享的：线程 A 改了，线程 B 也能看到。`threading.local()` 则相反：它创建的是**线程局部存储**，看起来像同一个变量，但每个线程看到的是自己的那一份。

```python
import threading


local_data = threading.local()


def worker(user_id: int) -> None:
    local_data.user_id = user_id
    print(threading.current_thread().name, local_data.user_id)


threads = [
    threading.Thread(target=worker, args=(i,), name=f"worker-{i}")
    for i in range(3)
]

for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
```

每个线程都会有自己的 `local_data.user_id`。线程之间不会互相覆盖。

它适合什么？

- 保存每个线程独立的上下文，例如 request id、trace id。
- 保存不方便到处传参的线程级状态。
- 某些老式同步框架里保存当前请求信息。

它不适合什么？

- 不适合传任务数据。传任务用 `Queue` 或函数参数。
- 不适合保存需要跨线程共享的状态。共享状态要显式加锁或用队列。
- 在线程池里要谨慎使用，因为线程会复用。上一个任务写入的 thread-local 数据，可能在同一个 worker 线程的下一个任务里还存在。用完最好清理。

### 3.14 Timer：延迟一段时间后执行函数

`threading.Timer` 是 `Thread` 的一个子类，用来“过一段时间后执行某个函数”。

```python
import threading


def say_hello() -> None:
    print("hello later")


timer = threading.Timer(2.0, say_hello)
timer.start()
```

它常用于简单延迟任务，比如 2 秒后提醒、5 秒后超时处理。但它不是严肃的定时任务调度器。

可以在执行前取消：

```python
timer = threading.Timer(5.0, say_hello)
timer.start()
timer.cancel()
```

注意：

- `Timer` 的时间不适合要求高精度的场景。
- 大量定时任务不要创建大量 `Timer` 线程，应该用调度器、事件循环或任务队列。
- 如果你已经在写 `asyncio` 程序，优先用事件循环的定时能力，而不是 `threading.Timer`。

### 3.15 Queue：线程之间最推荐的通信方式

线程通信有两种常见路线：

- 共享变量 + 锁。
- 队列传消息。

对初学者来说，优先选择队列。队列会让程序结构更清楚：producer 只负责生产任务，consumer 只负责消费任务，中间通过 `Queue` 交接。

共享 list + lock 可以写，但更推荐 `queue.Queue`。它天然线程安全，并且支持 `maxsize` 做背压。

```python
from queue import Queue
from threading import Thread
import time

SENTINEL = object()


def producer(q: Queue[object]) -> None:
    for i in range(10):
        q.put(i)
    q.put(SENTINEL)


def consumer(q: Queue[object]) -> None:
    while True:
        item = q.get()
        try:
            if item is SENTINEL:
                return
            time.sleep(0.1)
            print(f"consume {item}")
        finally:
            q.task_done()


q: Queue[object] = Queue(maxsize=3)
t1 = Thread(target=producer, args=(q,))
t2 = Thread(target=consumer, args=(q,))

t1.start()
t2.start()
t1.join()
q.join()
t2.join()
```

这里的 `maxsize=3` 很关键。消费者慢的时候，生产者会在 `put()` 处等待，避免无限堆积任务。

你需要理解 `Queue` 的几个动作：

- `put(item)`：放入一个任务。如果队列满了，会等待。
- `get()`：取出一个任务。如果队列空了，会等待。
- `task_done()`：告诉队列“刚才 get 出来的任务已经处理完”。
- `join()`：等待所有已放入队列的任务都被 `task_done()` 标记完成。

### 3.16 sentinel：让 worker 优雅退出

consumer 通常在 `while True` 里不断 `get()`，那它什么时候退出？

常见做法是放一个特殊对象，叫 sentinel，表示“没有更多任务了，可以退出”。

```python
SENTINEL = object()


def consumer(q: Queue[object]) -> None:
    while True:
        item = q.get()
        try:
            if item is SENTINEL:
                return
            print(f"handle {item}")
        finally:
            q.task_done()
```

如果有 4 个 consumer，就要放 4 个 sentinel。因为每个 sentinel 只能被一个 consumer 取走。

这正好对应后面的 Lab 2。Lab 2 不是孤立题目，它是在练习：有界队列、多个 producer、多个 consumer、sentinel、`task_done()`、`join()`。

### Lab 2：用 Condition 手写 BlockingQueue，再实现生产者消费者

目标：不用标准库 `queue.Queue`，自己用 `threading.Condition` 实现一个线程安全的有界阻塞队列，然后基于它写生产者消费者。

这个 lab 对应第 3.10、3.15、3.16 节：`Condition`、有界队列、背压、sentinel、`task_done()`、`join()`。

#### 1 先理解真正要手写的是什么

生产者消费者的本质不是某个 API，而是这个结构：

```text
producer -> bounded buffer -> consumer
```

中间的 `bounded buffer` 就是有界阻塞队列。它要满足：

```text
队列没满：producer 可以 put
队列满了：producer 等
队列不空：consumer 可以 get
队列空了：consumer 等
put 之后：通知 consumer
get 之后：通知 producer
```

这正好是 `Condition` 的经典用法：围绕共享状态 `items` 等待条件成立。

#### 2 为什么这个 lab 比直接用 queue.Queue 更有价值

直接用标准库 `queue.Queue` 可以学会应用层写法，但不一定理解底层原理。

手写 `BlockingQueue` 能帮你真正理解：

- `put()` 为什么队列满了会阻塞。
- `get()` 为什么队列空了会阻塞。
- 为什么 `Condition.wait_for()` 必须配合共享状态。
- 为什么状态变化后要 `notify_all()`。
- `task_done()` 和 `join()` 到底等的是什么。

标准库 `queue.Queue` 就是把这些细节封装好了。你平时应该用标准库；但学习时值得手写一次。

#### 3 要实现的接口

```python
class BlockingQueue:
    def __init__(self, maxsize: int):
        ...

    def put(self, item: object) -> None:
        ...

    def get(self) -> object:
        ...

    def task_done(self) -> None:
        ...

    def join(self) -> None:
        ...
```

最核心的是 `put()` 和 `get()`：

```text
put:
  等到 len(items) < maxsize
  append item
  unfinished_tasks += 1
  notify_all

get:
  等到 len(items) > 0
  popleft item
  notify_all
  return item
```

`task_done()` 和 `join()` 用来表达“任务是否全部处理完”：

```text
put 一个任务：unfinished_tasks += 1
consumer 处理完：task_done() -> unfinished_tasks -= 1
join()：等待 unfinished_tasks == 0
```

#### 4 练习前的骨架

```python
from __future__ import annotations

from collections import deque
import threading


class BlockingQueue:
    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")

        self._maxsize = maxsize
        self._items: deque[object] = deque()
        self._unfinished_tasks = 0
        self._condition = threading.Condition()

    def put(self, item: object) -> None:
        # TODO: 队列满时等待
        # TODO: 放入 item
        # TODO: unfinished_tasks += 1
        # TODO: 通知等待者
        pass

    def get(self) -> object:
        # TODO: 队列空时等待
        # TODO: 取出并返回 item
        # TODO: 通知等待者
        raise NotImplementedError

    def task_done(self) -> None:
        # TODO: unfinished_tasks -= 1
        # TODO: 如果所有任务完成，通知 join
        pass

    def join(self) -> None:
        # TODO: 等待 unfinished_tasks == 0
        pass
```

#### 5 参考解法

```python
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from threading import Lock, Thread
import threading
import time


class BlockingQueue:
    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")

        self._maxsize = maxsize
        self._items: deque[object] = deque()
        self._unfinished_tasks = 0
        self._condition = threading.Condition()

    def put(self, item: object) -> None:
        with self._condition:
            self._condition.wait_for(lambda: len(self._items) < self._maxsize)
            self._items.append(item)
            self._unfinished_tasks += 1
            self._condition.notify_all()

    def get(self) -> object:
        with self._condition:
            self._condition.wait_for(lambda: len(self._items) > 0)
            item = self._items.popleft()
            self._condition.notify_all()
            return item

    def task_done(self) -> None:
        with self._condition:
            if self._unfinished_tasks <= 0:
                raise ValueError("task_done() called too many times")

            self._unfinished_tasks -= 1
            if self._unfinished_tasks == 0:
                self._condition.notify_all()

    def join(self) -> None:
        with self._condition:
            self._condition.wait_for(lambda: self._unfinished_tasks == 0)


@dataclass(frozen=True, order=True)
class Job:
    producer_id: int
    value: int


SENTINEL = object()


def producer(q: BlockingQueue, producer_id: int, count: int) -> None:
    for value in range(count):
        q.put(Job(producer_id, value))


def consumer(q: BlockingQueue, results: list[Job], results_lock: Lock) -> None:
    while True:
        item = q.get()
        try:
            if item is SENTINEL:
                return

            assert isinstance(item, Job)
            time.sleep(0.01)
            with results_lock:
                results.append(item)
        finally:
            q.task_done()


def run(producer_count: int = 3, consumer_count: int = 4, jobs_per_producer: int = 10) -> list[Job]:
    q = BlockingQueue(maxsize=5)
    results: list[Job] = []
    results_lock = Lock()

    consumers = [
        Thread(target=consumer, args=(q, results, results_lock), name=f"consumer-{i}")
        for i in range(consumer_count)
    ]
    producers = [
        Thread(target=producer, args=(q, i, jobs_per_producer), name=f"producer-{i}")
        for i in range(producer_count)
    ]

    for thread in consumers:
        thread.start()
    for thread in producers:
        thread.start()

    for thread in producers:
        thread.join()

    for _ in range(consumer_count):
        q.put(SENTINEL)

    q.join()

    for thread in consumers:
        thread.join()

    expected_count = producer_count * jobs_per_producer
    assert len(results) == expected_count
    assert len(set(results)) == expected_count
    return results


if __name__ == "__main__":
    result = run()
    print(f"processed={len(result)}")
    print(sorted(result)[:5], "...")
```

#### 6 关键细节解释

为什么 `put()` 里要 `wait_for(lambda: len(self._items) < self._maxsize)`？

因为队列满了，producer 必须停下来等 consumer 取走任务。这就是背压。

为什么 `get()` 里要 `wait_for(lambda: len(self._items) > 0)`？

因为队列空了，consumer 没有东西可取，必须等 producer 放入任务。

为什么 `put()` 和 `get()` 都要 `notify_all()`？

- `put()` 之后，队列从空变成非空，可能有 consumer 可以继续。
- `get()` 之后，队列从满变成未满，可能有 producer 可以继续。

为什么 sentinel 也要 `task_done()`？

因为 sentinel 也是通过 `put()` 进入队列的，`put()` 会让 `_unfinished_tasks += 1`。所以 consumer 取到 sentinel 后也必须进入 `finally` 调用 `task_done()`，否则 `join()` 可能永远等下去。

为什么多个 consumer 要放多个 sentinel？

因为一个 sentinel 只能被一个 consumer 取走。要让 4 个 consumer 都退出，就要放 4 个 sentinel。

#### 7 思考题

- 如果 `task_done()` 忘记写在 `finally` 里，异常时会发生什么？
- 如果把 `notify_all()` 换成 `notify()`，这个实现是否一定正确？什么时候可能有问题？
- 如果 `maxsize` 设置得特别大，背压还明显吗？
- 标准库 `queue.Queue` 还支持哪些这个简化版没有实现的能力？

### 3.17 threading 的底层原理：OS 线程、共享内存和 GIL

学完 API 之后，需要把 threading 的运行模型收一下。

#### 线程是谁调度的

Python 的 `threading.Thread` 对应的是操作系统线程。线程什么时候运行、什么时候被切走，主要由操作系统调度器决定，不由你的 Python 代码精确控制。

这意味着：

- 线程执行顺序不可预测。
- 同一段代码多跑几次，日志顺序可能不同。
- 不能用 `sleep()` 猜线程顺序。
- 共享状态必须用锁、队列或其它同步工具保护。

#### 线程共享什么

同一个进程里的线程共享大部分内存：全局变量、堆上的对象、打开的文件描述符、网络连接等。

这带来两面性：

- 好处：线程之间传对象很方便，不需要序列化。
- 坏处：多个线程同时修改同一个对象，容易出现竞态。

所以 threading 编程的核心难点不是“怎么启动线程”，而是“怎么管理共享状态”。

#### GIL 对线程意味着什么

传统 CPython 有 GIL。简单理解：同一时刻，一个进程里通常只有一个线程在执行 Python 字节码。

所以：

- IO 密集任务适合线程。线程 A 等网络时，线程 B 可以继续跑。
- 纯 Python CPU 密集任务不适合靠线程提速。多个线程会争 GIL，未必更快。
- 想利用多核跑纯 Python 计算，通常用进程池。

不过 GIL 不等于“线程安全”。即使有 GIL，`count += 1` 这种读-改-写逻辑仍然可能在业务层面出错。GIL 保护的是解释器内部状态，不保护你的业务不变量。

#### threading 的几个同步工具本质上在做什么

| 工具 | 底层心智模型 |
| --- | --- |
| `Lock` | 一次只允许一个线程进入临界区 |
| `RLock` | 带“持有者”和“重入次数”的 Lock |
| `Semaphore` | 有 N 张票的 Lock |
| `Event` | 一个布尔标志 + 等待队列 |
| `Condition` | 一把锁 + 条件判断 + 等待队列 |
| `Barrier` | Condition + 到达计数 + 状态机 |
| `Queue` | Lock/Condition 封装好的线程安全缓冲区 |

真实开发里，优先级通常是：能用 `Queue` 就别自己造复杂 `Condition`；能用 `ThreadPoolExecutor` 就别手动管理一堆线程；必须共享状态时再认真设计锁。



## 4. 线程池：日常并发的第一选择

实际开发中，很多时候不需要手动创建线程，而是使用线程池。

你可以把线程池理解成一个“提前雇好的 worker 小队”：

```text
主线程 submit 任务 -> 任务队列 -> 固定数量 worker 线程 -> Future 保存结果
```

和手动创建 `Thread` 相比，线程池有几个明显好处：

- 不用为每个任务创建一个新线程，减少创建和销毁线程的成本。
- 可以用 `max_workers` 控制并发量，避免把下游接口、数据库或机器资源打爆。
- 每个任务都会返回 `Future`，主线程可以统一拿结果、拿异常、判断是否完成。
- 配合 `with ThreadPoolExecutor(...) as pool:` 可以自动关闭线程池。

这一节重点补齐 `ThreadPoolExecutor` 的几个常用能力：`submit()`、`Future.result()`、`wait()`、`as_completed()`、`map()`、`shutdown()`，最后完成一个手写线程池 lab。

### 4.1 submit + Future：提交任务，拿到未来结果

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def fetch(i: int) -> str:
    time.sleep(0.3)
    return f"result-{i}"


with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(fetch, i) for i in range(10)]
    for future in as_completed(futures):
        print(future.result())
```

要点：

- `submit()` 把任务放进线程池，通常立刻返回一个 `Future`。
- `Future` 是“未来会有结果的占位对象”。
- `as_completed()` 谁先完成就先处理谁。
- `future.result()` 会重新抛出 worker 里的异常，所以不要忽略它。

`Future` 常用方法：

| 方法 | 含义 |
| --- | --- |
| `future.result()` | 等任务完成并返回结果；如果 worker 抛异常，这里会重新抛出 |
| `future.result(timeout=3)` | 最多等 3 秒，超时抛 `TimeoutError` |
| `future.done()` | 任务是否已经结束，成功、异常、取消都算结束 |
| `future.exception()` | 获取任务异常；没有异常返回 `None` |
| `future.cancel()` | 尝试取消任务；任务已经运行后通常取消不了 |
| `future.cancelled()` | 任务是否已经取消 |

新手最容易忽略的一点：worker 线程里的异常不会自动打断主线程，异常会被保存到 `Future` 里。只有你调用 `future.result()` 时，异常才会在主线程重新抛出。

```python
from concurrent.futures import ThreadPoolExecutor


def boom() -> None:
    raise ValueError("bad task")


with ThreadPoolExecutor(max_workers=2) as pool:
    future = pool.submit(boom)

    try:
        future.result()
    except ValueError as exc:
        print(f"caught in main thread: {exc}")
```

如果你提交了任务，却从不调用 `result()` 或检查 `exception()`，很多错误就会被悄悄藏在 `Future` 里。

### 4.2 with 和 shutdown：线程池也需要关闭

推荐写法：

```python
from concurrent.futures import ThreadPoolExecutor


with ThreadPoolExecutor(max_workers=5) as pool:
    futures = [pool.submit(fetch, i) for i in range(10)]
    results = [future.result() for future in futures]
```

`with` 退出时会调用 `shutdown(wait=True)`，等待已经提交的任务执行完，再回收 worker 线程。

手动写法：

```python
pool = ThreadPoolExecutor(max_workers=5)
try:
    futures = [pool.submit(fetch, i) for i in range(10)]
    results = [future.result() for future in futures]
finally:
    pool.shutdown(wait=True)
```

常见参数：

```python
pool.shutdown(wait=True, cancel_futures=False)
```

- `wait=True`：等待已经开始和已经排队的任务结束。
- `wait=False`：发出关闭信号后立刻返回，但 Python 程序仍会等待非 daemon worker 线程结束。
- `cancel_futures=True`：取消还没开始运行的排队任务，已经开始的任务不会被强杀。

### 4.3 wait：等全部完成，还是等第一个完成

有时你不想一个一个 `result()`，而是想明确表达“等所有任务完成”或“只要有一个完成就继续”。这时用 `wait()`。

```python
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, ThreadPoolExecutor, wait
import time


def action(second: int) -> int:
    print(f"start {second}")
    time.sleep(second)
    return second


seconds = [4, 5, 2, 3]

with ThreadPoolExecutor(max_workers=2) as pool:
    futures = [pool.submit(action, second) for second in seconds]

    done, not_done = wait(futures, return_when=ALL_COMPLETED)
    print(f"done={len(done)}, not_done={len(not_done)}")
```

`wait(fs, timeout=None, return_when=ALL_COMPLETED)` 的关键参数：

| 参数 | 含义 |
| --- | --- |
| `fs` | 要等待的一组 Future |
| `timeout` | 最多等待多久，超时后返回当前完成和未完成的集合 |
| `return_when=ALL_COMPLETED` | 全部 Future 完成后返回 |
| `return_when=FIRST_COMPLETED` | 任意一个 Future 完成或取消后返回 |

只等第一个完成：

```python
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait


with ThreadPoolExecutor(max_workers=2) as pool:
    futures = [pool.submit(action, second) for second in [4, 5, 2, 3]]

    done, not_done = wait(futures, return_when=FIRST_COMPLETED)
    print("first batch done:", [future.result() for future in done])
    print("still running or queued:", len(not_done))
```

注意：`wait(FIRST_COMPLETED)` 返回后，没完成的任务并不会自动取消，它们还在继续跑。如果你后面又对所有 future 调 `result()`，主线程还是会等到所有任务结束。

### 4.4 as_completed：谁先完成，就先处理谁

`as_completed()` 适合“任务完成一个，就马上处理一个”的场景，例如并发下载图片、批量请求接口、并发处理文件。

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def action(second: int) -> int:
    time.sleep(second)
    return second


with ThreadPoolExecutor(max_workers=2) as pool:
    futures = [pool.submit(action, second) for second in [4, 5, 2, 3]]

    for future in as_completed(futures):
        print(f"{future.result()} returned")
```

`as_completed()` 的结果顺序不是提交顺序，而是完成顺序。谁先做完，谁先被 yield 出来。

这个特性很适合降低延迟：你不用等最慢的任务完成后才开始处理结果。

### 4.5 map：写起来简单，但结果按输入顺序返回

```python
from concurrent.futures import ThreadPoolExecutor


with ThreadPoolExecutor(max_workers=5) as pool:
    for result in pool.map(fetch, range(10)):
        print(result)
```

`map()` 的特点：

- 写法比 `submit()` 更短。
- 返回结果顺序和输入顺序一致。
- 如果前面的慢任务没完成，后面的快任务即使已经完成，也要等前面的结果先产出。
- 输入特别大时，要注意不要一次性制造过多任务。

例子：

```python
from concurrent.futures import ThreadPoolExecutor
import time


def action(second: int) -> int:
    time.sleep(second)
    return second


with ThreadPoolExecutor(max_workers=2) as pool:
    for result in pool.map(action, [5, 1, 2, 3]):
        print(f"{result} returned")
```

虽然 `1` 秒的任务会比 `5` 秒任务更早完成，但 `map()` 会先等输入里的第一个任务结果，所以你会先看到 `5 returned`。

怎么选：

| 需求 | 推荐写法 |
| --- | --- |
| 要拿到每个任务的 `Future`，方便取消、查异常、加回调 | `submit()` |
| 谁先完成就先处理谁 | `submit()` + `as_completed()` |
| 只想按输入顺序批量得到结果 | `map()` |
| 要等全部或等第一个完成 | `wait()` |

### 4.6 有界提交：控制 inflight 任务数量

线程池限制的是同时运行的 worker 数量，不一定限制“已经提交但还没运行的任务数量”。如果你把 100 万个任务一下子都 `submit()` 进去，任务队列会堆得很大。

更健康的做法是控制 inflight 数量：正在运行和正在排队的任务总数不超过某个上限。

```python
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor, Future, wait, FIRST_COMPLETED


def bounded_map(
    executor: Executor,
    fn: Callable[[int], str],
    items: Iterable[int],
    *,
    limit: int,
) -> Iterator[str]:
    iterator = iter(items)
    pending: set[Future[str]] = set()

    for _ in range(limit):
        try:
            pending.add(executor.submit(fn, next(iterator)))
        except StopIteration:
            break

    while pending:
        done, pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            yield future.result()
            try:
                pending.add(executor.submit(fn, next(iterator)))
            except StopIteration:
                pass
```

并发数量必须有上限。健康的并发程序通常会限制：

- worker 数量。
- 队列长度。
- 同时访问下游服务的请求数。
- 单个任务超时。
- 整体任务 deadline。

### Big Lab：手写线程池

目标：实现一个最小但可用的线程池，理解 `ThreadPoolExecutor` 背后的核心结构。

这个 lab 对应第 4 章全部内容。你要写的不是“创建几个线程”这么简单，而是把线程池拆成几个核心部件：

```text
submit() -> WorkItem -> Queue -> worker loop -> Future
```

#### 1 要实现的能力

功能要求：

- `submit(fn, *args, **kwargs)` 返回一个 `Future`。
- worker 从队列取任务执行。
- 支持任务返回值和异常传递。
- 支持 `map(fn, iterable)`，并且结果顺序和输入顺序一致。
- 支持 `shutdown(wait=True)`。
- shutdown 后不允许继续 submit。
- 任务队列有上限，避免无限堆积。
- 支持上下文管理器：`with MiniThreadPool(...) as pool:`。

#### 2 练习前的骨架

```python
from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from queue import Queue
from threading import Lock, Thread
from typing import Any, Callable, Iterable, Iterator


@dataclass(frozen=True)
class WorkItem:
    future: Future[Any]
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


SENTINEL = object()


class MiniThreadPool:
    def __init__(self, max_workers: int, queue_size: int = 0) -> None:
        ...

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        ...

    def map(self, fn: Callable[[Any], Any], iterable: Iterable[Any]) -> Iterator[Any]:
        ...

    def shutdown(self, wait: bool = True) -> None:
        ...

    def _worker(self) -> None:
        ...

    def __enter__(self) -> "MiniThreadPool":
        ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        ...
```

你可以先只实现 `submit()`、`_worker()`、`shutdown()`，跑通以后再加 `map()` 和测试。

#### 3 参考解法

这版实现故意保持简洁，但已经包含线程池的核心语义。

```python
from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
from queue import Queue
from threading import Lock, Thread
from typing import Any, Callable, Iterable, Iterator
import time


@dataclass(frozen=True)
class WorkItem:
    future: Future[Any]
    fn: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


SENTINEL = object()


class MiniThreadPool:
    def __init__(self, max_workers: int, queue_size: int = 0) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")

        self._queue: Queue[WorkItem | object] = Queue(maxsize=queue_size)
        self._shutdown = False
        self._lock = Lock()
        self._threads = [
            Thread(target=self._worker, name=f"mini-pool-{i}")
            for i in range(max_workers)
        ]

        for thread in self._threads:
            thread.start()

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot submit after shutdown")
            future: Future[Any] = Future()
            self._queue.put(WorkItem(future, fn, args, kwargs))
            return future

    def map(self, fn: Callable[[Any], Any], iterable: Iterable[Any]) -> Iterator[Any]:
        futures = [self.submit(fn, item) for item in iterable]
        for future in futures:
            yield future.result()

    def shutdown(self, wait: bool = True) -> None:
        with self._lock:
            if self._shutdown:
                return
            self._shutdown = True
            for _ in self._threads:
                self._queue.put(SENTINEL)

        if wait:
            for thread in self._threads:
                thread.join()

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is SENTINEL:
                    return

                assert isinstance(item, WorkItem)
                if not item.future.set_running_or_notify_cancel():
                    continue

                try:
                    result = item.fn(*item.args, **item.kwargs)
                except BaseException as exc:
                    item.future.set_exception(exc)
                else:
                    item.future.set_result(result)
            finally:
                self._queue.task_done()

    def __enter__(self) -> "MiniThreadPool":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.shutdown(wait=True)


def slow_square(x: int) -> int:
    time.sleep(0.05)
    return x * x


def boom() -> None:
    raise ValueError("worker error")


if __name__ == "__main__":
    with MiniThreadPool(max_workers=3, queue_size=5) as pool:
        futures = [pool.submit(slow_square, i) for i in range(10)]
        print([future.result() for future in futures])

        print(list(pool.map(slow_square, range(5))))

        bad_future = pool.submit(boom)
        try:
            bad_future.result()
        except ValueError as exc:
            print(f"caught: {exc}")
```

核心结构：

```text
submit() -> Queue[WorkItem] -> worker thread -> Future.set_result / set_exception
```

#### 4 自测用例

把下面的测试放到同一个文件里运行。不要只看打印结果，尽量用 `assert` 检查行为。

```python
def test_submit_result() -> None:
    with MiniThreadPool(max_workers=2) as pool:
        futures = [pool.submit(slow_square, i) for i in range(5)]
        assert [future.result() for future in futures] == [0, 1, 4, 9, 16]


def test_map_keeps_input_order() -> None:
    with MiniThreadPool(max_workers=3) as pool:
        assert list(pool.map(slow_square, range(5))) == [0, 1, 4, 9, 16]


def test_exception_propagates_to_future() -> None:
    with MiniThreadPool(max_workers=1) as pool:
        future = pool.submit(boom)
        try:
            future.result()
        except ValueError as exc:
            assert str(exc) == "worker error"
        else:
            raise AssertionError("expected ValueError")


def test_submit_after_shutdown_fails() -> None:
    pool = MiniThreadPool(max_workers=1)
    pool.shutdown()
    try:
        pool.submit(slow_square, 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError")


def test_cancel_before_running() -> None:
    with MiniThreadPool(max_workers=1) as pool:
        first = pool.submit(time.sleep, 0.2)
        second = pool.submit(slow_square, 10)
        assert second.cancel() is True
        first.result()
        assert second.cancelled() is True


if __name__ == "__main__":
    test_submit_result()
    test_map_keeps_input_order()
    test_exception_propagates_to_future()
    test_submit_after_shutdown_fails()
    test_cancel_before_running()
    print("all tests passed")
```

#### 5 关键细节解释

为什么 `submit()` 返回 `Future`？

因为任务在线程池里异步执行，主线程不能立刻拿到结果。`Future` 就是主线程和 worker 之间的结果句柄：worker 负责 `set_result()` 或 `set_exception()`，主线程负责 `result()`。

为什么 worker 要调用 `set_running_or_notify_cancel()`？

它的作用是把 Future 从“等待运行”推进到“正在运行”。如果任务在开始前已经被取消，这个方法会返回 False，worker 就不应该再执行这个任务。

为什么异常要 `future.set_exception(exc)`？

因为 worker 线程里的异常不能直接抛回主线程。把异常存进 Future 后，主线程调用 `future.result()` 时就能重新感知这个错误。

为什么 shutdown 要投递 sentinel？

worker 一直阻塞在 `queue.get()`。如果你只是把 `_shutdown = True`，正在等待任务的 worker 不一定有机会醒来。投递 sentinel 可以让每个 worker 从队列里取到“退出信号”，然后结束循环。

为什么 sentinel 数量要等于 worker 数量？

一个 sentinel 只能被一个 worker 取走。要让 3 个 worker 都退出，就要放 3 个 sentinel。

为什么队列满时 `submit()` 会阻塞？

这是背压。它告诉提交方：线程池现在处理不过来了，不要无限堆任务。真实工程里你也可以选择 `put(timeout=...)`，队列长时间满时直接失败。

为什么这个 `map()` 结果按输入顺序返回？

因为它先按输入顺序保存 futures，再按 futures 顺序逐个 `result()`。即使后面的任务先完成，也要等前面的结果先产出。这和标准库 `ThreadPoolExecutor.map()` 的直觉一致。

思考题：

- `Future` 为什么适合作为并发结果句柄？
- shutdown 为什么需要投递 sentinel？
- 如果队列满了，`submit()` 会发生什么？这是优点还是缺点？
- 如何给这个线程池增加 `as_completed()` 风格的完成顺序迭代？
- 如何给 `submit()` 增加 timeout，避免队列满时永久等待？
- 如果任务函数里又向同一个线程池提交任务并等待结果，什么时候可能死锁？


## 5. 进程模型：CPU 密集任务和多核并行

进程池适合纯 Python CPU 密集任务。

```python
from concurrent.futures import ProcessPoolExecutor


def cpu_heavy(n: int) -> int:
    total = 0
    for i in range(n):
        total += i * i
    return total


if __name__ == "__main__":
    with ProcessPoolExecutor() as pool:
        results = list(pool.map(cpu_heavy, [3_000_000] * 4))
    print(results)
```

进程池规则：

- 在 macOS/Windows 上尤其要把启动代码放进 `if __name__ == "__main__":`。
- 提交到进程池的函数通常要定义在模块顶层。
- 参数和返回值需要可序列化。
- 不要频繁传递巨大对象，序列化成本可能抵消并行收益。
- 进程之间默认不共享内存。需要通信时，优先设计成消息传递。

判断 CPU 密集还是 IO 密集：

- CPU 长时间接近满载，程序仍慢：偏 CPU 密集。
- CPU 使用率低，大量时间在等待：偏 IO 密集。

## 6. asyncio：事件循环和协作式调度

### 6.1 async/await 是什么

`async def` 调用后不会立刻执行函数体，而是创建 coroutine 对象。它需要被事件循环驱动。

```python
import asyncio


async def hello() -> str:
    await asyncio.sleep(1)
    return "hello"


async def main() -> None:
    result = await hello()
    print(result)


asyncio.run(main())
```

`await` 不是开线程，而是当前协程把控制权交还给事件循环，等等待对象完成后再恢复。

### 6.2 create_task：让多个协程同时推进

```python
import asyncio


async def fetch(i: int) -> str:
    await asyncio.sleep(0.3)
    return f"result-{i}"


async def main() -> None:
    tasks = [asyncio.create_task(fetch(i)) for i in range(5)]
    results = await asyncio.gather(*tasks)
    print(results)


asyncio.run(main())
```

注意：下面这样仍然是串行等待。

```python
for i in range(5):
    await fetch(i)
```

### 6.3 TaskGroup：结构化并发

Python 3.11 引入了 `asyncio.TaskGroup`。它让一组任务有清晰的生命周期：进入上下文创建任务，离开上下文等待任务完成；如果某个任务失败，相关任务会被取消，异常会被汇总抛出。

```python
import asyncio


async def fetch(i: int) -> str:
    await asyncio.sleep(0.1)
    return f"result-{i}"


async def main() -> None:
    tasks: list[asyncio.Task[str]] = []
    async with asyncio.TaskGroup() as tg:
        for i in range(5):
            tasks.append(tg.create_task(fetch(i)))

    print([task.result() for task in tasks])


asyncio.run(main())
```

工程上更推荐结构化并发，因为 task 不会无意中飘在后台。

### 6.4 Semaphore：限制 async 并发数

```python
import asyncio


async def call_api(i: int, sem: asyncio.Semaphore) -> str:
    async with sem:
        await asyncio.sleep(0.2)
        return f"ok-{i}"


async def main() -> None:
    sem = asyncio.Semaphore(3)
    results = await asyncio.gather(*(call_api(i, sem) for i in range(10)))
    print(results)


asyncio.run(main())
```

`Semaphore(3)` 表示最多 3 个协程同时进入临界区，常用于 API 限流、连接数限制、下载并发限制。

### 6.5 超时和取消

```python
import asyncio


async def slow() -> str:
    await asyncio.sleep(10)
    return "done"


async def main() -> None:
    try:
        async with asyncio.timeout(1):
            print(await slow())
    except TimeoutError:
        print("timeout")


asyncio.run(main())
```

取消不是边角料，而是并发程序的主路径之一。一个可维护的 coroutine 应该：

- 不随便吞掉 `asyncio.CancelledError`。
- 用 `try/finally` 释放资源。
- 给外部调用设置超时。

### 6.6 不要阻塞事件循环

错误示例：

```python
import time


async def bad() -> None:
    time.sleep(1)  # 阻塞整个事件循环
```

修复：

```python
async def good() -> None:
    await asyncio.sleep(1)
```

如果必须调用阻塞函数，用 `asyncio.to_thread()` 临时隔离：

```python
import asyncio
import time


def blocking_io() -> str:
    time.sleep(1)
    return "ok"


async def main() -> None:
    result = await asyncio.to_thread(blocking_io)
    print(result)


asyncio.run(main())
```

### 6.7 asyncio.Queue：async 世界里的队列和背压

`asyncio.Queue` 和线程里的 `queue.Queue` 很像，都是用来在 worker 之间传任务。区别是：

- `queue.Queue.get()` 会阻塞当前线程。
- `asyncio.Queue.get()` 需要 `await`，它不会阻塞整个事件循环，只会暂停当前 coroutine。

最小模型：

```python
import asyncio


async def producer(q: asyncio.Queue[int]) -> None:
    for i in range(5):
        await q.put(i)


async def consumer(q: asyncio.Queue[int]) -> None:
    while True:
        item = await q.get()
        try:
            print(f"consume {item}")
        finally:
            q.task_done()


async def main() -> None:
    q: asyncio.Queue[int] = asyncio.Queue(maxsize=2)
    consumer_task = asyncio.create_task(consumer(q))

    await producer(q)
    await q.join()
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


asyncio.run(main())
```

不过上面这个最小例子用 `cancel()` 结束 consumer，只适合演示。更工程化的方式和线程版一样：用 sentinel 明确告诉 worker 退出。

```python
SENTINEL = object()


async def consumer(q: asyncio.Queue[int | object]) -> None:
    while True:
        item = await q.get()
        try:
            if item is SENTINEL:
                return
            print(f"consume {item}")
        finally:
            q.task_done()
```

`asyncio.Queue(maxsize=n)` 也能提供背压：队列满了，`await q.put(item)` 会暂停当前 producer coroutine，让事件循环去执行别的任务。

这正好对应下面的 Lab 3。你会看到：线程版生产者消费者和 async 版流水线，本质结构非常像，只是阻塞等待变成了 `await`。

### Lab 3：asyncio 有界并发流水线

目标：模拟“抓取 URL -> 解析 -> 保存”的 async 流水线。

这个 lab 对应第 6.4、6.5、6.7 节和第 8 章。它是 Lab 2 的 async 版本：仍然是队列、worker、sentinel、背压，只是换成 `asyncio.Queue` 和 async worker。

你可以把它理解为：

```text
URL queue -> fetch workers -> page queue -> parse workers -> results
```

它和 Lab 2 的关系很紧：

- Lab 2 手写 `BlockingQueue`，Lab 3 使用 `asyncio.Queue`。
- Lab 2 的 worker 是线程，Lab 3 的 worker 是 `asyncio.Task`。
- Lab 2 用普通函数，Lab 3 用 `async def`。
- 两者都需要背压、sentinel 和 `join()`。

要求：

- 使用 `asyncio.Queue(maxsize=n)` 连接阶段。
- 使用多个 fetch worker 和 parse worker。
- 使用 `asyncio.Semaphore` 限制同时 fetch 数。
- 支持超时和优雅停止。
- 最后验证结果数量正确。

参考解法：

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(frozen=True)
class Page:
    url: str
    html: str


@dataclass(frozen=True)
class Record:
    url: str
    title: str


SENTINEL = object()


async def fake_fetch(url: str) -> Page:
    await asyncio.sleep(0.05)
    return Page(url=url, html=f"<title>{url}</title>")


async def fake_parse(page: Page) -> Record:
    await asyncio.sleep(0.02)
    title = page.html.removeprefix("<title>").removesuffix("</title>")
    return Record(url=page.url, title=title)


async def run(url_count: int = 20) -> list[Record]:
    fetch_workers = 4
    parse_workers = 3
    url_queue: asyncio.Queue[str | object] = asyncio.Queue(maxsize=5)
    page_queue: asyncio.Queue[Page | object] = asyncio.Queue(maxsize=5)
    results: list[Record] = []
    fetch_limit = asyncio.Semaphore(3)

    async def fetch_worker(worker_id: int) -> None:
        while True:
            item = await url_queue.get()
            try:
                if item is SENTINEL:
                    return

                assert isinstance(item, str)
                async with fetch_limit:
                    async with asyncio.timeout(1.0):
                        page = await fake_fetch(item)
                await page_queue.put(page)
            finally:
                url_queue.task_done()

    async def parse_worker(worker_id: int) -> None:
        while True:
            item = await page_queue.get()
            try:
                if item is SENTINEL:
                    return

                assert isinstance(item, Page)
                record = await fake_parse(item)
                results.append(record)
            finally:
                page_queue.task_done()

    fetch_tasks = [asyncio.create_task(fetch_worker(i)) for i in range(fetch_workers)]
    parse_tasks = [asyncio.create_task(parse_worker(i)) for i in range(parse_workers)]

    for i in range(url_count):
        await url_queue.put(f"https://example.test/{i}")
    for _ in range(fetch_workers):
        await url_queue.put(SENTINEL)

    await url_queue.join()
    await asyncio.gather(*fetch_tasks)

    await page_queue.join()
    for _ in range(parse_workers):
        await page_queue.put(SENTINEL)
    await page_queue.join()
    await asyncio.gather(*parse_tasks)

    assert len(results) == url_count
    assert {record.url for record in results} == {
        f"https://example.test/{i}" for i in range(url_count)
    }
    return results


if __name__ == "__main__":
    records = asyncio.run(run())
    print(f"records={len(records)}")
    print(records[:3], "...")
```

思考题：

- `url_queue.join()` 等待的是什么？
- 为什么 parse worker 的 sentinel 要等 page queue 清空后再投递？
- 如果 `fake_fetch()` 偶尔失败，如何保证 worker 不退出整个程序？



### Bonus Lab：手写协程池

协程池和线程池的形状很像：提交任务、放进队列、worker 执行、通过 future 回传结果。区别是 worker 是 `asyncio.Task`，调度发生在事件循环里。

参考解法：

```python
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable


@dataclass(frozen=True)
class AsyncWorkItem:
    future: asyncio.Future[Any]
    fn: Callable[..., Awaitable[Any]]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


SENTINEL = object()


class CoroutinePool:
    def __init__(self, workers: int, queue_size: int = 0) -> None:
        if workers <= 0:
            raise ValueError("workers must be positive")

        self._queue: asyncio.Queue[AsyncWorkItem | object] = asyncio.Queue(maxsize=queue_size)
        self._workers_count = workers
        self._tasks: list[asyncio.Task[None]] = []
        self._closed = False

    async def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self._workers_count)]

    async def submit(
        self,
        fn: Callable[..., Awaitable[Any]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> asyncio.Future[Any]:
        if self._closed:
            raise RuntimeError("cannot submit after shutdown")

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        await self._queue.put(AsyncWorkItem(future, fn, args, kwargs))
        return future

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        for _ in range(self._workers_count):
            await self._queue.put(SENTINEL)
        await self._queue.join()
        await asyncio.gather(*self._tasks)

    async def _worker(self, worker_id: int) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is SENTINEL:
                    return

                assert isinstance(item, AsyncWorkItem)
                if item.future.cancelled():
                    continue

                try:
                    result = await item.fn(*item.args, **item.kwargs)
                except BaseException as exc:
                    if not item.future.cancelled():
                        item.future.set_exception(exc)
                else:
                    if not item.future.cancelled():
                        item.future.set_result(result)
            finally:
                self._queue.task_done()

    async def __aenter__(self) -> "CoroutinePool":
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.shutdown()


async def async_square(x: int) -> int:
    await asyncio.sleep(0.05)
    return x * x


async def main() -> None:
    async with CoroutinePool(workers=3, queue_size=5) as pool:
        futures = [await pool.submit(async_square, i) for i in range(10)]
        results = await asyncio.gather(*futures)
        print(results)


if __name__ == "__main__":
    asyncio.run(main())
```

思考题：

- coroutine pool 和 thread pool 的结构有哪些相同点？
- 为什么 `submit()` 是 async 函数？
- 如果提交者取消了 future，worker 应该怎么处理？

### 6.8 asyncio 的底层原理：事件循环、Task 和协作式调度

`asyncio` 和 `threading` 最大的区别是：线程主要由操作系统抢占式调度，协程主要由事件循环协作式调度。

#### 事件循环是什么

事件循环 event loop 可以理解成一个调度器。它不断做几件事：

1. 看哪些定时器到期了。
2. 看哪些 IO 事件准备好了，例如 socket 可读、可写。
3. 找到可以继续运行的 task。
4. 运行 task，直到它遇到下一个 `await`。
5. task 让出控制权后，事件循环继续调度别的 task。

简化模型：

```text
event loop
  -> 运行 task A，直到 await
  -> 运行 task B，直到 await
  -> IO 返回，唤醒 task A
  -> 继续运行 task A
```

#### coroutine、Task、Future 的关系

几个词容易混：

- coroutine：调用 `async def` 得到的协程对象，表示一段可暂停的计算。
- Task：事件循环对 coroutine 的包装；被包装成 Task 后，协程才会被调度执行。
- Future：一个“未来会有结果”的占位对象，Task 本身也是 Future 的一种。

```python
coro = fetch(1)                  # coroutine，还没被调度
task = asyncio.create_task(coro) # Task，交给事件循环调度
result = await task              # 等它完成并拿结果
```

#### await 到底做了什么

`await` 的核心含义是：当前 coroutine 暂停，把控制权交回事件循环，等被 await 的对象完成后再恢复。

所以：

- `await asyncio.sleep(1)` 不会阻塞整个线程，只暂停当前 task。
- `time.sleep(1)` 会阻塞事件循环所在的线程，所有 task 都被卡住。
- async 并发能高效处理大量 IO，是因为等待 IO 时 task 会主动让出控制权。

#### asyncio 为什么适合高并发 IO

线程模型里，如果有 10000 个连接，可能需要很多线程或复杂线程池调度。asyncio 可以在一个线程里管理大量连接，因为大部分连接都处于“等待 IO”状态，不需要真的占用线程执行 Python 代码。

但它的前提是：你调用的库也要是 async 友好的。例如 async HTTP 客户端、async DB 客户端。如果你在 async 函数里调用阻塞 SDK，事件循环还是会卡住。

#### asyncio 也会有并发 bug

asyncio 通常在单线程里运行，少了很多传统线程的数据竞争，但不代表没有并发问题。

只要你在修改共享状态的过程中 `await`，其它 task 就可能插进来：

```python
items = []


async def add_item(x: int) -> None:
    if x not in items:
        await asyncio.sleep(0)
        items.append(x)
```

这里 `if x not in items` 和 `items.append(x)` 中间有 `await`，其它 task 可能趁机修改 `items`。所以 async 代码也要注意共享状态，只是切换点更明确：通常发生在 `await`。

#### threading 和 asyncio 的核心对比

| 维度 | threading | asyncio |
| --- | --- | --- |
| 调度者 | 操作系统 | 事件循环 |
| 切换时机 | 可能在很多地方被抢占 | 主要在 `await` 处让出 |
| 适合 | 同步阻塞 IO、同步 SDK | async IO、大量连接 |
| 共享状态风险 | 高，需要锁 | 仍然存在，但切换点更清楚 |
| CPU 密集任务 | 不适合靠线程提速 | 会阻塞事件循环，更不适合 |

一句话：threading 是“多条 OS 线程一起干活”，asyncio 是“一个事件循环调度很多可暂停任务”。


## 7. 网络编程：socket、HTTP、协议和并发服务端

并发和网络编程经常一起出现。原因很简单：网络 IO 大部分时间都在等。

你写一个程序请求 100 个接口，如果同步串行执行，就会把大量时间浪费在“等对方返回”上。线程池和 `asyncio` 最常见的用武之地，就是让程序在等待某个网络请求时，继续处理其它连接或其它请求。

这一章先不追求框架，而是从最底层的 socket 建立网络编程心智模型：客户端怎么连、服务端怎么监听、TCP 为什么要自己设计消息边界、HTTP 和 socket 是什么关系、并发服务端怎么写。

### 7.1 网络编程到底在写什么

网络编程本质上是在写两个程序之间的通信规则。

最小模型：

```text
client socket <---- network ----> server socket
```

你需要回答几个问题：

- 对方在哪里：IP 或域名。
- 对方哪个入口：端口 port。
- 用什么传输协议：TCP 还是 UDP。
- 发什么格式的数据：文本、JSON、二进制、自定义协议、HTTP。
- 怎么判断一条消息结束：换行、固定长度、长度前缀、HTTP 头。
- 等多久算失败：连接超时、读取超时、整体 deadline。
- 同时来了很多连接怎么办：线程池、进程池、selector、asyncio。

常见术语：

| 概念 | 小白理解 |
| --- | --- |
| IP | 一台机器在网络里的地址 |
| port | 一台机器上某个程序的入口号 |
| socket | 程序和网络之间的读写对象 |
| TCP | 可靠、有连接、字节流协议 |
| UDP | 无连接、消息报协议，不保证可靠到达 |
| HTTP | 基于 TCP 之上的应用层协议，规定了请求和响应格式 |

一句话：socket 是底层通信管道，HTTP 是建立在管道上的一套应用层规则。

### 7.2 TCP socket 客户端：连接、发送、接收、关闭

先写一个最小 TCP 客户端。它连接本机 `127.0.0.1:9000`，发送一段 bytes，再读取返回。

```python
import socket


def tcp_client() -> None:
    with socket.create_connection(("127.0.0.1", 9000), timeout=3) as sock:
        sock.sendall(b"hello")
        data = sock.recv(1024)
        print(data)
```

几个关键点：

- `socket.create_connection(...)` 会创建 socket 并连接服务端。
- `timeout=3` 表示连接或读写不能无限卡住。
- `sendall()` 会尽量把所有 bytes 发出去；不要用 `send()` 假设一次能发完。
- `recv(1024)` 最多读取 1024 字节，不代表一定读到完整消息。
- `with` 退出时会关闭 socket。

网络传输的是 bytes，不是 Python 字符串。字符串要先编码：

```python
message = "你好"
data = message.encode("utf-8")
text = data.decode("utf-8")
```

### 7.3 TCP socket 服务端：bind、listen、accept

服务端要做三件事：

1. 绑定地址和端口。
2. 监听连接。
3. 接受客户端连接并处理数据。

最小 echo server：

```python
import socket


def echo_server() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("127.0.0.1", 9000))
        server.listen()
        print("listening on 127.0.0.1:9000")

        while True:
            conn, addr = server.accept()
            with conn:
                print("client:", addr)
                data = conn.recv(1024)
                if data:
                    conn.sendall(b"echo: " + data)
```

问题：这个版本一次只能处理一个客户端。如果一个客户端连接后迟迟不发数据，服务端就会卡在 `conn.recv()`，后面的客户端即使连上了也要等。

这就是并发服务端要解决的问题。

### 7.4 TCP 是字节流：为什么会有粘包和拆包

这是网络编程最重要的坑之一：**TCP 没有消息边界。**

你以为自己发送的是：

```text
sendall(b"hello")
sendall(b"world")
```

接收方看到的可能是：

```text
recv() -> b"helloworld"
```

也可能是：

```text
recv() -> b"hel"
recv() -> b"loworld"
```

原因：TCP 保证的是字节按顺序可靠到达，不保证你每次 `sendall()` 对应一次 `recv()`。

所以真实协议必须设计“如何切分消息”。常见方案：

| 方案 | 思路 | 适合场景 |
| --- | --- | --- |
| 固定长度 | 每条消息固定 N 字节 | 协议很简单、字段固定 |
| 分隔符 | 用 `\n` 或特殊符号分隔 | 文本协议、日志协议 |
| 长度前缀 | 先发消息长度，再发消息内容 | 通用、最常见 |
| HTTP 格式 | 用 header 里的长度、chunk 等规则 | Web/接口请求 |

下面实现一个长度前缀协议：先发 4 字节无符号整数表示消息长度，再发正文。

```python
import socket
import struct


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_frame(sock: socket.socket) -> bytes:
    header = recv_exact(sock, 4)
    length = struct.unpack("!I", header)[0]
    return recv_exact(sock, length)
```

`!I` 的意思是：网络字节序、4 字节无符号整数。你不需要一开始背 `struct`，先记住它能把整数打包成固定长度的 bytes。

### 7.5 阻塞、timeout 和并发服务端

socket 默认是阻塞的：

- `accept()` 没有客户端连接时会等。
- `recv()` 没有数据时会等。
- `sendall()` 对方接收很慢时也可能等。

阻塞本身不是错，但必须有边界。网络代码里几乎所有等待都应该考虑 timeout。

```python
sock.settimeout(3)
```

并发服务端常见写法有三类：

| 模型 | 思路 | 适合 |
| --- | --- | --- |
| one thread per connection | 每个连接一个线程 | 小规模、教学、简单工具 |
| thread pool | accept 后把连接交给线程池 | 同步 socket 服务端、连接数可控 |
| asyncio streams | 一个事件循环管理大量连接 | 高并发 IO、async 生态 |

线程池服务端结构：

```text
main thread: accept connection
        |
        v
ThreadPoolExecutor.submit(handle_client, conn, addr)
        |
        v
worker thread: recv -> process -> send
```

这个模型非常适合连接第 4 章线程池：服务端主线程只负责接客，真正耗时的读写交给 worker。

### 7.6 HTTP：建立在 TCP 上的应用层协议

HTTP 不是另一种 socket。HTTP 通常跑在 TCP 之上，它规定了请求和响应长什么样。

一个极简 HTTP 请求大概是：

```http
GET / HTTP/1.1
Host: example.com
Connection: close

```

用原始 socket 发 HTTP 请求：

```python
import socket


request = (
    "GET / HTTP/1.1\r\n"
    "Host: example.com\r\n"
    "Connection: close\r\n"
    "\r\n"
)

with socket.create_connection(("example.com", 80), timeout=5) as sock:
    sock.sendall(request.encode("ascii"))
    chunks: list[bytes] = []
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)

print(b"".join(chunks).decode("iso-8859-1")[:500])
```

真实开发不要手写 HTTP 解析，优先用成熟客户端。标准库可以用 `urllib.request`：

```python
from urllib.request import urlopen


with urlopen("https://example.com", timeout=5) as response:
    body = response.read()
    print(response.status, len(body))
```

如果项目允许第三方库，业务代码里更常见的是 `requests` 或 async HTTP 客户端。但本教程为了降低依赖，示例优先使用标准库。

### 7.7 用线程池并发请求多个 URL

网络 IO 是线程池的典型场景。下面用标准库 `urllib.request` 模拟批量抓取 URL：

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen


def fetch_url(url: str) -> tuple[str, int]:
    with urlopen(url, timeout=5) as response:
        body = response.read()
        return url, len(body)


urls = [
    "https://example.com",
    "https://www.python.org",
]

with ThreadPoolExecutor(max_workers=4) as pool:
    futures = [pool.submit(fetch_url, url) for url in urls]
    for future in as_completed(futures):
        url, size = future.result()
        print(url, size)
```

工程提醒：不要把 `max_workers` 设置得很大然后无限请求同一个站点。真实系统要考虑连接池、限流、重试、超时和错误隔离。

### 7.8 asyncio streams：async 版本 TCP 客户端和服务端

`asyncio` 提供了更高层的 streams API，让你用 `async/await` 写 TCP 程序。

async echo server：

```python
import asyncio


async def handle_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.readline():
            writer.write(b"echo: " + data)
            await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def main() -> None:
    server = await asyncio.start_server(handle_echo, "127.0.0.1", 9000)
    async with server:
        await server.serve_forever()


asyncio.run(main())
```

async client：

```python
import asyncio


async def client() -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", 9000)
    writer.write(b"hello\n")
    await writer.drain()
    data = await reader.readline()
    print(data)
    writer.close()
    await writer.wait_closed()


asyncio.run(client())
```

注意：这里用 `readline()` 是因为协议选择了“换行分隔消息”。如果你的协议是长度前缀，就要按长度读取。

### Lab 4：线程池版 TCP Echo Server

目标：实现一个线程池版 TCP echo server。它要支持多个客户端并发连接，并用“4 字节长度前缀”解决 TCP 消息边界问题。

这个 lab 对应第 4 章线程池和第 7 章网络编程。你要练的是：

- 用 socket 写服务端和客户端。
- 理解 `sendall()` 和 `recv()` 不等价于“一条消息”。
- 用长度前缀协议解决粘包/拆包。
- 用 `ThreadPoolExecutor` 处理多个客户端。
- 支持启动、停止和基础自测。

#### 1 协议设计

我们定义每条消息格式：

```text
4 bytes length + payload bytes
```

例如发送 `b"hello"`：

```text
00 00 00 05 + h e l l o
```

服务端收到 payload 后返回：

```text
b"echo: " + payload
```

#### 2 练习前的骨架

```python
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
import socket
import struct


def recv_exact(sock: socket.socket, n: int) -> bytes:
    ...


def send_frame(sock: socket.socket, payload: bytes) -> None:
    ...


def recv_frame(sock: socket.socket) -> bytes | None:
    ...


def handle_client(conn: socket.socket, addr: tuple[str, int]) -> None:
    ...


class EchoServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, max_workers: int = 8) -> None:
        ...

    @property
    def address(self) -> tuple[str, int]:
        ...

    def start(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def _serve(self) -> None:
        ...


def echo_request(address: tuple[str, int], message: str) -> str:
    ...
```

#### 3 参考解法

```python
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Event, Thread
import socket
import struct


def recv_exact(sock: socket.socket, n: int) -> bytes:
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ConnectionError("connection closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)


def recv_frame(sock: socket.socket) -> bytes | None:
    try:
        header = recv_exact(sock, 4)
    except ConnectionError:
        return None

    length = struct.unpack("!I", header)[0]
    if length > 1024 * 1024:
        raise ValueError("frame too large")
    return recv_exact(sock, length)


def handle_client(conn: socket.socket, addr: tuple[str, int]) -> None:
    with conn:
        conn.settimeout(10)
        while True:
            payload = recv_frame(conn)
            if payload is None:
                return
            send_frame(conn, b"echo: " + payload)


class EchoServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0, max_workers: int = 8) -> None:
        self._stop = Event()
        self._pool = ThreadPoolExecutor(max_workers=max_workers)
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((host, port))
        self._server.listen()
        self._server.settimeout(0.2)
        self._thread = Thread(target=self._serve, name="echo-server")

    @property
    def address(self) -> tuple[str, int]:
        host, port = self._server.getsockname()
        return host, port

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join()
        self._server.close()
        self._pool.shutdown(wait=True, cancel_futures=True)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, addr = self._server.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self._pool.submit(handle_client, conn, addr)


def echo_request(address: tuple[str, int], message: str) -> str:
    with socket.create_connection(address, timeout=3) as sock:
        sock.settimeout(3)
        send_frame(sock, message.encode("utf-8"))
        response = recv_frame(sock)
        assert response is not None
        return response.decode("utf-8")


def test_echo_server() -> None:
    server = EchoServer()
    server.start()
    try:
        assert echo_request(server.address, "hello") == "echo: hello"
        assert echo_request(server.address, "python") == "echo: python"
    finally:
        server.stop()


if __name__ == "__main__":
    test_echo_server()
    print("echo server test passed")
```

#### 4 并发自测

把多个客户端也放进线程池，验证服务端能并发处理请求：

```python
from concurrent.futures import ThreadPoolExecutor, as_completed


def test_many_clients() -> None:
    server = EchoServer(max_workers=4)
    server.start()
    try:
        messages = [f"msg-{i}" for i in range(20)]
        with ThreadPoolExecutor(max_workers=10) as clients:
            futures = [
                clients.submit(echo_request, server.address, message)
                for message in messages
            ]
            results = [future.result() for future in as_completed(futures)]

        assert sorted(results) == sorted(f"echo: {message}" for message in messages)
    finally:
        server.stop()
```

#### 5 关键细节解释

为什么要用长度前缀？

因为 TCP 是字节流。`recv(1024)` 可能读到半条消息、一条消息，也可能读到多条消息拼在一起。长度前缀让接收方先知道“这一条消息到底有多长”。

为什么 `recv_exact()` 要循环？

因为一次 `recv(n)` 不保证返回 n 字节。它只能表示“最多读 n 字节”。要读满一条协议消息，就必须循环读到指定长度。

为什么 accept 线程不直接处理客户端？

如果主线程直接处理某个客户端，它就不能及时接受新连接。把连接丢给线程池后，主线程可以继续 `accept()`。

为什么服务端 socket 要 `settimeout(0.2)`？

这样 `_serve()` 可以定期醒来检查 `_stop`，否则它可能永远卡在 `accept()`，导致 `stop()` 等很久。

为什么 payload 要限制最大长度？

网络服务端不能相信客户端。如果客户端声称自己要发 10GB 数据，服务端不能傻傻分配内存或一直等待。这里用 1MB 做教学示例。

思考题：

- 如果客户端发了 header 后就断开，服务端会发生什么？
- 如果 `handle_client()` 抛异常，线程池会如何处理？你如何记录日志？
- 如何给服务端增加 JSON 协议，比如 payload 是 `{"action": "upper", "text": "hello"}`？
- 如何把这个线程池版服务端改写成 `asyncio.start_server()` 版本？

## 8. 并发工程化：限流、背压、超时、取消、关闭

### 8.1 限流

限流回答的问题是：同一时间最多允许多少任务执行某段逻辑。

工具：

- 线程：`Semaphore`
- asyncio：`asyncio.Semaphore`
- 线程池：`max_workers`
- HTTP/DB 客户端：连接池大小

### 8.2 背压

背压回答的问题是：下游慢的时候，上游要不要继续疯狂生产。

工具：

- 线程：`Queue(maxsize=n)`
- asyncio：`asyncio.Queue(maxsize=n)`
- 自定义池：有界任务队列

### 8.3 超时

没有超时的并发程序很容易永久卡死。

常见层次：

- 单次调用超时：一次 HTTP、一次 RPC、一次数据库查询。
- 队列等待超时：获取任务或投递任务不能无限等。
- 整体 deadline：整个业务请求最多 2 秒。

### 8.4 取消

取消回答的问题是：任务已经没有意义了，如何尽快停止并释放资源。

线程不能被外部安全强杀，所以通常使用协作式停止：`Event`、sentinel、状态位。

asyncio 支持 task cancellation，但 coroutine 要正确处理 `CancelledError` 和 `finally`。

### 8.5 关闭流程

典型关闭顺序：

1. 停止接收新任务。
2. 通知 worker 退出。
3. 等待队列清空或到达 deadline。
4. 取消剩余任务。
5. 关闭连接、文件、线程池、进程池。

## 9. 并发选型速查表

| 问题 | 优先选择 | 原因 |
| --- | --- | --- |
| 调用同步 HTTP/DB SDK，主要时间在等待 | 线程池 | 改造成本低，能隐藏 IO 等待 |
| 调用原生 async HTTP/DB SDK，连接数很多 | asyncio | 单线程管理大量 IO，切换成本低 |
| 纯 Python 大循环计算 | 进程池 | 使用多核，绕开传统 GIL 限制 |
| NumPy/Pandas 等 C 扩展计算 | 先基准测试 | C 扩展可能释放 GIL，也可能内部已并行 |
| 需要任务结果和异常传播 | Future / Task | 结果、异常、取消都有统一句柄 |
| 下游服务有限流 | Semaphore + 超时 | 控制并发量，失败可控 |
| 上游生产快，下游消费慢 | 有界 Queue | 背压，保护内存 |
| 任务需要优雅停止 | sentinel / Event / cancellation | 明确关闭协议 |

## 10. 常见错误清单

### 10.1 在 async 函数里调用阻塞函数

```python
async def handler() -> None:
    requests.get("https://example.com")
```

修复：用 async HTTP 客户端，或临时 `await asyncio.to_thread(...)`。

### 10.2 创建 task 后不 await

```python
asyncio.create_task(do_work())
```

这会让异常和生命周期都变得不清晰。修复：用 `TaskGroup`，或保存 task 并在关闭时 await/cancel。

### 10.3 Queue 放了 sentinel 但忘记 task_done

如果 consumer 对 sentinel 没有调用 `task_done()`，`queue.join()` 可能永远等下去。

### 10.4 进程池里提交 lambda 或局部函数

很多情况下它们不能被 pickle。把函数定义到模块顶层。

### 10.5 在线程池里做大量 CPU 计算

传统 CPython 下，这通常不会让纯 Python 代码跑满多核。用进程池或专门的数值计算库。

### 10.6 无限制创建任务

```python
tasks = [asyncio.create_task(fetch(url)) for url in huge_url_list]
```

如果 `huge_url_list` 有百万级元素，内存和下游都会被打爆。修复：使用队列、Semaphore、有界提交。

## 11. 继续练习路线

第 1 阶段：能跑起来

- 理解线程、锁、队列。
- 完成 Lab 1 和 Lab 2。
- 能解释 `Lock`、`Condition`、`Queue`、`Future`。

第 2 阶段：能选模型

- 把一个同步 IO 示例分别改成线程池和 asyncio。
- 把一个 CPU 示例改成进程池。
- 能解释为什么 IO 密集和 CPU 密集选型不同。

第 3 阶段：能工程化

- 给所有外部调用加超时。
- 给并发数量加上限。
- 给队列加 maxsize。
- 设计 shutdown 流程。
- 完成 Lab 4，能解释 TCP 字节流、消息边界和线程池服务端。

第 4 阶段：理解底层结构

- 完成手写线程池。
- 完成手写协程池。
- 尝试增加 `map()`、取消、统计指标、重试队列。

进一步挑战：

- 给生产者消费者增加 retry queue 和 dead-letter queue。
- 给 async pipeline 增加 rate limiter。
- 给线程池增加 `map(fn, iterable)`。
- 给线程池增加任务取消：未开始时可以取消，已开始时不能强杀线程。
- 给 Lab 4 增加 JSON 协议、日志、连接数统计和 graceful shutdown deadline。
- 用真实 HTTP 客户端替换模拟 sleep，观察吞吐、超时和限流。


## 12. Python 常用数据结构与 collections 速查

并发编程离不开数据结构：队列、集合、字典、计数器、双端队列都很常见。你不需要一开始背完所有 API，但要知道每种结构的特性、适用场景和常见增删改查写法。

这里的“数据结构”包含两类：一类是 Python 内置容器，比如 `list`、`dict`、`set`、`tuple`、`str`；另一类是标准库 `collections` 里更专业的容器，比如 `deque`、`Counter`、`defaultdict`。初学时先掌握内置容器，再把 `collections` 当作工具箱补强。

### 12.1 先建立一张地图

| 数据结构 | 是否有序 | 是否可变 | 是否允许重复 | 擅长什么 |
| --- | --- | --- | --- | --- |
| `list` | 有序 | 可变 | 允许 | 按顺序存一组元素 |
| `tuple` | 有序 | 不可变 | 允许 | 固定记录、函数多返回值 |
| `dict` | 插入有序 | 可变 | key 不重复 | key -> value 映射 |
| `set` | 无序 | 可变 | 不允许 | 去重、集合运算、快速判断存在 |
| `str` | 有序 | 不可变 | 允许 | 文本处理 |
| `deque` | 有序 | 可变 | 允许 | 两端高效插入/删除、队列 |
| `Counter` | 近似 dict | 可变 | key 不重复 | 计数、词频统计 |
| `defaultdict` | 插入有序 | 可变 | key 不重复 | 自动初始化 value |
| `OrderedDict` | 有序 | 可变 | key 不重复 | 需要移动顺序、LRU 类结构 |
| `ChainMap` | 多层映射 | 可变视图 | key 分层查找 | 配置合并、作用域查找 |

选择口诀：

- 要按位置访问：`list` / `tuple`。
- 要根据 key 查 value：`dict`。
- 要去重和集合运算：`set`。
- 要从两头频繁进出：`collections.deque`。
- 要统计次数：`collections.Counter`。
- 要字典 value 自动初始化：`collections.defaultdict`。

### 12.2 list：最常用的有序可变序列

`list` 适合保存一组有顺序的数据，例如任务列表、结果列表、用户列表。

创建：

```python
nums = [1, 2, 3]
empty = []
from_iterable = list(range(3))  # [0, 1, 2]
```

增：

```python
nums.append(4)          # 末尾加一个
nums.extend([5, 6])     # 末尾加多个
nums.insert(0, 0)       # 指定位置插入
```

删：

```python
nums.pop()              # 删除并返回最后一个
nums.pop(0)             # 删除并返回下标 0，注意 list 头部 pop 是 O(n)
nums.remove(3)          # 删除第一个值为 3 的元素
del nums[0]             # 按下标删除
nums.clear()            # 清空
```

改：

```python
nums[0] = 100
nums[1:3] = [200, 300]
```

查：

```python
first = nums[0]
last = nums[-1]
part = nums[1:3]
exists = 100 in nums
idx = nums.index(100)
```

常用操作：

```python
len(nums)
sorted(nums)
nums.sort()
nums.reverse()
[x * 2 for x in nums if x > 0]
```

复杂度和坑：

- `append()` 通常很快，均摊 O(1)。
- 按下标访问 O(1)。
- `pop(0)`、`insert(0, x)` 是 O(n)，因为后面的元素要整体移动。
- 需要频繁从头部弹出时，用 `deque`，不要用 list。
- 多线程共享 list 时，复杂业务逻辑仍要加锁或改用 `Queue`。

### 12.3 tuple：不可变的有序记录

`tuple` 和 `list` 很像，但不可变。它适合表达“固定结构的一条记录”。

```python
point = (3, 4)
name_age = ("Tom", 18)
single = (1,)  # 单元素 tuple 必须有逗号
```

查和解包：

```python
x = point[0]
y = point[1]
x, y = point
```

特点：

- 不可变，所以不能 `append`、不能改元素。
- 如果内部元素也可哈希，tuple 可以作为 dict key 或 set 元素。
- 常用于函数返回多个值。

```python
def divmod_like(a: int, b: int) -> tuple[int, int]:
    return a // b, a % b
```

### 12.4 dict：最重要的 key-value 映射

`dict` 用来根据 key 快速找到 value。

创建：

```python
user = {"id": 1, "name": "Alice"}
empty = {}
from_pairs = dict([("a", 1), ("b", 2)])
```

增/改：

```python
user["age"] = 18
user["name"] = "Bob"
user.update({"city": "Shanghai"})
```

删：

```python
age = user.pop("age")
city = user.pop("city", None)
del user["name"]
user.clear()
```

查：

```python
name = user["name"]      # key 不存在会 KeyError
name = user.get("name")  # key 不存在返回 None
age = user.get("age", 0)
exists = "id" in user
```

遍历：

```python
for key in user:
    print(key, user[key])

for key, value in user.items():
    print(key, value)
```

常用技巧：

```python
value = user.setdefault("tags", [])
value.append("python")

new_dict = {k: v for k, v in user.items() if v is not None}
```

特点和坑：

- Python 3.7+ 中，普通 dict 保持插入顺序。
- key 必须可哈希，例如 str、int、tuple；list 不能作为 key。
- 平均查找、插入、删除是 O(1)。
- 多线程里“先判断再修改”的复合逻辑仍要加锁。

### 12.5 set：去重和集合运算

`set` 是无序、不重复元素集合，适合去重、快速判断是否存在、做交并差。

创建：

```python
tags = {"python", "asyncio"}
empty = set()       # 空集合不能写 {}，{} 是空 dict
unique = set([1, 1, 2, 3])
```

增删查：

```python
tags.add("threading")
tags.update(["queue", "lock"])
tags.remove("python")   # 不存在会 KeyError
tags.discard("missing") # 不存在也不报错
item = tags.pop()
exists = "asyncio" in tags
```

集合运算：

```python
a = {1, 2, 3}
b = {3, 4, 5}

a | b   # 并集 {1, 2, 3, 4, 5}
a & b   # 交集 {3}
a - b   # 差集 {1, 2}
a ^ b   # 对称差集 {1, 2, 4, 5}
```

特点和坑：

- set 无序，不要依赖遍历顺序。
- 元素必须可哈希。
- 判断存在平均 O(1)，适合大列表去重和过滤。

### 12.6 str：不可变文本序列

`str` 是不可变序列。每次拼接、替换都会创建新字符串。

查找：

```python
text = "hello python"
text.startswith("hello")
text.endswith("python")
"py" in text
text.find("python")     # 找不到返回 -1
text.index("python")    # 找不到抛 ValueError
```

拆分、拼接、清洗、替换：

```python
items = "a,b,c".split(",")
line = ",".join(items)
name = "  Alice  ".strip()
lower = "PyThOn".lower()
text.replace("python", "Python")
```

格式化：

```python
name = "Alice"
age = 18
msg = f"{name} is {age} years old"
```

性能提醒：

```python
# 不推荐在大循环里不断 += 拼接字符串
s = ""
for part in parts:
    s += part

# 推荐
s = "".join(parts)
```

### 12.7 collections.deque：双端队列

`deque` 是 double-ended queue，适合从两端高效 append/pop。

```python
from collections import deque

q = deque()
q.append(1)
q.appendleft(0)
q.pop()
q.popleft()
```

常用场景：

- BFS 队列。
- 滑动窗口。
- 最近 N 条记录。

限制长度：

```python
history = deque(maxlen=3)
history.append("a")
history.append("b")
history.append("c")
history.append("d")  # 自动挤掉最旧的 "a"
```

特点：

- 两端 append/pop 是 O(1)。
- 中间随机访问不如 list。
- 单纯线程间任务通信时，优先用 `queue.Queue`，因为它自带阻塞、锁和 `join()` 语义。

### 12.8 collections.Counter：计数器

`Counter` 适合统计频次。

```python
from collections import Counter

words = ["python", "go", "python", "java"]
counter = Counter(words)

counter["python"]       # 2
counter["missing"]      # 0，不会 KeyError
counter.most_common(2)   # [('python', 2), ('go', 1)]
```

更新和运算：

```python
counter.update(["python", "rust"])
counter.subtract(["go"])

a = Counter(a=3, b=1)
b = Counter(a=1, b=2)

a + b   # 计数相加
a - b   # 计数相减，保留正数
a & b   # 每个 key 取最小计数
a | b   # 每个 key 取最大计数
```

适合场景：词频、日志状态码统计、任务结果分类统计。

### 12.9 collections.defaultdict：自动初始化 value

普通 dict 分组时经常写：

```python
groups = {}
for user in users:
    key = user["city"]
    if key not in groups:
        groups[key] = []
    groups[key].append(user)
```

用 `defaultdict(list)` 更简单：

```python
from collections import defaultdict

groups = defaultdict(list)
for user in users:
    groups[user["city"]].append(user)
```

计数也可以：

```python
counts = defaultdict(int)
for word in words:
    counts[word] += 1
```

常见工厂：

```python
defaultdict(list)
defaultdict(set)
defaultdict(int)
defaultdict(dict)
```

坑点：访问不存在的 key 会自动创建默认值。如果你只是想查有没有，使用：

```python
"missing" in groups
groups.get("missing")
```

### 12.10 collections.OrderedDict：需要移动顺序时再用

Python 3.7+ 的普通 dict 已经保持插入顺序，所以很多场景不再需要 `OrderedDict`。

但 `OrderedDict` 仍然有一些特殊能力：

```python
from collections import OrderedDict

od = OrderedDict()
od["a"] = 1
od["b"] = 2
od.move_to_end("a")       # 把 a 移到末尾
od.popitem(last=False)    # 弹出最前面的元素
```

适合：实现简单 LRU、需要频繁移动 key 的顺序。

### 12.11 collections.ChainMap：多层 dict 查找

`ChainMap` 可以把多个 dict 串起来查找，常用于配置优先级。

```python
from collections import ChainMap

cli_args = {"debug": True}
env_config = {"debug": False, "timeout": 3}
default_config = {"timeout": 10, "retries": 2}

config = ChainMap(cli_args, env_config, default_config)

config["debug"]    # True，优先 cli_args
config["timeout"]  # 3，来自 env_config
config["retries"]  # 2，来自 default_config
```

特点：

- 查找从左到右。
- 写入默认写到第一个 dict。
- 适合配置叠加、作用域链，不适合复杂数据合并。

### 12.12 数据结构和并发的关系

并发编程里，数据结构选错会直接影响正确性和性能。

| 场景 | 推荐结构 | 原因 |
| --- | --- | --- |
| 多线程传任务 | `queue.Queue` | 线程安全、支持阻塞和 join |
| 手写阻塞队列 | `deque + Condition` | 两端操作快，Condition 等条件 |
| async 任务队列 | `asyncio.Queue` | 不阻塞事件循环 |
| 去重任务 id | `set` | 快速判断是否见过 |
| 按 id 存任务结果 | `dict` | key-value 查找快 |
| 统计状态码/错误类型 | `Counter` | 计数方便 |
| 保存最近 N 条日志 | `deque(maxlen=N)` | 自动淘汰旧数据 |

线程安全提醒：

- Python 内置容器不是“业务线程安全”的万能容器。
- 单个操作可能看起来没问题，但“先检查再修改”的复合逻辑需要锁。
- 多线程生产消费者优先用 `queue.Queue`，不要用普通 list 自己忙等。
- async 代码里共享 list/dict 时，如果读写中间有 `await`，也要小心竞态。

### 12.13 速查口诀

```text
list：有序可变，适合顺序数据
tuple：有序不可变，适合固定记录
dict：key-value 映射，查找快
set：去重和集合运算
deque：两端进出快
Counter：统计频次
defaultdict：自动初始化 value
OrderedDict：需要移动顺序时再用
ChainMap：多层配置查找
Queue：线程间传任务
asyncio.Queue：协程间传任务
```

## 官方参考

- Python threading 文档：https://docs.python.org/3/library/threading.html
- Python queue 文档：https://docs.python.org/3/library/queue.html
- Python concurrent.futures 文档：https://docs.python.org/3/library/concurrent.futures.html
- Python multiprocessing 文档：https://docs.python.org/3/library/multiprocessing.html
- Python asyncio 文档：https://docs.python.org/3/library/asyncio.html
- Python asyncio TaskGroup 文档：https://docs.python.org/3/library/asyncio-task.html#task-groups
- Python socket 文档：https://docs.python.org/3/library/socket.html
- Python urllib.request 文档：https://docs.python.org/3/library/urllib.request.html
- PEP 703: Making the Global Interpreter Lock Optional in CPython：https://peps.python.org/pep-0703/
