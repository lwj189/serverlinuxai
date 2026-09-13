# serverlinux

从零实现的 Linux TCP / HTTP 服务器，按学习阶段拆成多个文件递进。
纯 C++17，不依赖任何框架，只用 Linux 系统调用与 C++ 标准库。

## 文件说明

| 文件 | 阶段 | 内容 |
| --- | --- | --- |
| `serverday2.cpp` | Day 2 | 阻塞式 TCP 服务器：`socket → setsockopt → bind → listen → accept → read → send → close`，返回固定 HTTP 响应 |
| `serverday3.cpp` | Day 3 | HTTP 请求解析（请求行 / 请求头 / `Content-Length`）+ **粘包半包处理** + 路由分发（GET / POST / 404） |
| `serverday4.cpp` | Day 4 | **非阻塞 IO + epoll**（水平触发），为每个连接维护读缓冲区 |
| `serverday5.cpp` | Day 5 | **epoll + 线程池**（单 Reactor 模型）：主线程只做 IO 与解析，业务交给工作线程 |
| `serverday6.cpp` | Day 6 | 在 Day5 基础上接入 **MySQL 连接池**与预处理语句（防 SQL 注入） |
| `notes/date.txt` | — | 学习笔记：HTTP 请求解析与 epoll 流程的伪代码 |
| `notes/practice_draft.txt` | — | 学习草稿：早期手写练习与伪代码，**不能编译**（见文件头说明） |
| `tests/test_client.py` | — | 半包 / 并发测试客户端（见下文） |

## 架构（Day 5 / Day 6）

```
                 主线程（Reactor）                    线程池（4 个 worker）
              ┌──────────────────────┐            ┌───────────────────────┐
  客户端 ───▶ │ epoll_wait           │            │                       │
              │   ├─ listen fd → accept             │  业务处理             │
              │   └─ client fd → read │            │  （Day5: 模拟耗时）   │
              │        ↓              │            │  （Day6: 查/写 MySQL）│
              │   累积到读缓冲区       │  enqueue   │        ↓              │
              │   解析完整 HTTP 请求 ──┼───────────▶│   send + close        │
              │   EPOLL_CTL_DEL（摘除）│            │                       │
              └──────────────────────┘            └───────────────────────┘
```

关键设计点：

1. **主线程与工作线程的 fd 所有权交接**
   请求解析完整后，主线程先 `EPOLL_CTL_DEL` 把 fd 从 epoll 摘除，再投递给线程池。
   否则 fd 仍被 epoll 监听（水平触发会反复通知），主线程可能对同一个 fd 重复入队，
   而工作线程此时可能已经 `close(fd)` —— 造成 use-after-close / fd 号被复用。

2. **每连接读缓冲区**
   TCP 是字节流，没有消息边界。一次 `read()` 可能只拿到半个请求（半包），
   也可能拿到好几个请求（粘包）。因此必须按连接累积缓冲区，收满一个完整请求再处理。

3. **安全上限**
   请求头 `MAX_HEADER_SIZE = 8KB`、请求体 `MAX_BODY_SIZE = 1MB`，
   防止客户端持续发送不含 `\r\n\r\n` 的数据把内存撑爆（DoS）。

4. **循环读到 `EAGAIN`**
   非阻塞 fd 上单次 `read` 不代表读完，必须循环读直到返回 `EAGAIN`。

## 编译运行

```bash
# Day2 ~ Day4（无额外依赖）
g++ -std=c++17 serverday4.cpp -o day4 && ./day4

# Day5（需要 pthread）
g++ -std=c++17 -pthread serverday5.cpp -o day5 && ./day5

# Day6（需要 MySQL 客户端库）
#   Ubuntu/Debian: sudo apt install libmysqlclient-dev
g++ -std=c++17 -pthread serverday6.cpp -o day6 -lmysqlclient && ./day6
```

监听端口 `8888`。

```bash
curl http://127.0.0.1:8888/                       # GET  首页
curl -X POST -d 'hello' http://127.0.0.1:8888/submit   # POST 回显（Day3 ~ Day5）
curl 'http://127.0.0.1:8888/users?name=Alice'     # Day6 查询
curl -X POST -d 'name=Bob&age=25' http://127.0.0.1:8888/users   # Day6 插入
```

## 测试：半包与并发

**为什么不能用 curl 测半包？**
curl 会把整个请求一次性发出去，服务器的 `read()` 一次就能拿到完整的 `\r\n\r\n`，
所以永远触发不到"数据不完整"的分支。

`tests/test_client.py` 故意把请求拆成多段、段间 `sleep`，强制制造半包：

```bash
# 终端 1
g++ -std=c++17 -pthread serverday5.cpp -o day5 && ./day5

# 终端 2
python3 tests/test_client.py
```

覆盖 6 个用例：完整请求基线、头部半包、逐字节发送、body 半包、头部超长保护、20 并发。

> 注意：`serverday5.cpp` 里有一个 `sleep(3s)` 用来模拟耗时业务，测试会比较慢，属正常现象。

## 修复记录

这些是开发过程中实际踩到的坑，按发现顺序记录：

| 问题 | 现象 | 原因与修复 |
| --- | --- | --- |
| `serverday4.cpp` 编译失败 | `error: 'epoll_fd' was not declared in this scope`（3 处） | `epoll_fd` 原本定义在 `main()` 内部，`handleClientRead()` 看不见它。提升为文件级全局变量 `g_epoll_fd` |
| Day5 丢失半包处理 | 客户端分片发送时收到空响应 | 引入线程池时把 Day4 的读缓冲区"简化"掉了，遇到不完整数据直接 `close`。补回 `ClientContext` 缓冲区 |
| Day6 同样丢失半包处理 | 同上 | 同上 |
| fd 所有权竞态 | 高并发/慢客户端下可能出现 use-after-close | 主线程入队前未把 fd 从 epoll 摘除。改为交接前 `EPOLL_CTL_DEL` |
| `Content-Length` 非法值 | `std::stoul` 抛异常，整个进程终止 | 加 `try / catch`，返回 400 |
| 请求头无长度上限 | 内存可被恶意撑爆（DoS） | 加 `MAX_HEADER_SIZE` / `MAX_BODY_SIZE`，超限返回 431 / 413 |
| `serverday6.cpp` 头文件被截断 | `#include <cb>` / `#include <uni>`，编译直接失败 | 修正为 `<cstdlib>` / `<unistd.h>` |
| 连接池析构未加锁 | 与工作线程数据竞争 | 析构加锁 + `shutting_down` 标志 + `notify_all` |
| 连接池关闭时连接泄漏 | 借出未归还的连接永不释放 | `returnConnection` 检测到关闭状态时直接 `mysql_close` |

## 已知不足

- **只有水平触发（LT），没有实现 ET**；`EPOLLRDHUP` 注册了但未在事件循环中判断。
- **不支持 keep-alive**：每个请求都 `Connection: close`，无法复用连接。
- **不支持 `Transfer-Encoding: chunked`**，只按 `Content-Length` 读取 body。
- **HTTP 头字段名大小写敏感**：RFC 7230 规定头字段名不区分大小写，当前用
  `headers.find("Content-Length")` 直接查找，客户端发 `content-length` 会漏掉。
- **`send()` 未处理部分写**：未检查返回值并循环发送。
- **单 Reactor**：`accept` 和读事件都在同一个线程，未做多 Reactor（muduo 那样的多线程 Reactor）。
- **全局变量**：`g_epoll_fd` / `g_pool` 等为文件级全局变量，更规范的做法是封装成 `Server` 类。
- **无日志系统**：目前只用 `std::cout` 打印。

## 后续计划

- [ ] 支持 ET（边缘触发）+ `EPOLLONESHOT`
- [ ] 支持 keep-alive 与 `Transfer-Encoding: chunked`
- [ ] HTTP 头字段名改为大小写不敏感
- [ ] `send()` 部分写处理与输出缓冲区
- [ ] 封装 `Server` 类，去掉全局变量
- [ ] 主从 Reactor：独立 accept 线程 + 多个 IO 线程
- [ ] 加入定时器处理空闲连接
