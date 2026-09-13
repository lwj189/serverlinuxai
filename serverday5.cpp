#include <iostream>
#include <cstring>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// ======= Day 5 新增：C++11 并发库头文件 =======
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <vector>

#define PORT 8888
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

// 请求头 / 请求体的安全上限：防止客户端一直发数据把内存撑爆（DoS）
#define MAX_HEADER_SIZE 8192
#define MAX_BODY_SIZE (1024 * 1024)

// =========================== 1. 数据结构 ===========================
struct HttpRequest {
    std::string method;
    std::string uri;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// =========================== 2. 完整的解析器（从 Day 3 复制） ===========================
bool parseRequest(const std::string& rawHeader, HttpRequest& req) {
    std::istringstream stream(rawHeader);
    std::string line;

    // 解析请求行
    if (!std::getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back();

    std::istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) {
        return false;
    }

    // 解析请求头
    while (std::getline(stream, line)) {
        if (line == "\r" || line.empty()) break;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string key = line.substr(0, colon);
            std::string value = line.substr(colon + 1);
            size_t start = value.find_first_not_of(" ");
            if (start != std::string::npos) value = value.substr(start);
            req.headers[key] = value;
        }
    }
    return true;
}

// =========================== 3. 完整的响应生成器（从 Day 3 复制） ===========================
std::string buildResponse(int statusCode, const std::string& body) {
    std::string statusLine;
    switch (statusCode) {
        case 200: statusLine = "HTTP/1.1 200 OK\r\n"; break;
        case 400: statusLine = "HTTP/1.1 400 Bad Request\r\n"; break;
        case 404: statusLine = "HTTP/1.1 404 Not Found\r\n"; break;
        case 413: statusLine = "HTTP/1.1 413 Payload Too Large\r\n"; break;
        case 431: statusLine = "HTTP/1.1 431 Request Header Fields Too Large\r\n"; break;
        default:  statusLine = "HTTP/1.1 500 Internal Server Error\r\n"; break;
    }

    std::string response = statusLine;
    response += "Content-Type: text/html; charset=utf-8\r\n";
    response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    response += "Connection: close\r\n";
    response += "\r\n";
    response += body;
    return response;
}

// =========================== 4. 线程池类 ===========================
class ThreadPool {
public:
    ThreadPool(size_t numThreads) : stop(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });
                        if (this->stop && this->tasks.empty()) return;
                        task = std::move(this->tasks.front());
                        this->tasks.pop();
                    }
                    task();
                }
            });
        }
    }

    template<class F>
    void enqueue(F&& f) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            tasks.emplace(std::forward<F>(f));
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (std::thread& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

private:
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queue_mutex;
    std::condition_variable condition;
    std::atomic<bool> stop;
};

// =========================== 5. 全局变量 ===========================
int epoll_fd_global = -1;
ThreadPool* g_pool = nullptr;

// ---------- 【Day5 修复】每个连接的读缓冲区：把 Day4 的半包处理补回来 ----------
// Day5 原来为了"简化"，一遇到"一次没读全"就直接 close 掉连接，
// 等于把 Day4 已经解决的半包问题又丢掉了（功能倒退）。
// 正确做法：为每个连接保存一个缓冲区，主线程把字节流攒成完整的 HTTP 请求后再交给线程池。
struct ClientContext {
    std::string read_buffer;    // 已收到、但还没组成完整请求的原始字节
    HttpRequest req;            // 解析后的请求
    bool   header_done = false; // 请求头是否已解析成功
    size_t header_end  = 0;     // 头部结束位置（"\r\n\r\n" 之后的下标）
    size_t body_need   = 0;     // Content-Length，body 一共需要多少字节
};

std::unordered_map<int, ClientContext> g_clients;

// 统一关闭一个客户端连接：从 epoll 摘除 -> close -> 清理上下文
void closeClient(int fd) {
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    g_clients.erase(fd);
}

// =========================== 6. 业务处理（在子线程中执行） ===========================
void handleBusiness(int fd, const HttpRequest& req) {
    // 模拟耗时业务（睡眠3秒）
    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::string response;
    if (req.method == "GET" && req.uri == "/") {
        response = buildResponse(200, "<h1>🏠 Day5 Home</h1><p>Processed by thread: " + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "</p>");
    } else if (req.method == "POST" && req.uri == "/submit") {
        response = buildResponse(200, "<h1>POST OK</h1><p>Received: " + req.body + " (handled by thread)</p>");
    } else {
        response = buildResponse(404, "<h1>404 Not Found</h1>");
    }

    send(fd, response.c_str(), response.size(), 0);
    // 【并发修复】这里不再调用 epoll_ctl 摘除 fd。
    // 主线程在把任务交给线程池之前就已经把 fd 从 epoll 摘掉了（见 handleClientRead），
    // 职责划分清楚：主线程管 epoll，工作线程只管业务和收发。
    close(fd);

    std::cout << "✅ Task completed on fd=" << fd << std::endl;
}

// =========================== 7. 处理客户端读事件（主线程执行） ===========================
// 【Day5 修复】把 Day4 的「读缓冲区 + 半包判断」补回来。
// 主线程只做 IO 与解析：数据不完整就返回、继续等 epoll 通知，
// 绝不因为"一次没读全"就关掉连接。
void handleClientRead(int fd) {
    ClientContext& ctx = g_clients[fd];
    char buffer[BUFFER_SIZE];

    // --- 1. 循环读取，直到内核缓冲区读空（EAGAIN）---
    // 非阻塞 fd 上必须循环读：一次 read 可能只拿到半个请求，
    // 也可能一次拿到好几个请求的字节。读到 EAGAIN 才说明这一轮真没数据了。
    while (true) {
        ssize_t n = read(fd, buffer, sizeof(buffer));

        if (n > 0) {
            ctx.read_buffer.append(buffer, n);

            // 安全上限：头部一直收不全却已经超过上限 -> 判定为异常/恶意请求
            if (!ctx.header_done && ctx.read_buffer.size() > MAX_HEADER_SIZE) {
                std::string resp = buildResponse(431, "<h1>431 Request Header Fields Too Large</h1>");
                send(fd, resp.c_str(), resp.size(), 0);
                closeClient(fd);
                return;
            }
            if (ctx.read_buffer.size() > MAX_HEADER_SIZE + MAX_BODY_SIZE) {
                std::string resp = buildResponse(413, "<h1>413 Payload Too Large</h1>");
                send(fd, resp.c_str(), resp.size(), 0);
                closeClient(fd);
                return;
            }
            continue;   // 可能还有数据，继续读
        }

        if (n == 0) {
            closeClient(fd);   // 对端正常关闭（收到 FIN）
            return;
        }

        // n < 0：区分「暂时没数据」和「真错误」
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // 读完了，正常退出循环
        if (errno == EINTR) continue;                        // 被信号打断，重试
        closeClient(fd);                                     // 真正的错误
        return;
    }

    // --- 2. 请求头是否完整？（解决"头部半包"）---
    if (!ctx.header_done) {
        size_t pos = ctx.read_buffer.find("\r\n\r\n");
        if (pos == std::string::npos) {
            return;   // ★ 头部还没收全：保留缓冲区，等下次 epoll 通知（半包处理的核心）
        }

        std::string header_part = ctx.read_buffer.substr(0, pos + 4);
        if (!parseRequest(header_part, ctx.req)) {
            std::string resp = buildResponse(400, "<h1>400 Bad Request</h1>");
            send(fd, resp.c_str(), resp.size(), 0);
            closeClient(fd);
            return;
        }

        ctx.header_done = true;
        ctx.header_end  = pos + 4;

        auto it = ctx.req.headers.find("Content-Length");
        if (it != ctx.req.headers.end()) {
            try {
                ctx.body_need = std::stoul(it->second);
            } catch (const std::exception&) {
                std::string resp = buildResponse(400, "<h1>400 Invalid Content-Length</h1>");
                send(fd, resp.c_str(), resp.size(), 0);
                closeClient(fd);
                return;
            }
        }
    }

    // --- 3. 请求体是否完整？（解决"body 半包"）---
    size_t body_have = ctx.read_buffer.size() - ctx.header_end;
    if (body_have < ctx.body_need) {
        return;   // ★ body 还没收全：同样保留缓冲区继续等，不能关连接
    }

    // --- 4. 请求完整了，拷贝出来交给工作线程 ---
    HttpRequest req = ctx.req;
    req.body = ctx.read_buffer.substr(ctx.header_end, ctx.body_need);

    // --- 5. 关键一步：先摘除 epoll，再交给线程池 ---
    // 【并发修复】如果不摘除：fd 仍然挂在 epoll 上（而且是水平触发 LT），
    // 客户端只要再发一点数据，epoll 就会再次通知主线程 -> 主线程又 read/enqueue 同一个 fd，
    // 而此时工作线程可能已经 close(fd) 了 —— 这就是 use-after-close（fd 所有权竞态）。
    // 正确做法：交接之前 EPOLL_CTL_DEL，让这个 fd 之后只归工作线程使用。
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    g_clients.erase(fd);   // 上下文已经用不到了（erase 之后不能再访问 ctx）

    std::cout << "📩 Main thread parsed: " << req.method << " " << req.uri << ", enqueuing task..." << std::endl;

    g_pool->enqueue([fd, req] {
        handleBusiness(fd, req);
    });
}

// =========================== 8. 主函数 ===========================
int main() {
    signal(SIGPIPE, SIG_IGN);

    // 创建线程池
    ThreadPool pool(4);
    g_pool = &pool;

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }
    if (listen(server_fd, 3) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    // 设置非阻塞 & epoll
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    int epoll_fd = epoll_create1(0);
    epoll_fd_global = epoll_fd;

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::cout << "🚀 Day 5 Server (epoll + ThreadPool) running on port " << PORT << std::endl;
    std::cout << "💡 Try: curl -X POST -d 'hello' http://127.0.0.1:" << PORT << "/submit" << std::endl;
    std::cout << "⏳ Main thread will NOT block. Processing logs appear after 3 seconds (simulated work)." << std::endl;

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == server_fd) {
                while (true) {
                    struct sockaddr_in client_addr;
                    socklen_t len = sizeof(client_addr);
                    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    int flags_c = fcntl(client_fd, F_GETFL, 0);
                    fcntl(client_fd, F_SETFL, flags_c | O_NONBLOCK);
                    ev.events = EPOLLIN | EPOLLRDHUP;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    g_clients[client_fd] = ClientContext();   // 为新连接初始化读缓冲区
                    std::cout << "✅ New client connected, fd=" << client_fd << std::endl;
                }
            } else {
                handleClientRead(fd);
            }
        }
    }

    close(server_fd);
    return 0;
}