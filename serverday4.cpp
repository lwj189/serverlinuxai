#include <iostream>
#include <cstring>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>          // fcntl
#include <errno.h>          // EAGAIN
#include <sys/socket.h>
#include <sys/epoll.h>      // epoll
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8888
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

// 请求头最大长度：防止客户端一直发数据却始终不出现 "\r\n\r\n"，
// 导致 read_buffer 无限增长、把内存撑爆（DoS）
#define MAX_HEADER_SIZE 8192

// =========================== 1. 数据结构（和 Day 3 一样） ===========================
struct HttpRequest {
    std::string method;
    std::string uri;
    std::string version;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

// =========================== 2. HTTP 解析器（完全复用 Day 3，一字不改） ===========================
bool parseRequest(const std::string& rawHeader, HttpRequest& req) {
    std::istringstream stream(rawHeader);
    std::string line;
    if (!std::getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    std::istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) return false;
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

// =========================== 3. 响应生成器（完全复用 Day 3，一字不改） ===========================
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

// =========================== 4. Day 4 新增：客户端状态管理 ===========================
// 【编译修复】handleClientRead() 里要用到 epoll 实例，但它原本定义在 main() 内部，
// 对这个函数来说是不可见的局部变量，所以编译会报错：
//     error: 'epoll_fd' was not declared in this scope   （第 88 / 111 / 148 行）
// 这里把它提升为文件级全局变量，由 main() 赋值。
// （更规范的做法是封装成 Server 类，用成员变量取代全局变量）
int g_epoll_fd = -1;

// 每个客户端连接都需要维护一个“读缓冲区”，用来拼凑不完整的TCP包
struct ClientContext {
    std::string read_buffer;    // 存储所有已读但未处理的原始数据
    bool request_ready;         // 标记是否已经拼凑出一个完整的HTTP请求
    HttpRequest req;            // 解析后的请求
};

// 全局Map: fd -> 对应的客户端上下文
std::unordered_map<int, ClientContext> clients;

// =========================== 5. 处理客户端读事件（核心逻辑） ===========================
void handleClientRead(int fd) {
    char buffer[BUFFER_SIZE];
    int n = read(fd, buffer, sizeof(buffer));
    
    if (n <= 0) {
        // 客户端断开或出错
        epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        clients.erase(fd);
        return;
    }

    // 1. 把新读到的数据追加到该客户端的缓冲区里
    ClientContext& ctx = clients[fd];
    ctx.read_buffer.append(buffer, n);

    // 【安全加固】请求头长度上限
    // 如果客户端一直发数据但始终不出现 "\r\n\r\n"，read_buffer 会无限增长 -> 内存耗尽（DoS）。
    // Day3 的 readUntilDoubleCRLF() 也有同样的隐患，这里补上限制。
    if (ctx.read_buffer.size() > MAX_HEADER_SIZE &&
        ctx.read_buffer.find("\r\n\r\n") == std::string::npos) {
        std::string resp = buildResponse(431, "<h1>431 Request Header Fields Too Large</h1>");
        send(fd, resp.c_str(), resp.size(), 0);
        epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        clients.erase(fd);
        return;
    }
    
    // 2. 尝试从缓冲区里切出一个完整的 HTTP 请求（处理半包）
    //    如果缓冲区包含 "\r\n\r\n"，说明头部完整了
    size_t header_end = ctx.read_buffer.find("\r\n\r\n");
    if (header_end == std::string::npos) {
        return; // 头部还没收全，继续等 epoll 通知
    }

    // 3. 解析头部
    std::string header_part = ctx.read_buffer.substr(0, header_end + 4);
    if (!parseRequest(header_part, ctx.req)) {
        // 解析失败，返回 400 并关闭
        std::string resp = buildResponse(400, "<h1>400 Bad Request</h1>");
        send(fd, resp.c_str(), resp.size(), 0);
        epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
        close(fd);
        clients.erase(fd);
        return;
    }

    // 4. 检查 Content-Length，判断 Body 是否收全
    size_t contentLen = 0;
    auto it = ctx.req.headers.find("Content-Length");
    if (it != ctx.req.headers.end()) {
        try {
            contentLen = std::stoul(it->second);
        } catch (const std::exception&) {
            // Content-Length 不是合法数字：Day3 有这层保护，Day4 漏掉了，这里补回来。
            // 如果不捕获，std::stoul 抛出的异常会直接终止整个服务器进程。
            std::string resp = buildResponse(400, "<h1>400 Invalid Content-Length</h1>");
            send(fd, resp.c_str(), resp.size(), 0);
            epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
            close(fd);
            clients.erase(fd);
            return;
        }
    }

    // 计算当前缓冲区里 body 部分的大小
    size_t body_start = header_end + 4;
    size_t body_received = ctx.read_buffer.size() - body_start;
    
    if (body_received < contentLen) {
        return; // Body 还没收全，继续等 epoll 通知（这就是非阻塞+缓冲区的精髓！）
    }

    // 5. Body 收全了！从缓冲区里截取出 Body
    ctx.req.body = ctx.read_buffer.substr(body_start, contentLen);
    ctx.request_ready = true;

    // 6. 路由分发（和 Day 3 一模一样）
    std::string response;
    if (ctx.req.method == "GET" && ctx.req.uri == "/") {
        response = buildResponse(200, "<h1>🏠 Day4 Home</h1><p>epoll works!</p>");
    } else if (ctx.req.method == "POST" && ctx.req.uri == "/submit") {
        response = buildResponse(200, "<h1>POST OK</h1><p>Received: " + ctx.req.body + "</p>");
    } else {
        response = buildResponse(404, "<h1>404 Not Found</h1>");
    }

    // 7. 发送响应，然后关闭连接（清理资源）
    send(fd, response.c_str(), response.size(), 0);
    epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    clients.erase(fd);
}

// =========================== 6. 主函数 ===========================
int main() {
    signal(SIGPIPE, SIG_IGN);

    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);
    
    // ---------- 6.1 创建、绑定、监听（复制 Day 2） ----------
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

    // ---------- 6.2 设置监听套接字为非阻塞 ----------
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    // ---------- 6.3 创建 epoll 实例 ----------
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        perror("epoll_create1");
        exit(EXIT_FAILURE);
    }
    g_epoll_fd = epoll_fd;   // 赋值给全局变量，让 handleClientRead() 能访问同一个 epoll 实例

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
        perror("epoll_ctl: server_fd");
        exit(EXIT_FAILURE);
    }

    std::cout << "🚀 Day 4 Server (epoll) running on port " << PORT << std::endl;
    std::cout << "📌 Open 2 terminals and run: curl http://127.0.0.1:" << PORT << "/" << std::endl;

    // ---------- 6.4 主事件循环 ----------
    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            // --- 情况 A：新客户端连接 ---
            if (fd == server_fd) {
                while (true) {
                    struct sockaddr_in client_addr;
                    socklen_t len = sizeof(client_addr);
                    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }
                    // 设置客户端为非阻塞
                    int flags_c = fcntl(client_fd, F_GETFL, 0);
                    fcntl(client_fd, F_SETFL, flags_c | O_NONBLOCK);

                    // 注册到 epoll
                    ev.events = EPOLLIN | EPOLLRDHUP;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);

                    // 初始化客户端上下文（清空缓冲区）
                    clients[client_fd] = ClientContext();
                    std::cout << "✅ New client connected, fd=" << client_fd << std::endl;
                }
            } 
            // --- 情况 B：已有客户端发来数据 ---
            else {
                handleClientRead(fd);
            }
        }
    }

    close(server_fd);
    return 0;
}