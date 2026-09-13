#include <iostream>
#include <cstring>
#include <string>
#include <unordered_map>
#include <sstream>
#include <cstdlib>                // 【修复】原来是 <cb>，被截断了，会导致编译失败
#include <csignal>
#include <unistd.h>               // 【修复】原来是 <uni>，被截断了，会导致编译失败
#include <fcntl.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <functional>
#include <atomic>
#include <vector>
#include <mysql/mysql.h>          // MySQL C API

// 【兼容处理】MySQL 8.0.34 起移除了 my_bool 类型
//（MySQL 5.7 里 my_bool 是 char，8.0 早期是 bool，8.0.34+ 直接删除）。
// 下面这个别名让代码在 MySQL 5.7 / 8.0.x 上都能编译。
#if defined(MYSQL_VERSION_ID) && MYSQL_VERSION_ID >= 80034
using my_bool_compat = bool;
#else
using my_bool_compat = my_bool;
#endif

using namespace std;
// ---------- 配置 ----------
#define PORT 8888
#define MAX_EVENTS 10
#define BUFFER_SIZE 4096

// 请求头 / 请求体的安全上限：防止客户端一直发数据把内存撑爆（DoS）
#define MAX_HEADER_SIZE 8192
#define MAX_BODY_SIZE (1024 * 1024)

// 数据库连接配置
const char* DB_HOST = "127.0.0.1";
const char* DB_USER = "root";
const char* DB_PASS = "***********";  // 改成你的密码
const char* DB_NAME = "test";
unsigned int DB_PORT = 3306;

// ---------- HTTP 请求结构 ----------
struct HttpRequest {
    string method;
    string uri;
    string version;
    unordered_map<string, string> headers;
    string body;
};

// ---------- HTTP 解析与响应生成（复用 Day3） ----------
bool parseRequest(const string& rawHeader, HttpRequest& req) {
    istringstream stream(rawHeader);
    string line;
    if (!getline(stream, line)) return false;
    if (!line.empty() && line.back() == '\r') line.pop_back();
    istringstream lineStream(line);
    if (!(lineStream >> req.method >> req.uri >> req.version)) return false;
    while (getline(stream, line)) {
        if (line == "\r" || line.empty()) break;
        if (!line.empty() && line.back() == '\r') line.pop_back();
        size_t colon = line.find(':');
        if (colon != string::npos) {
            string key = line.substr(0, colon);
            string value = line.substr(colon + 1);
            size_t start = value.find_first_not_of(" ");
            if (start != string::npos) value = value.substr(start);
            req.headers[key] = value;
        }
    }
    return true;
}

string buildResponse(int statusCode, const string& body) {
    string statusLine;
    switch (statusCode) {
        case 200: statusLine = "HTTP/1.1 200 OK\r\n"; break;
        case 400: statusLine = "HTTP/1.1 400 Bad Request\r\n"; break;
        case 404: statusLine = "HTTP/1.1 404 Not Found\r\n"; break;
        case 413: statusLine = "HTTP/1.1 413 Payload Too Large\r\n"; break;
        case 431: statusLine = "HTTP/1.1 431 Request Header Fields Too Large\r\n"; break;
        default:  statusLine = "HTTP/1.1 500 Internal Server Error\r\n"; break;
    }
    string response = statusLine;
    response += "Content-Type: text/html; charset=utf-8\r\n";
    response += "Content-Length: " + to_string(body.size()) + "\r\n";
    response += "Connection: close\r\n";
    response += "\r\n";
    response += body;
    return response;
}

// ---------- MySQL 连接池 ----------
class ConnectionPool {
public:
    ConnectionPool(size_t poolSize) {
        for (size_t i = 0; i < poolSize; ++i) {
            MYSQL* conn = mysql_init(nullptr);
            if (!conn) {
                cerr << "mysql_init failed" << endl;
                continue;
            }
            if (!mysql_real_connect(conn, DB_HOST, DB_USER, DB_PASS, DB_NAME, DB_PORT, nullptr, 0)) {
                cerr << "Connect failed: " << mysql_error(conn) << endl;
                mysql_close(conn);
                continue;
            }
            pool.push(conn);
        }
        if (pool.empty()) {
            cerr << "No database connections available. Exiting." << endl;
            exit(EXIT_FAILURE);
        }
        cout << "✅ Connection pool initialized with " << pool.size() << " connections." << endl;
    }

    // 【修复 1】加"关闭中"逃逸路径：否则析构时还在等连接的线程会永远阻塞在 cond.wait
    MYSQL* getConnection() {
        unique_lock<mutex> lock(mtx);
        cond.wait(lock, [this] { return !pool.empty() || shutting_down; });
        if (shutting_down || pool.empty()) return nullptr;
        MYSQL* conn = pool.front();
        pool.pop();
        return conn;
    }

    void returnConnection(MYSQL* conn) {
        if (!conn) return;
        unique_lock<mutex> lock(mtx);
        if (shutting_down) {
            // 【修复 2】池已经关闭：直接把这条连接关掉，否则它会永远泄漏
            mysql_close(conn);
            return;
        }
        pool.push(conn);
        cond.notify_one();
    }

    // 【修复 3】析构函数原来没有加锁，会和正在 getConnection / returnConnection 的
    //          工作线程产生数据竞争；这里补上锁，并唤醒所有等待者。
    //          注意：已经借出去、尚未归还的连接不在这里回收（只能靠调用方 returnConnection）。
    //          所以析构顺序很关键：必须让 ThreadPool 先析构（join 掉全部工作线程），
    //          ConnectionPool 再析构。main() 里的声明顺序正好保证了这一点。
    ~ConnectionPool() {
        unique_lock<mutex> lock(mtx);
        shutting_down = true;
        cond.notify_all();
        while (!pool.empty()) {
            mysql_close(pool.front());
            pool.pop();
        }
        cout << "🔒 Connection pool closed." << endl;
    }

private:
    queue<MYSQL*> pool;
    mutex mtx;
    condition_variable cond;
    bool shutting_down = false;   // 池是否正在关闭
};

// ---------- 线程池（复用 Day5） ----------
class ThreadPool {
public:
    ThreadPool(size_t numThreads) : stop(false) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back([this] {
                while (true) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(this->queue_mutex);
                        this->condition.wait(lock, [this] {
                            return this->stop || !this->tasks.empty();
                        });
                        if (this->stop && this->tasks.empty()) return;
                        task = move(this->tasks.front());
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
            unique_lock<mutex> lock(queue_mutex);
            tasks.emplace(forward<F>(f));
        }
        condition.notify_one();
    }

    ~ThreadPool() {
        {
            unique_lock<mutex> lock(queue_mutex);
            stop = true;
        }
        condition.notify_all();
        for (thread& worker : workers) {
            if (worker.joinable()) worker.join();
        }
    }

private:
    vector<thread> workers;
    queue<function<void()>> tasks;
    mutex queue_mutex;
    condition_variable condition;
    atomic<bool> stop;
};

// ---------- 全局变量 ----------
int epoll_fd_global = -1;
ThreadPool* g_thread_pool = nullptr;
ConnectionPool* g_conn_pool = nullptr;

// ---------- 【Day6 修复】每个连接的读缓冲区：把 Day4 的半包处理补回来 ----------
// Day5 / Day6 原来遇到"一次没读全"就直接 close 掉连接，
// 等于把 Day4 已经解决的半包问题又丢掉了（功能倒退）。
// 这里为每个连接保存缓冲区：主线程把字节流攒成完整的 HTTP 请求后，再交给线程池。
struct ClientContext {
    string read_buffer;         // 已收到、但还没组成完整请求的原始字节
    HttpRequest req;            // 解析后的请求
    bool   header_done = false; // 请求头是否已解析成功
    size_t header_end  = 0;     // 头部结束位置（"\r\n\r\n" 之后的下标）
    size_t body_need   = 0;     // Content-Length，body 一共需要多少字节
};

unordered_map<int, ClientContext> g_clients;

// 统一关闭一个客户端连接：从 epoll 摘除 -> close -> 清理上下文
void closeClient(int fd) {
    epoll_ctl(epoll_fd_global, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    g_clients.erase(fd);
}

// ---------- 预处理语句示例：查询用户 ----------
bool queryUserByName(MYSQL* conn, const string& name, string& result) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        result = "mysql_stmt_init failed";
        return false;
    }

    const char* sql = "SELECT id, name, age FROM users WHERE name = ?";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        result = "Prepare failed: " + string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    // 绑定参数
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    char name_buf[256];
    strncpy(name_buf, name.c_str(), sizeof(name_buf) - 1);
    unsigned long name_len = name.length();
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = name_buf;
    bind[0].buffer_length = sizeof(name_buf);
    bind[0].length = &name_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        result = "Bind param failed: " + string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        result = "Execute failed: " + string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    // 绑定结果
    MYSQL_BIND result_bind[3];
    memset(result_bind, 0, sizeof(result_bind));
    int id;
    char name_out[256];
    int age;
    unsigned long id_len, name_len_out, age_len;
    my_bool_compat is_null[3];

    result_bind[0].buffer_type = MYSQL_TYPE_LONG;
    result_bind[0].buffer = &id;
    result_bind[0].length = &id_len;
    result_bind[0].is_null = &is_null[0];

    result_bind[1].buffer_type = MYSQL_TYPE_STRING;
    result_bind[1].buffer = name_out;
    result_bind[1].buffer_length = sizeof(name_out);
    result_bind[1].length = &name_len_out;
    result_bind[1].is_null = &is_null[1];

    result_bind[2].buffer_type = MYSQL_TYPE_LONG;
    result_bind[2].buffer = &age;
    result_bind[2].length = &age_len;
    result_bind[2].is_null = &is_null[2];

    if (mysql_stmt_bind_result(stmt, result_bind) != 0) {
        result = "Bind result failed: " + string(mysql_stmt_error(stmt));
        mysql_stmt_close(stmt);
        return false;
    }

    bool found = false;
    if (mysql_stmt_fetch(stmt) == 0) {
        found = true;
        ostringstream oss;
        oss << "ID: " << id << ", Name: " << name_out << ", Age: " << age;
        result = oss.str();
    }

    mysql_stmt_close(stmt);
    if (!found) {
        result = "User not found";
        return false;
    }
    return true;
}

// ---------- 预处理语句示例：插入用户 ----------
bool insertUser(MYSQL* conn, const string& name, int age) {
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) return false;

    const char* sql = "INSERT INTO users (name, age) VALUES (?, ?)";
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        cerr << "Prepare insert failed: " << mysql_stmt_error(stmt) << endl;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    char name_buf[256];
    strncpy(name_buf, name.c_str(), sizeof(name_buf) - 1);
    unsigned long name_len = name.length();
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = name_buf;
    bind[0].buffer_length = sizeof(name_buf);
    bind[0].length = &name_len;

    bind[1].buffer_type = MYSQL_TYPE_LONG;
    bind[1].buffer = &age;
    bind[1].is_null = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        cerr << "Bind insert failed: " << mysql_stmt_error(stmt) << endl;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        cerr << "Execute insert failed: " << mysql_stmt_error(stmt) << endl;
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    return true;
}

// ---------- 业务处理函数（工作线程执行） ----------
void handleBusiness(int fd, const HttpRequest& req) {
    // 1. 从连接池借一个连接
    MYSQL* conn = g_conn_pool->getConnection();
    if (!conn) {
        // 连接池正在关闭（或拿不到连接）：直接回 500，
        // 不要让后面的 mysql_* 拿着空指针把整个进程搞崩
        string resp = buildResponse(500, "<h1>500 Internal Server Error</h1><p>No database connection</p>");
        send(fd, resp.c_str(), resp.size(), 0);
        close(fd);
        return;
    }

    string response_body;
    int status_code = 200;

    // 2. 根据路由处理
    if (req.method == "GET" && req.uri == "/") {
        response_body = "<h1>🏠 Day6 Home</h1><p>Use GET /users?name=xxx or POST /users (body: name=xxx&age=xx)</p>";
    }
    else if (req.method == "GET" && req.uri.find("/users") == 0) {
        // 解析查询参数 name=xxx
        string name;
        size_t q = req.uri.find('?');
        if (q != string::npos) {
            string query = req.uri.substr(q + 1);
            size_t eq = query.find('=');
            if (eq != string::npos && query.substr(0, eq) == "name") {
                name = query.substr(eq + 1);
            }
        }
        if (name.empty()) {
            status_code = 400;
            response_body = "<h1>400 Bad Request</h1><p>Missing name parameter</p>";
        } else {
            string result;
            bool ok = queryUserByName(conn, name, result);
            if (ok) {
                response_body = "<h1>User found</h1><p>" + result + "</p>";
            } else {
                status_code = 404;
                response_body = "<h1>404 Not Found</h1><p>" + result + "</p>";
            }
        }
    }
    else if (req.method == "POST" && req.uri == "/users") {
        // 解析 body 中的 name=xxx&age=xx
        string name;
        int age = 0;
        istringstream bodyStream(req.body);
        string pair;
        while (getline(bodyStream, pair, '&')) {
            size_t eq = pair.find('=');
            if (eq != string::npos) {
                string key = pair.substr(0, eq);
                string value = pair.substr(eq + 1);
                if (key == "name") name = value;
                else if (key == "age") age = stoi(value);
            }
        }
        if (name.empty() || age <= 0) {
            status_code = 400;
            response_body = "<h1>400 Bad Request</h1><p>Missing name or invalid age</p>";
        } else {
            if (insertUser(conn, name, age)) {
                response_body = "<h1>User inserted successfully</h1>";
            } else {
                status_code = 500;
                response_body = "<h1>500 Internal Server Error</h1><p>Database insert failed</p>";
            }
        }
    }
    else {
        status_code = 404;
        response_body = "<h1>404 Not Found</h1>";
    }

    // 3. 归还连接（一定要归还！）
    g_conn_pool->returnConnection(conn);

    // 4. 生成响应并发送
    string response = buildResponse(status_code, response_body);
    send(fd, response.c_str(), response.size(), 0);

    // 5. 清理 socket
    // 【并发修复】不再调用 epoll_ctl：主线程在把任务交给线程池之前已经摘除过 fd 了
    //（见 handleClientRead），这里只负责关闭。
    // 职责划分：主线程管 epoll，工作线程只管业务与收发。
    close(fd);
    cout << "✅ Task completed on fd=" << fd << endl;
}

// ---------- 主线程读事件处理（快速解析，入队） ----------
// 【Day6 修复】把 Day4 的「读缓冲区 + 半包判断」补回来。
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
                string resp = buildResponse(431, "<h1>431 Request Header Fields Too Large</h1>");
                send(fd, resp.c_str(), resp.size(), 0);
                closeClient(fd);
                return;
            }
            if (ctx.read_buffer.size() > MAX_HEADER_SIZE + MAX_BODY_SIZE) {
                string resp = buildResponse(413, "<h1>413 Payload Too Large</h1>");
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
        if (pos == string::npos) {
            return;   // ★ 头部还没收全：保留缓冲区，等下次 epoll 通知（半包处理的核心）
        }

        string header_part = ctx.read_buffer.substr(0, pos + 4);
        if (!parseRequest(header_part, ctx.req)) {
            string resp = buildResponse(400, "<h1>400 Bad Request</h1>");
            send(fd, resp.c_str(), resp.size(), 0);
            closeClient(fd);
            return;
        }

        ctx.header_done = true;
        ctx.header_end  = pos + 4;

        auto it = ctx.req.headers.find("Content-Length");
        if (it != ctx.req.headers.end()) {
            try {
                ctx.body_need = stoul(it->second);
            } catch (const exception&) {
                string resp = buildResponse(400, "<h1>400 Invalid Content-Length</h1>");
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

    cout << "📩 Main thread parsed: " << req.method << " " << req.uri << ", enqueuing task..." << endl;

    g_thread_pool->enqueue([fd, req] {
        handleBusiness(fd, req);
    });
}

// ---------- 主函数 ----------
int main() {
    signal(SIGPIPE, SIG_IGN);

    // 1. 创建连接池（10 个连接）
    ConnectionPool connPool(10);
    g_conn_pool = &connPool;

    // 2. 创建线程池（4 个工作线程）
    ThreadPool threadPool(4);
    g_thread_pool = &threadPool;

    // 3. 创建 socket、bind、listen（同之前）
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

    // 4. 设置非阻塞 & epoll
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    int epoll_fd = epoll_create1(0);
    epoll_fd_global = epoll_fd;

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    cout << "🚀 Day 6 Server (epoll + ThreadPool + MySQL Pool) running on port " << PORT << endl;
    cout << "📌 Test GET: curl 'http://127.0.0.1:8888/users?name=Alice'" << endl;
    cout << "📌 Test POST: curl -X POST -d 'name=Bob&age=25' http://127.0.0.1:8888/users" << endl;

    // 5. 主事件循环
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
                    cout << "✅ New client connected, fd=" << client_fd << endl;
                }
            } else {
                handleClientRead(fd);
            }
        }
    }

    close(server_fd);
    return 0;
}