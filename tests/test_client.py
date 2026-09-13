#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
半包（TCP 拆包/粘包）与并发测试客户端

为什么需要这个脚本？
    用 curl 测不出半包问题——curl 会把整个 HTTP 请求一次性发出去，
    所以服务器的 read() 一次就能收到完整的 "\\r\\n\\r\\n"。
    本脚本故意把请求拆成多段、并且段与段之间 sleep，
    强制让服务器遇到"一次读不全"的情况。

用法：
    终端 1（先启动服务器）：
        g++ -std=c++17 -pthread serverday5.cpp -o day5 && ./day5

    终端 2（跑测试）：
        python3 test_client.py
        python3 test_client.py --port 8888

注意：
    serverday5.cpp 里有一个 sleep(3s) 用来模拟耗时业务，
    所以每次请求都要等 3 秒左右，测试会比较慢，属正常现象。
"""

import argparse
import socket
import sys
import time

HOST = '127.0.0.1'
READ_TIMEOUT = 8.0      # 服务器有 3 秒模拟耗时，超时要给足


# ----------------------------------------------------------------------
# 工具函数
# ----------------------------------------------------------------------
def send_pieces(pieces, port, gap=0.3, read_timeout=READ_TIMEOUT):
    """按顺序发送多个分片（中间可 sleep），返回服务器发回的全部字节。

    注意：服务器遇到半包时可能会直接关闭连接（甚至发 RST），
    此时 recv/sendall 会抛 ConnectionError —— 这本身就是"半包处理失败"的表现，
    所以要捕获它并返回已经收到的数据，而不是让整个脚本崩掉。
    """
    s = socket.create_connection((HOST, port), timeout=5)
    data = b''
    try:
        for i, piece in enumerate(pieces):
            try:
                s.sendall(piece)
            except ConnectionError:
                break                    # 对端已经不接受了
            if gap and i != len(pieces) - 1:
                time.sleep(gap)          # 故意制造"半包"

        s.settimeout(read_timeout)
        while True:
            try:
                chunk = s.recv(4096)
            except socket.timeout:
                break
            except ConnectionError:
                break                    # 连接被重置：服务器提前关掉了连接
            if not chunk:
                break
            data += chunk
        return data
    finally:
        s.close()


def status_of(resp):
    """从响应里取出状态码，取不到返回 None。"""
    if not resp:
        return None
    first_line = resp.split(b'\r\n', 1)[0]
    parts = first_line.split(b' ')
    if len(parts) >= 2 and parts[1].isdigit():
        return int(parts[1])
    return None


class Runner:
    def __init__(self):
        self.passed = 0
        self.failed = 0

    def check(self, name, condition, detail=''):
        if condition:
            self.passed += 1
            print('  [PASS] %s' % name)
        else:
            self.failed += 1
            print('  [FAIL] %s   %s' % (name, detail))


# ----------------------------------------------------------------------
# 测试用例
# ----------------------------------------------------------------------
def t1_normal_get(r, port):
    """基线：请求一次性发完（curl 的行为），应当返回 200"""
    print('\n[1] 基线测试：完整请求一次发出（等价于 curl）')
    resp = send_pieces([b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n'],
                       port, gap=0)
    r.check('返回 200', status_of(resp) == 200, '实际=%s' % status_of(resp))


def t2_split_header(r, port):
    """头部半包：\r\n\r\n 被拆到第二个分片里"""
    print('\n[2] 头部半包：请求头拆成两段发送（中间停 0.3s）')
    resp = send_pieces([
        b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n',
        b'Connection: close\r\n\r\n',
    ], port)
    st = status_of(resp)
    r.check('返回 200（说明服务器正确拼接了不完整头部）', st == 200,
            '实际=%s，响应=%r  <-- 旧版 day5/day6 在这里会直接关连接' % (st, resp[:60]))


def t3_byte_by_byte(r, port):
    """逐字节发送请求行，极限半包"""
    print('\n[3] 极限半包：头部逐字节发送')
    request = b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n'
    pieces = [bytes([b]) for b in request]
    resp = send_pieces(pieces, port, gap=0.02)
    st = status_of(resp)
    r.check('返回 200', st == 200, '实际=%s' % st)


def t4_split_body(r, port):
    """body 半包：Content-Length 声明了长度，但 body 分两次到达"""
    print('\n[4] Body 半包：POST 的 body 拆成两段发送')
    resp = send_pieces([
        b'POST /submit HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 10\r\nConnection: close\r\n\r\n',
        b'hello',
        b'world',
    ], port)
    st = status_of(resp)
    ok = st == 200 and b'helloworld' in resp
    r.check('返回 200 且回显了完整的 helloworld', ok,
            '实际状态=%s，响应=%r' % (st, resp[:200]))


def t5_header_too_large(r, port):
    """安全上限：一直发不含 \r\n\r\n 的数据，服务器应拒绝而不是把内存撑爆"""
    print('\n[5] 头部超长保护：发送 9KB 不含 \\r\\n\\r\\n 的数据')
    resp = send_pieces([b'X' * 9000], port, gap=0)
    st = status_of(resp)
    ok = st in (431, 413) or resp == b''
    r.check('返回 431/413 或直接断开（没有 OOM）', ok,
            '实际状态=%s' % st)


def t6_concurrent(r, port, n=20):
    """并发：同时发起 n 个请求，全部应当成功（验证 fd 竞态修复）"""
    print('\n[6] 并发测试：同时发起 %d 个请求' % n)
    socks = []
    try:
        for _ in range(n):
            s = socket.create_connection((HOST, port), timeout=5)
            s.sendall(b'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n')
            socks.append(s)

        ok_count = 0
        for s in socks:
            s.settimeout(READ_TIMEOUT)
            data = b''
            while True:
                try:
                    chunk = s.recv(4096)
                except socket.timeout:
                    break
                except ConnectionError:
                    break
                if not chunk:
                    break
                data += chunk
            if status_of(data) == 200:
                ok_count += 1

        r.check('%d 个并发请求全部返回 200' % n, ok_count == n,
                '成功 %d / %d' % (ok_count, n))
    finally:
        for s in socks:
            try:
                s.close()
            except OSError:
                pass


# ----------------------------------------------------------------------
def main():
    global HOST

    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=8888)
    ap.add_argument('--host', default=HOST)
    args = ap.parse_args()

    HOST = args.host

    print('=' * 62)
    print(' 半包 / 并发 测试  ->  %s:%d' % (HOST, args.port))
    print('=' * 62)

    # 先探测服务器是否在跑
    try:
        socket.create_connection((HOST, args.port), timeout=3).close()
    except OSError as e:
        print('\n[错误] 连不上 %s:%d —— 请先在另一个终端启动服务器。' % (HOST, args.port))
        print('       g++ -std=c++17 -pthread serverday5.cpp -o day5 && ./day5')
        print('       详细原因: %s' % e)
        return 2

    r = Runner()
    t1_normal_get(r, args.port)
    t2_split_header(r, args.port)
    t3_byte_by_byte(r, args.port)
    t4_split_body(r, args.port)
    t5_header_too_large(r, args.port)
    t6_concurrent(r, args.port)

    print('\n' + '=' * 62)
    print(' 结果：通过 %d 项，失败 %d 项' % (r.passed, r.failed))
    print('=' * 62)
    if r.failed:
        print('\n提示：如果 [2] / [4] 失败，说明服务器没有正确处理半包——')
        print('      这正是 day5 / day6 早期版本的问题：遇到"一次读不全"就直接关连接。')
    return 0 if r.failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
