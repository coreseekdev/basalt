#!/usr/bin/env python3
"""内部 RPC 端口冒烟测试——四轮 review P0-1 回归锁定。

7d5f11d 曾在 Arc 贯通重构中丢失 internal::serve 接线：内部端口 TCP 可连
（backlog）但永不 accept/应答，多节点 Register/Heartbeat/MetaSync/
CreateTopic/FetchSlice 全部静默挂死（InternalClient::call 无读超时）。
单节点不受影响，故常规测试全绿——必须显式探测内部端口。

探针：MSG_HEARTBEAT（应答空帧）/ MSG_CREATE_TOPIC（应答 2B=0i16）。
连接成功≠服务在：以"限时应答"为通过标准。
用法：testing/e2e/run_e2e.sh internal_smoke.py"""
import socket, struct, time

HOST = "localhost"
PORT = 9093  # = 客户端端口 9092 + 1（main.rs internal_port = cfg.port + 1）


def req(sock, msg_type: int, payload: bytes, timeout=3.0):
    sock.settimeout(timeout)
    sock.sendall(struct.pack(">I", len(payload) + 1) + bytes([msg_type]) + payload)
    head = b""
    while len(head) < 4:
        chunk = sock.recv(4 - len(head))
        if not chunk:
            raise AssertionError("连接被关闭（未收到响应帧头）")
        head += chunk
    (length,) = struct.unpack(">I", head)
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            raise AssertionError("连接被关闭（响应体不完整）")
        body += chunk
    return body


def main():
    # [1] Heartbeat：应答空帧（0 长度响应体）
    s = socket.create_connection((HOST, PORT), timeout=3)
    t0 = time.monotonic()
    body = req(s, 2, struct.pack(">i", 0))
    assert body == b"", f"heartbeat 应答异常：{body!r}"
    print(f"[1] MSG_HEARTBEAT -> {len(body)}B（{time.monotonic()-t0:.3f}s）✔")
    s.close()

    # [2] CreateTopic：应答 2B = 0i16（POC 应答；有响应即证明 serve 已接线）
    s = socket.create_connection((HOST, PORT), timeout=3)
    name = b"smoke-topic"
    payload = struct.pack(">h", len(name)) + name + struct.pack(">ii", 2, 1)
    t0 = time.monotonic()
    body = req(s, 4, payload)
    assert body == struct.pack(">h", 0), f"create_topic 应答异常：{body!r}"
    print(f"[2] MSG_CREATE_TOPIC -> {len(body)}B（{time.monotonic()-t0:.3f}s）✔")
    s.close()
    print("PASS ✔")


if __name__ == "__main__":
    main()
