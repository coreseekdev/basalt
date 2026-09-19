#!/usr/bin/env python3
"""basalt e2e：内部口 HMAC 质询-应答（B1）。

positive — python 实现 v2 握手（HMAC-SHA256）后 MSG_HEARTBEAT 得到应答
negative — 旧明文 token 握手被拒绝（fail-closed）
"""
import hashlib
import hmac
import socket
import struct
import sys
import time

HOST = "localhost"
INTERNAL_PORT = 9093  # = 9092 + 1
TOKEN = b"test-secret-123"
MSG_AUTH = 0
MSG_HEARTBEAT = 2
CHALLENGE_LEN = 32


def send_frame(sock, msg_type, payload):
    sock.sendall(struct.pack(">I", len(payload) + 1) + bytes([msg_type]) + payload)


def read_frame(sock, timeout=3.0):
    sock.settimeout(timeout)
    head = b""
    while len(head) < 4:
        chunk = sock.recv(4 - len(head))
        if not chunk:
            raise ConnectionError("closed")
        head += chunk
    (length,) = struct.unpack(">I", head)
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            raise ConnectionError("closed")
        body += chunk
    return body


def handshake_v2(sock):
    send_frame(sock, MSG_AUTH, b"\x02")
    reply = read_frame(sock)
    assert reply[0] == MSG_AUTH and reply[1] == 0x00, "bad challenge frame"
    challenge = reply[2:]
    assert len(challenge) == CHALLENGE_LEN, "challenge 长度错误"
    mac = hmac.new(TOKEN, challenge, hashlib.sha256).digest()
    send_frame(sock, MSG_AUTH, b"\x03" + mac)
    ack = read_frame(sock)
    assert ack[0] == MSG_AUTH and ack[1] == 0x00, "HMAC 验收失败"


def positive():
    for _ in range(20):
        try:
            sock = socket.create_connection((HOST, INTERNAL_PORT), timeout=3)
            break
        except OSError:
            time.sleep(0.5)
    else:
        raise AssertionError("internal port unreachable")
    with sock:
        handshake_v2(sock)
        send_frame(sock, MSG_HEARTBEAT, b"")
        resp = read_frame(sock, timeout=5)
        assert isinstance(resp, bytes), "heartbeat 无应答"
    print("[1] v2 HMAC 握手 + heartbeat 应答 ✓")


def negative():
    sock = socket.create_connection((HOST, INTERNAL_PORT), timeout=3)
    with sock:
        send_frame(sock, MSG_AUTH, TOKEN)  # 旧明文
        try:
            resp = read_frame(sock, timeout=3)
            assert False, f"明文握手被放行（收到 {resp!r}）"
        except (ConnectionError, socket.timeout, ConnectionResetError):
            print("[2] 旧明文握手被拒绝（fail-closed）✓")
    print("PASS")


if __name__ == "__main__":
    phase = sys.argv[1] if len(sys.argv) > 1 else ""
    if phase == "positive":
        positive()
    elif phase == "negative":
        negative()
    else:
        print("usage: internal_auth_probe.py positive|negative")
        sys.exit(2)
