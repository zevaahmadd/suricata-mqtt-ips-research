"""Helper paket MQTT 3.1.1 mentah (tanpa library eksternal).

Dipakai oleh legit_client.py dan attack_flood.py agar alamat IP sumber
bisa diatur (bind) dan setiap tahap koneksi dapat diukur terpisah.
"""
import socket
import struct


class MQTTError(Exception):
    pass


def enc_len(n):
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def mqtt_str(s):
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def connect_pkt(client_id, keepalive=60):
    body = mqtt_str("MQTT") + bytes([4, 0x02]) + struct.pack("!H", keepalive) + mqtt_str(client_id)
    return b"\x10" + enc_len(len(body)) + body


def publish_pkt(topic, payload, qos=0, pid=1):
    body = mqtt_str(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += payload
    return bytes([0x30 | (qos << 1)]) + enc_len(len(body)) + body


def recv_exact(s, n):
    buf = b""
    while len(buf) < n:
        chunk = s.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("koneksi ditutup oleh peer")
        buf += chunk
    return buf


def read_packet(s):
    """Baca satu paket MQTT; kembalikan (byte header, body)."""
    header = recv_exact(s, 1)[0]
    mult, rl = 1, 0
    while True:
        b = recv_exact(s, 1)[0]
        rl += (b & 0x7F) * mult
        if not b & 0x80:
            break
        mult *= 128
    return header, (recv_exact(s, rl) if rl else b"")


def tcp_connect(broker, port, src_ip, timeout):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    if src_ip:
        s.bind((src_ip, 0))
    s.connect((broker, port))
    return s


def mqtt_handshake(s, client_id):
    """Kirim CONNECT dan tunggu CONNACK. Raise MQTTError jika ditolak."""
    s.sendall(connect_pkt(client_id))
    header, body = read_packet(s)
    if header != 0x20 or len(body) < 2:
        raise MQTTError(f"balasan bukan CONNACK (0x{header:02x})")
    if body[1] != 0:
        raise MQTTError(f"CONNACK ditolak, return code {body[1]}")
