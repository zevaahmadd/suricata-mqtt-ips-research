#!/usr/bin/env python3
"""Klien MQTT SAH: N publisher mengirim PUBLISH QoS 1 secara periodik.

Setiap pesan dicatat: waktu kirim, status (ok/timeout/error/connect_fail),
dan RTT PUBLISH -> PUBACK (ms). Dari berkas CSV ini dihitung delay,
throughput, dan packet loss trafik sah.

Contoh:
  python3 legit_client.py --broker 192.168.20.10 --src 192.168.10.11 \
      --publishers 5 --rate 1 --duration 600 --out legit.csv
"""
import argparse
import csv
import socket
import statistics
import struct
import threading
import time

from mqtt_raw import MQTTError, mqtt_handshake, publish_pkt, read_packet, tcp_connect

ap = argparse.ArgumentParser()
ap.add_argument("--broker", default="192.168.20.10")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--src", default="192.168.10.11", help="IP sumber klien sah (beda dari penyerang)")
ap.add_argument("--publishers", type=int, default=5)
ap.add_argument("--rate", type=float, default=1.0, help="pesan/detik per publisher")
ap.add_argument("--duration", type=int, default=600, help="detik")
ap.add_argument("--timeout", type=float, default=2.0, help="batas tunggu PUBACK/CONNACK (detik)")
ap.add_argument("--payload", type=int, default=64, help="ukuran payload (byte)")
ap.add_argument("--out", default="legit.csv")
args = ap.parse_args()

rows = []
lock = threading.Lock()
start = time.time()
stop_at = start + args.duration


def close(s):
    try:
        s.close()
    except OSError:
        pass


def publisher(idx):
    topic = f"sensor/{idx}"
    s, pid, seq = None, 0, 0
    interval = 1.0 / args.rate
    next_t = time.time()
    while time.time() < stop_at:
        now = time.time()
        if now < next_t:
            time.sleep(next_t - now)
        elif now - next_t > interval:
            next_t = now  # jangan kirim beruntun setelah tertunda
        next_t += interval
        seq += 1
        status, rtt = "ok", ""
        if s is None:
            try:
                s = tcp_connect(args.broker, args.port, args.src, args.timeout)
                mqtt_handshake(s, f"legit-{idx}")
            except (OSError, MQTTError, ConnectionError):
                if s:
                    close(s)
                s = None
                with lock:
                    rows.append([f"{time.time():.6f}", idx, seq, "connect_fail", "", args.payload])
                continue
        pid = pid % 65535 + 1
        payload = (f"{seq:08d}".encode() * args.payload)[: args.payload]
        t0 = time.time()
        try:
            s.sendall(publish_pkt(topic, payload, qos=1, pid=pid))
            while True:
                header, body = read_packet(s)
                if header >> 4 == 4 and struct.unpack("!H", body[:2])[0] == pid:
                    break
            rtt = f"{(time.time() - t0) * 1000:.3f}"
        except socket.timeout:
            status = "timeout"
        except (OSError, ConnectionError):
            status = "error"
        if status != "ok":
            close(s)
            s = None
        with lock:
            rows.append([f"{t0:.6f}", idx, seq, status, rtt, args.payload])
    if s:
        close(s)


threads = [threading.Thread(target=publisher, args=(i,), daemon=True) for i in range(1, args.publishers + 1)]
for t in threads:
    t.start()
for t in threads:
    t.join()

rows.sort(key=lambda r: float(r[0]))
with open(args.out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["t_kirim", "publisher", "seq", "status", "rtt_ms", "payload_byte"])
    w.writerows(rows)

total = len(rows)
ok = [float(r[4]) for r in rows if r[3] == "ok"]
dur = time.time() - start
print("=" * 45)
print("RINGKASAN KLIEN SAH")
print("=" * 45)
print(f"Durasi                : {dur:.1f} s")
print(f"Pesan dicoba          : {total}")
print(f"Pesan terkonfirmasi   : {len(ok)}")
if total:
    print(f"Packet loss (aplikasi): {(total - len(ok)) / total * 100:.2f} %")
if ok:
    q = statistics.quantiles(ok, n=20) if len(ok) >= 20 else [max(ok)] * 19
    print(f"Delay rerata (RTT)    : {statistics.mean(ok):.3f} ms")
    print(f"Delay median / p95    : {statistics.median(ok):.3f} / {q[18]:.3f} ms")
    print(f"Throughput            : {len(ok) / dur:.2f} pesan/s = {len(ok) * args.payload * 8 / dur:.0f} bit/s (payload)")
print(f"Data per pesan        : {args.out}")
