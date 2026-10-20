#!/usr/bin/env python3
"""Uji tangga (ramp-up test): naikkan jumlah worker bertahap dalam SATU
proses, lalu ukur laju maksimum yang benar-benar dicapai VM Anda.

mode connect : setiap percobaan = buka koneksi baru + handshake CONNECT.
mode publish : setiap worker membuka SATU koneksi (bertahan), lalu mengirim
    banyak PUBLISH QoS 1 beruntun lewat koneksi yang sama, menunggu PUBACK
    tiap pesan (selaras dengan attack_flood.py versi terbaru). Kolom
    'connects' dan 'conn_resets' menunjukkan seberapa sering koneksi harus
    dibuka ulang -- idealnya connects mendekati jumlah worker dan
    conn_resets mendekati nol kalau tidak ada yang memblokir.

PENTING: jalankan ini saat IPS MATI (./teardown_ips.sh di VM Suricata).
Tujuannya mengukur kapasitas kirim murni (VM + jaringan + broker),
bukan efek pemblokiran Suricata. Kalau IPS menyala, "gagal"/"conn_resets"
bisa berarti diblokir Suricata, bukan batas kapasitas -- dua hal itu tidak
boleh tercampur di uji tangga ini.

Contoh:
  python3 attack_ramp.py --broker 192.168.20.10 --src 192.168.10.10 \
      --mode connect --duration 8 --workers 5,10,20,50,100,200,400

  python3 attack_ramp.py --broker 192.168.20.10 --src 192.168.10.10 \
      --mode publish --duration 8 --workers 5,10,20,50,100
  (publish RTT-bound / menunggu PUBACK tiap pesan -- mulai dari worker
  lebih kecil dulu dibanding connect)
"""
import argparse
import socket
import struct
import threading
import time
import csv

from mqtt_raw import publish_pkt, read_packet, tcp_connect, mqtt_handshake, MQTTError

ap = argparse.ArgumentParser()
ap.add_argument("--broker", default="192.168.20.10")
ap.add_argument("--port", type=int, default=1883)
ap.add_argument("--src", default="192.168.10.10")
ap.add_argument("--mode", choices=["connect", "publish"], default="connect")
ap.add_argument("--duration", type=int, default=8, help="detik per tahap (jangan terlalu lama)")
ap.add_argument("--cooldown", type=int, default=3, help="jeda antar-tahap (detik) agar socket bekas sempat tertutup")
ap.add_argument("--workers", default="5,10,20,50,100,200,400", help="daftar jumlah worker, dipisah koma")
ap.add_argument("--timeout", type=float, default=1.0, help="batas tunggu CONNACK/PUBACK (detik)")
ap.add_argument("--out", default="ramp.csv")
args = ap.parse_args()

steps = [int(x) for x in args.workers.split(",") if x.strip()]


def classify(e):
    if isinstance(e, ConnectionRefusedError):
        return "refused"
    if isinstance(e, socket.timeout):
        return "timeout"
    if isinstance(e, OSError) and getattr(e, "errno", None) in (98, 99):
        return "no_port"  # EADDRINUSE/EADDRNOTAVAIL -> port lokal (TIME_WAIT) habis
    return "other"


def run_step(n_workers, duration, mode):
    stop_at = time.time() + duration
    counter = {"sent": 0, "ok": 0, "refused": 0, "timeout": 0, "no_port": 0, "other": 0,
               "connects": 0, "conn_resets": 0}
    lock = threading.Lock()

    def record(outcome):
        with lock:
            counter["sent"] += 1
            counter[outcome] += 1

    def worker_connect(idx):
        n = 0
        while time.time() < stop_at:
            n += 1
            outcome = "ok"
            try:
                s = tcp_connect(args.broker, args.port, args.src, args.timeout)
                mqtt_handshake(s, f"ramp{idx}-{n}")
                s.close()
            except (ConnectionRefusedError, socket.timeout, OSError, MQTTError, ConnectionError) as e:
                outcome = classify(e)
            record(outcome)

    def worker_publish(idx):
        s, pid = None, 0
        while time.time() < stop_at:
            if s is None:
                try:
                    s = tcp_connect(args.broker, args.port, args.src, args.timeout)
                    mqtt_handshake(s, f"rampp{idx}")
                    with lock:
                        counter["connects"] += 1
                except (ConnectionRefusedError, socket.timeout, OSError, MQTTError, ConnectionError) as e:
                    record(classify(e))
                    s = None
                    time.sleep(0.05)
                    continue
            pid = pid % 65535 + 1
            outcome = "ok"
            try:
                s.sendall(publish_pkt(f"ramp/{idx}", b"x" * 64, qos=1, pid=pid))
                while True:
                    header, body = read_packet(s)
                    if header >> 4 == 4 and struct.unpack("!H", body[:2])[0] == pid:
                        break
            except (socket.timeout, OSError, ConnectionError) as e:
                outcome = classify(e) if not isinstance(e, ConnectionError) else "other"
            record(outcome)
            if outcome != "ok":
                try:
                    s.close()
                except OSError:
                    pass
                s = None
                with lock:
                    counter["conn_resets"] += 1
        if s:
            try:
                s.close()
            except OSError:
                pass

    worker_fn = worker_connect if mode == "connect" else worker_publish
    t0 = time.time()
    threads = [threading.Thread(target=worker_fn, args=(i,), daemon=True) for i in range(n_workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=duration + args.timeout + 5)
    dur = time.time() - t0
    counter["duration"] = round(dur, 2)
    counter["rate"] = round(counter["sent"] / dur, 1) if dur > 0 else 0
    return counter


print("=" * 78)
print(f"UJI TANGGA ({args.mode.upper()}) ke {args.broker}:{args.port} dari {args.src}")
print("PASTIKAN IPS MATI saat menjalankan ini (./teardown_ips.sh di VM Suricata)")
print("=" * 78)
if args.mode == "connect":
    print(f"{'workers':>8} {'durasi':>7} {'terkirim':>9} {'laju/s':>9} {'ok':>7} {'refused':>8} {'timeout':>8} {'no_port':>8} {'other':>7}")
else:
    print(f"{'workers':>8} {'durasi':>7} {'terkirim':>9} {'laju/s':>9} {'ok':>7} {'timeout':>8} {'no_port':>8} {'other':>7} {'connects':>9} {'resets':>7}")

rows = []
prev_rate = 0
for n in steps:
    r = run_step(n, args.duration, args.mode)
    rows.append({"workers": n, **r})
    if args.mode == "connect":
        print(f"{n:>8} {r['duration']:>7} {r['sent']:>9} {r['rate']:>9} {r['ok']:>7} {r['refused']:>8} {r['timeout']:>8} {r['no_port']:>8} {r['other']:>7}")
    else:
        print(f"{n:>8} {r['duration']:>7} {r['sent']:>9} {r['rate']:>9} {r['ok']:>7} {r['timeout']:>8} {r['no_port']:>8} {r['other']:>7} {r['connects']:>9} {r['conn_resets']:>7}")
        if r["conn_resets"] > r["sent"] * 0.3:
            print(f"  -> conn_resets tinggi meski IPS mati: kemungkinan broker/VM kewalahan di laju ini, bukan cuma batas jaringan.")

    if r["no_port"] > r["sent"] * 0.2:
        print(f"  -> BANYAK 'no_port' (ephemeral port habis). Ini batas OS, bukan batas nyata jaringan.")
        print(f"     Lihat 'CATATAN' di akhir skrip ini untuk cara menaikkan batasnya.")
    if r["rate"] < prev_rate * 1.05 and n > steps[0]:
        print(f"  -> Laju TIDAK naik banyak dari tahap sebelumnya ({prev_rate}/s -> {r['rate']}/s). "
              f"Kemungkinan sudah mendekati batas kapasitas di titik ini.")
    prev_rate = r["rate"]

    if n != steps[-1]:
        time.sleep(args.cooldown)

with open(args.out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["workers", "duration", "sent", "rate", "ok",
                                       "refused", "timeout", "no_port", "other",
                                       "connects", "conn_resets"])
    w.writeheader()
    w.writerows(rows)

best = max(rows, key=lambda r: r["rate"])
print("=" * 78)
print(f"Laju tertinggi tercapai : {best['rate']} /s, pada {best['workers']} worker")
if args.mode == "publish":
    print(f"  (connects={best['connects']}, conn_resets={best['conn_resets']} pada titik puncak itu)")
print(f"Data lengkap tersimpan  : {args.out}")
print("=" * 78)
print("CATATAN jika banyak 'no_port': tiap koneksi baru memakai satu port lokal")
print("sementara (ephemeral) yang butuh waktu ~60 detik untuk bisa dipakai lagi")
print("(status TIME_WAIT). Pada laju sangat tinggi, port bisa habis lebih cepat")
print("dari yang dibebaskan. Ini batas sistem operasi, bisa dilonggarkan (opsional):")
print("  sudo sysctl -w net.ipv4.ip_local_port_range='1024 65535'")
print("  sudo sysctl -w net.ipv4.tcp_tw_reuse=1")
