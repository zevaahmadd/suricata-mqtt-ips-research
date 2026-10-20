#!/usr/bin/env python3
"""Kumpulkan semua CSV/log hasil pengujian (termasuk Baseline-IPS/D) dari VM
Mosquitto dan VM Suricata ke satu folder lokal di Kali, plus menyalin file
lokal (legit_*.csv, attack_*.csv) yang sudah ada di ~ (Kali) ke folder yang
sama.

Jalankan SETELAH pengujian selesai, dan SETELAH menjalankan:
  VM Mosquitto: python3 -m http.server 8001   (di folder berisi broker_*.csv)
  VM Suricata : python3 -m http.server 8002   (di folder berisi suricata_*.csv
                dan fastlog_*.log)

Contoh:
  python3 collect_results.py --mosquitto-ip 192.168.20.10 --suricata-ip 192.168.10.1
"""
import argparse
import os
import shutil
import urllib.request
import urllib.error

ap = argparse.ArgumentParser()
ap.add_argument("--mosquitto-ip", default="192.168.20.10")
ap.add_argument("--mosquitto-port", type=int, default=8001)
ap.add_argument("--suricata-ip", default="192.168.10.1")
ap.add_argument("--suricata-port", type=int, default=8002)
ap.add_argument("--reps", type=int, default=5, help="jumlah pengulangan per skenario")
ap.add_argument("--home", default=os.path.expanduser("~"), help="folder tempat legit_*/attack_*.csv di Kali")
ap.add_argument("--out", default=os.path.join(os.path.expanduser("~"), "hasil"))
args = ap.parse_args()

os.makedirs(args.out, exist_ok=True)
ok, fail = [], []
R = range(1, args.reps + 1)


def fetch(url, dest):
    try:
        urllib.request.urlretrieve(url, dest)
        ok.append(os.path.basename(dest))
    except (urllib.error.URLError, urllib.error.HTTPError, ConnectionError) as e:
        fail.append((os.path.basename(dest), str(e)))


def move_local(names):
    for name in names:
        src = os.path.join(args.home, name)
        dst = os.path.join(args.out, name)
        if os.path.exists(src):
            shutil.copy(src, dst)
            ok.append(name + " (lokal)")
        else:
            fail.append((name, "tidak ditemukan di " + args.home))


# 1) File lokal di Kali: legit_*.csv (semua skenario) dan attack_*.csv (B & C)
local_names = [f"legit_A_{n}.csv" for n in R] + [f"legit_D_{n}.csv" for n in R]
for tag in ["connect", "publish"]:
    for skenario in ["B", "C"]:
        for n in R:
            local_names.append(f"legit_{skenario}_{tag}_{n}.csv")
            local_names.append(f"attack_{skenario}_{tag}_{n}.csv")
move_local(local_names)

# 2) File dari Mosquitto (broker_*.csv), semua skenario termasuk D
mosq_base = f"http://{args.mosquitto_ip}:{args.mosquitto_port}"
mosq_names = [f"broker_A_{n}.csv" for n in R] + [f"broker_D_{n}.csv" for n in R]
for tag in ["connect", "publish"]:
    for skenario in ["B", "C"]:
        for n in R:
            mosq_names.append(f"broker_{skenario}_{tag}_{n}.csv")
for name in mosq_names:
    fetch(f"{mosq_base}/{name}", os.path.join(args.out, name))

# 3) File dari Suricata (suricata_*.csv, fastlog_*.log) -- skenario C dan D
suri_base = f"http://{args.suricata_ip}:{args.suricata_port}"
suri_names = [f"suricata_D_{n}.csv" for n in R] + [f"fastlog_D_{n}.log" for n in R]
for tag in ["connect", "publish"]:
    for n in R:
        suri_names.append(f"suricata_C_{tag}_{n}.csv")
        suri_names.append(f"fastlog_C_{tag}_{n}.log")
for name in suri_names:
    fetch(f"{suri_base}/{name}", os.path.join(args.out, name))

print("=" * 60)
print(f"BERHASIL : {len(ok)} file")
print(f"GAGAL    : {len(fail)} file")
if fail:
    print("-" * 60)
    print("Daftar yang GAGAL (cek: nama file, server http.server sudah jalan, atau apakah run itu benar dijalankan):")
    for name, err in fail:
        print(f"  - {name}  ({err})")
print("=" * 60)
print(f"Total seharusnya : {len(local_names) + len(mosq_names) + len(suri_names)} file")
print(f"Tersimpan di     : {args.out}")
