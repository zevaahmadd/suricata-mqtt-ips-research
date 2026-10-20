#!/usr/bin/env python3
"""Ringkas hasil satu skenario menjadi angka siap-tulis untuk Bab 4.

Contoh:
  python3 analyze.py --legit legit_mit.csv --attack attack_mit.csv \
      --res-suricata suricata_mit.csv --res-broker broker_mit.csv \
      --skenario "Mitigation - Connect 100"

Menghasilkan: QoS trafik sah (delay/throughput/loss), efektivitas mitigasi
(% serangan diblokir), dan overhead CPU/RAM. Butuh: pip install pandas numpy
"""
import argparse
import pandas as pd
import numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--legit")
ap.add_argument("--attack")
ap.add_argument("--res-suricata")
ap.add_argument("--res-broker")
ap.add_argument("--skenario", default="(tanpa nama)")
args = ap.parse_args()

print("=" * 60)
print(f"RINGKASAN SKENARIO: {args.skenario}")
print("=" * 60)

if args.legit:
    d = pd.read_csv(args.legit)
    ok = d[d.status == "ok"]
    total, nok = len(d), len(ok)
    dur = d.t_kirim.max() - d.t_kirim.min() if total else 0
    print("\n[QoS TRAFIK SAH]")
    print(f"  Pesan dicoba / terkonfirmasi : {total} / {nok}")
    print(f"  Packet loss (aplikasi)       : {(total - nok) / total * 100:.2f} %" if total else "  -")
    if nok:
        r = ok.rtt_ms.astype(float)
        print(f"  Delay RTT rerata / median    : {r.mean():.3f} / {r.median():.3f} ms")
        print(f"  Delay p95 / maks             : {np.percentile(r, 95):.3f} / {r.max():.3f} ms")
        print(f"  Throughput                   : {nok / dur:.2f} pesan/s" if dur else "  -")

if args.attack:
    a = pd.read_csv(args.attack).iloc[0]
    sent, ok, drop = int(a.percobaan), int(a.berhasil), int(a.gagal_drop)
    print("\n[EFEKTIVITAS MITIGASI]")
    print(f"  Serangan terkirim            : {sent} ({a.laju_aktual_per_s}/s)")
    print(f"  Diblokir (drop/timeout)      : {drop} ({drop / sent * 100:.1f} %)" if sent else "  -")
    print(f"  Lolos ke broker             : {ok} ({ok / sent * 100:.1f} %)" if sent else "  -")
    print("  Catatan: pada skenario MITIGATION, 'Diblokir' tinggi = mitigasi bekerja.")
    print("           pada skenario UNDER ATTACK (tanpa IPS), 'Lolos' harus ~100%.")

for tag, path in [("PELADEN SURICATA", args.res_suricata), ("PELADEN BROKER", args.res_broker)]:
    if path:
        r = pd.read_csv(path)
        print(f"\n[SUMBER DAYA - {tag}]")
        print(f"  CPU rerata / puncak          : {r.cpu_persen.mean():.2f} / {r.cpu_persen.max():.2f} %")
        print(f"  RAM rerata / puncak          : {r.rss_kb.mean() / 1024:.1f} / {r.rss_kb.max() / 1024:.1f} MiB")

print("\nSelesai. Ulangi untuk tiap skenario, lalu satukan ke tabel Bab 4.")
