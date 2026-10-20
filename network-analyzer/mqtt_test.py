import socket
import time

TARGET_IP = "192.168.20.10"
TARGET_PORT = 1883
TOTAL_PACKETS = 1000      # Jumlah percobaan koneksi (dapat disesuaikan)
DELAY_BETWEEN = 0.05    # Jeda 50 milidetik antar-permintaan

success_count = 0
dropped_count = 0

# Format biner resmi paket CONNECT protokol MQTT 3.1.1
mqtt_packet = b'\x10\x13\x00\x04MQTT\x04\x02\x00<\x00\x07kali001'

print(f"[*] Menjalankan pengujian beban MQTT ke {TARGET_IP}:{TARGET_PORT}")
print(f"[*] Total pengiriman: {TOTAL_PACKETS} paket\n")

start_time = time.time()

for i in range(1, TOTAL_PACKETS + 1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.0)  # Batas waktu respons 1 detik
    
    try:
        s.connect((TARGET_IP, TARGET_PORT))
        s.send(mqtt_packet)
        success_count += 1
        print(f"[{i:02d}/{TOTAL_PACKETS}] Berhasil tersambung ke broker.")
        s.close()
    except (socket.timeout, OSError):
        # Saat Suricata membuang paket, socket akan mengalami batas waktu habis (timeout)
        dropped_count += 1
        print(f"[{i:02d}/{TOTAL_PACKETS}] Terblokir / timeout (Mitigasi Suricata aktif).")
        s.close()
        
    time.sleep(DELAY_BETWEEN)

duration = time.time() - start_time

print("\n" + "="*45)
print("RINGKASAN PENGUJIAN KUANTITATIF")
print("="*45)
print(f"Waktu Eksekusi     : {duration:.2f} detik")
print(f"Total Permintaan   : {TOTAL_PACKETS}")
print(f"Koneksi Diterima   : {success_count} ({(success_count/TOTAL_PACKETS)*100:.1f}%)")
print(f"Koneksi Ditolak    : {dropped_count} ({(dropped_count/TOTAL_PACKETS)*100:.1f}%)")
print("="*45)
