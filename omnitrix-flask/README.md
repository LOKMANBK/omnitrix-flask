# OMNİTRİX — ALNS Flask Backend

Limak Çimento Dijital İkiz uygulamasının ALNS (Adaptive Large Neighborhood Search) 
optimizasyon motoru, monolitik HTML dosyasından ayrılarak Python/Flask backend'e taşınmıştır.

## Mimari

```
                    ┌─────────────────────────────┐
                    │   Frontend (index.html)      │
                    │  ─ Harita, Gantt, Simülasyon  │
                    │  ─ ALNS Panel UI              │
                    │  ─ Analytics Raporları         │
                    └──────────┬──────────────────┘
                               │ fetch('/api/solve')
                               │ SSE stream
                    ┌──────────▼──────────────────┐
                    │   Flask Backend (app.py)      │
                    │  ─ SSE endpoint (/api/solve)  │
                    │  ─ Sync endpoint (fallback)   │
                    └──────────┬──────────────────┘
                               │
                    ┌──────────▼──────────────────┐
                    │   ALNS Engine (alns_engine.py)│
                    │  ─ 5 Destroy operatörü        │
                    │  ─ 4 Repair operatörü         │
                    │  ─ SA kabul kriteri            │
                    │  ─ Ağırlık adaptasyonu         │
                    │  ─ Pompa/trafik modelleme      │
                    └─────────────────────────────┘
```

## Dosya Yapısı

```
omnitrix-flask/
├── app.py               # Flask sunucusu + SSE/sync endpoints
├── alns_engine.py       # Saf Python ALNS motoru (800+ satır)
├── requirements.txt     # Python bağımlılıkları
├── README.md            # Bu dosya
└── templates/
    └── index.html       # Dijital İkiz UI (ALNS motoru kaldırılmış)
```

## Kurulum & Çalıştırma

```bash
pip install -r requirements.txt
python app.py
```

Tarayıcıda `http://localhost:5000` adresini açın.

## API Endpoints

### POST /api/solve (SSE Stream)
Gerçek zamanlı ilerleme bilgisi ile ALNS çözümü çalıştırır.

**SSE Event Türleri:**
- `log` → `{ message: "..." }` — Log mesajları
- `progress` → `{ iter, total, bestObj, temp }` — İterasyon ilerlemesi
- `result` → `{ routes, pump_assignments, stats, ... }` — Final sonuç
- `error` → `{ message: "..." }` — Hata durumu
- `done` → `{}` — Stream sonu

### POST /api/solve-sync (JSON)
Tek seferde JSON yanıt döner. SSE desteklemeyen istemciler için fallback.

## ALNS Motor Bileşenleri

| Bileşen | Açıklama |
|---------|----------|
| **Destroy Operatörleri** | Random, Worst-Cost, Related, Time-Window, Truck-Chain |
| **Repair Operatörleri** | Greedy, Regret-2, Cheapest-Plant, Random |
| **Kabul Kriteri** | Simulated Annealing (SA) |
| **Adaptasyon** | Roulette-wheel ağırlık güncelleme (segment bazlı) |
| **Kısıtlar** | Beton ömrü ≤60dk, zaman penceresi, kapasite, pompa uyumu |
| **Maliyet** | Makespan + yakıt + bekleme + pompa boşta + karşılanamayan talep |

## Orijinal JS'den Farklar

- Motor 1:1 Python'a port edilmiştir, aynı seeded RNG ile deterministik sonuçlar üretir
- `convertToD()` fonksiyonu backend'de çalışır, frontend'e hazır D formatı gelir
- `applyALNSSolution()` frontend'de kalır (global state mutation)
- `on_progress` / `on_log` callback'leri SSE event'lerine dönüştürülmüştür
