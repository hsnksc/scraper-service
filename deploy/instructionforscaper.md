# Scraper Servis Entegrasyon Kılavuzu — loca-ai Backend

Bu dosya, loca-ai backend'inin **Scraper Servisi (Servis C)** ile nasıl iletişim kurduğunu
açıklar. Copilot bu dosyayı referans alarak scraper entegrasyonuyla ilgili kod yazmalı,
düzenlemeli ve hata ayıklamalıdır.

---

## 1. Servis Kimliği

| Özellik | Değer |
|---|---|
| İç URL (Docker) | `http://scraper:8010` |
| Dış URL (üretim) | `http://82.29.173.6:8010` |
| Docker ağı | `loca_network` (external) |
| Auth header | `X-API-Key: <SCRAPER_API_KEY>` |
| Env var | `SCRAPER_SERVICE_URL`, `SCRAPER_API_KEY` |
| Timeout bütçesi | 60s (discovery), 120s (full scrape) |
| Health | `GET /health` → `{"status": "ok"}` |

---

## 2. Kimlik Doğrulama

Her istekte header olarak gönderilmeli:

```
X-API-Key: {SCRAPER_API_KEY}
```

`SCRAPER_API_KEY` ortam değişkeninden okunur. Sabit kod yazılmaz.

---

## 3. Endpoint Kataloğu

### 3.1 Sistem

#### `GET /health`
Servis hazırlık durumu.

```json
{ "status": "ok" }
```

---

### 3.2 Discovery (Hızlı Mod) ⭐

#### `POST /api/discovery/run` ⭐ (En sık kullanılan)

Playwright kullanmadan, sadece arama API'lerinden (Tavily + Exa + Serper) anlık piyasa
verisi toplar. Yaklaşık yanıt süresi: 15–40s.

**Request Body:**

| Alan | Tip | Zorunlu | Varsayılan | Açıklama |
|---|---|---|---|---|
| `lat` | string \| float | ✅ | — | Enlem (WGS84) |
| `lng` | string \| float | ✅ | — | Boylam (WGS84) |
| `listing_type` | string | — | `"all"` | `"all"` / `"satilik"` / `"kiralik"` |
| `property_type` | string | — | `"all"` | `"all"` / `"konut"` / `"arsa"` / `"ticari"` / `"isyeri"` |
| `num_pages` | int | — | `1` | Arama sayfalama derinliği |
| `strict_detail_only` | bool | — | `false` | `true` → sadece doğrudan ilan sayfaları kabul edilir |
| `strict_district_header` | bool | — | `true` | `true` → locality skoru düşük adaylar elenir |

**Başarılı Yanıt (HTTP 200):**

```json
{
  "status": "ok",
  "location": {
    "lat": 36.857,
    "lng": 30.787,
    "district": "Muratpaşa",
    "neighborhood": "Fener Mah.",
    "city": "Antalya",
    "road": "Atatürk Cad."
  },
  "candidate_count": 34,
  "market_rows": [
    {
      "listing_type": "satilik",
      "property_type": "konut",
      "listing_kind": "daire",
      "price_try": 4200000,
      "size_m2_try": 110,
      "price_per_m2_try": 38181,
      "title": "Muratpaşa'da 3+1 satılık daire",
      "source_url": "https://www.hepsiemlak.com/ilan/..."
    }
  ]
}
```

**`market_rows` Alan Açıklamaları:**

| Alan | Tip | Açıklama |
|---|---|---|
| `listing_type` | string | `"satilik"` / `"kiralik"` / `null` |
| `property_type` | string | `"konut"` / `"arsa"` / `"ticari"` / `"isyeri"` / `null` |
| `listing_kind` | string | `"daire"` / `"villa"` / `"dükkan"` / `"arsa"` / `null` |
| `price_try` | float \| null | TRY cinsinden fiyat |
| `size_m2_try` | float \| null | m² (yoksa null; kayıt yine listede bulunur) |
| `price_per_m2_try` | float \| null | `price_try / size_m2_try` (her ikisi varsa) |
| `title` | string | İlan başlığı |
| `source_url` | string | Kaynak URL |

**Önemli:** `size_m2_try` null olabilir. Sadece `price_try` olan satırlar da `market_rows`'a dahil edilir.

---

### 3.3 Full Scrape (Playwright Modu)

#### `POST /api/scrape/run`

Playwright ile ilan sayfalarına girerek detaylı veri çeker. Yavaştır (30–120s),
ancak fiyat/m²/oda gibi yapısal alanlar çok daha güvenilirdir.

**Request Body:** `discovery/run` ile aynı parametreler.

**Yanıt:** `discovery/run` ile aynı format.

---

#### `POST /api/scrape/stream`

Playwright scrape sonuçlarını Server-Sent Events (SSE) akışı olarak döner.
Her ilan bulundukça anında iletilir. Kullanıcıya canlı güncelleme göstermek için tercih edilir.

**Headers:** `Accept: text/event-stream`

**Stream formatı:**
```
data: {"listing_type": "satilik", "price_try": 3500000, ...}
data: {"listing_type": "kiralik", "price_try": 18000, ...}
data: [DONE]
```

---

### 3.4 Genel Notlar

#### Desteklenen Kaynaklar (Arama Portalleri)
`market_rows` içindeki `source_url` alanı şu domainlerden biri olabilir:
- `sahibinden.com`
- `hepsiemlak.com`
- `emlakjet.com`
- `hurriyetemlak.com`
- `emlakcarsi.com`
- `remax.com.tr`
- `emlakgo.net`
- `rentola.com.tr`
- `flatspotter.com` / `tr.flatspotter.com`

#### Arama API Sağlayıcıları (Paralel)
Tavily + Exa + Serper paralel çalışır. Herhangi biri başarısız olursa diğerleri devam eder.

---

## 4. Ortam Değişkenleri

| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `SCRAPER_API_KEY` | — | Zorunlu. Servis kimlik doğrulama anahtarı |
| `DISCOVERY_PROVIDER_QUERY_LIMIT` | `3` | Sağlayıcı başına max sorgu sayısı |
| `DISCOVERY_MAX_RESULTS_PER_PROVIDER` | `10` | Sağlayıcı başına max sonuç |
| `DISCOVERY_PROVIDER_TIMEOUT_SECONDS` | `8` | Sağlayıcı başına timeout (saniye) |
| `MARKET_ROWS_LIMIT` | `80` | `market_rows` listesindeki max kayıt sayısı |
| `TAVILY_API_KEY` | — | Tavily arama API anahtarı |
| `EXA_API_KEY` | — | Exa arama API anahtarı |
| `SERPER_API_KEY` | — | Serper arama API anahtarı |

---

## 5. Hata Durumları

| HTTP Kodu | Durum |
|---|---|
| `200` | Başarılı; `market_rows` boş olabilir (ilan bulunamadı) |
| `401` | Geçersiz veya eksik `X-API-Key` |
| `422` | Geçersiz body (lat/lng eksik) |
| `500` | Tüm sağlayıcılar başarısız veya Playwright hata |
| `504` | Timeout (discovery >60s, scrape >120s) |

---

## 6. Orchestrator Entegrasyon Notları

- `market_rows` boş döndüğünde orchestrator bunu "piyasa verisi yok" olarak yorumlar,
  fallback olarak emlak_service `/listings/nearby` sonuçlarını kullanır.
- `strict_detail_only: false` + `strict_district_header: true` dengesi çoğu sorgu için idealdir.
- Discovery, değerleme pipeline'ından önce paralel tetiklenebilir; sonuçlar
  `price_try` + `size_m2_try` verisiyle `emlak_service`'e gelen değerlemeyi zenginleştirir.
- `source_url` alanları dış kullanıcıya iletilirken mutlaka validate edilmeli (open redirect riski).

---

## 7. Örnek Kullanım

### Discovery (Hızlı Mod):
```python
import httpx

resp = httpx.post(
    f"{settings.scraper_service_url}/api/discovery/run",
    json={
        "lat": 36.85762999247417,
        "lng": 30.787314375010823,
        "listing_type": "all",
        "property_type": "all",
        "num_pages": 1,
        "strict_detail_only": False,
        "strict_district_header": True,
    },
    headers={"X-API-Key": settings.scraper_api_key},
    timeout=60.0,
)
data = resp.json()
market_rows = data.get("market_rows", [])
```

### Sonuç Yorumlama:
```python
# Sadece m2 ve fiyat olan satırlar
complete_rows = [r for r in market_rows if r["price_try"] and r["size_m2_try"]]

# Medyan m2 fiyatı (satilik konut)
satilik_konut = [
    r for r in complete_rows
    if r.get("listing_type") == "satilik" and r.get("property_type") == "konut"
]
prices_per_m2 = [r["price_per_m2_try"] for r in satilik_konut if r.get("price_per_m2_try")]
median_per_m2 = sorted(prices_per_m2)[len(prices_per_m2) // 2] if prices_per_m2 else None
```
