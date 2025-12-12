# System Design for Phone Number Acquisition and Sales

## 1. Database Recommendation

Untuk menangani data sebesar 6GB/hari (sekitar 6-10 juta baris) dengan kebutuhan query cepat dan deduplikasi, database transaksional biasa (seperti MySQL standard) akan kesulitan.

**Rekomendasi Utama: ClickHouse (atau DuckDB untuk skala single-server/local)**

*   **Kenapa?** Designed for OLAP (Online Analytical Processing). Sangat cepat untuk query aggregasi dan filter data besar. Kompresi datanya sangat tinggi (6GB text bisa jadi <1GB di disk).
*   **Alternatif (Standard):** PostgreSQL dengan Partitioning (per hari/bulan).

## 2. Anti-Duplicate Strategy

Masalah: Mengecek 1-per-1 saat insert atau select akan membunuh performa ("server tewas").

**Solusi: Batch Processing & Set Difference (Anti-Join)**

Jangan cek satu per satu. Lakukan operasi himpunan (Set Operation).

### Workflow:

1.  **Ingestion (Data Masuk)**:
    *   Simpan data mentah ke DB, dipartisi berdasarkan `tanggal` dan `provider`.
    *   Jangan lakukan cek duplikat berat saat *insert*. Biarkan data masuk dulu (Raw Pool).

2.  **Order Processing (Saat Jualan)**:
    *   User minta 1 Juta nomor Tsel.
    *   System mengambil kandidat dari Raw Pool (misal: 3 hari terakhir).
    *   System mengambil daftar `Blacklist` (Nomor yang terjual 30 hari terakhir).
    *   Lakukan **Left Anti-Join** (Ambil dari Pool DI MANA TIDAK ADA di Blacklist).
    *   Ambil 1 Juta teratas.
    *   Tandai 1 Juta nomor tersebut sebagai `Sold` (masukkan ke tabel Blacklist dengan timestamp now).

### Illustrasi Query (SQL Concept):

```sql
WITH Candidates AS (
    SELECT phone, raw_data 
    FROM raw_numbers 
    WHERE provider = 'tsel' AND imported_at >= NOW() - INTERVAL 3 DAY
),
RecentlySold AS (
    SELECT phone 
    FROM sales_history 
    WHERE sold_at >= NOW() - INTERVAL 30 DAY
)
SELECT c.phone, c.raw_data
FROM Candidates c
LEFT JOIN RecentlySold s ON c.phone = s.phone
WHERE s.phone IS NULL
LIMIT 1000000;
```

## 3. Implementation Plan (Simulation)

Saya akan membuat simulasi menggunakan **Python + DuckDB**. DuckDB bekerja seperti SQLite tapi sangat cepat untuk data besar (jutaan baris) dan file parquet/csv.

**File Structure:**
- `ingest.py`: Script untuk generate dummy data & load ke DB.
- `order.py`: Script untuk memproses order dengan logika anti-duplikat.
- `schema.sql`: Struktur database.

