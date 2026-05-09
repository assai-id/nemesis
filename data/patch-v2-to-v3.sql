-- Patch v2 → v3: covering index untuk filter wilayah/provinsi.
--
-- Konteks:
-- Query filter di halaman detail wilayah (mis. Jakarta Pusat ~72k paket)
-- melakukan JOIN package_regions × packages, lalu filter berdasarkan
-- severity / owner_type / is_priority. Tanpa index ini setiap baris
-- hasil JOIN harus mengambil row dari heap packages untuk pengecekan
-- filter — pada DB 2.5 GB dengan cache default itu memicu I/O acak
-- besar dan response time bisa 5+ detik pada cold cache.
--
-- Index ini jadi covering: planner cukup melihat index untuk semua
-- kolom filter. Hasil benchmark Jakarta Pusat: 4.85s → 0.25s.
--
-- Idempotent. Apply sekali via:
--   sqlite3 data/dashboard.sqlite < data/patch-v2-to-v3.sql
--
-- Backend (src/backend/db.js) juga menjalankan `CREATE INDEX IF NOT
-- EXISTS` ini di startup sebagai safety net, jadi DB yang belum
-- dipatch akan otomatis ter-upgrade saat server pertama kali boot.

CREATE INDEX IF NOT EXISTS idx_packages_filter
  ON packages(id, severity, owner_type, is_priority);
