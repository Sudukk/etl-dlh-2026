-- ============================================================
-- reset_olap.sql
-- Jalankan di database: goods_receiving_olap
-- Menghapus semua data di OLAP tables + reset sequences
-- ============================================================

-- Fact dulu (karena FK ke dimensions)
TRUNCATE TABLE fact_goodsreceiving CASCADE;

-- Lalu dimensions
TRUNCATE TABLE dim_date CASCADE;
TRUNCATE TABLE dim_vendor CASCADE;
TRUNCATE TABLE dim_product CASCADE;
TRUNCATE TABLE dim_shipmethod CASCADE;

-- Reset sequences (SERIAL) supaya mulai dari 1 lagi
ALTER SEQUENCE dim_vendor_vendor_key_seq RESTART WITH 1;
ALTER SEQUENCE dim_product_product_key_seq RESTART WITH 1;
ALTER SEQUENCE dim_shipmethod_ship_method_key_seq RESTART WITH 1;
ALTER SEQUENCE fact_goodsreceiving_fact_id_seq RESTART WITH 1;

-- Verifikasi
SELECT 'dim_date' AS tabel, COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_vendor', COUNT(*) FROM dim_vendor
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_shipmethod', COUNT(*) FROM dim_shipmethod
UNION ALL SELECT 'fact_goodsreceiving', COUNT(*) FROM fact_goodsreceiving;
