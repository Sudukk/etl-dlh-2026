-- ============================================================
-- reset_staging.sql
-- Jalankan di database: goods_receiving_staging
-- Menghapus semua data di staging tables
-- ============================================================

TRUNCATE TABLE stg_purchaseorderdetail CASCADE;
TRUNCATE TABLE stg_purchaseorderheader CASCADE;
TRUNCATE TABLE stg_productvendor CASCADE;
TRUNCATE TABLE stg_product CASCADE;
TRUNCATE TABLE stg_productsubcategory CASCADE;
TRUNCATE TABLE stg_productcategory CASCADE;
TRUNCATE TABLE stg_vendor CASCADE;
TRUNCATE TABLE stg_shipmethod CASCADE;
TRUNCATE TABLE stg_businessentityaddress CASCADE;
TRUNCATE TABLE stg_address CASCADE;
TRUNCATE TABLE stg_stateprovince CASCADE;
TRUNCATE TABLE stg_countryregion CASCADE;

-- Verifikasi
SELECT 'stg_purchaseorderheader' AS tabel, COUNT(*) FROM stg_purchaseorderheader
UNION ALL SELECT 'stg_purchaseorderdetail', COUNT(*) FROM stg_purchaseorderdetail
UNION ALL SELECT 'stg_vendor', COUNT(*) FROM stg_vendor
UNION ALL SELECT 'stg_shipmethod', COUNT(*) FROM stg_shipmethod
UNION ALL SELECT 'stg_product', COUNT(*) FROM stg_product;
