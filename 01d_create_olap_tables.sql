-- ============================================================
-- 01d_create_olap_tables.sql
-- Jalankan SETELAH terkoneksi ke database: goods_receiving_olap
-- 
-- Star Schema: Fact_GoodsReceiving + 4 Dimension Tables
-- ============================================================

-- ============================================================
-- 1. DIMENSION TABLES
-- ============================================================

-- Dim_Date (SCD Type 0 - Fixed / Generated)
CREATE TABLE dim_date (
    date_key            INT PRIMARY KEY,          -- Format YYYYMMDD
    full_date           DATE NOT NULL UNIQUE,
    day_name            VARCHAR(10) NOT NULL,
    month_number        SMALLINT NOT NULL,
    month_name          VARCHAR(15) NOT NULL,
    quarter             SMALLINT NOT NULL,
    year                SMALLINT NOT NULL,
    fiscal_year         SMALLINT NOT NULL,         -- Juli = awal fiscal year
    is_weekend          BOOLEAN NOT NULL
);

-- Dim_Vendor (SCD Type 2 - Track: credit_rating, preferred_vendor_status)
-- Sumber: Purchasing.Vendor + Person.Address + Person.StateProvince + Person.CountryRegion
CREATE TABLE dim_vendor (
    vendor_key              SERIAL PRIMARY KEY,
    business_entity_id      INT NOT NULL,          -- NK dari Purchasing.Vendor
    vendor_name             VARCHAR(50) NOT NULL,   -- Purchasing.Vendor.Name
    credit_rating           SMALLINT NOT NULL,      -- SCD TRACKED
    preferred_vendor_status BOOLEAN NOT NULL,       -- SCD TRACKED
    active_flag             BOOLEAN NOT NULL,       -- Purchasing.Vendor.ActiveFlag
    vendor_city             VARCHAR(30),            -- Person.Address.City
    vendor_state            VARCHAR(50),            -- Person.StateProvince.Name
    vendor_country          VARCHAR(50),            -- Person.CountryRegion.Name
    scd_effective_date      DATE NOT NULL,
    scd_expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    scd_is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dim_Product (SCD Type 2 - Track: standard_price)
-- Sumber: Production.Product + Purchasing.ProductVendor + Production.ProductSubcategory + Production.ProductCategory
CREATE TABLE dim_product (
    product_key             SERIAL PRIMARY KEY,
    product_id              INT NOT NULL,           -- NK dari Production.Product
    product_name            VARCHAR(50) NOT NULL,    -- Production.Product.Name
    product_number          VARCHAR(25) NOT NULL,    -- Production.Product.ProductNumber
    color                   VARCHAR(15),            -- Production.Product.Color
    standard_price          DECIMAL(19,4) NOT NULL, -- SCD TRACKED - Purchasing.ProductVendor.StandardPrice
    unit_measure_code       VARCHAR(3),             -- Purchasing.ProductVendor.UnitMeasureCode
    subcategory_name        VARCHAR(50),            -- Production.ProductSubcategory.Name
    category_name           VARCHAR(50),            -- Production.ProductCategory.Name
    scd_effective_date      DATE NOT NULL,
    scd_expiry_date         DATE NOT NULL DEFAULT '9999-12-31',
    scd_is_current          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Dim_ShipMethod (SCD Type 1 - Overwrite)
-- Sumber: Purchasing.ShipMethod
CREATE TABLE dim_shipmethod (
    ship_method_key         SERIAL PRIMARY KEY,
    ship_method_id          INT NOT NULL UNIQUE,    -- NK dari Purchasing.ShipMethod
    ship_method_name        VARCHAR(50) NOT NULL     -- Purchasing.ShipMethod.Name
);

-- ============================================================
-- 2. FACT TABLE
-- ============================================================

-- Fact_GoodsReceiving (Transaction Fact Table)
-- Grain: satu line item pada purchase order detail
CREATE TABLE fact_goodsreceiving (
    fact_id                 SERIAL PRIMARY KEY,

    -- Foreign Keys (5)
    date_key_order          INT NOT NULL REFERENCES dim_date(date_key),
    date_key_ship           INT REFERENCES dim_date(date_key),
    vendor_key              INT NOT NULL REFERENCES dim_vendor(vendor_key),
    product_key             INT NOT NULL REFERENCES dim_product(product_key),
    ship_method_key         INT NOT NULL REFERENCES dim_shipmethod(ship_method_key),

    -- Degenerate Dimensions (2)
    purchase_order_id       INT NOT NULL,            -- DD: Nomor PO
    po_status               SMALLINT NOT NULL,       -- DD: 1=Pending 2=Approved 3=Rejected 4=Complete

    -- Measures (6)
    order_qty               INT NOT NULL,            -- Additive
    received_qty            INT NOT NULL,            -- Additive
    rejected_qty            INT NOT NULL,            -- Additive
    stocked_qty             INT NOT NULL,            -- Additive (derived: received - rejected)
    unit_price              DECIMAL(19,4) NOT NULL,  -- Non-Additive
    line_total              DECIMAL(19,4) NOT NULL   -- Additive
);

-- ============================================================
-- 3. INDEX untuk performa query OLAP
-- ============================================================

CREATE INDEX idx_fact_date_order ON fact_goodsreceiving(date_key_order);
CREATE INDEX idx_fact_date_ship ON fact_goodsreceiving(date_key_ship);
CREATE INDEX idx_fact_vendor ON fact_goodsreceiving(vendor_key);
CREATE INDEX idx_fact_product ON fact_goodsreceiving(product_key);
CREATE INDEX idx_fact_shipmethod ON fact_goodsreceiving(ship_method_key);
