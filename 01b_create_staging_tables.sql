-- ============================================================
-- 01b_create_staging_tables.sql
-- Jalankan SETELAH terkoneksi ke database: goods_receiving_staging
-- 
-- Tabel staging = mirror dari OLTP (hanya kolom yg dibutuhkan)
-- Sumber: Purchasing, Production, Person schema
-- ============================================================

-- ============================================================
-- 1. Dari Schema: Purchasing
-- ============================================================

CREATE TABLE stg_purchaseorderheader (
    purchaseorderid     INT PRIMARY KEY,
    status              SMALLINT,
    vendorid            INT,
    shipmethodid        INT,
    orderdate           TIMESTAMP,
    shipdate            TIMESTAMP,
    subtotal            DECIMAL(19,4),
    taxamt              DECIMAL(19,4),
    freight             DECIMAL(19,4)
);

CREATE TABLE stg_purchaseorderdetail (
    purchaseorderid     INT,
    purchaseorderdetailid INT,
    orderqty            SMALLINT,
    productid           INT,
    unitprice           DECIMAL(19,4),
    linetotal           DECIMAL(19,4),
    receivedqty         DECIMAL(8,2),
    rejectedqty         DECIMAL(8,2),
    stockedqty          DECIMAL(9,2),
    PRIMARY KEY (purchaseorderid, purchaseorderdetailid)
);

CREATE TABLE stg_vendor (
    businessentityid    INT PRIMARY KEY,
    name                VARCHAR(50),
    creditrating        SMALLINT,
    preferredvendorstatus BOOLEAN,
    activeflag          BOOLEAN
);

CREATE TABLE stg_shipmethod (
    shipmethodid        INT PRIMARY KEY,
    name                VARCHAR(50)
);

CREATE TABLE stg_productvendor (
    productid           INT,
    businessentityid    INT,
    standardprice       DECIMAL(19,4),
    unitmeasurecode     VARCHAR(3),
    PRIMARY KEY (productid, businessentityid)
);

-- ============================================================
-- 2. Dari Schema: Production (pendukung Dim_Product)
-- ============================================================

CREATE TABLE stg_product (
    productid           INT PRIMARY KEY,
    name                VARCHAR(50),
    productnumber       VARCHAR(25),
    color               VARCHAR(15),
    productsubcategoryid INT
);

CREATE TABLE stg_productsubcategory (
    productsubcategoryid INT PRIMARY KEY,
    productcategoryid   INT,
    name                VARCHAR(50)
);

CREATE TABLE stg_productcategory (
    productcategoryid   INT PRIMARY KEY,
    name                VARCHAR(50)
);

-- ============================================================
-- 3. Dari Schema: Person (pendukung lokasi Dim_Vendor)
-- ============================================================

CREATE TABLE stg_businessentityaddress (
    businessentityid    INT,
    addressid           INT,
    addresstypeid       INT,
    PRIMARY KEY (businessentityid, addressid, addresstypeid)
);

CREATE TABLE stg_address (
    addressid           INT PRIMARY KEY,
    city                VARCHAR(30),
    stateprovinceid     INT
);

CREATE TABLE stg_stateprovince (
    stateprovinceid     INT PRIMARY KEY,
    name                VARCHAR(50),
    countryregioncode   VARCHAR(3)
);

CREATE TABLE stg_countryregion (
    countryregioncode   VARCHAR(3) PRIMARY KEY,
    name                VARCHAR(50)
);
