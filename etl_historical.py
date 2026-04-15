"""
etl_historical.py - One-Time Historical Load (Initial Full Load)
================================================================
Dijalankan SEKALI saat pertama kali mengisi Data Warehouse.
Memuat SELURUH data historis dari OLTP ke Staging → OLAP.

Flow:
  [OLTP: adventureworks] → EXTRACT → [STAGING: goods_receiving_staging]
                                              ↓
                                        TRANSFORM + LOAD
                                              ↓
                                     [OLAP: goods_receiving_olap]

Cara pakai:
  python etl_historical.py
"""

import psycopg2
import time
from datetime import date, timedelta, datetime
from config import OLTP_CONFIG, STAGING_CONFIG, OLAP_CONFIG


# ============================================================
# CONNECTION HELPERS
# ============================================================
def get_oltp_conn():
    return psycopg2.connect(**OLTP_CONFIG)

def get_staging_conn():
    return psycopg2.connect(**STAGING_CONFIG)

def get_olap_conn():
    return psycopg2.connect(**OLAP_CONFIG)


# ============================================================
# PHASE 1: EXTRACT (OLTP → Staging)
# ============================================================
def extract_table(oltp_cur, stg_cur, source_query, target_table, columns):
    """Generic extract: query OLTP → insert ke staging."""
    oltp_cur.execute(source_query)
    rows = oltp_cur.fetchall()
    if not rows:
        print(f"  [EXTRACT] {target_table}: 0 rows")
        return 0
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    stg_cur.executemany(
        f"INSERT INTO {target_table} ({col_names}) VALUES ({placeholders})", rows
    )
    print(f"  [EXTRACT] {target_table}: {len(rows)} rows")
    return len(rows)


def run_extract_historical():
    """Extract SELURUH data dari OLTP ke Staging (full refresh)."""
    print("\n" + "=" * 60)
    print("PHASE 1: EXTRACT (OLTP → Staging DB) - FULL HISTORICAL")
    print("=" * 60)

    oltp_conn = get_oltp_conn()
    stg_conn = get_staging_conn()
    oltp_cur = oltp_conn.cursor()
    stg_cur = stg_conn.cursor()

    try:
        # Truncate semua staging tables
        staging_tables = [
            "stg_purchaseorderheader", "stg_purchaseorderdetail",
            "stg_vendor", "stg_shipmethod", "stg_productvendor",
            "stg_product", "stg_productsubcategory", "stg_productcategory",
            "stg_businessentityaddress", "stg_address",
            "stg_stateprovince", "stg_countryregion",
        ]
        for t in staging_tables:
            stg_cur.execute(f"TRUNCATE TABLE {t} CASCADE;")
        print("  [EXTRACT] All staging tables truncated")

        # ============================================================
        # Purchasing schema
        # ============================================================

        # PurchaseOrderHeader
        extract_table(oltp_cur, stg_cur,
            """SELECT purchaseorderid, status, vendorid, shipmethodid,
                      orderdate, shipdate, subtotal, taxamt, freight
               FROM purchasing.purchaseorderheader""",
            "stg_purchaseorderheader",
            ["purchaseorderid", "status", "vendorid", "shipmethodid",
             "orderdate", "shipdate", "subtotal", "taxamt", "freight"])

        # PurchaseOrderDetail
        # NOTE: linetotal is a COMPUTED column in AdventureWorks PostgreSQL
        #       so we calculate it manually: orderqty * unitprice
        extract_table(oltp_cur, stg_cur,
            """SELECT purchaseorderid, purchaseorderdetailid,
                      orderqty, productid, unitprice,
                      (orderqty * unitprice) AS linetotal,
                      receivedqty, rejectedqty,
                      (receivedqty - rejectedqty) AS stockedqty
               FROM purchasing.purchaseorderdetail""",
            "stg_purchaseorderdetail",
            ["purchaseorderid", "purchaseorderdetailid",
             "orderqty", "productid", "unitprice", "linetotal",
             "receivedqty", "rejectedqty", "stockedqty"])

        # Vendor
        extract_table(oltp_cur, stg_cur,
            """SELECT businessentityid, name, creditrating,
                      preferredvendorstatus, activeflag
               FROM purchasing.vendor""",
            "stg_vendor",
            ["businessentityid", "name", "creditrating",
             "preferredvendorstatus", "activeflag"])

        # ShipMethod
        extract_table(oltp_cur, stg_cur,
            """SELECT shipmethodid, name
               FROM purchasing.shipmethod""",
            "stg_shipmethod",
            ["shipmethodid", "name"])

        # ProductVendor
        extract_table(oltp_cur, stg_cur,
            """SELECT productid, businessentityid, standardprice, unitmeasurecode
               FROM purchasing.productvendor""",
            "stg_productvendor",
            ["productid", "businessentityid", "standardprice", "unitmeasurecode"])

        # ============================================================
        # Production schema (pendukung Dim_Product)
        # ============================================================

        # Product
        extract_table(oltp_cur, stg_cur,
            """SELECT productid, name, productnumber, color, productsubcategoryid
               FROM production.product""",
            "stg_product",
            ["productid", "name", "productnumber", "color", "productsubcategoryid"])

        # ProductSubcategory
        extract_table(oltp_cur, stg_cur,
            """SELECT productsubcategoryid, productcategoryid, name
               FROM production.productsubcategory""",
            "stg_productsubcategory",
            ["productsubcategoryid", "productcategoryid", "name"])

        # ProductCategory
        extract_table(oltp_cur, stg_cur,
            """SELECT productcategoryid, name
               FROM production.productcategory""",
            "stg_productcategory",
            ["productcategoryid", "name"])

        # ============================================================
        # Person schema (pendukung lokasi Dim_Vendor)
        # ============================================================

        # BusinessEntityAddress
        extract_table(oltp_cur, stg_cur,
            """SELECT businessentityid, addressid, addresstypeid
               FROM person.businessentityaddress""",
            "stg_businessentityaddress",
            ["businessentityid", "addressid", "addresstypeid"])

        # Address
        extract_table(oltp_cur, stg_cur,
            """SELECT addressid, city, stateprovinceid
               FROM person.address""",
            "stg_address",
            ["addressid", "city", "stateprovinceid"])

        # StateProvince
        extract_table(oltp_cur, stg_cur,
            """SELECT stateprovinceid, name, countryregioncode
               FROM person.stateprovince""",
            "stg_stateprovince",
            ["stateprovinceid", "name", "countryregioncode"])

        # CountryRegion
        extract_table(oltp_cur, stg_cur,
            """SELECT countryregioncode, name
               FROM person.countryregion""",
            "stg_countryregion",
            ["countryregioncode", "name"])

        stg_conn.commit()
        print("  [EXTRACT] ✓ Historical extract complete")

    except Exception as e:
        stg_conn.rollback()
        raise e
    finally:
        oltp_cur.close()
        stg_cur.close()
        oltp_conn.close()
        stg_conn.close()


# ============================================================
# TRANSFORM HELPER
# ============================================================
def null_to_tidak_ada(value):
    """Replace None/NULL with 'Tidak ada' for text fields."""
    return "Tidak ada" if value is None else value


# ============================================================
# PHASE 2: TRANSFORM & LOAD DIMENSIONS (Staging → OLAP)
# ============================================================

def load_dim_date_historical(stg_cur, olap_cur):
    """Dim_Date: Generate seluruh range tanggal historis."""
    print("\n  [DIM_DATE] Historical load...")
    stg_cur.execute("""
        SELECT MIN(LEAST(orderdate, COALESCE(shipdate, orderdate))),
               MAX(GREATEST(orderdate, COALESCE(shipdate, orderdate)))
        FROM stg_purchaseorderheader
    """)
    min_date, max_date = stg_cur.fetchone()
    if not min_date:
        print("  [DIM_DATE] No data. Skipped.")
        return

    # Convert to date if timestamp
    if hasattr(min_date, 'date'):
        min_date = min_date.date()
    if hasattr(max_date, 'date'):
        max_date = max_date.date()

    start = date(min_date.year - 1, 1, 1)
    end = date(max_date.year + 1, 12, 31)

    month_names = ["", "January", "February", "March", "April", "May", "June",
                   "July", "August", "September", "October", "November", "December"]
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    olap_cur.execute("TRUNCATE TABLE dim_date CASCADE;")

    rows = []
    current = start
    while current <= end:
        dk = int(current.strftime("%Y%m%d"))
        mn = current.month
        rows.append((
            dk, current, day_names[current.weekday()], mn, month_names[mn],
            (mn - 1) // 3 + 1, current.year,
            current.year + 1 if mn >= 7 else current.year,
            current.weekday() >= 5
        ))
        current += timedelta(days=1)

    olap_cur.executemany("""
        INSERT INTO dim_date (date_key, full_date, day_name, month_number,
            month_name, quarter, year, fiscal_year, is_weekend)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    print(f"  [DIM_DATE] {len(rows)} rows loaded ({start} -> {end})")


def load_dim_shipmethod_historical(stg_cur, olap_cur):
    """Dim_ShipMethod: Full load (SCD Type 1)."""
    print("\n  [DIM_SHIPMETHOD] Historical load...")
    olap_cur.execute("TRUNCATE TABLE dim_shipmethod CASCADE;")
    stg_cur.execute("SELECT shipmethodid, name FROM stg_shipmethod")
    rows = [(r[0], null_to_tidak_ada(r[1])) for r in stg_cur.fetchall()]
    olap_cur.executemany("""
        INSERT INTO dim_shipmethod (ship_method_id, ship_method_name)
        VALUES (%s, %s)
    """, rows)
    print(f"  [DIM_SHIPMETHOD] {len(rows)} rows loaded")


def load_dim_vendor_historical(stg_cur, olap_cur):
    """Dim_Vendor: Full load semua vendor dengan lokasi (SCD Type 2 - initial)."""
    print("\n  [DIM_VENDOR] Historical load...")
    olap_cur.execute("TRUNCATE TABLE dim_vendor CASCADE;")

    stg_cur.execute("""
        SELECT DISTINCT ON (v.businessentityid)
            v.businessentityid, v.name, v.creditrating,
            v.preferredvendorstatus, v.activeflag,
            a.city, sp.name, cr.name
        FROM stg_vendor v
        LEFT JOIN stg_businessentityaddress bea
            ON v.businessentityid = bea.businessentityid
        LEFT JOIN stg_address a
            ON bea.addressid = a.addressid
        LEFT JOIN stg_stateprovince sp
            ON a.stateprovinceid = sp.stateprovinceid
        LEFT JOIN stg_countryregion cr
            ON sp.countryregioncode = cr.countryregioncode
        ORDER BY v.businessentityid, a.addressid
    """)
    rows = stg_cur.fetchall()

    initial_date = date(1900, 1, 1)
    for row in rows:
        (beid, vname, crating, prefstatus, aflag, vcity, vstate, vcountry) = row
        vname = null_to_tidak_ada(vname)
        vcity = null_to_tidak_ada(vcity)
        vstate = null_to_tidak_ada(vstate)
        vcountry = null_to_tidak_ada(vcountry)
        olap_cur.execute("""
            INSERT INTO dim_vendor
            (business_entity_id, vendor_name, credit_rating,
             preferred_vendor_status, active_flag,
             vendor_city, vendor_state, vendor_country,
             scd_effective_date, scd_expiry_date, scd_is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
        """, (beid, vname, crating, prefstatus, aflag,
              vcity, vstate, vcountry, initial_date))

    print(f"  [DIM_VENDOR] {len(rows)} rows loaded")


def load_dim_product_historical(stg_cur, olap_cur):
    """Dim_Product: Full load dengan hierarki kategori (SCD Type 2 - initial)."""
    print("\n  [DIM_PRODUCT] Historical load...")
    olap_cur.execute("TRUNCATE TABLE dim_product CASCADE;")

    stg_cur.execute("""
        SELECT
            p.productid,
            p.name,
            p.productnumber,
            p.color,
            COALESCE(pv.standardprice, 0) AS standardprice,
            pv.unitmeasurecode,
            psc.name AS subcategory_name,
            pc.name AS category_name
        FROM stg_product p
        LEFT JOIN (
            SELECT productid,
                   MAX(standardprice) AS standardprice,
                   MIN(unitmeasurecode) AS unitmeasurecode
            FROM stg_productvendor
            GROUP BY productid
        ) pv ON p.productid = pv.productid
        LEFT JOIN stg_productsubcategory psc
            ON p.productsubcategoryid = psc.productsubcategoryid
        LEFT JOIN stg_productcategory pc
            ON psc.productcategoryid = pc.productcategoryid
        WHERE p.productid IN (
            SELECT DISTINCT productid FROM stg_purchaseorderdetail
        )
    """)
    rows = stg_cur.fetchall()

    initial_date = date(1900, 1, 1)
    for row in rows:
        (pid, pname, pnum, color, stdprice, umc, subcat, cat) = row
        pname = null_to_tidak_ada(pname)
        pnum = null_to_tidak_ada(pnum)
        color = null_to_tidak_ada(color)
        umc = null_to_tidak_ada(umc)
        subcat = null_to_tidak_ada(subcat)
        cat = null_to_tidak_ada(cat)
        olap_cur.execute("""
            INSERT INTO dim_product
            (product_id, product_name, product_number, color,
             standard_price, unit_measure_code, subcategory_name, category_name,
             scd_effective_date, scd_expiry_date, scd_is_current)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
        """, (pid, pname, pnum, color, stdprice, umc, subcat, cat, initial_date))

    print(f"  [DIM_PRODUCT] {len(rows)} rows loaded")


def run_transform_dimensions_historical():
    print("\n" + "=" * 60)
    print("PHASE 2: TRANSFORM & LOAD DIMENSIONS - FULL HISTORICAL")
    print("=" * 60)

    stg_conn = get_staging_conn()
    olap_conn = get_olap_conn()
    stg_cur = stg_conn.cursor()
    olap_cur = olap_conn.cursor()

    try:
        load_dim_date_historical(stg_cur, olap_cur)
        load_dim_shipmethod_historical(stg_cur, olap_cur)
        load_dim_vendor_historical(stg_cur, olap_cur)
        load_dim_product_historical(stg_cur, olap_cur)
        olap_conn.commit()
        print("\n  [DIMENSIONS] ✓ Historical dimension load complete")
    except Exception as e:
        olap_conn.rollback()
        raise e
    finally:
        stg_cur.close()
        olap_cur.close()
        stg_conn.close()
        olap_conn.close()


# ============================================================
# PHASE 3: TRANSFORM & LOAD FACT (Staging + Dims → OLAP)
# ============================================================

def run_transform_fact_historical():
    print("\n" + "=" * 60)
    print("PHASE 3: TRANSFORM & LOAD FACT - FULL HISTORICAL")
    print("=" * 60)

    stg_conn = get_staging_conn()
    olap_conn = get_olap_conn()
    stg_cur = stg_conn.cursor()
    olap_cur = olap_conn.cursor()

    try:
        olap_cur.execute("TRUNCATE TABLE fact_goodsreceiving CASCADE;")

        # Read staging data
        stg_cur.execute("""
            SELECT
                pod.purchaseorderid,
                poh.orderdate,
                poh.shipdate,
                poh.vendorid,
                poh.shipmethodid,
                poh.status,
                pod.productid,
                pod.orderqty,
                pod.receivedqty,
                pod.rejectedqty,
                pod.stockedqty,
                pod.unitprice,
                pod.linetotal
            FROM stg_purchaseorderdetail pod
            JOIN stg_purchaseorderheader poh
                ON pod.purchaseorderid = poh.purchaseorderid
        """)
        staging_rows = stg_cur.fetchall()

        # Build lookups from OLAP dimensions
        olap_cur.execute("SELECT business_entity_id, vendor_key FROM dim_vendor WHERE scd_is_current = TRUE")
        vendor_lk = {r[0]: r[1] for r in olap_cur.fetchall()}

        olap_cur.execute("SELECT product_id, product_key FROM dim_product WHERE scd_is_current = TRUE")
        product_lk = {r[0]: r[1] for r in olap_cur.fetchall()}

        olap_cur.execute("SELECT ship_method_id, ship_method_key FROM dim_shipmethod")
        shipmethod_lk = {r[0]: r[1] for r in olap_cur.fetchall()}

        fact_rows = []
        skipped = 0
        for row in staging_rows:
            (po_id, orderdate, shipdate, vendorid, shipmethodid, status,
             productid, orderqty, receivedqty, rejectedqty, stockedqty,
             unitprice, linetotal) = row

            vk = vendor_lk.get(vendorid)
            pk = product_lk.get(productid)
            smk = shipmethod_lk.get(shipmethodid)

            if not vk or not pk or not smk:
                skipped += 1
                continue

            # Convert dates to YYYYMMDD integer keys
            dk_order = int(orderdate.strftime("%Y%m%d")) if orderdate else None
            dk_ship = int(shipdate.strftime("%Y%m%d")) if shipdate else None

            if dk_order is None:
                skipped += 1
                continue

            fact_rows.append((
                dk_order, dk_ship, vk, pk, smk,
                po_id, status,
                int(orderqty), int(receivedqty), int(rejectedqty), int(stockedqty),
                float(unitprice), float(linetotal)
            ))

        olap_cur.executemany("""
            INSERT INTO fact_goodsreceiving
            (date_key_order, date_key_ship, vendor_key, product_key, ship_method_key,
             purchase_order_id, po_status,
             order_qty, received_qty, rejected_qty, stocked_qty, unit_price, line_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, fact_rows)

        olap_conn.commit()
        print(f"  [FACT] {len(fact_rows)} rows loaded")
        if skipped:
            print(f"  [FACT] {skipped} rows skipped (dim lookup failed or null date)")
        print("\n  [FACT] ✓ Historical fact load complete")

    except Exception as e:
        olap_conn.rollback()
        raise e
    finally:
        stg_cur.close()
        olap_cur.close()
        stg_conn.close()
        olap_conn.close()


# ============================================================
# MAIN
# ============================================================
def main():
    start = time.time()
    print("=" * 60)
    print("ONE-TIME HISTORICAL LOAD")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    run_extract_historical()
    run_transform_dimensions_historical()
    run_transform_fact_historical()

    elapsed = time.time() - start
    print("\n" + "=" * 60)
    print(f"HISTORICAL LOAD COMPLETE! ({elapsed:.2f}s)")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
