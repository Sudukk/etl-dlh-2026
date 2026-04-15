# config.py - Database connection configuration
# ================================================
# Single PostgreSQL instance, 3 databases:
#   1. adventureworks            (OLTP - sudah ada)
#   2. goods_receiving_staging   (Staging)
#   3. goods_receiving_olap      (OLAP / Star Schema)
# ================================================

OLTP_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "adventureworks_local",
    "user": "postgres",
    "password": "admin123"
}

STAGING_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "goods_receiving_staging",
    "user": "postgres",
    "password": "admin123"
}

OLAP_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "goods_receiving_olap",
    "user": "postgres",
    "password": "admin123"
}
