"""
clone_adventureworks.py
========================
Clone AdventureWorks guru ke lokal tanpa pg_dump.
Clone SEMUA: schemas, tables, data, PKs, FKs, unique constraints,
check constraints, indexes, defaults, views.

Uses pg_catalog instead of information_schema for constraint discovery
(more reliable with restricted permissions).
"""

import psycopg2
import time
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================
REMOTE = {
    "host": "10.183.26.99",
    "port": 5432,
    "dbname": "adventureworks",
    "user": "dlhstudent",
    "password": "passworddlh"
}

LOCAL = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "adventureworks_local",
    "user": "postgres",
    "password": "admin123"
}

SKIP_SCHEMAS = ('pg_catalog', 'information_schema', 'pg_toast')


def get_remote():
    return psycopg2.connect(**REMOTE)

def get_local():
    return psycopg2.connect(**LOCAL)

def safe_exec(cur, conn, sql):
    try:
        cur.execute(sql)
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False


def main():
    start = time.time()
    print("=" * 60)
    print("CLONE ADVENTUREWORKS (FULL - NO pg_dump)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print(f"Source: {REMOTE['host']}:{REMOTE['port']}/{REMOTE['dbname']}")
    print(f"Target: {LOCAL['host']}:{LOCAL['port']}/{LOCAL['dbname']}")

    rc = get_remote()
    lc = get_local()
    r = rc.cursor()
    l = lc.cursor()

    stats = {"schemas": 0, "tables": 0, "rows": 0, "pks": 0,
             "fks": 0, "uniques": 0, "checks": 0, "indexes": 0,
             "views": 0, "failed": []}

    # ============================================================
    # 1. SCHEMAS
    # ============================================================
    print("\n--- 1. Cloning schemas ---")
    r.execute("""
        SELECT nspname FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%%'
          AND nspname != 'information_schema'
        ORDER BY nspname
    """)
    schemas = [row[0] for row in r.fetchall()]

    for s in schemas:
        l.execute(f'CREATE SCHEMA IF NOT EXISTS "{s}";')
    lc.commit()
    stats["schemas"] = len(schemas)
    print(f"  ✓ {len(schemas)} schemas: {', '.join(schemas)}")

    # ============================================================
    # 2. TABLES (structure + data)
    # ============================================================
    print("\n--- 2. Cloning tables + data ---")

    r.execute("""
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'r'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
        ORDER BY n.nspname, c.relname
    """)
    all_tables = r.fetchall()

    for schema, tname in all_tables:
        fn = f'"{schema}"."{tname}"'

        try:
            r.execute("""
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale, is_nullable,
                       column_default, udt_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, tname))
            cols = r.fetchall()
            if not cols:
                continue

            col_defs = []
            col_names = []

            for cname, dtype, clen, nprec, nscale, nullable, default, udt in cols:
                col_names.append(cname)

                if dtype in ('character varying',):
                    ts = f'VARCHAR({clen})' if clen else 'TEXT'
                elif dtype == 'character':
                    ts = f'CHAR({clen})' if clen else 'CHAR(1)'
                elif dtype == 'numeric':
                    if nprec and nscale is not None:
                        ts = f'NUMERIC({nprec},{nscale})'
                    elif nprec:
                        ts = f'NUMERIC({nprec})'
                    else:
                        ts = 'NUMERIC'
                elif dtype == 'integer':
                    ts = 'INT'
                elif dtype == 'smallint':
                    ts = 'SMALLINT'
                elif dtype == 'bigint':
                    ts = 'BIGINT'
                elif dtype == 'boolean':
                    ts = 'BOOLEAN'
                elif dtype == 'text':
                    ts = 'TEXT'
                elif dtype in ('timestamp without time zone', 'timestamp with time zone'):
                    ts = 'TIMESTAMP' if 'without' in dtype else 'TIMESTAMPTZ'
                elif dtype == 'date':
                    ts = 'DATE'
                elif dtype == 'uuid':
                    ts = 'UUID'
                elif dtype == 'bytea':
                    ts = 'BYTEA'
                elif dtype == 'xml':
                    ts = 'XML'
                elif dtype == 'json':
                    ts = 'JSON'
                elif dtype == 'jsonb':
                    ts = 'JSONB'
                elif dtype in ('double precision',):
                    ts = 'DOUBLE PRECISION'
                elif dtype == 'real':
                    ts = 'REAL'
                elif dtype == 'money':
                    ts = 'NUMERIC(19,4)'
                elif dtype == 'interval':
                    ts = 'INTERVAL'
                elif dtype == 'name':
                    ts = 'NAME'
                elif dtype == 'USER-DEFINED':
                    ts = 'TEXT'
                elif dtype == 'ARRAY':
                    ts = 'TEXT'
                else:
                    ts = 'TEXT'

                ns = '' if nullable == 'YES' else ' NOT NULL'

                ds = ''
                if default and 'nextval' not in default and '::regclass' not in default:
                    if default in ("true", "false"):
                        ds = f' DEFAULT {default}'
                    elif default.startswith("'") or default.replace('.', '').replace('-', '').isdigit():
                        ds = f' DEFAULT {default}'
                    elif '::' in default:
                        ds = f' DEFAULT {default}'

                col_defs.append(f'  "{cname}" {ts}{ns}{ds}')

            l.execute(f'DROP TABLE IF EXISTS {fn} CASCADE;')
            create_sql = f'CREATE TABLE {fn} (\n' + ',\n'.join(col_defs) + '\n);'
            l.execute(create_sql)
            lc.commit()

            qcols = ', '.join([f'"{c}"' for c in col_names])
            r.execute(f'SELECT {qcols} FROM {fn}')
            rows = r.fetchall()

            if rows:
                ph = ', '.join(['%s'] * len(col_names))
                batch = 1000
                for i in range(0, len(rows), batch):
                    l.executemany(
                        f'INSERT INTO {fn} ({qcols}) VALUES ({ph})',
                        rows[i:i + batch]
                    )
                lc.commit()

            stats["tables"] += 1
            stats["rows"] += len(rows)
            print(f"  ✓ {fn}: {len(rows):,} rows")

        except Exception as e:
            emsg = str(e).strip().split('\n')[0]
            print(f"  ✗ {fn}: {emsg}")
            stats["failed"].append((fn, emsg))
            lc.rollback()

    # ============================================================
    # 3. PRIMARY KEYS (via pg_catalog)
    # ============================================================
    print("\n--- 3. Adding primary keys ---")
    r.execute("""
        SELECT
            n.nspname AS schema_name,
            t.relname AS table_name,
            con.conname AS constraint_name,
            pg_get_constraintdef(con.oid) AS constraint_def
        FROM pg_constraint con
        JOIN pg_class t ON con.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE con.contype = 'p'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
        ORDER BY n.nspname, t.relname
    """)
    pk_rows = r.fetchall()
    for schema, tname, cname, cdef in pk_rows:
        fn = f'"{schema}"."{tname}"'
        sql = f'ALTER TABLE {fn} ADD CONSTRAINT "{cname}" {cdef};'
        if safe_exec(l, lc, sql):
            stats["pks"] += 1
    print(f"  ✓ {stats['pks']}/{len(pk_rows)} primary keys")

    # ============================================================
    # 4. UNIQUE CONSTRAINTS (via pg_catalog)
    # ============================================================
    print("\n--- 4. Adding unique constraints ---")
    r.execute("""
        SELECT
            n.nspname,
            t.relname,
            con.conname,
            pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class t ON con.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE con.contype = 'u'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
        ORDER BY n.nspname, t.relname
    """)
    uq_rows = r.fetchall()
    for schema, tname, cname, cdef in uq_rows:
        fn = f'"{schema}"."{tname}"'
        sql = f'ALTER TABLE {fn} ADD CONSTRAINT "{cname}" {cdef};'
        if safe_exec(l, lc, sql):
            stats["uniques"] += 1
    print(f"  ✓ {stats['uniques']}/{len(uq_rows)} unique constraints")

    # ============================================================
    # 5. FOREIGN KEYS (via pg_catalog)
    # ============================================================
    print("\n--- 5. Adding foreign keys ---")
    r.execute("""
        SELECT
            n.nspname AS schema_name,
            t.relname AS table_name,
            con.conname AS constraint_name,
            pg_get_constraintdef(con.oid) AS constraint_def
        FROM pg_constraint con
        JOIN pg_class t ON con.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE con.contype = 'f'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
        ORDER BY n.nspname, t.relname
    """)
    fk_rows = r.fetchall()
    for schema, tname, cname, cdef in fk_rows:
        fn = f'"{schema}"."{tname}"'
        sql = f'ALTER TABLE {fn} ADD CONSTRAINT "{cname}" {cdef};'
        if safe_exec(l, lc, sql):
            stats["fks"] += 1
    print(f"  ✓ {stats['fks']}/{len(fk_rows)} foreign keys")

    # ============================================================
    # 6. CHECK CONSTRAINTS (via pg_catalog)
    # ============================================================
    print("\n--- 6. Adding check constraints ---")
    r.execute("""
        SELECT
            n.nspname,
            t.relname,
            con.conname,
            pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class t ON con.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE con.contype = 'c'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
    """)
    ck_rows = r.fetchall()
    for schema, tname, cname, cdef in ck_rows:
        fn = f'"{schema}"."{tname}"'
        sql = f'ALTER TABLE {fn} ADD CONSTRAINT "{cname}" {cdef};'
        if safe_exec(l, lc, sql):
            stats["checks"] += 1
    print(f"  ✓ {stats['checks']}/{len(ck_rows)} check constraints")

    # ============================================================
    # 7. INDEXES (non-constraint)
    # ============================================================
    print("\n--- 7. Adding indexes ---")
    r.execute("""
        SELECT
            n.nspname,
            ic.relname AS index_name,
            pg_get_indexdef(i.indexrelid) AS index_def
        FROM pg_index i
        JOIN pg_class ic ON i.indexrelid = ic.oid
        JOIN pg_class tc ON i.indrelid = tc.oid
        JOIN pg_namespace n ON tc.relnamespace = n.oid
        WHERE n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
          AND NOT i.indisprimary
          AND NOT i.indisunique
    """)
    idx_rows = r.fetchall()
    for schema, idxname, idxdef in idx_rows:
        if safe_exec(l, lc, idxdef):
            stats["indexes"] += 1
    print(f"  ✓ {stats['indexes']}/{len(idx_rows)} indexes")

    # ============================================================
    # 8. VIEWS
    # ============================================================
    print("\n--- 8. Cloning views ---")
    r.execute("""
        SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, true)
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'v'
          AND n.nspname NOT LIKE 'pg_%%'
          AND n.nspname != 'information_schema'
        ORDER BY n.nspname, c.relname
    """)
    views = r.fetchall()
    for schema, vname, vdef in views:
        fn = f'"{schema}"."{vname}"'
        sql = f'CREATE OR REPLACE VIEW {fn} AS {vdef}'
        if safe_exec(l, lc, sql):
            stats["views"] += 1
    print(f"  ✓ {stats['views']}/{len(views)} views")

    # ============================================================
    # SUMMARY
    # ============================================================
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"CLONE COMPLETE! ({elapsed:.1f}s)")
    print(f"{'=' * 60}")
    print(f"  Schemas:      {stats['schemas']}")
    print(f"  Tables:       {stats['tables']}/{len(all_tables)}")
    print(f"  Total rows:   {stats['rows']:,}")
    print(f"  Primary keys: {stats['pks']}/{len(pk_rows)}")
    print(f"  Foreign keys: {stats['fks']}/{len(fk_rows)}")
    print(f"  Unique:       {stats['uniques']}/{len(uq_rows)}")
    print(f"  Checks:       {stats['checks']}/{len(ck_rows)}")
    print(f"  Indexes:      {stats['indexes']}/{len(idx_rows)}")
    print(f"  Views:        {stats['views']}/{len(views)}")
    if stats["failed"]:
        print(f"\n  Failed ({len(stats['failed'])}):")
        for ft, err in stats["failed"]:
            print(f"    ✗ {ft}: {err}")
    print(f"\n{'=' * 60}")
    print(f"Update config.py:")
    print(f'  OLTP_CONFIG = {{')
    print(f'      "host": "127.0.0.1",')
    print(f'      "port": 5432,')
    print(f'      "dbname": "adventureworks_local",')
    print(f'      "user": "postgres",')
    print(f'      "password": "admin123"')
    print(f'  }}')

    r.close()
    l.close()
    rc.close()
    lc.close()


if __name__ == "__main__":
    main()