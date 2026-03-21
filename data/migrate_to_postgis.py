#!/usr/bin/env python3
"""
SQLite-to-PostGIS migration tool.

Reads all tables from pharmacy_finder.db, creates equivalent PostGIS tables
with spatial columns, copies data, creates indexes, views, and helper functions.

Requirements:
  - PostgreSQL with PostGIS extension
  - Database: pharmacyfinder, User: pharmacyfinder, Password: pharmacyfinder
  - pip install psycopg2-binary

Usage: py -3.12 data/migrate_to_postgis.py
"""
import os
import sqlite3
import sys
from typing import List, Optional, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQLITE_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "pharmacyfinder",
    "user": "pharmacyfinder",
    "password": "pharmacyfinder",
}

SKIP_TABLES = {"sqlite_sequence"}

TYPE_MAP = {
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "REAL": "DOUBLE PRECISION",
    "TEXT": "TEXT",
    "BLOB": "BYTEA",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
}


def sqlite_type_to_pg(sqlite_type: str) -> str:
    upper = (sqlite_type or "TEXT").upper().split("(")[0]
    return TYPE_MAP.get(upper, "TEXT")


def get_sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    return [
        r[0] for r in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        if r[0] not in SKIP_TABLES
    ]


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[Tuple[str, str, int]]:
    """Returns (name, type, pk) for each column."""
    cur = conn.cursor()
    rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
    return [(r[1], r[2], r[5]) for r in rows]


def has_lat_lon(columns: List[Tuple[str, str, int]]) -> Tuple[bool, Optional[str], Optional[str]]:
    names = [c[0].lower() for c in columns]
    if "latitude" in names and "longitude" in names:
        return True, "latitude", "longitude"
    if "opportunity_lat" in names and "opportunity_lng" in names:
        return True, "opportunity_lat", "opportunity_lng"
    return False, None, None


def needs_polygon(table: str) -> bool:
    return table in ("shopping_centres", "hospitals")


def quote_ident(name: str) -> str:
    if name.lower() in ("name", "state", "order", "user", "limit"):
        return f'"{name}"'
    return name


def build_create_sql(
    table: str,
    columns: List[Tuple[str, str, int]],
    has_point: bool,
    lat_col: Optional[str],
    lon_col: Optional[str],
) -> str:
    """Build CREATE TABLE statement. Keeps lat/lon, adds geom."""
    parts = []
    for name, stype, pk in columns:
        pg_type = sqlite_type_to_pg(stype)
        if name.lower() == "id" and pk:
            parts.append(f"  {quote_ident(name)} {pg_type} PRIMARY KEY")
        else:
            parts.append(f"  {quote_ident(name)} {pg_type}")

    if has_point and lat_col and lon_col:
        parts.append("  geom geography(POINT, 4326)")
    if needs_polygon(table):
        parts.append("  boundary_geom geometry(POLYGON, 4326)")

    return f'CREATE TABLE "{table}" (\n' + ",\n".join(parts) + "\n)"


def copy_table(
    sqlite_conn: sqlite3.Connection,
    pg_conn,
    table: str,
    columns: List[Tuple[str, str, int]],
    has_point: bool,
    lat_col: Optional[str],
    lon_col: Optional[str],
) -> int:
    """Copy rows. Returns count."""
    cur = sqlite_conn.cursor()
    cur.execute(f'SELECT * FROM "{table}"')
    rows = cur.fetchall()

    if not rows:
        return 0

    col_names = [c[0] for c in columns]
    insert_cols = [quote_ident(c[0]) for c in columns]
    if has_point and lat_col and lon_col:
        insert_cols.append("geom")

    lat_idx = col_names.index(lat_col) if lat_col and lat_col in col_names else -1
    lon_idx = col_names.index(lon_col) if lon_col and lon_col in col_names else -1

    pg_cur = pg_conn.cursor()
    n_data = len(columns)
    if has_point and lat_col and lon_col:
        ph = ", ".join(["%s"] * n_data) + ", ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography"
    else:
        ph = ", ".join(["%s"] * n_data)

    sql = f'INSERT INTO "{table}" (' + ", ".join(insert_cols) + ") VALUES (" + ph + ")"

    for row in rows:
        vals = list(row)
        if has_point and lat_idx >= 0 and lon_idx >= 0:
            lat, lon = row[lat_idx], row[lon_idx]
            if lat is not None and lon is not None:
                vals.extend([lon, lat])
            else:
                vals.extend([None, None])
        try:
            pg_cur.execute(sql, vals)
        except Exception as e:
            pg_conn.rollback()
            raise RuntimeError(f"Insert failed for table {table}: {e}") from e

    pg_conn.commit()
    return len(rows)


def run_migration() -> dict:
    import psycopg2

    if not os.path.exists(SQLITE_PATH):
        raise FileNotFoundError(f"SQLite database not found: {SQLITE_PATH}")

    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    pg_conn = psycopg2.connect(**PG_CONFIG)
    pg_conn.autocommit = False

    pg_cur = pg_conn.cursor()
    pg_cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    pg_conn.commit()

    tables = get_sqlite_tables(sqlite_conn)
    counts = {}

    for table in tables:
        columns = get_table_columns(sqlite_conn, table)
        if not columns:
            continue

        has_point, lat_col, lon_col = has_lat_lon(columns)

        pg_cur.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
        create_sql = build_create_sql(table, columns, has_point, lat_col, lon_col)
        pg_cur.execute(create_sql)
        pg_conn.commit()

        try:
            n = copy_table(sqlite_conn, pg_conn, table, columns, has_point, lat_col, lon_col)
        except Exception as e:
            pg_conn.rollback()
            print(f"  Error copying {table}: {e}")
            n = 0

        counts[table] = n

    # Spatial indexes
    for table in tables:
        try:
            pg_cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_geom ON "{table}" USING GIST (geom)')
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
        try:
            pg_cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_boundary ON "{table}" USING GIST (boundary_geom)')
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()

    # Views
    pg_cur.execute("""
        DROP VIEW IF EXISTS pharmacies_per_town CASCADE;
        CREATE VIEW pharmacies_per_town AS
        SELECT state, postcode, COALESCE(suburb, '') AS town, COUNT(*) AS pharmacy_count
        FROM pharmacies
        GROUP BY state, postcode, COALESCE(suburb, '')
        ORDER BY pharmacy_count DESC;
    """)
    pg_conn.commit()

    pg_cur.execute("""
        DROP VIEW IF EXISTS opportunity_zones CASCADE;
        CREATE VIEW opportunity_zones AS
        SELECT v.id, v.name, v.address, v.latitude, v.longitude, v.state, v.source_type,
               v.passed_any, v.primary_rule, v.commercial_score, v.best_confidence,
               v.date_evaluated,
               (SELECT COUNT(*) FROM pharmacies p
                WHERE p.geom IS NOT NULL AND v.latitude IS NOT NULL AND v.longitude IS NOT NULL
                AND ST_DWithin(p.geom, ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography, 5000)) AS pharmacies_5km,
               (SELECT COUNT(*) FROM gps g
                WHERE g.geom IS NOT NULL AND v.latitude IS NOT NULL AND v.longitude IS NOT NULL
                AND ST_DWithin(g.geom, ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography, 1000)) AS gps_1km,
               (SELECT COUNT(*) FROM supermarkets s
                WHERE s.geom IS NOT NULL AND v.latitude IS NOT NULL AND v.longitude IS NOT NULL
                AND ST_DWithin(s.geom, ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography, 1000)) AS supermarkets_1km
        FROM v2_results v;
    """)
    pg_conn.commit()

    # Functions
    pg_cur.execute("""
        CREATE OR REPLACE FUNCTION nearest_pharmacies(p_lat double precision, p_lon double precision, p_limit int DEFAULT 10)
        RETURNS TABLE(id integer, name text, address text, latitude double precision, longitude double precision, distance_km double precision) AS $$
        DECLARE pt geography;
        BEGIN
            pt := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography;
            RETURN QUERY
            SELECT p.id, p.name, p.address, p.latitude, p.longitude,
                   (ST_Distance(p.geom, pt) / 1000.0)::double precision
            FROM pharmacies p
            WHERE p.geom IS NOT NULL
            ORDER BY p.geom <-> pt
            LIMIT p_limit;
        END;
        $$ LANGUAGE plpgsql;
    """)
    pg_conn.commit()

    pg_cur.execute("""
        CREATE OR REPLACE FUNCTION pharmacies_within_radius(p_lat double precision, p_lon double precision, radius_km double precision)
        RETURNS TABLE(id integer, name text, address text, latitude double precision, longitude double precision, distance_km double precision) AS $$
        DECLARE pt geography;
        BEGIN
            pt := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography;
            RETURN QUERY
            SELECT p.id, p.name, p.address, p.latitude, p.longitude,
                   (ST_Distance(p.geom, pt) / 1000.0)::double precision
            FROM pharmacies p
            WHERE p.geom IS NOT NULL AND ST_DWithin(p.geom, pt, radius_km * 1000.0)
            ORDER BY ST_Distance(p.geom, pt);
        END;
        $$ LANGUAGE plpgsql;
    """)
    pg_conn.commit()

    pg_cur.execute("""
        CREATE OR REPLACE FUNCTION same_town_check(lat1 double precision, lon1 double precision, lat2 double precision, lon2 double precision)
        RETURNS boolean AS $$
        BEGIN
            RETURN ST_DWithin(
                ST_SetSRID(ST_MakePoint(lon1, lat1), 4326)::geography,
                ST_SetSRID(ST_MakePoint(lon2, lat2), 4326)::geography,
                2000
            );
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;
    """)
    pg_conn.commit()

    sqlite_conn.close()
    pg_conn.close()
    return counts


def test_connection() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT PostGIS_Version()")
        print(f"  PostGIS version: {cur.fetchone()[0]}")
        conn.close()
        return True
    except Exception as e:
        print(f"  Connection failed: {e}")
        return False


def main() -> int:
    print("PharmacyFinder SQLite -> PostGIS Migration")
    print("=" * 50)

    print("\n1. Testing PostgreSQL connection...")
    if not test_connection():
        return 1

    print("\n2. Running migration...")
    try:
        counts = run_migration()
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n3. Row counts per table:")
    print("-" * 40)
    for table in sorted(counts.keys()):
        print(f"  {table}: {counts[table]} rows")
    print("-" * 40)
    print(f"  Total: {sum(counts.values())} rows")

    print("\nMigration complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
