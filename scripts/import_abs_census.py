"""Import ABS 2021 Census data into pharmacy_finder.db.
Creates census_sa1, census_sa2, census_lga tables with population + demographics.
"""
import json, os, sqlite3, sys

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pharmacy_finder.db")
CENSUS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "abs_census")

def import_layer(conn, name, filename):
    filepath = os.path.join(CENSUS_DIR, filename)
    if not os.path.exists(filepath):
        print(f"  SKIP {name}: {filename} not found")
        return 0
    
    with open(filepath) as f:
        records = json.load(f)
    
    if not records:
        print(f"  SKIP {name}: empty")
        return 0
    
    table = f"census_{name}"
    
    # Drop and recreate
    conn.execute(f"DROP TABLE IF EXISTS [{table}]")
    
    # Get all field names from first record
    fields = list(records[0].keys())
    
    # Key fields we want to ensure exist
    key_fields = {
        'Tot_P_P': 'INTEGER',  # Total persons
        'Tot_P_M': 'INTEGER',
        'Tot_P_F': 'INTEGER',
        'Median_age_persons': 'INTEGER',
        'Median_tot_prsnl_inc_weekly': 'INTEGER',
        'Median_tot_hhd_inc_weekly': 'INTEGER',
        'Median_rent_weekly': 'INTEGER',
        'Median_mortgage_repay_monthly': 'INTEGER',
        'Average_household_size': 'REAL',
        '_lat': 'REAL',
        '_lon': 'REAL',
    }
    
    # Build CREATE TABLE with all fields
    col_defs = []
    for field in fields:
        if field in key_fields:
            col_defs.append(f"[{field}] {key_fields[field]}")
        elif field.startswith('_'):
            col_defs.append(f"[{field}] REAL")
        elif 'CODE' in field or 'NAME' in field or 'OBJECTID' in field:
            col_defs.append(f"[{field}] TEXT")
        else:
            col_defs.append(f"[{field}] INTEGER")
    
    create_sql = f"CREATE TABLE [{table}] ({', '.join(col_defs)})"
    conn.execute(create_sql)
    
    # Insert all records
    placeholders = ', '.join(['?' for _ in fields])
    insert_sql = f"INSERT INTO [{table}] ({', '.join(f'[{f}]' for f in fields)}) VALUES ({placeholders})"
    
    batch = []
    for r in records:
        values = [r.get(f) for f in fields]
        batch.append(values)
    
    conn.executemany(insert_sql, batch)
    
    # Create spatial index
    if '_lat' in fields and '_lon' in fields:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_lat ON [{table}]([_lat])")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_lon ON [{table}]([_lon])")
    
    # Create code index
    for f in fields:
        if 'CODE' in f:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{f} ON [{table}]([{f}])")
            break
    
    conn.commit()
    
    # Stats
    total_pop = conn.execute(f"SELECT SUM([Tot_P_P]) FROM [{table}]").fetchone()[0] or 0
    print(f"  {table}: {len(records):,} records imported | Total population: {total_pop:,}")
    return len(records)


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    
    print("Importing ABS 2021 Census data...", flush=True)
    print(f"DB: {DB_PATH}", flush=True)
    
    total = 0
    total += import_layer(conn, "sa1", "census_2021_sa1.json")
    total += import_layer(conn, "sa2", "census_2021_sa2.json")
    total += import_layer(conn, "lga", "census_2021_lga.json")
    
    conn.close()
    print(f"\nDone: {total:,} total records imported", flush=True)
