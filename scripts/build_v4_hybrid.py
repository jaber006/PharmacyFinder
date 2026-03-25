"""
Build hybrid v4 database:
- Pharmacies: v4 (6,503 from protobuf + GapMaps metadata) 
- Medical centres: Old DB geocoded + GapMaps GP count enrichment
- Supermarkets: Old DB geocoded + GapMaps GLA enrichment  
- GPs, Hospitals, Shopping Centres: Old DB as-is
"""
import sqlite3
import os
import csv
from collections import defaultdict

OLD_DB = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"
V4_DB = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder_v4.db"
HYBRID_DB = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder_v4_hybrid.db"

print("=" * 60)
print("BUILDING HYBRID V4 DATABASE")
print("=" * 60)

# Remove old hybrid if exists
if os.path.exists(HYBRID_DB):
    os.remove(HYBRID_DB)

# Copy old DB as base
import shutil
shutil.copy2(OLD_DB, HYBRID_DB)
print(f"Copied old DB as base")

conn = sqlite3.connect(HYBRID_DB)
conn.row_factory = sqlite3.Row
c = conn.cursor()

# 1. REPLACE pharmacies with v4 data (6,503 with coords)
print("\n--- Replacing pharmacies with v4 data ---")
c.execute("DROP TABLE IF EXISTS pharmacies")
c.execute('''CREATE TABLE pharmacies (
    id INTEGER PRIMARY KEY, name TEXT, brand TEXT, address TEXT, suburb TEXT,
    postcode TEXT, state TEXT, phone TEXT, gla REAL, latitude REAL, longitude REAL,
    bpl_uuid TEXT)''')

v4conn = sqlite3.connect(V4_DB)
v4conn.row_factory = sqlite3.Row
v4c = v4conn.cursor()
v4c.execute("SELECT * FROM pharmacies")
count = 0
for row in v4c.fetchall():
    c.execute('INSERT INTO pharmacies VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
        (row['id'], row['name'], row['brand'], row['address'], row['suburb'],
         row['postcode'], row['state'], row['phone'], row['gla'],
         row['latitude'], row['longitude'], row['bpl_uuid']))
    count += 1
print(f"  Inserted {count} pharmacies (all with coordinates)")

# 2. ENRICH medical_centres with GP counts from v4
print("\n--- Enriching medical centres with GP counts ---")

# Load v4 medical data (has GP counts but no coords)
v4c.execute("SELECT * FROM medical_centres WHERE num_gps IS NOT NULL AND num_gps > 0")
v4_med = [dict(r) for r in v4c.fetchall()]
print(f"  v4 medical centres with GP data: {len(v4_med)}")

# Build lookup by normalized name+suburb
v4_med_lookup = {}
for m in v4_med:
    key = (m['name'].strip().lower(), (m['suburb'] or '').strip().lower())
    if key not in v4_med_lookup:
        v4_med_lookup[key] = m

# Check if medical_centres table has num_gps column
c.execute("PRAGMA table_info(medical_centres)")
cols = [r[1] for r in c.fetchall()]
print(f"  Old medical_centres columns: {cols}")

if 'num_gps' not in cols:
    c.execute("ALTER TABLE medical_centres ADD COLUMN num_gps INTEGER")
    print("  Added num_gps column")

if 'centre_size' not in cols:
    c.execute("ALTER TABLE medical_centres ADD COLUMN centre_size TEXT")
    print("  Added centre_size column")

# Enrich existing records - match by normalized name
c.execute("SELECT id, name, address FROM medical_centres")
enriched = 0
for row in c.fetchall():
    name = row[1].strip().lower() if row[1] else ''
    # Try name+empty suburb first, then just name
    for suburb_try in ['', None]:
        key = (name, suburb_try or '')
        if key in v4_med_lookup:
            v4m = v4_med_lookup[key]
            c.execute("UPDATE medical_centres SET num_gps=?, centre_size=? WHERE id=?",
                      (v4m['num_gps'], v4m.get('centre_size', ''), row[0]))
            enriched += 1
            break
    else:
        # Try matching by name alone (first match)
        for k, v in v4_med_lookup.items():
            if k[0] == name:
                c.execute("UPDATE medical_centres SET num_gps=?, centre_size=? WHERE id=?",
                          (v['num_gps'], v.get('centre_size', ''), row[0]))
                enriched += 1
                break

print(f"  Enriched {enriched} medical centres with GP counts")

# Count enriched
c.execute("SELECT COUNT(*) FROM medical_centres WHERE num_gps IS NOT NULL AND num_gps > 0")
print(f"  Total with GP count: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM medical_centres WHERE num_gps >= 8")
print(f"  8+ GPs (Item 136): {c.fetchone()[0]}")

# 3. ENRICH supermarkets with GLA from v4
print("\n--- Enriching supermarkets with GLA ---")

v4c.execute("SELECT * FROM supermarkets WHERE gla IS NOT NULL AND gla > 0")
v4_super = [dict(r) for r in v4c.fetchall()]
print(f"  v4 supermarkets with GLA: {len(v4_super)}")

v4_super_lookup = {}
for s in v4_super:
    key = (s['name'].strip().lower(), (s['suburb'] or '').strip().lower())
    if key not in v4_super_lookup:
        v4_super_lookup[key] = s

c.execute("PRAGMA table_info(supermarkets)")
cols = [r[1] for r in c.fetchall()]
if 'gla' not in cols:
    c.execute("ALTER TABLE supermarkets ADD COLUMN gla REAL")
    print("  Added gla column")

c.execute("PRAGMA table_info(supermarkets)")
super_cols = [r[1] for r in c.fetchall()]
gla_col = 'gla' if 'gla' in super_cols else 'estimated_gla'

c.execute("SELECT id, name, address FROM supermarkets")
enriched_super = 0
for row in c.fetchall():
    name = row[1].strip().lower() if row[1] else ''
    for suburb_try in ['', None]:
        key = (name, suburb_try or '')
        if key in v4_super_lookup:
            c.execute(f"UPDATE supermarkets SET {gla_col}=? WHERE id=?",
                      (v4_super_lookup[key]['gla'], row[0]))
            enriched_super += 1
            break
    else:
        for k, v in v4_super_lookup.items():
            if k[0] == name:
                c.execute(f"UPDATE supermarkets SET {gla_col}=? WHERE id=?",
                          (v['gla'], row[0]))
                enriched_super += 1
                break

print(f"  Enriched {enriched_super} supermarkets with GLA")
c.execute(f"SELECT COUNT(*) FROM supermarkets WHERE {gla_col} IS NOT NULL AND {gla_col} > 0")
print(f"  Total with GLA ({gla_col}): {c.fetchone()[0]}")
c.execute(f"SELECT COUNT(*) FROM supermarkets WHERE {gla_col} >= 2500")
print(f"  GLA >= 2500sqm: {c.fetchone()[0]}")

v4conn.close()

# Create indexes
print("\n--- Creating indexes ---")
c.execute("CREATE INDEX IF NOT EXISTS idx_pharm_coords ON pharmacies(latitude, longitude)")
c.execute("CREATE INDEX IF NOT EXISTS idx_pharm_state ON pharmacies(state)")
c.execute("CREATE INDEX IF NOT EXISTS idx_med_gps ON medical_centres(num_gps)")
c.execute("CREATE INDEX IF NOT EXISTS idx_super_gla ON supermarkets(gla)")

conn.commit()

# Summary
print("\n" + "=" * 60)
print("HYBRID V4 DATABASE COMPLETE")
print("=" * 60)
for table in ['pharmacies', 'gps', 'supermarkets', 'hospitals', 'shopping_centres', 'medical_centres']:
    c.execute(f"SELECT COUNT(*) FROM {table}")
    total = c.fetchone()[0]
    c.execute(f"SELECT COUNT(*) FROM {table} WHERE latitude IS NOT NULL AND latitude != 0")
    with_coords = c.fetchone()[0]
    print(f"  {table}: {total} total, {with_coords} with coords")

print(f"\nDatabase: {HYBRID_DB}")
conn.close()
