"""
Build PharmacyFinder v4 database from GapMaps exports.
Merges:
- v4 exports (filtered pharmacy+supermarket, uncapped states)
- v5 exports (filtered pharmacy+supermarket, NSW+VIC splits)
- v3 exports (all categories, uncapped regions - for medical centres)
- Protobuf pharmacies (6,503 complete with coordinates)
- v1 exports (medical centres from original round)
"""
import csv
import os
import sqlite3
from collections import defaultdict

BASE = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\data"
V4_DIR = os.path.join(BASE, "gapmaps_exports_v4")
V3_DIR = os.path.join(BASE, "gapmaps_exports_v3") 
V1_DIR = os.path.join(BASE, "gapmaps_exports")
PROTO_FILE = os.path.join(BASE, "gapmaps_pharmacies.csv")  # decoded protobuf
DB_PATH = os.path.join(os.path.dirname(BASE), "pharmacy_finder_v4.db")

def load_csvs(directory, prefix_filter=None):
    """Load all CSVs from a directory, optionally filtering by filename prefix."""
    rows = []
    for fname in os.listdir(directory):
        if not fname.endswith('.csv'):
            continue
        if prefix_filter and not any(fname.startswith(p) for p in prefix_filter):
            continue
        filepath = os.path.join(directory, fname)
        source = fname.replace('.csv', '')
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['_source'] = source
                rows.append(row)
    return rows

def dedup(rows, key_fn):
    """Deduplicate rows using a key function."""
    seen = {}
    for row in rows:
        key = key_fn(row)
        if key and key not in seen:
            seen[key] = row
    return list(seen.values())

print("=" * 60)
print("BUILDING PHARMACYFINDER V4 DATABASE")
print("=" * 60)

# 1. PHARMACIES - from filtered v4/v5 exports
print("\n--- PHARMACIES ---")
pharm_rows = []

# v4 uncapped (non-NSW, non-VIC)
v4_files = load_csvs(V4_DIR)
v4_pharm = [r for r in v4_files if r.get('Classification') == 'Pharmacy']
print(f"v4/v5 pharmacy rows (raw): {len(v4_pharm)}")

# Dedup by Title + Address + Suburb + State
pharm_unique = dedup(v4_pharm, lambda r: (
    r.get('Title', '').strip().lower(),
    r.get('Address', '').strip().lower(),
    r.get('Suburb', '').strip().lower(),
    r.get('State', '').strip().lower()
))
print(f"v4/v5 pharmacy rows (deduped): {len(pharm_unique)}")

# Also load protobuf pharmacies for coordinates
proto_pharm = []
if os.path.exists(PROTO_FILE):
    with open(PROTO_FILE, 'r', encoding='utf-8') as f:
        proto_pharm = list(csv.DictReader(f))
    print(f"Protobuf pharmacies: {len(proto_pharm)}")

# 2. SUPERMARKETS - from filtered v4/v5 exports
print("\n--- SUPERMARKETS ---")
super_rows = [r for r in v4_files if r.get('Classification') == 'Supermarket and Grocery Stores']
print(f"v4/v5 supermarket rows (raw): {len(super_rows)}")

# Remove butchers and fish retailers
super_filtered = [r for r in super_rows if r.get('Business_Name', '') not in ('All Butchers', 'Fish & Seafood Retailers')]
print(f"After removing butchers/fish: {len(super_filtered)}")

super_unique = dedup(super_filtered, lambda r: (
    r.get('Title', '').strip().lower(),
    r.get('Address', '').strip().lower(),
    r.get('Suburb', '').strip().lower(),
    r.get('State', '').strip().lower()
))
print(f"Supermarkets (deduped): {len(super_unique)}")

# GLA stats
gla_by_brand = defaultdict(list)
for r in super_unique:
    brand = r.get('Business_Name', '')
    gla = r.get('GLA', '')
    if gla and gla.strip():
        try:
            gla_by_brand[brand].append(float(gla))
        except:
            pass

print("\nSupermarket brands with GLA:")
brands = defaultdict(int)
for r in super_unique:
    brands[r.get('Business_Name', '')] += 1
for brand, count in sorted(brands.items(), key=lambda x: -x[1])[:15]:
    gla_vals = gla_by_brand.get(brand, [])
    avg = f" (avg GLA: {sum(gla_vals)/len(gla_vals):.0f}sqm)" if gla_vals else ""
    print(f"  {count} - {brand}{avg}")

# 3. MEDICAL CENTRES - from v3 uncapped + v1 exports
print("\n--- MEDICAL CENTRES ---")
# Load v3 uncapped exports
v3_rows = load_csvs(V3_DIR) if os.path.exists(V3_DIR) else []
v3_medical = [r for r in v3_rows if r.get('Classification') == 'Clinical Services']
print(f"v3 medical rows: {len(v3_medical)}")

# Load v1 exports  
v1_rows = load_csvs(V1_DIR) if os.path.exists(V1_DIR) else []
v1_medical = [r for r in v1_rows if r.get('Classification') == 'Clinical Services']
print(f"v1 medical rows: {len(v1_medical)}")

# Combine and dedup
all_medical = v3_medical + v1_medical
medical_unique = dedup(all_medical, lambda r: (
    r.get('Title', '').strip().lower(),
    r.get('Address', '').strip().lower(),
    r.get('Suburb', '').strip().lower(),
    r.get('State', '').strip().lower()
))
print(f"Medical centres (deduped): {len(medical_unique)}")

# GP count stats
gp_counts = []
for r in medical_unique:
    gp = r.get("Number of GP's", '')
    if gp and gp.strip():
        try:
            gp_counts.append(int(float(gp)))
        except:
            pass
if gp_counts:
    print(f"  With GP count data: {len(gp_counts)}")
    print(f"  Total GPs: {sum(gp_counts)}")
    print(f"  Average GPs per centre: {sum(gp_counts)/len(gp_counts):.1f}")
    print(f"  8+ GPs (Item 136): {len([g for g in gp_counts if g >= 8])}")

# 4. BUILD SQLITE DATABASE
print("\n--- BUILDING DATABASE ---")
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Pharmacies table
c.execute('''CREATE TABLE pharmacies (
    id INTEGER PRIMARY KEY,
    name TEXT,
    brand TEXT,
    address TEXT,
    suburb TEXT,
    postcode TEXT,
    state TEXT,
    phone TEXT,
    gla REAL,
    latitude REAL,
    longitude REAL,
    source TEXT
)''')

# Try to match v4/v5 pharmacy data with protobuf coordinates
proto_lookup = {}
for p in proto_pharm:
    key = p.get('name', '').strip().lower()
    if key:
        proto_lookup[key] = p

matched = 0
for r in pharm_unique:
    name = r.get('Title', '')
    brand = r.get('Business_Name', '') or r.get('Organisation', '')
    lat, lng = None, None
    
    # Try to match with protobuf by name
    name_lower = name.strip().lower()
    if name_lower in proto_lookup:
        p = proto_lookup[name_lower]
        lat = float(p.get('latitude', 0)) if p.get('latitude') else None
        lng = float(p.get('longitude', 0)) if p.get('longitude') else None
        if lat: matched += 1
    
    gla = None
    if r.get('GLA', '').strip():
        try: gla = float(r['GLA'])
        except: pass
    
    c.execute('INSERT INTO pharmacies (name, brand, address, suburb, postcode, state, phone, gla, latitude, longitude, source) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (name, brand, r.get('Address',''), r.get('Suburb',''), r.get('Postcode',''),
         r.get('State',''), r.get('Phone Number',''), gla, lat, lng, 'gapmaps_v4'))

# Add protobuf-only pharmacies (ones not in CSV exports)
csv_names = {r.get('Title','').strip().lower() for r in pharm_unique}
proto_only = 0
for p in proto_pharm:
    name = p.get('name', '').strip()
    if name.lower() not in csv_names:
        lat = float(p.get('latitude', 0)) if p.get('latitude') else None
        lng = float(p.get('longitude', 0)) if p.get('longitude') else None
        c.execute('INSERT INTO pharmacies (name, brand, address, suburb, postcode, state, phone, gla, latitude, longitude, source) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (name, '', '', '', '', '', '', None, lat, lng, 'gapmaps_proto'))
        proto_only += 1

print(f"Pharmacies: {len(pharm_unique)} from CSV + {proto_only} proto-only = {len(pharm_unique) + proto_only} total")
print(f"  CSV->Proto coordinate match: {matched}/{len(pharm_unique)} ({matched*100//len(pharm_unique)}%)")

# Medical centres table
c.execute('''CREATE TABLE medical_centres (
    id INTEGER PRIMARY KEY,
    name TEXT,
    organisation TEXT,
    address TEXT,
    suburb TEXT,
    postcode TEXT,
    state TEXT,
    phone TEXT,
    num_gps INTEGER,
    centre_size TEXT,
    source TEXT
)''')

for r in medical_unique:
    num_gps = None
    gp_str = r.get("Number of GP's", '')
    if gp_str and gp_str.strip():
        try: num_gps = int(float(gp_str))
        except: pass
    
    c.execute('INSERT INTO medical_centres (name, organisation, address, suburb, postcode, state, phone, num_gps, centre_size, source) VALUES (?,?,?,?,?,?,?,?,?,?)',
        (r.get('Title',''), r.get('Organisation',''), r.get('Address',''), r.get('Suburb',''),
         r.get('Postcode',''), r.get('State',''), r.get('Phone Number',''), num_gps,
         r.get('Centre Size',''), 'gapmaps'))

print(f"Medical centres: {len(medical_unique)}")

# Supermarkets table
c.execute('''CREATE TABLE supermarkets (
    id INTEGER PRIMARY KEY,
    name TEXT,
    brand TEXT,
    address TEXT,
    suburb TEXT,
    postcode TEXT,
    state TEXT,
    phone TEXT,
    gla REAL,
    store_type TEXT,
    source TEXT
)''')

for r in super_unique:
    gla = None
    if r.get('GLA', '').strip():
        try: gla = float(r['GLA'])
        except: pass
    
    c.execute('INSERT INTO supermarkets (name, brand, address, suburb, postcode, state, phone, gla, store_type, source) VALUES (?,?,?,?,?,?,?,?,?,?)',
        (r.get('Title',''), r.get('Business_Name',''), r.get('Address',''), r.get('Suburb',''),
         r.get('Postcode',''), r.get('State',''), r.get('Phone Number',''), gla,
         r.get('Store Type',''), 'gapmaps_v4'))

print(f"Supermarkets: {len(super_unique)}")

conn.commit()

# Summary
print("\n" + "=" * 60)
print("V4 DATABASE COMPLETE")
print("=" * 60)
c.execute("SELECT COUNT(*) FROM pharmacies")
print(f"Pharmacies: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM pharmacies WHERE latitude IS NOT NULL")
print(f"  With coordinates: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM medical_centres")
print(f"Medical centres: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM medical_centres WHERE num_gps IS NOT NULL")
print(f"  With GP count: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM medical_centres WHERE num_gps >= 8")
print(f"  8+ GPs (Item 136): {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets")
print(f"Supermarkets: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets WHERE gla >= 2500")
print(f"  GLA >= 2500sqm (Item 132): {c.fetchone()[0]}")
print(f"\nDatabase: {DB_PATH}")
conn.close()
