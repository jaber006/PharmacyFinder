"""
PharmacyFinder v4 Database Builder (v2)
Strategy: 
- Pharmacies: Protobuf (6,503) is AUTHORITATIVE. Enrich with CSV metadata.
- Supermarkets: Deduplicate aggressively from filtered exports.
- Medical Centres: Deduplicate aggressively from all exports.
"""
import csv
import os
import sqlite3
import re
from collections import defaultdict
from difflib import SequenceMatcher

BASE = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\data"
V4_DIR = os.path.join(BASE, "gapmaps_exports_v4")
V3_DIR = os.path.join(BASE, "gapmaps_exports_v3")
V1_DIR = os.path.join(BASE, "gapmaps_exports")
PROTO_FILE = os.path.join(BASE, "gapmaps_pharmacies.csv")
DB_PATH = os.path.join(os.path.dirname(BASE), "pharmacy_finder_v4.db")

def load_all_csvs(*dirs):
    rows = []
    for d in dirs:
        if not os.path.exists(d):
            continue
        for fname in os.listdir(d):
            if not fname.endswith('.csv'):
                continue
            with open(os.path.join(d, fname), 'r', encoding='utf-8-sig') as f:
                rows.extend(csv.DictReader(f))
    return rows

def normalize(s):
    """Normalize string for matching."""
    if not s:
        return ''
    s = s.strip().lower()
    # Remove common suffixes/prefixes for better matching
    s = re.sub(r'\s+', ' ', s)
    return s

def postcode_key(row):
    """Create key: normalized_name + postcode."""
    name = normalize(row.get('Title', ''))
    pc = row.get('Postcode', '').strip()
    return f"{name}|{pc}" if name and pc else None

print("=" * 60)
print("PHARMACYFINDER V4 DATABASE (v2 build)")
print("=" * 60)

# ============================================================
# 1. PHARMACIES - Protobuf is authoritative (6,503)
# ============================================================
print("\n--- PHARMACIES (protobuf = source of truth) ---")

# Load protobuf
proto = []
with open(PROTO_FILE, 'r', encoding='utf-8') as f:
    proto = list(csv.DictReader(f))
print(f"Protobuf pharmacies: {len(proto)}")

# Load all CSV pharmacy data for enrichment
all_csv = load_all_csvs(V4_DIR, V3_DIR, V1_DIR)
csv_pharm = [r for r in all_csv if r.get('Classification') == 'Pharmacy']
print(f"CSV pharmacy rows (raw): {len(csv_pharm)}")

# Build lookup by normalized name for CSV enrichment
csv_lookup = {}
for r in csv_pharm:
    name = normalize(r.get('Title', ''))
    if name and name not in csv_lookup:
        csv_lookup[name] = r

# Match protobuf to CSV
matched = 0
enriched_pharmacies = []
for p in proto:
    name = p.get('name', '').strip()
    lat = float(p.get('latitude', 0)) if p.get('latitude') else None
    lng = float(p.get('longitude', 0)) if p.get('longitude') else None
    
    # Try exact name match to CSV
    csv_row = csv_lookup.get(normalize(name))
    
    entry = {
        'name': name,
        'brand': csv_row.get('Business_Name', '') if csv_row else '',
        'address': csv_row.get('Address', '') if csv_row else '',
        'suburb': csv_row.get('Suburb', '') if csv_row else '',
        'postcode': csv_row.get('Postcode', '') if csv_row else '',
        'state': csv_row.get('State', '') if csv_row else '',
        'phone': csv_row.get('Phone Number', '') if csv_row else '',
        'gla': None,
        'latitude': lat,
        'longitude': lng,
        'bpl_uuid': p.get('bpl_uuid', ''),
    }
    
    if csv_row:
        matched += 1
        gla = csv_row.get('GLA', '')
        if gla and gla.strip():
            try: entry['gla'] = float(gla)
            except: pass
    
    enriched_pharmacies.append(entry)

print(f"Enriched with CSV metadata: {matched}/{len(proto)} ({matched*100//len(proto)}%)")

# State breakdown from enriched data
by_state = defaultdict(int)
for p in enriched_pharmacies:
    by_state[p['state'] or 'Unknown'] += 1
print("By state:")
for s, c in sorted(by_state.items(), key=lambda x: -x[1]):
    print(f"  {s}: {c}")

# ============================================================
# 2. SUPERMARKETS - Aggressive dedup from filtered exports
# ============================================================
print("\n--- SUPERMARKETS ---")
all_super = [r for r in load_all_csvs(V4_DIR) if r.get('Classification') == 'Supermarket and Grocery Stores']
# Remove butchers and fish
all_super = [r for r in all_super if r.get('Business_Name', '') not in ('All Butchers', 'Fish & Seafood Retailers')]
print(f"Supermarket rows (raw, filtered): {len(all_super)}")

# Dedup by name + postcode (more aggressive than name+suburb+state)
seen = {}
for r in all_super:
    key = postcode_key(r)
    if key and key not in seen:
        seen[key] = r
super_unique = list(seen.values())
print(f"Supermarkets (deduped name+postcode): {len(super_unique)}")

# Brand stats
brands = defaultdict(int)
gla_by_brand = defaultdict(list)
for r in super_unique:
    brand = r.get('Business_Name', '')
    brands[brand] += 1
    gla = r.get('GLA', '')
    if gla and gla.strip():
        try: gla_by_brand[brand].append(float(gla))
        except: pass

print("Top brands:")
for brand, count in sorted(brands.items(), key=lambda x: -x[1])[:10]:
    gla_vals = gla_by_brand.get(brand, [])
    avg = f" (avg {sum(gla_vals)/len(gla_vals):.0f}sqm)" if gla_vals else ""
    print(f"  {count} - {brand}{avg}")

# ============================================================
# 3. MEDICAL CENTRES - From all exports
# ============================================================
print("\n--- MEDICAL CENTRES ---")
all_med = [r for r in load_all_csvs(V4_DIR, V3_DIR, V1_DIR) if r.get('Classification') == 'Clinical Services']
print(f"Medical rows (raw): {len(all_med)}")

# Dedup by name + postcode
seen = {}
for r in all_med:
    key = postcode_key(r)
    if key and key not in seen:
        seen[key] = r
med_unique = list(seen.values())
print(f"Medical centres (deduped): {len(med_unique)}")

gp_counts = []
for r in med_unique:
    gp = r.get("Number of GP's", '')
    if gp and gp.strip():
        try: gp_counts.append(int(float(gp)))
        except: pass
print(f"  With GP count: {len(gp_counts)}, Total GPs: {sum(gp_counts)}")
print(f"  8+ GPs (Item 136 candidates): {len([g for g in gp_counts if g >= 8])}")

# ============================================================
# 4. BUILD SQLITE
# ============================================================
print("\n--- BUILDING SQLITE DATABASE ---")
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

c.execute('''CREATE TABLE pharmacies (
    id INTEGER PRIMARY KEY, name TEXT, brand TEXT, address TEXT, suburb TEXT,
    postcode TEXT, state TEXT, phone TEXT, gla REAL, latitude REAL, longitude REAL,
    bpl_uuid TEXT)''')

for p in enriched_pharmacies:
    c.execute('INSERT INTO pharmacies (name,brand,address,suburb,postcode,state,phone,gla,latitude,longitude,bpl_uuid) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
        (p['name'], p['brand'], p['address'], p['suburb'], p['postcode'],
         p['state'], p['phone'], p['gla'], p['latitude'], p['longitude'], p['bpl_uuid']))

c.execute('''CREATE TABLE medical_centres (
    id INTEGER PRIMARY KEY, name TEXT, organisation TEXT, address TEXT, suburb TEXT,
    postcode TEXT, state TEXT, phone TEXT, num_gps INTEGER, centre_size TEXT)''')

for r in med_unique:
    num_gps = None
    gp_str = r.get("Number of GP's", '')
    if gp_str and gp_str.strip():
        try: num_gps = int(float(gp_str))
        except: pass
    c.execute('INSERT INTO medical_centres (name,organisation,address,suburb,postcode,state,phone,num_gps,centre_size) VALUES (?,?,?,?,?,?,?,?,?)',
        (r.get('Title',''), r.get('Organisation',''), r.get('Address',''), r.get('Suburb',''),
         r.get('Postcode',''), r.get('State',''), r.get('Phone Number',''), num_gps, r.get('Centre Size','')))

c.execute('''CREATE TABLE supermarkets (
    id INTEGER PRIMARY KEY, name TEXT, brand TEXT, address TEXT, suburb TEXT,
    postcode TEXT, state TEXT, phone TEXT, gla REAL, store_type TEXT)''')

for r in super_unique:
    gla = None
    if r.get('GLA', '').strip():
        try: gla = float(r['GLA'])
        except: pass
    c.execute('INSERT INTO supermarkets (name,brand,address,suburb,postcode,state,phone,gla,store_type) VALUES (?,?,?,?,?,?,?,?,?)',
        (r.get('Title',''), r.get('Business_Name',''), r.get('Address',''), r.get('Suburb',''),
         r.get('Postcode',''), r.get('State',''), r.get('Phone Number',''), gla, r.get('Store Type','')))

# Create indexes
c.execute('CREATE INDEX idx_pharm_state ON pharmacies(state)')
c.execute('CREATE INDEX idx_pharm_suburb ON pharmacies(suburb)')
c.execute('CREATE INDEX idx_pharm_coords ON pharmacies(latitude, longitude)')
c.execute('CREATE INDEX idx_med_state ON medical_centres(state)')
c.execute('CREATE INDEX idx_med_gps ON medical_centres(num_gps)')
c.execute('CREATE INDEX idx_super_state ON supermarkets(state)')
c.execute('CREATE INDEX idx_super_gla ON supermarkets(gla)')

conn.commit()

# Final summary
print("\n" + "=" * 60)
print("PHARMACYFINDER V4 DATABASE COMPLETE")
print("=" * 60)
for table in ['pharmacies', 'medical_centres', 'supermarkets']:
    c.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"{table}: {c.fetchone()[0]}")

c.execute("SELECT COUNT(*) FROM pharmacies WHERE latitude IS NOT NULL AND latitude != 0")
print(f"  Pharmacies with coordinates: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM pharmacies WHERE address != ''")
print(f"  Pharmacies with address: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM medical_centres WHERE num_gps >= 8")
print(f"  Medical 8+ GPs (Item 136): {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets WHERE gla >= 2500")
print(f"  Supermarkets GLA>=2500 (Item 132): {c.fetchone()[0]}")
print(f"\nDatabase: {DB_PATH}")
conn.close()
