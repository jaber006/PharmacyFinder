"""
Merge all GapMaps export CSVs into clean datasets for PharmacyFinder v4.
Deduplicates, filters to relevant categories, and outputs clean CSVs.
"""
import csv
import os
from collections import defaultdict

EXPORT_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\data\gapmaps_exports"
OUTPUT_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\data"

# Categories we care about for ACPA rules
PHARMACY_CLASS = "Pharmacy"
MEDICAL_CLASS = "Clinical Services"
SUPERMARKET_CLASS = "Supermarket and Grocery Stores"

# Read all CSVs
all_rows = []
for fname in os.listdir(EXPORT_DIR):
    if not fname.endswith('.csv'):
        continue
    source = fname.replace('.csv', '')
    with open(os.path.join(EXPORT_DIR, fname), 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row['_source'] = source
            all_rows.append(row)

print(f"Total rows loaded: {len(all_rows)}")

# Separate by classification
pharmacies = [r for r in all_rows if r.get('Classification') == PHARMACY_CLASS]
medical = [r for r in all_rows if r.get('Classification') == MEDICAL_CLASS]
supermarkets = [r for r in all_rows if r.get('Classification') == SUPERMARKET_CLASS]

print(f"Pharmacies (raw): {len(pharmacies)}")
print(f"Medical centres (raw): {len(medical)}")
print(f"Supermarkets (raw): {len(supermarkets)}")

def dedup_rows(rows, key_fields):
    """Deduplicate rows based on key fields."""
    seen = {}
    for row in rows:
        key = tuple(row.get(f, '').strip().lower() for f in key_fields)
        if key not in seen:
            seen[key] = row
    return list(seen.values())

# Dedup each category
KEY_FIELDS = ['Title', 'Address', 'Suburb', 'State']
pharmacies_unique = dedup_rows(pharmacies, KEY_FIELDS)
medical_unique = dedup_rows(medical, KEY_FIELDS)
supermarkets_unique = dedup_rows(supermarkets, KEY_FIELDS)

print(f"\nAfter dedup:")
print(f"Pharmacies: {len(pharmacies_unique)}")
print(f"Medical centres: {len(medical_unique)}")
print(f"Supermarkets: {len(supermarkets_unique)}")

# Key fields to keep for each category
PHARMACY_FIELDS = ['Classification', 'Business_Name', 'Title', 'Address', 'Suburb', 
                   'Postcode', 'State', 'Country', 'Phone Number', 'Store Type',
                   'Year Opened', 'Organisation', 'GLA']

MEDICAL_FIELDS = ['Classification', 'Business_Name', 'Title', 'Address', 'Suburb',
                  'Postcode', 'State', 'Country', 'Phone Number', 'Centre Size',
                  "Number of GP's", 'Practitioners', 'Organisation']

SUPERMARKET_FIELDS = ['Classification', 'Business_Name', 'Title', 'Address', 'Suburb',
                      'Postcode', 'State', 'Country', 'Phone Number', 'Store Type',
                      'GLA', 'Year Opened', 'Organisation', 'Drive Thru']

def write_clean_csv(rows, fields, filename):
    """Write rows to CSV with only specified fields."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    # Get all available fields from first row
    available = set(rows[0].keys()) if rows else set()
    use_fields = [f for f in fields if f in available]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=use_fields, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Written {len(rows)} rows to {filename}")
    return filepath

# Write clean CSVs
write_clean_csv(pharmacies_unique, PHARMACY_FIELDS, 'gapmaps_pharmacies_clean.csv')
write_clean_csv(medical_unique, MEDICAL_FIELDS, 'gapmaps_medical_centres_clean.csv')
write_clean_csv(supermarkets_unique, SUPERMARKET_FIELDS, 'gapmaps_supermarkets_clean.csv')

# Stats breakdown
print("\n=== PHARMACY STATS ===")
by_state = defaultdict(int)
for r in pharmacies_unique:
    by_state[r.get('State', 'Unknown')] += 1
for state, count in sorted(by_state.items(), key=lambda x: -x[1]):
    print(f"  {state}: {count}")

brands = defaultdict(int)
for r in pharmacies_unique:
    org = r.get('Organisation', '') or r.get('Business_Name', '') or ''
    brands[org] += 1
print("\nTop pharmacy brands:")
for brand, count in sorted(brands.items(), key=lambda x: -x[1])[:15]:
    print(f"  {count} - {brand}")

print("\n=== MEDICAL CENTRE STATS ===")
by_state_med = defaultdict(int)
for r in medical_unique:
    by_state_med[r.get('State', 'Unknown')] += 1
for state, count in sorted(by_state_med.items(), key=lambda x: -x[1]):
    print(f"  {state}: {count}")

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
    print(f"\nGP counts: {len(gp_counts)} centres with data")
    print(f"  Total GPs: {sum(gp_counts)}")
    print(f"  Average: {sum(gp_counts)/len(gp_counts):.1f}")
    print(f"  Max: {max(gp_counts)}")
    # Centres with 8+ GPs (Item 136 threshold)
    large = [g for g in gp_counts if g >= 8]
    print(f"  8+ GPs (Item 136 candidates): {len(large)}")

print("\n=== SUPERMARKET STATS ===")
by_brand = defaultdict(int)
gla_by_brand = defaultdict(list)
for r in supermarkets_unique:
    brand = r.get('Business_Name', '') or r.get('Title', '')
    by_brand[brand] += 1
    gla = r.get('GLA', '')
    if gla and gla.strip():
        try:
            gla_by_brand[brand].append(float(gla))
        except:
            pass

print("By brand (with avg GLA):")
for brand, count in sorted(by_brand.items(), key=lambda x: -x[1])[:15]:
    gla_vals = gla_by_brand.get(brand, [])
    avg_gla = f" (avg GLA: {sum(gla_vals)/len(gla_vals):.0f}sqm)" if gla_vals else ""
    print(f"  {count} - {brand}{avg_gla}")

print("\n=== DONE ===")
