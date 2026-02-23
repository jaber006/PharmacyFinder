import json, sqlite3

conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

# Overall breakdown
c.execute("SELECT verification, COUNT(*) FROM opportunities GROUP BY verification ORDER BY verification")
print("=" * 60)
print("PHARMACY VALIDATION SUMMARY")
print("=" * 60)
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

# False positives by state
print("\nFALSE POSITIVES by state:")
c.execute("""SELECT region, COUNT(*) FROM opportunities 
             WHERE verification='FALSE_POSITIVE' 
             GROUP BY region ORDER BY COUNT(*) DESC""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Show some false positive examples
print("\nTop FALSE_POSITIVE examples (towns with pharmacy our DB missed):")
c.execute("""SELECT nearest_town, region, nearest_pharmacy_km, poi_name 
             FROM opportunities WHERE verification='FALSE_POSITIVE'
             ORDER BY nearest_pharmacy_km DESC LIMIT 15""")
for row in c.fetchall():
    print(f"  {row[0]}, {row[1]}: nearest pharmacy {row[2]:.1f}km (POI: {row[3]})")

# Verified genuine opportunities
print("\nVERIFIED opportunities (genuine) by state:")
c.execute("""SELECT region, COUNT(*) FROM opportunities 
             WHERE verification='VERIFIED' 
             GROUP BY region ORDER BY COUNT(*) DESC""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Unverified
print("\nUNVERIFIED remaining:")
c.execute("""SELECT region, COUNT(*) FROM opportunities 
             WHERE verification='UNVERIFIED' 
             GROUP BY region ORDER BY COUNT(*) DESC""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Load validation data
with open('output/pharmacy_validation.json') as f:
    val = json.load(f)

no_pharm_towns = [k for k, v in val.items() if not v]
print(f"\n{'='*60}")
print(f"TOWNS WITH NO PHARMACY ({len(no_pharm_towns)} total):")
print(f"{'='*60}")
for t in sorted(no_pharm_towns):
    print(f"  {t.replace('_', ', ')}")

conn.close()
