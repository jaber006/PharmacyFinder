import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

# Get all table schemas
c.execute("SELECT sql FROM sqlite_master WHERE type='table'")
for r in c.fetchall():
    if r[0]:
        print(r[0])
        print()

# Count rows
for table in ['opportunities', 'pharmacies', 'geocode_cache']:
    try:
        c.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"{table}: {c.fetchone()[0]} rows")
    except:
        print(f"{table}: does not exist")

# Sample opportunity
print("\n--- Sample opportunity ---")
c.execute("SELECT * FROM opportunities LIMIT 1")
cols = [d[0] for d in c.description]
row = c.fetchone()
for col, val in zip(cols, row):
    print(f"  {col}: {val}")

# Check Gordonvale specifically
print("\n--- Gordonvale ---")
c.execute("SELECT nearest_town, region, latitude, longitude, pharmacy_10km, nearest_pharmacy_km FROM opportunities WHERE nearest_town LIKE '%Gordonvale%'")
for r in c.fetchall():
    print(r)

# Check geocode_cache structure
print("\n--- geocode_cache ---")
try:
    c.execute("SELECT sql FROM sqlite_master WHERE name='geocode_cache'")
    print(c.fetchone())
    c.execute("SELECT * FROM geocode_cache LIMIT 3")
    cols = [d[0] for d in c.description]
    print("Columns:", cols)
    for r in c.fetchall():
        print(r)
except Exception as e:
    print(f"Error: {e}")

# Check verification columns
print("\n--- Verification columns ---")
c.execute("SELECT DISTINCT verification_status FROM opportunities")
for r in c.fetchall():
    print(r)

conn.close()
