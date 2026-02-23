import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

# Check population data for Norwood area
c.execute("SELECT poi_name, nearest_town, pop_5km, pop_10km, pop_15km FROM opportunities WHERE poi_name LIKE '%Norwood%' AND region = 'TAS'")
print("Opportunity population data:")
for r in c.fetchall():
    print(f"  {r[0]} | Town: {r[1]} | 5km: {r[2]:,} | 10km: {r[3]:,} | 15km: {r[4]:,}")

# Check if we have suburb-level population
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in c.fetchall()]
print(f"\nTables: {tables}")

for t in ['towns', 'suburbs', 'localities', 'populations']:
    if t in tables:
        c.execute(f"SELECT * FROM {t} WHERE name LIKE '%Norwood%' OR name LIKE '%norwood%' LIMIT 5")
        rows = c.fetchall()
        if rows:
            print(f"\n{t} matching Norwood: {rows}")

conn.close()
