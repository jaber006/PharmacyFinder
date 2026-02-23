import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

cur.execute("""SELECT poi_name, region, pop_10km, pharmacy_10km, nearest_pharmacy_km, qualifying_rules,
               CASE WHEN pharmacy_10km > 0 THEN CAST(pop_10km AS FLOAT) / pharmacy_10km ELSE pop_10km END as ratio
               FROM opportunities 
               WHERE verification = 'VERIFIED' AND qualifying_rules != 'NONE'
               ORDER BY ratio DESC""")

print(f"{'#':<4} {'Name':<45} {'ST':<4} {'Pop10km':<9} {'Ph10':<6} {'Ratio':<8} {'NearPh km':<10} {'Rules'}")
print("-" * 130)
for i, r in enumerate(cur.fetchall(), 1):
    name = str(r[0])[:44]
    ratio = r[6]
    print(f"{i:<4} {name:<45} {r[1]:<4} {r[2]:<9} {r[3]:<6} {ratio:<8.0f} {r[4]:<10.1f} {r[5]}")

print()
cur.execute("SELECT verification, COUNT(*) FROM opportunities GROUP BY verification ORDER BY COUNT(*) DESC")
print("Database status:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")
conn.close()
