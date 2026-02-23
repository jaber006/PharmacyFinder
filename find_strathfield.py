import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()
cur.execute("SELECT poi_name, latitude, longitude, region, pop_10km, pharmacy_10km, composite_score, qualifying_rules FROM opportunities WHERE poi_name LIKE '%Strathfield%'")
for r in cur.fetchall():
    print(f"Name: {r[0]}")
    print(f"Lat/Lng: {r[1]}, {r[2]}")
    print(f"State: {r[3]} | Pop: {r[4]} | Pharmacies: {r[5]} | Score: {r[6]}")
    print(f"Rules: {r[7]}")
    print()

# Also check: how many opportunities have score > 0 now?
cur.execute("SELECT COUNT(*) FROM opportunities WHERE composite_score > 0")
print(f"Opportunities with score > 0: {cur.fetchone()[0]}")

cur.execute("SELECT COUNT(*) FROM opportunities WHERE composite_score = 0")
print(f"Opportunities with score = 0: {cur.fetchone()[0]}")

conn.close()
