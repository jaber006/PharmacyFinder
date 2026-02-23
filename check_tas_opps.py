import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

c.execute("SELECT verification, COUNT(*) FROM opportunities WHERE region = 'TAS' GROUP BY verification")
print("TAS breakdown:")
for v, cnt in c.fetchall():
    print(f"  {v}: {cnt}")

print("\nVerified TAS opportunities:")
c.execute("SELECT poi_name, nearest_town, composite_score, pop_5km, nearest_pharmacy_km, qualifying_rules FROM opportunities WHERE region = 'TAS' AND verification = 'VERIFIED' ORDER BY composite_score DESC")
for i, r in enumerate(c.fetchall(), 1):
    print(f"{i}. {r[0]} ({r[1]}) - Score: {r[2]:.0f} | Pop: {r[3]:,} | Nearest pharm: {r[4]:.1f}km | {r[5]}")

conn.close()
