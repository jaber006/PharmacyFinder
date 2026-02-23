import sqlite3
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
rows = conn.execute("SELECT title, state FROM broker_listings WHERE source='Agile BB' ORDER BY title").fetchall()
for r in rows:
    print(f"  {r[0][:80]}  [{r[1]}]")
print(f"\nTotal Agile BB: {len(rows)}")
conn.close()
