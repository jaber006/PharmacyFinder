import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM pharmacies")
print(f"Total: {cur.fetchone()[0]}")
cur.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source ORDER BY COUNT(*) DESC")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")
conn.close()
