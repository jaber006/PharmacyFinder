import sqlite3
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
conn.execute('DELETE FROM broker_listings')
conn.commit()
cur = conn.execute('SELECT COUNT(*) FROM broker_listings')
print(f'Cleared. Rows remaining: {cur.fetchone()[0]}')
conn.close()
