import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

# Check for key tables
for tbl in ['pharmacies', 'shopping_centres', 'hospitals', 'supermarkets', 'gps', 'medical_centres']:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
    exists = cur.fetchone()
    if exists:
        cur.execute(f"SELECT COUNT(*) FROM [{tbl}]")
        cnt = cur.fetchone()[0]
        cur.execute(f"PRAGMA table_info([{tbl}])")
        cols = [r[1] for r in cur.fetchall()]
        print(f"{tbl}: {cnt} rows, columns: {cols}")
        # Sample row
        cur.execute(f"SELECT * FROM [{tbl}] LIMIT 1")
        print(f"  Sample: {cur.fetchone()}")
    else:
        print(f"{tbl}: NOT FOUND")
    print()

conn.close()
