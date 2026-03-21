import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for t in tables:
    print(f"\n--- {t[0]} ---")
    for row in c.execute(f"PRAGMA table_info({t[0]})").fetchall():
        print(row)
    count = c.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
    print(f"Row count: {count}")
conn.close()
