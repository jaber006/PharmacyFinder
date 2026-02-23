import json, sqlite3

conn = sqlite3.connect('output/pharmacy_opportunities.db')
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:', c.fetchall())

for table_row in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
    table = table_row[0]
    c.execute(f"PRAGMA table_info({table})")
    print(f"\n{table} columns:", c.fetchall())
    c.execute(f"SELECT * FROM {table} LIMIT 3")
    print(f"{table} sample:", c.fetchall())
    c.execute(f"SELECT COUNT(*) FROM {table}")
    print(f"{table} count:", c.fetchone()[0])

conn.close()
