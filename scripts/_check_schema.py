import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()
cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
for name, sql in cur.fetchall():
    print(f"=== {name} ===")
    print(sql)
    # Show row count
    cur2 = conn.cursor()
    cur2.execute(f"SELECT COUNT(*) FROM [{name}]")
    print(f"Rows: {cur2.fetchone()[0]}")
    print()
conn.close()
