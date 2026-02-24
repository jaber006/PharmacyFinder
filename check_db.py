import sqlite3
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:', [r[0] for r in cur.fetchall()])

for table in ['pharmacies', 'pharmacy', 'opportunities', 'pois']:
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if cols:
            print(f'\n{table} columns: {cols}')
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            print(f'{table} rows: {cur.fetchone()[0]}')
            cur.execute(f"SELECT * FROM {table} LIMIT 2")
            for row in cur.fetchall():
                print(f'  {row}')
    except:
        pass
conn.close()
