import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
conn = sqlite3.connect(os.path.join(BASE_DIR, 'pharmacy_finder.db'))
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
