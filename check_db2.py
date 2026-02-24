import os
import sqlite3, os, glob

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Find all DB files
db_files = glob.glob(os.path.join(BASE_DIR, '**', '*.db'), recursive=True)
db_files += glob.glob(os.path.join(BASE_DIR, '*.db'))
print(f"DB files found: {db_files}")

for db in set(db_files):
    print(f"\n=== {db} ===")
    conn = sqlite3.connect(db)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in c.fetchall()]
    print(f"Tables: {tables}")
    for t in tables:
        c.execute(f"SELECT COUNT(*) FROM [{t}]")
        count = c.fetchone()[0]
        c.execute(f"PRAGMA table_info([{t}])")
        cols = [r[1] for r in c.fetchall()]
        print(f"  {t}: {count} rows, cols={cols}")
    conn.close()
