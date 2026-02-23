import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("Tables:", [r[0] for r in cursor.fetchall()])

for table in ['pharmacies', 'pharmacy', 'approved_pharmacies']:
    try:
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
        result = cursor.fetchone()
        if result:
            print(f"\n{table} schema:")
            print(result[0])
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"Count: {cursor.fetchone()[0]}")
            cursor.execute(f"SELECT * FROM {table} LIMIT 2")
            cols = [d[0] for d in cursor.description]
            print(f"Columns: {cols}")
            for row in cursor.fetchall():
                print(row)
    except:
        pass

# Also check all tables
for table_name in ['pharmacies', 'pharmacy', 'approved_pharmacies', 'pbs_pharmacies', 'opportunities', 'locations']:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"\n{table_name}: {count} rows")
    except:
        pass

# Just list all tables with schemas
cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
for name, sql in cursor.fetchall():
    cursor.execute(f"SELECT COUNT(*) FROM [{name}]")
    count = cursor.fetchone()[0]
    print(f"\n=== {name} ({count} rows) ===")
    print(sql)

conn.close()
