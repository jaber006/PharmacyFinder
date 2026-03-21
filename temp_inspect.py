import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()
c.execute("SELECT sql FROM sqlite_master WHERE name='v2_results'")
print(c.fetchone()[0])
c.execute('SELECT COUNT(*) FROM v2_results')
print('Count:', c.fetchone()[0])
c.execute('SELECT * FROM v2_results LIMIT 1')
cols = [d[0] for d in c.description]
print('Cols:', cols)
r = c.fetchone()
for col, val in zip(cols, r):
    print(f'  {col}: {val}')
# Check existing tables
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('\nAll tables:', [r[0] for r in c.fetchall()])
