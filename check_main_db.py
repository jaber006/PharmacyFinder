import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()
tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
print('Tables:', tables)
for t in tables:
    tn = t[0]
    c.execute(f'SELECT COUNT(*) FROM [{tn}]')
    print(f'{tn}: {c.fetchone()[0]} rows')

# Check opportunities table structure
c.execute("PRAGMA table_info(opportunities)")
print('\nOpportunities columns:')
for col in c.fetchall():
    print(f'  {col}')

# Sample a few rows  
c.execute("SELECT poi_name, region, nearest_town, verification, nearest_pharmacy_km FROM opportunities LIMIT 5")
for row in c.fetchall():
    print(row)

# Check verification values
c.execute("SELECT verification, COUNT(*) FROM opportunities GROUP BY verification")
print('\nVerification breakdown:')
for row in c.fetchall():
    print(f'  {row[0]}: {row[1]}')

conn.close()
