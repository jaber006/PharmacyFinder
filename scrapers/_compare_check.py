import json, sqlite3

with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\national_pharmacies_2026-02-23.json') as f:
    data = json.load(f)

# Check member types
types = {}
for p in data['pharmacies']:
    t = p.get('member_type', 'unknown')
    types[t] = types.get(t, 0) + 1
print('Member types:', types)

# Check DB sources
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
cur = conn.cursor()
cur.execute('SELECT source, COUNT(*) FROM pharmacies GROUP BY source')
print('\nDB sources:', cur.fetchall())

cur.execute('SELECT COUNT(*) FROM pharmacies')
print('Total in DB:', cur.fetchone()[0])

# Compare NSW
cur.execute("SELECT name FROM pharmacies WHERE state='NSW'")
db_nsw = set(r[0] for r in cur.fetchall())
scraped_nsw = set(p['name'] for p in data['pharmacies'] if p['state'] == 'NSW')
print(f'\nNSW: DB={len(db_nsw)}, Scraped={len(scraped_nsw)}')
print(f'In DB but not scraped: {len(db_nsw - scraped_nsw)}')
print(f'In scraped but not DB: {len(scraped_nsw - db_nsw)}')
print('Examples in DB not scraped:', list(db_nsw - scraped_nsw)[:15])
print('Examples in scraped not DB:', list(scraped_nsw - db_nsw)[:10])

# Compare all
cur.execute("SELECT name, state FROM pharmacies")
db_all = set()
for r in cur.fetchall():
    db_all.add((r[0], r[1] or ''))

scraped_all = set()
for p in data['pharmacies']:
    scraped_all.add((p['name'], p['state']))

print(f'\nOverall: DB={len(db_all)}, Scraped={len(scraped_all)}')
print(f'In DB but not scraped: {len(db_all - scraped_all)}')
print(f'In scraped but not DB: {len(scraped_all - db_all)}')
