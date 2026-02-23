import sqlite3
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')

print("=== BROKER SCAN RESULTS ===\n")

# Total
total = conn.execute("SELECT COUNT(*) FROM broker_listings").fetchone()[0]
print(f"Total listings: {total}\n")

# By source
print("BY SOURCE:")
for r in conn.execute("SELECT source, COUNT(*) FROM broker_listings GROUP BY source ORDER BY COUNT(*) DESC"):
    print(f"  {r[0]:25s} {r[1]:3d}")

# By state
print("\nBY STATE:")
for r in conn.execute("SELECT COALESCE(NULLIF(state,''), 'Unknown'), COUNT(*) FROM broker_listings GROUP BY state ORDER BY COUNT(*) DESC"):
    print(f"  {r[0]:10s} {r[1]:3d}")

# Tasmania listings (priority)
print("\n=== TASMANIA LISTINGS (PRIORITY) ===")
for r in conn.execute("SELECT title, price, url FROM broker_listings WHERE state='TAS' ORDER BY title"):
    print(f"\n  {r[0]}")
    if r[1]: print(f"    Price: {r[1]}")
    print(f"    URL: {r[2]}")

# All active (non-sold) listings
print("\n=== ALL ACTIVE LISTINGS ===")
for r in conn.execute("SELECT title, state, price, source FROM broker_listings WHERE title NOT LIKE '%SOLD%' ORDER BY state, source, title"):
    price_str = f" - {r[2]}" if r[2] else ""
    print(f"  [{r[1] or '??':3s}] [{r[3]:15s}] {r[0][:65]}{price_str}")

conn.close()
