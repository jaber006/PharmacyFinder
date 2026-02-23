import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

c.execute("""SELECT poi_name, nearest_town, region, composite_score, pop_5km, pop_10km,
             nearest_pharmacy_km, nearest_pharmacy_name, qualifying_rules, pharmacy_5km,
             competition_score
             FROM opportunities 
             WHERE qualifying_rules IS NOT NULL AND qualifying_rules != '' 
             AND qualifying_rules != 'NONE'
             ORDER BY composite_score DESC
             LIMIT 30""")
rows = c.fetchall()
print(f"Top 30 nationally (by composite score):\n")
for i, (name, town, region, score, pop5, pop10, dist, nearest, rules, ph5, comp) in enumerate(rows, 1):
    pop5_str = f"{pop5:,}" if pop5 else "0"
    pop10_str = f"{pop10:,}" if pop10 else "0"
    print(f"{i}. {name} ({region})")
    print(f"   Town: {town} | Score: {score:.0f}")
    print(f"   Pop: {pop5_str} (5km) / {pop10_str} (10km) | Pharmacies in 5km: {ph5}")
    print(f"   Nearest pharmacy: {nearest} ({dist:.1f}km)")
    print(f"   Rules: {rules}")
    print()
conn.close()
