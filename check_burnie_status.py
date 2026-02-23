import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

c.execute("""SELECT id, poi_name, address, qualifying_rules, confidence, 
             nearest_pharmacy_km, nearest_pharmacy_name, verification, evidence
          FROM opportunities 
          WHERE address LIKE '%urnie%' OR poi_name LIKE '%urnie%' 
             OR poi_name LIKE '%TAS Family%' OR address LIKE '%Reeves%'""")
rows = c.fetchall()
print(f"Burnie-related opportunities: {len(rows)}")
for r in rows:
    print(f"\n  ID: {r[0]}")
    print(f"  POI: {r[1]}")
    print(f"  Address: {r[2]}")
    print(f"  Rules: {r[3]}")
    print(f"  Confidence: {r[4]}")
    print(f"  Nearest pharmacy: {r[5]}km ({r[6]})")
    print(f"  Verification: {r[7]}")
    ev = (r[8] or "None")[:500].encode('ascii', 'replace').decode()
    print(f"  Evidence: {ev}")

print("\n\n=== ALL TAS Item 136 ===")
c.execute("""SELECT id, poi_name, address, qualifying_rules, nearest_pharmacy_km, verification
          FROM opportunities 
          WHERE qualifying_rules LIKE '%136%' AND (address LIKE '%TAS%' OR region LIKE '%TAS%')""")
rows136 = c.fetchall()
print(f"Found: {len(rows136)}")
for r in rows136:
    print(f"  {r[0]} | {r[1]} | rules={r[3]} | {r[4]}km | {r[5]}")

# Check if TAS Family Medical is in medical_centres table
print("\n\n=== Medical Centres with 'Burnie' or 'TAS Family' ===")
c.execute("""SELECT id, name, address, num_gps, hours_per_week FROM medical_centres
          WHERE name LIKE '%urnie%' OR name LIKE '%TAS Family%' OR address LIKE '%urnie%' OR address LIKE '%Reeves%'""")
for r in c.fetchall():
    print(f"  {r[0]} | {r[1]} | {r[2]} | GPs={r[3]} | hrs={r[4]}")

conn.close()
