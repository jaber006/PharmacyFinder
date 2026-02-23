import csv

with open('output/population_ranked_TAS.csv', 'r', encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    rows = list(reader)

print(f'Total TAS opportunities: {len(rows)}')
print()

# Search for Burnie/medical centre
print('=== BURNIE / MEDICAL CENTRE MENTIONS ===')
for r in rows:
    addr = r.get('Address','').lower()
    name = r.get('POI Name','').lower()
    evidence = r.get('Evidence','').lower()
    rules = r.get('Qualifying Rules','').lower()
    all_text = f'{addr} {name} {evidence} {rules}'
    if 'burnie' in all_text or 'family medical' in all_text or 'medical cent' in all_text:
        print(f"  Name: {r.get('POI Name','?')}")
        print(f"  Address: {r.get('Address','?')}")
        print(f"  Rules: {r.get('Qualifying Rules','?')}")
        nk = [k for k in r.keys() if 'Nearest Pharmacy' in k and 'km' in k]
        nn = [k for k in r.keys() if 'Nearest Pharmacy' in k and 'Name' in k]
        if nk: print(f"  Nearest Pharmacy: {r.get(nk[0],'?')} km")
        if nn: print(f"  Nearest Pharmacy Name: {r.get(nn[0],'?')}")
        print(f"  Pop 10km: {r.get('Pop 10km','?')}")
        print(f"  Composite Score: {r.get('Composite Score','?')}")
        print(f"  Lat/Lng: {r.get('Latitude','?')}, {r.get('Longitude','?')}")
        print()

# All near Burnie coords
print('=== ALL OPPORTUNITIES NEAR BURNIE ===')
for r in rows:
    try:
        lat = float(r.get('Latitude',0))
        lon = float(r.get('Longitude',0))
        if -41.15 < lat < -40.95 and 145.8 < lon < 146.0:
            nk = [k for k in r.keys() if 'Nearest Pharmacy' in k and 'km' in k]
            dist = r.get(nk[0],'?') if nk else '?'
            print(f"  {r.get('POI Name','?')} | {r.get('Address','?')} | Rx: {dist}km | Pop: {r.get('Pop 10km','?')} | Score: {r.get('Composite Score','?')}")
    except:
        pass

# Also check what pharmacies exist near Burnie
import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()
cur.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude BETWEEN -41.15 AND -40.95 AND longitude BETWEEN 145.8 AND 146.0")
print()
print('=== EXISTING PHARMACIES NEAR BURNIE ===')
for r in cur.fetchall():
    print(f"  {r[0]} | {r[1]} | ({r[2]:.4f}, {r[3]:.4f})")
conn.close()
