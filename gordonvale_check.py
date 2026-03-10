import sqlite3, math

c = sqlite3.connect('pharmacy_finder.db')

rows = c.execute("SELECT id, address, nearest_town, region, latitude, longitude, pop_10km, pharmacy_10km, nearest_pharmacy_km, nearest_pharmacy_name, qualifying_rules FROM opportunities WHERE nearest_town = 'Gordonvale'").fetchall()
for r in rows:
    print(f"ID:{r[0]} | Addr:{r[1]} | Town:{r[2]},{r[3]} | Coords:{r[4]},{r[5]} | Pop:{r[6]} | Pharm:{r[7]} | Near:{r[9]} ({r[8]:.1f}km) | Rules:{r[10]}")

def hav(lat1,lon1,lat2,lon2):
    R=6371
    dlat=math.radians(lat2-lat1)
    dlon=math.radians(lon2-lon1)
    a=math.sin(dlat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R*2*math.asin(math.sqrt(a))

print()
print("=== PHARMACIES WITHIN 20km OF GORDONVALE (-17.10, 145.78) ===")
pharms = c.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL").fetchall()
nearby = []
for p in pharms:
    d = hav(-17.10, 145.78, p[2], p[3])
    if d < 20:
        nearby.append((d, p[0], p[1], p[2], p[3]))
nearby.sort()
for d, name, addr, lat, lon in nearby:
    print(f"  {d:.1f}km | {name} | {addr}")

if not nearby:
    print("  None found within 20km!")
    print("  Expanding to 30km...")
    for p in pharms:
        d = hav(-17.10, 145.78, p[2], p[3])
        if d < 30:
            nearby.append((d, p[0], p[1], p[2], p[3]))
    nearby.sort()
    for d, name, addr, lat, lon in nearby:
        print(f"  {d:.1f}km | {name} | {addr}")
