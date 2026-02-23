"""Recalculate all nearest pharmacy distances using verified coordinates."""
import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# Load all pharmacies
c.execute("SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
pharmacies = [(name, lat, lng) for name, lat, lng in c.fetchall()]
print(f"Loaded {len(pharmacies)} pharmacies")

# Load all opportunities
c.execute("SELECT id, poi_name, latitude, longitude, nearest_pharmacy_km, nearest_pharmacy_name, region FROM opportunities WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
opportunities = c.fetchall()
print(f"Processing {len(opportunities)} opportunities\n")

fixed = 0
now_too_close = 0  # opportunities where nearest pharmacy is now < threshold
big_changes = []

for opp_id, poi_name, opp_lat, opp_lng, old_dist, old_name, region in opportunities:
    # Find actual nearest pharmacy
    best_dist = float('inf')
    best_name = None
    for ph_name, ph_lat, ph_lng in pharmacies:
        d = haversine(opp_lat, opp_lng, ph_lat, ph_lng) / 1000  # km
        if d < best_dist:
            best_dist = d
            best_name = ph_name
    
    # Update if different
    if old_dist is None or abs(best_dist - old_dist) > 0.01:  # >10m difference
        change = best_dist - (old_dist or 0)
        if abs(change) > 0.1:  # >100m change
            big_changes.append((poi_name, region, old_dist, best_dist, old_name, best_name))
        
        c.execute("UPDATE opportunities SET nearest_pharmacy_km = ?, nearest_pharmacy_name = ? WHERE id = ?",
                  (round(best_dist, 3), best_name, opp_id))
        fixed += 1
    
    if best_dist < 1.5:
        now_too_close += 1

conn.commit()

print(f"Updated: {fixed} opportunities")
print(f"Opportunities now < 1.5km from pharmacy: {now_too_close}")
print(f"\n=== BIG CHANGES (>100m shift) ===")
big_changes.sort(key=lambda x: abs((x[3] or 0) - (x[2] or 0)), reverse=True)
for name, region, old, new, old_ph, new_ph in big_changes[:30]:
    old_str = f"{old:.2f}km" if old else "None"
    print(f"  {name} ({region}): {old_str} -> {new:.2f}km | Was: {old_ph} | Now: {new_ph}")

# How many TAS opportunities still have >= 1.5km?
print(f"\n=== TAS OPPORTUNITIES >= 1.5km after recalc ===")
c.execute("""SELECT poi_name, nearest_pharmacy_km, nearest_pharmacy_name, composite_score 
             FROM opportunities WHERE region = 'TAS' AND nearest_pharmacy_km >= 1.5 
             ORDER BY composite_score DESC""")
for name, dist, ph, score in c.fetchall():
    print(f"  {name} | {dist:.2f}km from {ph} | Score: {score:.0f}")

conn.close()
