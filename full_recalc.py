"""Full recalculation of all opportunity metrics using verified coordinates."""
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
c.execute("""SELECT id, poi_name, latitude, longitude, region, nearest_pharmacy_km, 
             nearest_pharmacy_name, verification, composite_score, pop_5km, pop_10km, pop_15km,
             pharmacy_5km, pharmacy_10km, competition_score, qualifying_rules
             FROM opportunities WHERE latitude IS NOT NULL AND longitude IS NOT NULL""")
opportunities = c.fetchall()
print(f"Processing {len(opportunities)} opportunities\n")

updated = 0
new_false_positives = 0
was_verified_now_close = []

for row in opportunities:
    opp_id, poi_name, opp_lat, opp_lng, region, old_dist, old_ph_name, old_verif, old_score, \
        pop5, pop10, pop15, old_ph5, old_ph10, old_comp, rules = row
    
    # 1. Recalculate nearest pharmacy + distance
    best_dist = float('inf')
    best_name = None
    ph_within_5km = 0
    ph_within_10km = 0
    
    for ph_name, ph_lat, ph_lng in pharmacies:
        d = haversine(opp_lat, opp_lng, ph_lat, ph_lng) / 1000  # km
        if d < best_dist:
            best_dist = d
            best_name = ph_name
        if d <= 5:
            ph_within_5km += 1
        if d <= 10:
            ph_within_10km += 1
    
    # 2. Recalculate competition score (pop / pharmacies in area)
    competition_score = 0
    if ph_within_5km > 0 and pop5 and pop5 > 0:
        competition_score = round(pop5 / ph_within_5km, 1)
    elif ph_within_10km > 0 and pop10 and pop10 > 0:
        competition_score = round(pop10 / ph_within_10km, 1)
    
    # 3. Recalculate composite score
    # Score = population weight + distance weight + competition weight
    pop_score = 0
    if pop5 and pop5 > 0:
        pop_score = min(pop5 / 100, 500)  # cap at 500
    
    dist_score = 0
    if best_dist >= 1.5:
        dist_score = min(best_dist * 100, 1000)  # 1.5km = 150, cap at 1000
    
    comp_score_part = 0
    if competition_score > 0:
        comp_score_part = min(competition_score / 10, 300)  # cap at 300
    
    composite_score = round(pop_score + dist_score + comp_score_part)
    
    # 4. Update verification - mark as false positive if pharmacy is very close
    new_verif = old_verif
    if old_verif == 'VERIFIED' and best_dist < 0.5:
        # Pharmacy within 500m - this is not a real opportunity
        new_verif = 'FALSE_POSITIVE'
        new_false_positives += 1
        was_verified_now_close.append((poi_name, region, best_dist, best_name))
    elif old_verif == 'VERIFIED' and best_dist < 1.5:
        # Under threshold - doesn't qualify under distance rules
        new_verif = 'BELOW_THRESHOLD'
        was_verified_now_close.append((poi_name, region, best_dist, best_name))
    
    # 5. Write updates
    c.execute("""UPDATE opportunities SET 
                 nearest_pharmacy_km = ?, nearest_pharmacy_name = ?,
                 pharmacy_5km = ?, pharmacy_10km = ?,
                 competition_score = ?, composite_score = ?,
                 verification = ?
                 WHERE id = ?""",
              (round(best_dist, 3), best_name,
               ph_within_5km, ph_within_10km,
               competition_score, composite_score,
               new_verif, opp_id))
    updated += 1

conn.commit()

# Summary
print(f"Updated: {updated} opportunities")
print(f"New false positives (pharmacy <500m): {new_false_positives}")
print(f"Previously 'verified' now too close: {len(was_verified_now_close)}")

if was_verified_now_close:
    print(f"\n=== PREVIOUSLY VERIFIED, NOW DISQUALIFIED ===")
    was_verified_now_close.sort(key=lambda x: x[2])
    for name, region, dist, ph in was_verified_now_close:
        status = "FALSE_POSITIVE (<500m)" if dist < 0.5 else "BELOW_THRESHOLD (<1.5km)"
        print(f"  {name} ({region}) | {dist:.2f}km from {ph} | {status}")

# Final count by region and verification
print(f"\n=== FINAL COUNTS BY REGION ===")
c.execute("""SELECT region, verification, COUNT(*), 
             ROUND(AVG(nearest_pharmacy_km), 1), ROUND(AVG(composite_score), 0)
             FROM opportunities 
             GROUP BY region, verification 
             ORDER BY region, verification""")
for region, verif, cnt, avg_dist, avg_score in c.fetchall():
    print(f"  {region} | {verif}: {cnt} opps | avg dist: {avg_dist}km | avg score: {avg_score}")

# TAS final picture
print(f"\n=== TAS FINAL VERIFIED OPPORTUNITIES ===")
c.execute("""SELECT poi_name, nearest_pharmacy_km, nearest_pharmacy_name, composite_score, 
             pop_5km, pharmacy_5km, competition_score, qualifying_rules
             FROM opportunities 
             WHERE region = 'TAS' AND verification = 'VERIFIED'
             ORDER BY composite_score DESC""")
rows = c.fetchall()
print(f"Total TAS verified: {len(rows)}")
for i, (name, dist, ph, score, pop, ph5, comp, rules) in enumerate(rows, 1):
    print(f"  {i}. {name} | {dist:.2f}km from {ph} | Score: {score} | Pop: {pop:,} | Pharmacies in 5km: {ph5} | {rules}")

conn.close()
