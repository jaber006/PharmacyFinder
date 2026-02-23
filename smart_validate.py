"""
Smart validation approach:
1. For each opportunity town, check if there's already a pharmacy in our DB 
   within the town (small radius like 5-10km from the town center)
2. For towns where nearest_pharmacy_km < 5, the pharmacy is close = likely has one
3. For remote towns (nearest_pharmacy > 50km), these are genuinely underserved
4. The middle ground needs web verification

Also categorize towns:
- Major cities (pop > 20k): Almost certainly have pharmacies (FALSE_POSITIVE if dist < 5km)
- Medium towns (pop 5k-20k): Very likely have pharmacies
- Small towns (pop 1k-5k): May or may not
- Remote communities (<1k): Usually only health clinics, not retail pharmacies
"""

import json, sqlite3
from math import radians, sin, cos, sqrt, atan2

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))

# Load town details
with open('output/town_details.json') as f:
    town_details = json.load(f)

# Load town batches
with open('output/town_batches.json') as f:
    batches = json.load(f)

# Get all towns to validate
towns_to_check = []
for batch in batches['batches']:
    for t in batch:
        if not t.startswith('Unknown'):
            parts = t.rsplit(', ', 1)
            key = f"{parts[0]}_{parts[1]}"
            towns_to_check.append({
                'name': parts[0],
                'state': parts[1],
                'key': key,
                'details': town_details.get(key, {})
            })

# Connect to DB and get all pharmacy locations
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()
c.execute("SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
pharmacies = c.fetchall()
print(f"Total pharmacies in DB: {len(pharmacies)}")

# Get opportunity details
c.execute("""SELECT id, poi_name, nearest_town, region, nearest_pharmacy_km, 
             nearest_pharmacy_name, latitude, longitude, pop_5km
             FROM opportunities""")
opps = c.fetchall()
print(f"Total opportunities: {len(opps)}")

# For each town, check if it has pharmacies nearby in our DB
# Use the opportunity's own coordinates as reference
town_pharmacy_check = {}
for town in towns_to_check:
    details = town['details']
    pharm_dist = details.get('max_pharmacy_dist', 999)
    pop = details.get('max_pop', 0)
    
    town_pharmacy_check[town['key']] = {
        'town': town['name'],
        'state': town['state'],
        'nearest_pharmacy_km': pharm_dist,
        'population': pop,
        'opp_count': details.get('opp_count', 0)
    }

# Print categorization
categories = {
    'major_city': [],      # pop > 20k, definitely have pharmacies
    'large_town': [],      # pop 5k-20k, very likely
    'medium_town': [],     # pop 2k-5k, probably
    'small_town': [],      # pop 500-2k, maybe
    'remote': [],          # pop < 500
    'close_pharmacy': [],  # nearest pharmacy < 2km (regardless of pop)
}

for key, info in town_pharmacy_check.items():
    pop = info['population']
    dist = info['nearest_pharmacy_km']
    
    if dist < 2:
        categories['close_pharmacy'].append(key)
    elif pop > 20000:
        categories['major_city'].append(key)
    elif pop > 5000:
        categories['large_town'].append(key)
    elif pop > 2000:
        categories['medium_town'].append(key)
    elif pop > 500:
        categories['small_town'].append(key)
    else:
        categories['remote'].append(key)

for cat, towns in categories.items():
    print(f"\n{cat}: {len(towns)}")
    for t in sorted(towns)[:5]:
        info = town_pharmacy_check[t]
        print(f"  {t}: pop={info['population']}, dist={info['nearest_pharmacy_km']:.1f}km")

conn.close()

# Save for review
with open('output/town_categories.json', 'w') as f:
    json.dump({
        'categories': {k: sorted(v) for k, v in categories.items()},
        'details': town_pharmacy_check
    }, f, indent=2)

print(f"\nSaved to output/town_categories.json")
