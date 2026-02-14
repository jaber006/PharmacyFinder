"""Check all medical centres for Item 136 eligibility."""
import sys
sys.stdout.reconfigure(encoding='utf-8')

from utils.database import Database
from utils.distance import haversine_distance, find_nearest, find_within_radius, format_distance

db = Database('pharmacy_finder.db')
db.connect()

pharmacies = db.get_all_pharmacies()
centres = db.get_all_medical_centres()

print(f"Checking {len(centres)} medical centres against {len(pharmacies)} pharmacies\n")
print(f"{'Centre':<40s} | GPs | FTE  | Nearest Pharmacy     | Dist    | Item 136?")
print("-" * 120)

qualified = []
for mc in sorted(centres, key=lambda x: x.get('num_gps', 0), reverse=True):
    name = mc.get('name', '?')[:38]
    num_gps = mc.get('num_gps', 0)
    fte = mc.get('total_fte', 0) or num_gps * 0.8
    
    nearest_pharm, dist = find_nearest(mc['latitude'], mc['longitude'], pharmacies)
    pharm_name = (nearest_pharm.get('name', '?') if nearest_pharm else 'N/A')[:20]
    
    # Check eligibility
    meets_fte = fte >= 8.0 or num_gps >= 8
    no_pharm_in_centre = dist > 0.1 if dist else True
    meets_300m = dist >= 0.3 if dist else True
    
    if meets_fte and no_pharm_in_centre and meets_300m:
        status = "YES !!!"
        qualified.append(mc)
    elif meets_fte and no_pharm_in_centre:
        status = f"NO (pharmacy {format_distance(dist)})"
    elif meets_fte:
        status = "NO (pharm in centre)"
    else:
        status = f"NO (only {fte:.1f} FTE)"
    
    dist_str = format_distance(dist) if dist else 'N/A'
    print(f"{name:<40s} | {num_gps:>3d} | {fte:>4.1f} | {pharm_name:<20s} | {dist_str:>7s} | {status}")

print(f"\n{'=' * 60}")
if qualified:
    print(f"QUALIFIED CENTRES: {len(qualified)}")
    for mc in qualified:
        print(f"  * {mc['name']} ({mc.get('address', '')})")
else:
    print("No centres qualify under Item 136")
    print("All large centres already have pharmacies nearby")

db.close()
