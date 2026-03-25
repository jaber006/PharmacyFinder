"""Quick debug to understand why Item 132 finds 0 results."""
import warnings
warnings.filterwarnings("ignore")
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from engine.context import EvaluationContext
from collections import defaultdict
import requests

TOWN_RADIUS_KM = 5.0

ctx = EvaluationContext()

# Group pharmacies by town
town_pharmacies = defaultdict(list)
for p in ctx.pharmacies:
    suburb = (p.get("suburb") or "").upper().strip()
    postcode = (p.get("postcode") or "").strip()
    if suburb and postcode:
        town_pharmacies[f"{suburb}|{postcode}"].append(p)

one_pharmacy_towns = {k: v for k, v in town_pharmacies.items() if len(v) == 1}
print(f"One-pharmacy towns: {len(one_pharmacy_towns)}")

# Check why they fail
fail_reasons = defaultdict(int)
close_calls = []

for town_key, pharmacies in list(one_pharmacy_towns.items())[:200]:
    pharm = pharmacies[0]
    plat, plon = pharm["latitude"], pharm["longitude"]
    
    # Check other pharmacies
    other_pharmacies = ctx.pharmacies_within_radius(plat, plon, 15.0)
    others = [(p, d) for p, d in other_pharmacies if p["id"] != pharm["id"]]
    
    too_close = any(d < 5.0 for _, d in others)
    if too_close:
        fail_reasons["other_pharmacy_too_close"] += 1
        continue
    
    # Check GPs
    nearby_gps = ctx.gps_within_radius(plat, plon, TOWN_RADIUS_KM)
    total_fte = sum((g.get("fte") or 1.0) for g, d in nearby_gps)
    
    if total_fte < 4.0:
        fail_reasons[f"insufficient_gps (fte={total_fte:.1f})"] += 1
        if total_fte >= 2.0:
            close_calls.append(f"{town_key}: {total_fte:.1f} FTE GPs")
        continue
    
    # Check supermarkets
    nearby_supers = ctx.supermarkets_within_radius(plat, plon, TOWN_RADIUS_KM)
    if not nearby_supers:
        fail_reasons["no_supermarket"] += 1
        close_calls.append(f"{town_key}: {total_fte:.1f} FTE GPs but no supermarket")
        continue
    
    super_glas = sorted(
        [(s.get("estimated_gla") or s.get("floor_area_sqm") or 0) for s, d in nearby_supers],
        reverse=True
    )
    best_combined = super_glas[0] if len(super_glas) >= 1 else 0
    if len(super_glas) >= 2:
        best_combined = max(best_combined, super_glas[0] + super_glas[1])
    
    if best_combined < 2500:
        fail_reasons[f"supermarket_gla_too_small (best={best_combined:.0f})"] += 1
        close_calls.append(f"{town_key}: {total_fte:.1f} FTE, super GLA={best_combined:.0f}")
        continue
    
    fail_reasons["PASSED_ALL_CHECKS"] += 1
    close_calls.append(f"PASS: {town_key}: {total_fte:.1f} FTE, super GLA={best_combined:.0f}")

print("\nFail reasons (first 200 towns):")
for reason, count in sorted(fail_reasons.items(), key=lambda x: -x[1]):
    print(f"  {reason}: {count}")

print(f"\nClose calls ({len(close_calls)}):")
for cc in close_calls[:20]:
    print(f"  {cc}")
