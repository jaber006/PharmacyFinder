"""
Item 134A National Scanner — Large shopping centre additional pharmacy.

Finds large shopping centres where tenant count allows an additional pharmacy.
Requirements:
- Large shopping centre (same as Item 134)
- Already has pharmacy(ies) inside
- Tenant tiers:
  - 100-199 tenants + max 1 existing pharmacy → room for 2nd
  - ≥200 tenants + max 2 existing pharmacies → room for 3rd
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, extract_suburb_postcode, ALL_STATES
)
from engine.context import EvaluationContext
import json

MIN_CENTRE_GLA = 5000
MIN_TENANTS_LARGE = 50
MIN_TENANTS_TIER1 = 100   # 100-199 → allows 2nd pharmacy (max 1 existing)
MIN_TENANTS_TIER2 = 200   # ≥200 → allows 3rd pharmacy (max 2 existing)
MIN_SUPERMARKET_GLA = 2500
PHARMACY_IN_CENTRE_KM = 0.3

BRAND_DEFAULT_GLA = {
    "woolworths": 3500, "coles": 3500, "aldi": 1700,
}


def has_qualifying_supermarket(centre: dict, ctx) -> tuple:
    """Check for supermarket ≥2,500sqm in the centre."""
    majors = centre.get("major_supermarkets") or []
    if isinstance(majors, str):
        try:
            majors = json.loads(majors)
        except:
            majors = []
    
    if majors:
        for name in majors:
            name_lower = name.lower() if isinstance(name, str) else ""
            for brand, gla in BRAND_DEFAULT_GLA.items():
                if brand in name_lower and gla >= MIN_SUPERMARKET_GLA:
                    return True, f"{name} (~{gla}sqm)"
    
    nearby = ctx.supermarkets_within_radius(centre['latitude'], centre['longitude'], 0.15)
    for s, d in nearby:
        gla = s.get("estimated_gla") or s.get("floor_area_sqm") or 0
        if gla >= MIN_SUPERMARKET_GLA:
            return True, f"{s['name']} ({gla:.0f}sqm)"
        brand = (s.get("brand") or s.get("name") or "").lower()
        for bk, bgla in BRAND_DEFAULT_GLA.items():
            if bk in brand and bgla >= MIN_SUPERMARKET_GLA:
                return True, f"{s['name']} (~{bgla}sqm)"
    
    return False, ""


@timed_run
def scan_item134a():
    """Run Item 134A scanner nationally."""
    print("[Item 134A] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.shopping_centres)
    print(f"[Item 134A] Scanning {total} shopping centres for additional pharmacy opportunities...")
    
    for i, centre in enumerate(ctx.shopping_centres):
        clat = centre["latitude"]
        clon = centre["longitude"]
        
        # Must be a LARGE shopping centre
        centre_gla = centre.get("estimated_gla") or centre.get("gla_sqm") or 0
        tenants = centre.get("estimated_tenants") or 0
        
        if centre_gla > 0 and centre_gla < MIN_CENTRE_GLA:
            continue
        if tenants < MIN_TENANTS_TIER1:
            continue  # Need at least 100 tenants for additional pharmacy
        
        # Must have qualifying supermarket
        has_super, super_detail = has_qualifying_supermarket(centre, ctx)
        if not has_super:
            continue
        
        # Count existing pharmacies in/near the centre
        pharmacies_in_centre = ctx.pharmacies_within_radius(clat, clon, PHARMACY_IN_CENTRE_KM)
        existing_count = len(pharmacies_in_centre)
        
        if existing_count == 0:
            continue  # No existing pharmacy — this is Item 134, not 134A
        
        # Apply tier logic
        max_allowed = 0
        tier = ""
        if tenants >= MIN_TENANTS_TIER2:
            max_allowed = 3
            tier = f"Tier 2 (≥{MIN_TENANTS_TIER2} tenants → up to 3 pharmacies)"
        elif tenants >= MIN_TENANTS_TIER1:
            max_allowed = 2
            tier = f"Tier 1 ({MIN_TENANTS_TIER1}-{MIN_TENANTS_TIER2-1} tenants → up to 2 pharmacies)"
        
        if existing_count >= max_allowed:
            continue  # Already at max
        
        room_for = max_allowed - existing_count
        
        existing_names = [p['name'] for p, d in pharmacies_in_centre]
        
        # Calculate confidence
        confidence = 0.75
        if centre_gla >= MIN_CENTRE_GLA:
            confidence += 0.05
        if tenants >= MIN_TENANTS_TIER2:
            confidence += 0.05  # Higher tier = more certain
        confidence = min(confidence, 0.95)
        
        nearest_pharm, nearest_dist = ctx.nearest_pharmacy(clat, clon)
        nearest_pharmacy_km = round(nearest_dist, 3) if nearest_dist != float('inf') else 999
        
        addr = centre.get("address", "")
        state = detect_state_from_address(addr)
        suburb, postcode = extract_suburb_postcode(addr)
        
        reason = (f"Large SC additional pharmacy: {centre['name']}, "
                 f"tenants={tenants}, {tier}. "
                 f"Existing pharmacies ({existing_count}): {', '.join(existing_names)}. "
                 f"Room for {room_for} more. Supermarket: {super_detail}")
        
        results.append({
            "lat": round(clat, 6),
            "lon": round(clon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 134A",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": nearest_pharmacy_km,
            "reason": reason,
            "name": centre.get("name", ""),
            "address": addr,
            "source_type": "shopping_centre",
            "centre_gla": centre_gla,
            "estimated_tenants": tenants,
            "existing_pharmacies": existing_count,
            "room_for": room_for,
            "tier": tier,
            "existing_pharmacy_names": ", ".join(existing_names),
        })
    
    results.sort(key=lambda x: (-x["confidence"], -x["room_for"]))
    
    write_results(results, "Item 134A", extra_columns=[
        "centre_gla", "estimated_tenants", "existing_pharmacies", 
        "room_for", "tier", "existing_pharmacy_names"
    ])
    print_summary(results, "Item 134A")
    return results


if __name__ == "__main__":
    scan_item134a()
