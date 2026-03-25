"""
Item 134 National Scanner — Large shopping centre with NO existing pharmacy.

Requirements:
- Large shopping centre: GLA ≥5,000sqm, ≥50 tenants, supermarket ≥2,500sqm, parking
- No approved pharmacy in the centre
- No distance requirement from external pharmacies

These are rare but gold — a large centre without a pharmacy is near-guaranteed approval.
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
MIN_SUPERMARKET_GLA = 2500
PHARMACY_IN_CENTRE_KM = 0.3   # 300m proxy for "inside the centre"

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
def scan_item134():
    """Run Item 134 scanner nationally."""
    print("[Item 134] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.shopping_centres)
    print(f"[Item 134] Scanning {total} shopping centres for large centres without pharmacy...")
    
    for i, centre in enumerate(ctx.shopping_centres):
        clat = centre["latitude"]
        clon = centre["longitude"]
        
        # Must be a LARGE shopping centre
        centre_gla = centre.get("estimated_gla") or centre.get("gla_sqm") or 0
        tenants = centre.get("estimated_tenants") or 0
        
        # Filter: need GLA ≥5,000 and ≥50 tenants
        if centre_gla > 0 and centre_gla < MIN_CENTRE_GLA:
            continue
        if tenants > 0 and tenants < MIN_TENANTS_LARGE:
            continue
        # If both unknown, skip (too uncertain)
        if centre_gla == 0 and tenants == 0:
            continue
        
        # Must have qualifying supermarket
        has_super, super_detail = has_qualifying_supermarket(centre, ctx)
        if not has_super:
            continue
        
        # Check: NO pharmacy in the centre
        pharmacies_in_centre = ctx.pharmacies_within_radius(clat, clon, PHARMACY_IN_CENTRE_KM)
        if pharmacies_in_centre:
            continue  # Already has pharmacy
        
        # Also check has_pharmacy flag from DB
        if centre.get("has_pharmacy"):
            continue
        
        # This is a large centre with no pharmacy — gold!
        nearest_pharm, nearest_dist = ctx.nearest_pharmacy(clat, clon)
        nearest_pharmacy_km = round(nearest_dist, 3) if nearest_dist != float('inf') else 999
        
        confidence = 0.8
        if centre_gla >= MIN_CENTRE_GLA and tenants >= MIN_TENANTS_LARGE:
            confidence = 0.9
        elif centre_gla == 0 or tenants == 0:
            confidence = 0.6  # Missing data
        
        addr = centre.get("address", "")
        state = detect_state_from_address(addr)
        suburb, postcode = extract_suburb_postcode(addr)
        
        reason = (f"Large SC without pharmacy: {centre['name']}, "
                 f"GLA={centre_gla:.0f}sqm, tenants={tenants or '?'}, "
                 f"supermarket: {super_detail}. "
                 f"Nearest external pharmacy: {nearest_pharmacy_km}km")
        
        results.append({
            "lat": round(clat, 6),
            "lon": round(clon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 134",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": nearest_pharmacy_km,
            "reason": reason,
            "name": centre.get("name", ""),
            "address": addr,
            "source_type": "shopping_centre",
            "centre_gla": centre_gla,
            "estimated_tenants": tenants,
            "supermarket": super_detail,
        })
    
    results.sort(key=lambda x: (-x["confidence"], -x["estimated_tenants"]))
    
    write_results(results, "Item 134", extra_columns=["centre_gla", "estimated_tenants", "supermarket"])
    print_summary(results, "Item 134")
    return results


if __name__ == "__main__":
    scan_item134()
