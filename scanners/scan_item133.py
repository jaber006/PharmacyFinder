"""
Item 133 National Scanner — Small shopping centre pharmacy.

Finds small shopping centres where a new pharmacy could open.
Requirements:
- GLA ≥5,000 sqm
- ≥15 tenants (but <50 — else Item 134)
- Contains supermarket ≥2,500 sqm GLA
- Customer parking (assumed if shopping centre)
- No existing pharmacy in the centre
- ≥500m from nearest pharmacy (excluding those in large SCs or private hospitals)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, extract_suburb_postcode, ALL_STATES
)
from engine.context import EvaluationContext

MIN_CENTRE_GLA = 5000
MIN_TENANTS = 15
MAX_TENANTS_SMALL = 49  # <50 for small (≥50 is large/Item 134)
MIN_SUPERMARKET_GLA = 2500
PHARMACY_DISTANCE_M = 500   # 500m from nearest non-excluded pharmacy
PHARMACY_IN_CENTRE_KM = 0.1  # 100m proxy for "inside the centre"

# Default GLA estimates for brand supermarkets
BRAND_DEFAULT_GLA = {
    "woolworths": 3500, "coles": 3500, "aldi": 1700, "iga": 800,
}


def has_qualifying_supermarket(centre: dict, ctx: 'EvaluationContext') -> tuple:
    """Check if centre has a supermarket ≥2,500sqm GLA. Returns (bool, detail)."""
    # Check major_supermarkets field
    majors = centre.get("major_supermarkets") or []
    if isinstance(majors, str):
        import json
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
    
    # Also check supermarkets from DB near the centre
    nearby = ctx.supermarkets_within_radius(centre['latitude'], centre['longitude'], 0.15)
    for s, d in nearby:
        gla = s.get("estimated_gla") or s.get("floor_area_sqm") or 0
        if gla >= MIN_SUPERMARKET_GLA:
            return True, f"{s['name']} ({gla:.0f}sqm)"
        # Use brand defaults
        brand = (s.get("brand") or s.get("name") or "").lower()
        for bk, bgla in BRAND_DEFAULT_GLA.items():
            if bk in brand and bgla >= MIN_SUPERMARKET_GLA:
                return True, f"{s['name']} (~{bgla}sqm)"
    
    return False, ""


@timed_run
def scan_item133():
    """Run Item 133 scanner nationally."""
    print("[Item 133] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.shopping_centres)
    print(f"[Item 133] Scanning {total} shopping centres...")
    
    for i, centre in enumerate(ctx.shopping_centres):
        if (i + 1) % 200 == 0:
            print(f"  Progress: {i+1}/{total} ({len(results)} hits)")
        
        clat = centre["latitude"]
        clon = centre["longitude"]
        
        # Check GLA
        centre_gla = centre.get("estimated_gla") or centre.get("gla_sqm") or 0
        if centre_gla > 0 and centre_gla < MIN_CENTRE_GLA:
            continue
        
        # Check tenants — must be ≥15 and <50 (small centre)
        tenants = centre.get("estimated_tenants") or 0
        if tenants > 0:
            if tenants < MIN_TENANTS or tenants > MAX_TENANTS_SMALL:
                continue
        else:
            # Unknown tenant count — include with reduced confidence
            pass
        
        # Check for qualifying supermarket
        has_super, super_detail = has_qualifying_supermarket(centre, ctx)
        if not has_super:
            continue
        
        # Check: no existing pharmacy in the centre (within 100m)
        pharmacies_in_centre = ctx.pharmacies_within_radius(clat, clon, PHARMACY_IN_CENTRE_KM)
        if pharmacies_in_centre:
            continue  # Already has a pharmacy
        
        # Check: nearest pharmacy (excluding large SC / private hospital ones) ≥500m
        nearest_excl, dist_excl = ctx.nearest_pharmacy_excluding_complexes(clat, clon)
        
        if nearest_excl and dist_excl < (PHARMACY_DISTANCE_M / 1000.0):
            continue  # Too close to non-excluded pharmacy
        
        nearest_pharmacy_km = round(dist_excl, 3) if dist_excl != float('inf') else 999
        margin_m = (dist_excl * 1000 - PHARMACY_DISTANCE_M) if dist_excl != float('inf') else 999000
        
        # Calculate confidence
        confidence = 0.7
        if centre_gla >= MIN_CENTRE_GLA:
            confidence += 0.05
        if tenants >= MIN_TENANTS:
            confidence += 0.1
        elif tenants == 0:
            confidence -= 0.15  # Unknown tenants
        if margin_m > 200:
            confidence += 0.05
        confidence = min(max(confidence, 0.3), 0.95)
        
        addr = centre.get("address", "")
        state = detect_state_from_address(addr)
        suburb, postcode = extract_suburb_postcode(addr)
        
        reason = (f"Small SC: {centre['name']}, GLA={centre_gla:.0f}sqm, "
                 f"tenants={tenants or '?'}, supermarket: {super_detail}. "
                 f"Nearest non-excluded pharmacy: {nearest_pharmacy_km}km (margin +{margin_m:.0f}m)")
        
        results.append({
            "lat": round(clat, 6),
            "lon": round(clon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 133",
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
    
    results.sort(key=lambda x: (-x["confidence"], -x["nearest_pharmacy_km"]))
    
    write_results(results, "Item 133", extra_columns=["centre_gla", "estimated_tenants", "supermarket"])
    print_summary(results, "Item 133")
    return results


if __name__ == "__main__":
    scan_item133()
