"""
Item 130 National Scanner — Distance + Supermarket opportunities.

Finds supermarkets that are ≥1.5km from any pharmacy, qualifying under Item 130.
Logic:
  (a) ≥1.5km straight-line from nearest pharmacy
  (b)(i)  Supermarket ≥1,000 sqm GLA within 500m AND GP within 500m, OR
  (b)(ii) Supermarket ≥2,500 sqm GLA within 500m (no GP needed)

Since we scan FROM supermarkets, the supermarket IS the candidate anchor.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, extract_suburb_postcode, ALL_STATES
)
from engine.context import EvaluationContext

PHARMACY_DISTANCE_KM = 1.5
GP_RADIUS_KM = 0.5
SMALL_GLA = 1000
LARGE_GLA = 2500

# Default GLA by brand when unknown
BRAND_DEFAULT_GLA = {
    "woolworths": 3500,
    "coles": 3500,
    "aldi": 1700,
    "iga": 800,
    "foodworks": 600,
    "drakes": 2000,
    "harris farm": 2500,
    "costco": 10000,
    "metcash": 1500,
}


def get_supermarket_gla(s: dict) -> float:
    """Get GLA for a supermarket, using brand default if needed."""
    gla = s.get("estimated_gla") or s.get("floor_area_sqm") or 0
    if gla > 0:
        return gla
    brand = (s.get("brand") or s.get("name") or "").lower()
    for key, default in BRAND_DEFAULT_GLA.items():
        if key in brand:
            return default
    return 0


@timed_run
def scan_item130():
    """Run Item 130 scanner nationally."""
    print("[Item 130] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.supermarkets)
    
    print(f"[Item 130] Scanning {total} supermarkets nationally...")
    
    for i, supermarket in enumerate(ctx.supermarkets):
        if (i + 1) % 500 == 0:
            print(f"  Progress: {i+1}/{total} ({len(results)} hits so far)")
        
        slat = supermarket["latitude"]
        slon = supermarket["longitude"]
        
        # (a) Check distance to nearest pharmacy
        nearest_pharm, dist_km = ctx.nearest_pharmacy(slat, slon)
        if nearest_pharm and dist_km < PHARMACY_DISTANCE_KM:
            continue  # Too close to a pharmacy
        
        nearest_pharmacy_km = round(dist_km, 3) if dist_km != float("inf") else 999
        margin_m = (dist_km - PHARMACY_DISTANCE_KM) * 1000 if dist_km != float("inf") else 999000
        
        # GLA of this supermarket
        gla = get_supermarket_gla(supermarket)
        
        # Determine pass path
        reason = ""
        confidence = 0.0
        
        if gla >= LARGE_GLA:
            # (b)(ii) — large supermarket, no GP needed
            reason = f"Item 130(b)(ii): {supermarket['name']} GLA {gla:.0f}sqm ≥{LARGE_GLA}sqm, nearest pharmacy {nearest_pharmacy_km}km (margin +{margin_m:.0f}m)"
            confidence = _confidence(margin_m, gla_known=(supermarket.get("estimated_gla") or 0) > 0)
        elif gla >= SMALL_GLA:
            # (b)(i) — smaller supermarket + GP needed
            nearby_gps = ctx.gps_within_radius(slat, slon, GP_RADIUS_KM)
            if nearby_gps:
                gp, gp_dist = nearby_gps[0]
                reason = f"Item 130(b)(i): {supermarket['name']} GLA {gla:.0f}sqm + GP {gp['name']} within {gp_dist:.2f}km, nearest pharmacy {nearest_pharmacy_km}km"
                confidence = _confidence(margin_m, gla_known=(supermarket.get("estimated_gla") or 0) > 0) * 0.9
            else:
                continue  # No GP nearby — doesn't qualify
        else:
            continue  # GLA too small
        
        # Determine state
        addr = supermarket.get("address", "")
        state = detect_state_from_address(addr)
        suburb, postcode = extract_suburb_postcode(addr)
        
        results.append({
            "lat": round(slat, 6),
            "lon": round(slon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 130",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": nearest_pharmacy_km,
            "reason": reason,
            "name": supermarket.get("name", ""),
            "address": addr,
            "source_type": "supermarket",
            "gla_sqm": gla,
            "brand": supermarket.get("brand", ""),
            "margin_m": round(margin_m, 0),
        })
    
    # Sort by confidence descending, then distance margin
    results.sort(key=lambda x: (-x["confidence"], -x.get("margin_m", 0)))
    
    write_results(results, "Item 130", extra_columns=["gla_sqm", "brand", "margin_m"])
    print_summary(results, "Item 130")
    return results


def _confidence(margin_m: float, gla_known: bool = True) -> float:
    """Calculate confidence based on distance margin and data quality."""
    if margin_m > 500:
        base = 0.95
    elif margin_m > 200:
        base = 0.85
    elif margin_m > 50:
        base = 0.75
    else:
        base = 0.65
    if not gla_known:
        base *= 0.8  # Reduce confidence for estimated GLA
    return base


if __name__ == "__main__":
    scan_item130()
