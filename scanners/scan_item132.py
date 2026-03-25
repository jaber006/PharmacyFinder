"""
Item 132 National Scanner — One-pharmacy town second pharmacy.

Finds towns with exactly 1 pharmacy where all other pharmacies are ≥10km by road,
AND the town has ≥4 FTE GPs + 1-2 supermarkets with combined GLA ≥2,500sqm.

Logic:
1. Group pharmacies by town (suburb+postcode)
2. Find one-pharmacy towns
3. Check road distance to other pharmacies (must all be ≥10km)
4. Check GP FTE count in town
5. Check supermarket presence (1-2 with combined GLA ≥2,500sqm)
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, ALL_STATES
)
from engine.context import EvaluationContext
from collections import defaultdict
import requests

NEAREST_DISTANCE_M = 200        # Must be ≥200m from existing pharmacy
OTHER_ROUTE_KM = 10.0           # All other pharmacies ≥10km by road
MIN_FTE_GPS = 4.0
MIN_SUPERMARKET_GLA = 2500
MAX_SUPERMARKETS = 2
TOWN_RADIUS_KM = 5.0            # Radius to search for GPs/supermarkets in same town
OSRM_URL = "http://localhost:5000"

# Default GLA by brand
BRAND_DEFAULT_GLA = {
    "woolworths": 3500, "coles": 3500, "aldi": 1700, "iga": 800,
    "foodworks": 600, "drakes": 2000, "costco": 10000,
}


_osrm_alive = None  # None = untested, True/False after first try

def osrm_route_distance(lat1, lon1, lat2, lon2):
    """Get driving distance via local OSRM. Returns km or None."""
    global _osrm_alive
    if _osrm_alive is False:
        return None  # Skip if already known offline
    try:
        url = f"{OSRM_URL}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
        resp = requests.get(url, timeout=1)
        if resp.status_code == 200:
            data = resp.json()
            _osrm_alive = True
            if data.get("code") == "Ok" and data.get("routes"):
                return data["routes"][0]["distance"] / 1000.0
    except:
        _osrm_alive = False
    return None


def get_gla(s: dict) -> float:
    """Get supermarket GLA, using brand default if needed."""
    gla = s.get("estimated_gla") or s.get("floor_area_sqm") or 0
    if gla > 0:
        return gla
    brand = (s.get("brand") or s.get("name") or "").lower()
    for key, default in BRAND_DEFAULT_GLA.items():
        if key in brand:
            return default
    return 0


@timed_run
def scan_item132():
    """Run Item 132 scanner nationally."""
    print("[Item 132] Loading data...")
    ctx = EvaluationContext()
    
    # Group pharmacies by town (suburb + postcode)
    print("[Item 132] Grouping pharmacies by town (suburb+postcode)...")
    town_pharmacies = defaultdict(list)
    for p in ctx.pharmacies:
        suburb = (p.get("suburb") or "").upper().strip()
        postcode = (p.get("postcode") or "").strip()
        if suburb and postcode:
            town_key = f"{suburb}|{postcode}"
            town_pharmacies[town_key].append(p)
    
    # Find one-pharmacy towns
    one_pharmacy_towns = {k: v for k, v in town_pharmacies.items() if len(v) == 1}
    print(f"[Item 132] Found {len(one_pharmacy_towns)} one-pharmacy towns")
    
    results = []
    checked = 0
    
    for town_key, pharmacies in one_pharmacy_towns.items():
        checked += 1
        if checked % 100 == 0:
            print(f"  Progress: {checked}/{len(one_pharmacy_towns)} ({len(results)} hits)")
        
        pharm = pharmacies[0]
        plat, plon = pharm["latitude"], pharm["longitude"]
        suburb, postcode = town_key.split("|")
        state = pharm.get("state", "") or detect_state_from_address(pharm.get("address", ""))
        
        # Check: all OTHER pharmacies must be ≥10km by road
        # First, quick geodesic filter — if any other pharmacy <7km geodesic, skip
        other_pharmacies = ctx.pharmacies_within_radius(plat, plon, 15.0)
        
        # Filter out the pharmacy itself
        others = [(p, d) for p, d in other_pharmacies if p["id"] != pharm["id"]]
        
        # Quick fail: if any other pharmacy is <4km geodesic, almost certainly <10km by road
        # (using 4km instead of 5km to catch borderline cases where road is circuitous)
        too_close_geodesic = any(d < 4.0 for _, d in others)
        if too_close_geodesic:
            continue
        
        # Check the closest other pharmacy by road
        closest_other_road = float('inf')
        closest_other_name = ""
        osrm_verified = False
        
        if others:
            # Sort by geodesic, check the closest few
            others.sort(key=lambda x: x[1])
            for other_p, other_geo_d in others[:5]:  # Check up to 5 nearest
                if other_geo_d > 20:
                    break  # Beyond 20km geodesic, definitely >10km by road
                
                # Try OSRM for accuracy
                road_d = osrm_route_distance(plat, plon, other_p['latitude'], other_p['longitude'])
                if road_d is not None:
                    osrm_verified = True
                    if road_d < closest_other_road:
                        closest_other_road = road_d
                        closest_other_name = other_p['name']
                else:
                    # Estimate
                    est = other_geo_d * 1.4
                    if est < closest_other_road:
                        closest_other_road = est
                        closest_other_name = other_p['name']
        else:
            closest_other_road = 999
        
        if closest_other_road < OTHER_ROUTE_KM:
            continue  # Other pharmacy too close
        
        # Check GPs in town
        nearby_gps = ctx.gps_within_radius(plat, plon, TOWN_RADIUS_KM)
        # Sum FTE of GPs in same suburb if possible, else use radius
        total_fte = sum(
            (g.get("fte") or 1.0)
            for g, d in nearby_gps
        )
        
        gp_pass = total_fte >= MIN_FTE_GPS
        if total_fte < 2.0:
            continue  # Way too few GPs, skip entirely
        
        # Check supermarkets in town (1 or 2 with combined GLA ≥2,500)
        nearby_supers = ctx.supermarkets_within_radius(plat, plon, TOWN_RADIUS_KM)
        super_glas = sorted(
            [(s, get_gla(s)) for s, d in nearby_supers],
            key=lambda x: -x[1]
        )
        
        # Check best 1 or best 2
        supermarket_pass = False
        super_detail = ""
        if super_glas:
            if super_glas[0][1] >= MIN_SUPERMARKET_GLA:
                supermarket_pass = True
                super_detail = f"1 supermarket: {super_glas[0][0]['name']} ({super_glas[0][1]:.0f}sqm)"
            elif len(super_glas) >= 2:
                combined = super_glas[0][1] + super_glas[1][1]
                if combined >= MIN_SUPERMARKET_GLA:
                    supermarket_pass = True
                    super_detail = f"2 supermarkets: {super_glas[0][0]['name']}+{super_glas[1][0]['name']} ({combined:.0f}sqm)"
        
        if not supermarket_pass and not gp_pass:
            continue  # Neither requirement met
        
        # This town qualifies or is a near-miss!
        confidence = 0.5
        if gp_pass and supermarket_pass:
            confidence = 0.75
        elif gp_pass:
            confidence = 0.6  # Has GPs but weak supermarket
        elif supermarket_pass:
            confidence = 0.55  # Has supermarket but needs more GPs
        
        if osrm_verified:
            confidence += 0.1
        if total_fte >= 6:
            confidence = min(confidence + 0.05, 0.95)
        
        status = "QUALIFIES" if (gp_pass and supermarket_pass) else "NEAR-MISS"
        issues = []
        if not gp_pass:
            issues.append(f"needs {MIN_FTE_GPS-total_fte:.1f} more FTE GPs")
        if not supermarket_pass:
            issues.append("needs qualifying supermarket")
        issue_str = f" [{'; '.join(issues)}]" if issues else ""
        
        reason = (f"{status}: One-pharmacy town ({suburb} {postcode}): {pharm['name']}.{issue_str} "
                 f"Nearest other pharmacy: {closest_other_name} at {closest_other_road:.1f}km by road. "
                 f"GPs: {total_fte:.1f} FTE. {super_detail}")
        
        results.append({
            "lat": round(plat, 6),
            "lon": round(plon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 132",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": round(closest_other_road, 3),
            "reason": reason,
            "name": f"{suburb} (near {pharm['name']})",
            "address": pharm.get("address", ""),
            "source_type": "one_pharmacy_town",
            "existing_pharmacy": pharm['name'],
            "gp_fte": round(total_fte, 1),
            "supermarket_detail": super_detail,
            "osrm_verified": osrm_verified,
        })
    
    results.sort(key=lambda x: (-x["confidence"], -x["nearest_pharmacy_km"]))
    
    write_results(results, "Item 132", extra_columns=["existing_pharmacy", "gp_fte", "supermarket_detail", "osrm_verified"])
    print_summary(results, "Item 132")
    return results


if __name__ == "__main__":
    scan_item132()
