"""
Item 131 National Scanner — Rural/remote pharmacy gaps.

Finds locations ≥10km by road from any pharmacy where population exists.
Strategy:
1. Find pharmacy pairs >20km apart (geodesic) — gaps exist between them
2. Compute midpoints and check for populated areas
3. Also check known towns from census_sa1 that are far from any pharmacy
4. Use OSRM for borderline cases, estimate (1.4x geodesic) otherwise
"""
import sys
import os
# Force unbuffered output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, ALL_STATES
)
from engine.context import EvaluationContext
from geopy.distance import geodesic
import sqlite3
import requests

ROUTE_DISTANCE_KM = 10.0
GEODESIC_CLEAR_PASS_KM = 8.0   # 8km straight ≈ 11.2km by road
MIN_POPULATION = 200            # Minimum population to flag
OSRM_URL = "http://localhost:5000"
_osrm_available = None  # Cached after first check


def _check_osrm():
    """Check if OSRM is running. Caches result."""
    global _osrm_available
    if _osrm_available is not None:
        return _osrm_available
    try:
        resp = requests.get(f"{OSRM_URL}/route/v1/driving/149,-33;149.1,-33?overview=false", timeout=3)
        _osrm_available = resp.status_code == 200
    except:
        _osrm_available = False
    if not _osrm_available:
        print("[Item 131] OSRM offline — using 1.4x geodesic estimates only")
    else:
        print("[Item 131] OSRM available — will verify borderline cases")
    return _osrm_available


def osrm_route_distance(lat1, lon1, lat2, lon2):
    """Get driving distance via local OSRM. Returns km or None."""
    if not _check_osrm():
        return None
    try:
        url = f"{OSRM_URL}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "Ok" and data.get("routes"):
                return data["routes"][0]["distance"] / 1000.0
    except:
        pass
    return None


@timed_run
def scan_item131():
    """Run Item 131 scanner nationally."""
    print("[Item 131] Loading data...")
    ctx = EvaluationContext()
    
    # Strategy 1: Find census SA1 areas far from pharmacies
    print("[Item 131] Checking census SA1 centroids for pharmacy gaps...")
    db_path = ctx.db_path
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    # Load SA1 centroids with population
    # SA1_CODE_2021 first digit = state (1=NSW, 2=VIC, 3=QLD, 4=SA, 5=WA, 6=TAS, 7=NT, 8=ACT)
    SA1_STATE_MAP = {"1": "NSW", "2": "VIC", "3": "QLD", "4": "SA", "5": "WA", "6": "TAS", "7": "NT", "8": "ACT"}
    cur.execute("""
        SELECT SA1_CODE_2021, _lat, _lon, Tot_P_P
        FROM census_sa1
        WHERE _lat IS NOT NULL AND _lon IS NOT NULL
        AND Tot_P_P > 0
    """)
    sa1_rows = []
    for r in cur.fetchall():
        row = dict(r)
        code = str(row.get("SA1_CODE_2021", ""))
        sa1_rows.append({
            "sa1_code": code,
            "latitude": row["_lat"],
            "longitude": row["_lon"],
            "population": row["Tot_P_P"] or 0,
            "state": SA1_STATE_MAP.get(code[0], "") if code else "",
            "suburb": "",
            "postcode": "",
        })
    conn.close()
    
    print(f"[Item 131] Loaded {len(sa1_rows)} populated SA1 areas")
    
    results = []
    checked = 0
    
    # Check each populated SA1 area
    for sa1 in sa1_rows:
        checked += 1
        if checked % 5000 == 0:
            print(f"  Progress: {checked}/{len(sa1_rows)} ({len(results)} hits)")
            sys.stdout.flush()
        
        lat, lon = sa1["latitude"], sa1["longitude"]
        pop = sa1.get("population", 0) or 0
        
        if pop < MIN_POPULATION:
            continue
        
        # Quick geodesic check to nearest pharmacy
        nearest_pharm, geo_dist = ctx.nearest_pharmacy(lat, lon)
        
        if nearest_pharm is None:
            # Very remote — no pharmacy at all
            results.append(_make_result(sa1, 999, None, "No pharmacy found in database"))
            continue
        
        if geo_dist < 5.0:
            # Clearly too close even by road
            continue
        
        # Estimate road distance
        est_road = geo_dist * 1.4
        
        if geo_dist >= GEODESIC_CLEAR_PASS_KM:
            # Clearly passes — geodesic >8km means road likely >10km
            reason = (f"Nearest pharmacy {nearest_pharm['name']} is {geo_dist:.1f}km geodesic "
                     f"(~{est_road:.1f}km by road), pop={pop}")
            
            # Try OSRM for accuracy on promising ones
            road_dist = None
            if geo_dist < 15:  # Only OSRM for borderline-ish
                road_dist = osrm_route_distance(lat, lon, nearest_pharm['latitude'], nearest_pharm['longitude'])
            
            actual_dist = road_dist if road_dist else est_road
            if actual_dist >= ROUTE_DISTANCE_KM:
                results.append(_make_result(sa1, actual_dist, nearest_pharm, reason, road_dist is not None))
            continue
        
        # Borderline (5-8km geodesic) — try OSRM
        road_dist = osrm_route_distance(lat, lon, nearest_pharm['latitude'], nearest_pharm['longitude'])
        if road_dist and road_dist >= ROUTE_DISTANCE_KM:
            reason = (f"OSRM verified: {road_dist:.1f}km by road to {nearest_pharm['name']} "
                     f"({geo_dist:.1f}km geodesic), pop={pop}")
            results.append(_make_result(sa1, road_dist, nearest_pharm, reason, True))
        elif road_dist is None and est_road >= ROUTE_DISTANCE_KM:
            reason = (f"Estimated: ~{est_road:.1f}km by road to {nearest_pharm['name']} "
                     f"({geo_dist:.1f}km geodesic, OSRM unavailable), pop={pop}")
            results.append(_make_result(sa1, est_road, nearest_pharm, reason, False))
    
    # Strategy 2: Pharmacy pair midpoints (find gaps between isolated pharmacies)
    print("[Item 131] Checking pharmacy-pair midpoints...")
    sys.stdout.flush()
    checked_pairs = set()
    for i, p1 in enumerate(ctx.pharmacies):
        if i % 1000 == 0 and i > 0:
            print(f"  Pharmacy pairs: {i}/{len(ctx.pharmacies)}")
            sys.stdout.flush()
        
        # Find nearest neighbour pharmacy using spatial index
        best_dist = float('inf')
        best_neighbour = None
        candidates = ctx._pharm_idx.candidates_near(p1['latitude'], p1['longitude'], 50)
        for p2 in candidates:
            if p2['id'] == p1['id']:
                continue
            d = ctx.geodesic_km(p1['latitude'], p1['longitude'], p2['latitude'], p2['longitude'])
            if d < best_dist:
                best_dist = d
                best_neighbour = p2
        
        if best_neighbour and best_dist > 20:
            # Avoid duplicate pairs
            pair_key = tuple(sorted([p1['id'], best_neighbour['id']]))
            if pair_key in checked_pairs:
                continue
            checked_pairs.add(pair_key)
            
            # Big gap -- check midpoint
            mid_lat = (p1['latitude'] + best_neighbour['latitude']) / 2
            mid_lon = (p1['longitude'] + best_neighbour['longitude']) / 2
            
            mid_nearest, mid_dist = ctx.nearest_pharmacy(mid_lat, mid_lon)
            if mid_dist >= GEODESIC_CLEAR_PASS_KM:
                is_dupe = any(
                    abs(r['lat'] - mid_lat) < 0.05 and abs(r['lon'] - mid_lon) < 0.05
                    for r in results
                )
                if not is_dupe:
                    state = p1.get('state', '') or detect_state_from_address(p1.get('address', ''))
                    results.append({
                        "lat": round(mid_lat, 6),
                        "lon": round(mid_lon, 6),
                        "suburb": f"Midpoint: {p1.get('suburb','?')}-{best_neighbour.get('suburb','?')}",
                        "postcode": "",
                        "state": state,
                        "rule_item": "Item 131",
                        "confidence": 0.5,
                        "nearest_pharmacy_km": round(mid_dist, 3),
                        "reason": f"Gap midpoint between {p1['name']} and {best_neighbour['name']} ({best_dist:.1f}km apart)",
                        "name": f"Gap: {p1['name']} -- {best_neighbour['name']}",
                        "address": "",
                        "source_type": "pharmacy_gap",
                        "population": 0,
                        "osrm_verified": False,
                    })
    
    # Sort by distance (furthest from pharmacy = best opportunity)
    results.sort(key=lambda x: -x["nearest_pharmacy_km"])
    
    write_results(results, "Item 131", extra_columns=["population", "osrm_verified"])
    print_summary(results, "Item 131")
    return results


def _make_result(sa1, dist_km, nearest_pharm, reason, osrm_verified=False):
    """Create a result dict from SA1 data."""
    state = sa1.get("state", "")
    if not state and nearest_pharm:
        state = nearest_pharm.get("state", "")
    
    return {
        "lat": round(sa1["latitude"], 6),
        "lon": round(sa1["longitude"], 6),
        "suburb": sa1.get("suburb", "") or "",
        "postcode": sa1.get("postcode", "") or "",
        "state": state or "",
        "rule_item": "Item 131",
        "confidence": 0.8 if osrm_verified else 0.6,
        "nearest_pharmacy_km": round(dist_km, 3),
        "reason": reason,
        "name": sa1.get("suburb", "") or f"SA1 {sa1.get('sa1_code', '')}",
        "address": "",
        "source_type": "census_sa1",
        "population": sa1.get("population", 0) or 0,
        "osrm_verified": osrm_verified,
    }


if __name__ == "__main__":
    scan_item131()
