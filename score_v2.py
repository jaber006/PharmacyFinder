#!/usr/bin/env python3
"""
PharmacyFinder Scorer v2 — MULTI-RULE CHECKER

For EVERY opportunity in the database, check ALL 7 rules (131-136).
Each rule returns PASS / FAIL / UNVERIFIED with reason.
Overall verdict: PASS (any rule passes), LIKELY (any rule unverified, none fail-only),
                 FAIL (all rules fail).

Score reflects rule compliance, NOT population ratios.
"""

import sqlite3, json, os, math, time, urllib.request, sys
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'pharmacy_finder.db')
CACHE_DIR = os.path.join(SCRIPT_DIR, 'cache')
OSRM_CACHE_FILE = os.path.join(CACHE_DIR, 'osrm_cache.json')
OUTPUT_DIR = os.path.join(SCRIPT_DIR, 'output')
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

STATE_BOUNDS = {
    'NSW': {'lat': (-37.6, -28.1), 'lng': (140.9, 153.7)},
    'VIC': {'lat': (-39.2, -33.9), 'lng': (140.9, 150.1)},
    'QLD': {'lat': (-29.2, -10.0), 'lng': (137.9, 153.6)},
    'SA':  {'lat': (-38.1, -25.9), 'lng': (129.0, 141.0)},
    'WA':  {'lat': (-35.2, -13.6), 'lng': (112.9, 129.0)},
    'TAS': {'lat': (-43.7, -39.5), 'lng': (143.5, 148.5)},
    'NT':  {'lat': (-26.1, -10.9), 'lng': (128.9, 138.0)},
    'ACT': {'lat': (-36.0, -35.1), 'lng': (148.7, 149.4)},
}

# =====================================================================
# OSRM (cached, rate-limited)
# =====================================================================
_osrm_cache = {}
_osrm_last_call = 0
_osrm_calls_made = 0

def _load_osrm_cache():
    global _osrm_cache
    if os.path.exists(OSRM_CACHE_FILE):
        try:
            with open(OSRM_CACHE_FILE, 'r') as f:
                _osrm_cache = json.load(f)
            print(f"  Loaded {len(_osrm_cache)} OSRM cache entries")
        except Exception:
            _osrm_cache = {}

def _save_osrm_cache():
    with open(OSRM_CACHE_FILE, 'w') as f:
        json.dump(_osrm_cache, f)

def _osrm_key(lat1, lng1, lat2, lng2):
    a = (round(lat1, 5), round(lng1, 5))
    b = (round(lat2, 5), round(lng2, 5))
    pair = tuple(sorted([a, b]))
    return f"{pair[0][0]},{pair[0][1]}|{pair[1][0]},{pair[1][1]}"

def get_road_km(lat1, lng1, lat2, lng2):
    """Get driving distance in km via OSRM. Returns float or None."""
    global _osrm_last_call, _osrm_calls_made
    key = _osrm_key(lat1, lng1, lat2, lng2)
    if key in _osrm_cache:
        return _osrm_cache[key]
    
    now = time.time()
    wait = 1.1 - (now - _osrm_last_call)
    if wait > 0:
        time.sleep(wait)
    
    url = f"http://router.project-osrm.org/route/v1/driving/{lng1},{lat1};{lng2},{lat2}?overview=false"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/2.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
        _osrm_last_call = time.time()
        _osrm_calls_made += 1
        if _osrm_calls_made % 10 == 0:
            _save_osrm_cache()
            print(f"    [OSRM] {_osrm_calls_made} API calls made, cache: {len(_osrm_cache)}")
        if data.get('code') == 'Ok' and data.get('routes'):
            km = round(data['routes'][0]['distance'] / 1000.0, 2)
            _osrm_cache[key] = km
            return km
        _osrm_cache[key] = None
        return None
    except Exception:
        _osrm_cache[key] = None
        return None


# =====================================================================
# Geometry helpers
# =====================================================================
def hav(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon/2)**2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def detect_state(lat, lng):
    """Detect state from coordinates using bounding boxes."""
    # ACT first (contained within NSW bounds)
    if STATE_BOUNDS['ACT']['lat'][0] <= lat <= STATE_BOUNDS['ACT']['lat'][1] and \
       STATE_BOUNDS['ACT']['lng'][0] <= lng <= STATE_BOUNDS['ACT']['lng'][1]:
        return 'ACT'
    for state, b in STATE_BOUNDS.items():
        if state == 'ACT':
            continue
        if b['lat'][0] <= lat <= b['lat'][1] and b['lng'][0] <= lng <= b['lng'][1]:
            return state
    return 'UNKNOWN'


# =====================================================================
# Spatial index for fast pharmacy lookups
# =====================================================================
class PharmacyGrid:
    def __init__(self, pharmacies, cell_deg=0.5):
        self.cell = cell_deg
        self.grid = defaultdict(list)
        self.all = pharmacies
        for ph in pharmacies:
            key = (int(ph['lat'] / cell_deg), int(ph['lng'] / cell_deg))
            self.grid[key].append(ph)

    def nearest(self, lat, lng, exclude_ids=None):
        """Find nearest pharmacy. Returns (dist_km, pharmacy_dict) or (inf, None)."""
        best_d, best_ph = float('inf'), None
        ci, cj = int(lat / self.cell), int(lng / self.cell)
        for ring in range(0, 25):
            for di in range(-ring, ring + 1):
                for dj in range(-ring, ring + 1):
                    if abs(di) != ring and abs(dj) != ring:
                        continue
                    for ph in self.grid.get((ci + di, cj + dj), []):
                        if exclude_ids and ph['id'] in exclude_ids:
                            continue
                        d = hav(lat, lng, ph['lat'], ph['lng'])
                        if d < best_d:
                            best_d, best_ph = d, ph
            if best_ph and ring >= 1 and best_d < (ring - 1) * self.cell * 111:
                break
        return best_d, best_ph

    def within_km(self, lat, lng, km):
        """All pharmacies within km. Returns [(dist_km, pharmacy), ...]."""
        deg = km / 111.0 + 0.1
        ci, cj = int(lat / self.cell), int(lng / self.cell)
        cells = int(deg / self.cell) + 1
        result = []
        for di in range(-cells, cells + 1):
            for dj in range(-cells, cells + 1):
                for ph in self.grid.get((ci + di, cj + dj), []):
                    d = hav(lat, lng, ph['lat'], ph['lng'])
                    if d <= km:
                        result.append((d, ph))
        result.sort(key=lambda x: x[0])
        return result

    def nearest_outside_town(self, lat, lng, town_key):
        """Find nearest pharmacy NOT in the given town."""
        best_d, best_ph = float('inf'), None
        ci, cj = int(lat / self.cell), int(lng / self.cell)
        for ring in range(0, 30):
            for di in range(-ring, ring + 1):
                for dj in range(-ring, ring + 1):
                    if abs(di) != ring and abs(dj) != ring:
                        continue
                    for ph in self.grid.get((ci + di, cj + dj), []):
                        if ph.get('_town') == town_key:
                            continue
                        d = hav(lat, lng, ph['lat'], ph['lng'])
                        if d < best_d:
                            best_d, best_ph = d, ph
            if best_ph and ring >= 1 and best_d < (ring - 1) * self.cell * 111:
                break
        return best_d, best_ph


# =====================================================================
# Data loaders
# =====================================================================
def load_all(conn):
    c = conn.cursor()

    # Pharmacies
    c.execute("SELECT id, name, address, latitude, longitude, suburb, state, postcode FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    pharmacies = []
    for r in c.fetchall():
        sub = (r[5] or '').upper().strip()
        pc = (r[7] or '').strip()
        pharmacies.append({
            'id': r[0], 'name': r[1], 'address': r[2],
            'lat': r[3], 'lng': r[4],
            'suburb': sub, 'state': r[6], 'postcode': pc,
            '_town': f"{sub}|{pc}" if sub and pc else None
        })

    # Opportunities
    c.execute("""SELECT id, latitude, longitude, address, qualifying_rules, evidence,
                        nearest_pharmacy_km, nearest_pharmacy_name, poi_name, poi_type,
                        region, verification, pop_5km, pop_10km, pop_15km,
                        pharmacy_5km, pharmacy_10km, pharmacy_15km,
                        nearest_town, growth_indicator, growth_details
                 FROM opportunities WHERE latitude IS NOT NULL AND longitude IS NOT NULL""")
    opportunities = []
    for r in c.fetchall():
        opportunities.append({
            'id': r[0], 'lat': r[1], 'lng': r[2], 'address': r[3] or '',
            'original_rules': r[4] or '', 'evidence': r[5] or '',
            'nearest_pharmacy_km': r[6] or 999, 'nearest_pharmacy_name': r[7] or '',
            'poi_name': r[8] or '', 'poi_type': r[9] or '',
            'region': r[10] or '', 'verification': r[11] or '',
            'pop_5km': r[12] or 0, 'pop_10km': r[13] or 0, 'pop_15km': r[14] or 0,
            'pharmacy_5km': r[15] or 0, 'pharmacy_10km': r[16] or 0, 'pharmacy_15km': r[17] or 0,
            'nearest_town': r[18] or '', 'growth_indicator': r[19] or '', 'growth_details': r[20] or '',
        })

    # Supermarkets
    c.execute("SELECT id, name, address, latitude, longitude, estimated_gla, floor_area_sqm, brand, gla_confidence FROM supermarkets WHERE latitude IS NOT NULL")
    supermarkets = []
    for r in c.fetchall():
        supermarkets.append({
            'id': r[0], 'name': r[1], 'address': r[2] or '',
            'lat': r[3], 'lng': r[4],
            'gla': r[5] or r[6] or 0, 'brand': r[7] or '', 'gla_conf': r[8] or ''
        })

    # Medical centres
    c.execute("SELECT id, name, address, latitude, longitude, num_gps, total_fte, hours_per_week, state FROM medical_centres WHERE latitude IS NOT NULL")
    med_centres = []
    for r in c.fetchall():
        med_centres.append({
            'id': r[0], 'name': r[1], 'address': r[2] or '',
            'lat': r[3], 'lng': r[4],
            'gps': r[5] or 0, 'fte': r[6] or 0.0, 'hrs': r[7] or 0.0, 'state': r[8] or ''
        })

    # Shopping centres
    c.execute("SELECT id, name, address, latitude, longitude, gla_sqm, estimated_gla, estimated_tenants, centre_class, major_supermarkets FROM shopping_centres WHERE latitude IS NOT NULL")
    shopping_centres = []
    for r in c.fetchall():
        shopping_centres.append({
            'id': r[0], 'name': r[1], 'address': r[2] or '',
            'lat': r[3], 'lng': r[4],
            'gla': r[5] or r[6] or 0, 'tenants': r[7] or 0, 'cls': r[8] or '', 'supers': r[9] or ''
        })

    # GPs
    c.execute("SELECT id, name, address, latitude, longitude, fte, hours_per_week FROM gps WHERE latitude IS NOT NULL")
    gps_list = []
    for r in c.fetchall():
        gps_list.append({
            'id': r[0], 'name': r[1], 'address': r[2] or '',
            'lat': r[3], 'lng': r[4], 'fte': r[5] or 0.0, 'hrs': r[6] or 0.0
        })

    # Hospitals
    c.execute("SELECT id, name, address, latitude, longitude, bed_count, hospital_type FROM hospitals WHERE latitude IS NOT NULL")
    hospitals = []
    for r in c.fetchall():
        hospitals.append({
            'id': r[0], 'name': r[1], 'address': r[2] or '',
            'lat': r[3], 'lng': r[4], 'beds': r[5] or 0, 'type': r[6] or 'unknown'
        })

    return pharmacies, opportunities, supermarkets, med_centres, shopping_centres, gps_list, hospitals


# =====================================================================
# Rule checkers — each returns dict with verdict + checks
# =====================================================================

def check_item_131(opp, grid, pharmacies):
    """
    Item 131: Proposed premises >= 10km by shortest lawful access route from nearest pharmacy.
    """
    checks = {}
    
    # Find nearest pharmacy by straight line
    sl_km, nearest_ph = grid.nearest(opp['lat'], opp['lng'])
    
    if nearest_ph is None:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'No pharmacies found'}
    
    sl_m = sl_km * 1000
    
    # If straight line < 8km, road distance almost certainly < 10km
    if sl_km < 8.0:
        checks['nearest_10km_road'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {sl_km:.1f}km straight line — too close for 10km road"
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': f"SL {sl_km:.1f}km < 8km threshold"}
    
    # 8-15km SL — need OSRM
    if sl_km < 15:
        road_km = get_road_km(opp['lat'], opp['lng'], nearest_ph['lat'], nearest_ph['lng'])
        if road_km is not None:
            if road_km >= 10:
                checks['nearest_10km_road'] = {
                    'status': 'PASS', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {road_km:.1f}km by road (SL: {sl_km:.1f}km)"
                }
            else:
                checks['nearest_10km_road'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) only {road_km:.1f}km by road (SL: {sl_km:.1f}km)"
                }
        else:
            if sl_km >= 10:
                checks['nearest_10km_road'] = {
                    'status': 'UNVERIFIED', 'auto': False,
                    'detail': f"OSRM failed. SL={sl_km:.1f}km to {nearest_ph['name']}. Verify road distance >= 10km."
                }
            else:
                checks['nearest_10km_road'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"SL only {sl_km:.1f}km and OSRM unavailable"
                }
    else:
        # SL >= 15km — very likely qualifies, but still check OSRM
        road_km = get_road_km(opp['lat'], opp['lng'], nearest_ph['lat'], nearest_ph['lng'])
        if road_km is not None:
            if road_km >= 10:
                checks['nearest_10km_road'] = {
                    'status': 'PASS', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {road_km:.1f}km by road (SL: {sl_km:.1f}km)"
                }
            else:
                checks['nearest_10km_road'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) only {road_km:.1f}km by road despite {sl_km:.1f}km SL"
                }
        else:
            checks['nearest_10km_road'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"OSRM failed. SL={sl_km:.1f}km to {nearest_ph['name']}. Likely qualifies — verify."
            }
    
    # Determine verdict
    c = checks.get('nearest_10km_road', {})
    if c.get('status') == 'PASS':
        return {'verdict': 'PASS', 'checks': checks, 'reason': c['detail']}
    elif c.get('status') == 'FAIL':
        return {'verdict': 'FAIL', 'checks': checks, 'reason': c['detail']}
    else:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': c.get('detail', 'Unknown')}


def check_item_132(opp, grid, pharmacies, supermarkets, med_centres, gps_list):
    """
    Item 132: New additional pharmacy in same town as existing pharmacy.
    (a)(i) Same town as an existing pharmacy
    (a)(ii) >= 200m straight line from nearest pharmacy
    (a)(iii) >= 10km road from ALL other pharmacies (except nearest)
    (b)(i) >= 4 FTE GPs in same town
    (b)(ii) 1-2 supermarkets with combined GLA >= 2,500m² in same town
    """
    checks = {}
    
    # Find nearest pharmacy
    sl_km, nearest_ph = grid.nearest(opp['lat'], opp['lng'])
    if nearest_ph is None:
        return {'verdict': 'FAIL', 'checks': {}, 'reason': 'No pharmacies found'}
    
    sl_m = sl_km * 1000
    
    # (a)(i) Same town — check if nearest pharmacy shares a "town" (suburb+postcode)
    # We check if opportunity is near a pharmacy in a single-pharmacy town
    # Find all pharmacies within 5km
    nearby = grid.within_km(opp['lat'], opp['lng'], 5.0)
    
    if not nearby:
        checks['a_i_same_town'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'No pharmacy within 5km — cannot be in same town'
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'No nearby pharmacy for same-town check'}
    
    # The nearest pharmacy defines the "town" for item 132
    town_ph = nearby[0][1]
    town_key = town_ph.get('_town')
    
    if town_key:
        # Count pharmacies in this town
        town_phs = [ph for ph in pharmacies if ph.get('_town') == town_key]
        checks['a_i_same_town'] = {
            'status': 'PASS' if len(town_phs) >= 1 else 'FAIL',
            'auto': True,
            'detail': f"Town {town_ph['suburb']} {town_ph['postcode']} has {len(town_phs)} pharmacy(ies)"
        }
    else:
        checks['a_i_same_town'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"Cannot determine town for nearest pharmacy ({town_ph['name']}). Verify same-town status."
        }
    
    # (a)(ii) >= 200m straight line from nearest pharmacy
    if sl_m >= 200:
        checks['a_ii_200m'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {sl_m:.0f}m away"
        }
    else:
        checks['a_ii_200m'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"Nearest pharmacy ({nearest_ph['name']}) only {sl_m:.0f}m away (need >= 200m)"
        }
    
    # (a)(iii) All OTHER pharmacies (not the nearest) must be >= 10km by road
    # Find the second nearest pharmacy
    second_sl, second_ph = grid.nearest(opp['lat'], opp['lng'], exclude_ids={nearest_ph['id']})
    
    if second_ph is None:
        checks['a_iii_others_10km'] = {
            'status': 'PASS', 'auto': True,
            'detail': 'No other pharmacies found — condition satisfied trivially'
        }
    elif second_sl < 8.0:
        checks['a_iii_others_10km'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"Second nearest ({second_ph['name']}) is {second_sl:.1f}km SL — too close"
        }
    else:
        road_km = get_road_km(opp['lat'], opp['lng'], second_ph['lat'], second_ph['lng'])
        if road_km is not None:
            if road_km >= 10:
                checks['a_iii_others_10km'] = {
                    'status': 'PASS', 'auto': True,
                    'detail': f"Second nearest ({second_ph['name']}) is {road_km:.1f}km by road"
                }
            else:
                checks['a_iii_others_10km'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"Second nearest ({second_ph['name']}) only {road_km:.1f}km by road (need >= 10km)"
                }
        else:
            if second_sl >= 10:
                checks['a_iii_others_10km'] = {
                    'status': 'UNVERIFIED', 'auto': False,
                    'detail': f"OSRM failed. SL={second_sl:.1f}km to {second_ph['name']}. Verify >= 10km road."
                }
            else:
                checks['a_iii_others_10km'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"SL only {second_sl:.1f}km to {second_ph['name']} and OSRM unavailable"
                }
    
    # (b)(i) >= 4 FTE GPs in same town (within 5km as proxy)
    nearby_gps = [g for g in gps_list if hav(opp['lat'], opp['lng'], g['lat'], g['lng']) < 5]
    nearby_mcs = [mc for mc in med_centres if hav(opp['lat'], opp['lng'], mc['lat'], mc['lng']) < 5]
    total_fte = sum(g['fte'] for g in nearby_gps) + sum(mc['fte'] for mc in nearby_mcs)
    total_gps = len(nearby_gps) + sum(mc['gps'] for mc in nearby_mcs)
    
    if total_fte >= 4:
        checks['b_i_4fte_gps'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{total_fte:.1f} FTE from {len(nearby_gps)} GP practices + {len(nearby_mcs)} med centres"
        }
    elif total_gps >= 4:
        checks['b_i_4fte_gps'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{total_gps} GPs found ({total_fte:.1f} est FTE). Verify >= 4 FTE at 38hrs/wk."
        }
    elif total_gps > 0:
        checks['b_i_4fte_gps'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"Only {total_gps} GPs ({total_fte:.1f} FTE) found — data may be incomplete."
        }
    else:
        checks['b_i_4fte_gps'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': 'No GP data near this location. Verify >= 4 FTE prescribers in town.'
        }
    
    # (b)(ii) 1-2 supermarkets with combined GLA >= 2,500m²
    nearby_supers = sorted(
        [(hav(opp['lat'], opp['lng'], s['lat'], s['lng']), s) for s in supermarkets
         if hav(opp['lat'], opp['lng'], s['lat'], s['lng']) < 5],
        key=lambda x: x[0])
    
    if nearby_supers:
        top2 = sorted(nearby_supers[:10], key=lambda x: -x[1]['gla'])[:2]
        combined = sum(s['gla'] for _, s in top2)
        names = [f"{s['name']} ({s['gla']:.0f}m²)" for _, s in top2]
        if combined >= 2500:
            checks['b_ii_supermarket_2500'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"Combined GLA {combined:.0f}m²: {', '.join(names)}"
            }
        elif combined >= 2000:
            checks['b_ii_supermarket_2500'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"GLA {combined:.0f}m² (est, need >= 2500). Verify: {', '.join(names)}"
            }
        else:
            checks['b_ii_supermarket_2500'] = {
                'status': 'FAIL', 'auto': True,
                'detail': f"GLA only {combined:.0f}m²: {', '.join(names)}"
            }
    else:
        checks['b_ii_supermarket_2500'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': 'No supermarket data within 5km. Verify >= 2,500m² combined GLA.'
        }
    
    # Determine overall verdict for this rule
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'One or more criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Some criteria need manual verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All criteria pass'}


def check_item_133(opp, grid, pharmacies, shopping_centres, supermarkets):
    """
    Item 133: New pharmacy in small shopping centre.
    (a) In a small shopping centre: single mgmt, GLA >= 5,000m², supermarket >= 2,500m², >= 15 tenants, parking
    (b) >= 500m SL from nearest pharmacy (excl large SC / hospital pharmacies)
    (c) No pharmacy already in the shopping centre
    """
    checks = {}
    
    # Find nearby shopping centres (within 500m = "in")
    nearby_sc = [(hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']), sc) 
                 for sc in shopping_centres
                 if hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']) < 0.5]
    nearby_sc.sort(key=lambda x: x[0])
    
    if not nearby_sc:
        checks['in_shopping_centre'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'Not within 500m of any shopping centre in database'
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Not in a shopping centre'}
    
    sc_dist, sc = nearby_sc[0]
    
    # Must be a SMALL SC (15-49 tenants) not large (>= 50)
    tenants = sc['tenants']
    gla = sc['gla']
    
    if tenants >= 50:
        checks['small_sc_tenants'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{sc['name']} has {tenants} tenants — this is a LARGE centre (use Item 134)"
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Large SC, not small — use Item 134'}
    
    if tenants >= 15:
        checks['small_sc_tenants'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{sc['name']}: {tenants} tenants (need >= 15)"
        }
    elif tenants >= 10:
        checks['small_sc_tenants'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{sc['name']}: {tenants} est. tenants (need >= 15). Verify actual count."
        }
    else:
        checks['small_sc_tenants'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{sc['name']}: only {tenants} tenants (need >= 15)"
        }
    
    # GLA >= 5,000m²
    if gla >= 5000:
        checks['gla_5000'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"GLA = {gla:,.0f}m²"
        }
    elif gla >= 4000:
        checks['gla_5000'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"GLA = {gla:,.0f}m² (est, need >= 5,000). Verify."
        }
    else:
        checks['gla_5000'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"GLA only {gla:,.0f}m² (need >= 5,000)"
        }
    
    # Supermarket >= 2,500m² in the centre
    sc_supers = [(hav(sc['lat'], sc['lng'], s['lat'], s['lng']), s)
                 for s in supermarkets if hav(sc['lat'], sc['lng'], s['lat'], s['lng']) < 0.5]
    if sc_supers:
        best_super = max(sc_supers, key=lambda x: x[1]['gla'])
        if best_super[1]['gla'] >= 2500:
            checks['supermarket_2500'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"{best_super[1]['name']}: {best_super[1]['gla']:.0f}m² GLA"
            }
        elif best_super[1]['gla'] >= 2000:
            checks['supermarket_2500'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"{best_super[1]['name']}: {best_super[1]['gla']:.0f}m² (est, need >= 2,500). Verify."
            }
        else:
            checks['supermarket_2500'] = {
                'status': 'FAIL', 'auto': True,
                'detail': f"Largest supermarket ({best_super[1]['name']}) only {best_super[1]['gla']:.0f}m²"
            }
    else:
        checks['supermarket_2500'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"No supermarket data near {sc['name']}. Verify >= 2,500m² supermarket inside."
        }
    
    # Single management + parking — always manual
    checks['single_mgmt_parking'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': f"Verify {sc['name']} has single management and customer parking"
    }
    
    # (b) >= 500m SL from nearest pharmacy (excl large SC/hospital pharmacies)
    sl_km, nearest_ph = grid.nearest(opp['lat'], opp['lng'])
    if nearest_ph:
        sl_m = sl_km * 1000
        if sl_m >= 500:
            checks['500m_from_pharmacy'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {sl_m:.0f}m"
            }
        else:
            # Check if nearest pharmacy is in a large SC or hospital (excluded from 500m check)
            ph_in_large_sc = False
            for sc2 in shopping_centres:
                if sc2['tenants'] >= 50 and hav(nearest_ph['lat'], nearest_ph['lng'], sc2['lat'], sc2['lng']) < 0.2:
                    ph_in_large_sc = True
                    break
            if ph_in_large_sc:
                checks['500m_from_pharmacy'] = {
                    'status': 'PASS', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) is {sl_m:.0f}m but in large SC (excluded)"
                }
            else:
                checks['500m_from_pharmacy'] = {
                    'status': 'FAIL', 'auto': True,
                    'detail': f"Nearest pharmacy ({nearest_ph['name']}) only {sl_m:.0f}m (need >= 500m)"
                }
    
    # (c) No pharmacy in the shopping centre
    ph_in_sc = grid.within_km(sc['lat'], sc['lng'], 0.2)
    if ph_in_sc:
        checks['no_pharmacy_in_sc'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{len(ph_in_sc)} pharmacy(ies) within 200m of centre: {', '.join(p['name'] for _, p in ph_in_sc[:3])}"
        }
    else:
        checks['no_pharmacy_in_sc'] = {
            'status': 'PASS', 'auto': True,
            'detail': 'No pharmacy within 200m of centre'
        }
    
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'One or more criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Some criteria need verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All criteria pass'}


def check_item_134(opp, grid, pharmacies, shopping_centres, supermarkets):
    """
    Item 134: New pharmacy in large shopping centre (no existing pharmacy).
    (a) In a large SC: single mgmt, GLA >= 5,000m², supermarket >= 2,500m², >= 50 tenants, parking
    (b) No pharmacy in the shopping centre
    """
    checks = {}
    
    nearby_sc = [(hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']), sc)
                 for sc in shopping_centres
                 if hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']) < 0.5]
    nearby_sc.sort(key=lambda x: x[0])
    
    if not nearby_sc:
        checks['in_large_sc'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'Not within 500m of any shopping centre'
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Not in a shopping centre'}
    
    sc_dist, sc = nearby_sc[0]
    tenants = sc['tenants']
    gla = sc['gla']
    
    # >= 50 tenants
    if tenants >= 50:
        checks['50_tenants'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{sc['name']}: {tenants} tenants"
        }
    elif tenants >= 40:
        checks['50_tenants'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{sc['name']}: {tenants} est. tenants (need >= 50). Verify."
        }
    else:
        checks['50_tenants'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{sc['name']}: only {tenants} tenants (need >= 50)"
        }
    
    # GLA >= 5,000m²
    if gla >= 5000:
        checks['gla_5000'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"GLA = {gla:,.0f}m²"
        }
    elif gla >= 4000:
        checks['gla_5000'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"GLA = {gla:,.0f}m² (need >= 5,000). Verify."
        }
    else:
        checks['gla_5000'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"GLA only {gla:,.0f}m² (need >= 5,000)"
        }
    
    # Supermarket >= 2,500m²
    sc_supers = [(hav(sc['lat'], sc['lng'], s['lat'], s['lng']), s)
                 for s in supermarkets if hav(sc['lat'], sc['lng'], s['lat'], s['lng']) < 0.5]
    if sc_supers:
        best = max(sc_supers, key=lambda x: x[1]['gla'])
        if best[1]['gla'] >= 2500:
            checks['supermarket_2500'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"{best[1]['name']}: {best[1]['gla']:.0f}m²"
            }
        elif best[1]['gla'] >= 2000:
            checks['supermarket_2500'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"{best[1]['name']}: {best[1]['gla']:.0f}m² (need >= 2,500). Verify."
            }
        else:
            checks['supermarket_2500'] = {
                'status': 'FAIL', 'auto': True,
                'detail': f"Largest supermarket ({best[1]['name']}) only {best[1]['gla']:.0f}m²"
            }
    else:
        checks['supermarket_2500'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"No supermarket data near {sc['name']}. Verify >= 2,500m² supermarket."
        }
    
    checks['single_mgmt_parking'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': f"Verify {sc['name']} has single management and customer parking"
    }
    
    # (b) No pharmacy in the centre
    ph_in_sc = grid.within_km(sc['lat'], sc['lng'], 0.2)
    if ph_in_sc:
        checks['no_pharmacy_in_sc'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{len(ph_in_sc)} pharmacy(ies) within 200m: {', '.join(p['name'] for _, p in ph_in_sc[:3])}"
        }
    else:
        checks['no_pharmacy_in_sc'] = {
            'status': 'PASS', 'auto': True,
            'detail': 'No pharmacy within 200m of centre'
        }
    
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Needs verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All pass'}


def check_item_134a(opp, grid, pharmacies, shopping_centres, supermarkets):
    """
    Item 134A: Additional pharmacy in large shopping centre.
    Must have >= 50 tenants + existing pharmacy + tier allows more.
    100-199 tenants: max 1 existing → allows 2nd
    200+ tenants: max 2 existing → allows 3rd
    """
    checks = {}
    
    nearby_sc = [(hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']), sc)
                 for sc in shopping_centres
                 if hav(opp['lat'], opp['lng'], sc['lat'], sc['lng']) < 0.5]
    nearby_sc.sort(key=lambda x: x[0])
    
    if not nearby_sc:
        checks['in_large_sc'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'Not within 500m of any shopping centre'
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Not in a shopping centre'}
    
    sc_dist, sc = nearby_sc[0]
    tenants = sc['tenants']
    gla = sc['gla']
    
    # Need >= 100 tenants for 134A
    if tenants >= 200:
        max_existing = 2
        tier = '200+ tenants'
    elif tenants >= 100:
        max_existing = 1
        tier = '100-199 tenants'
    elif tenants >= 80:
        checks['100_tenants'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{sc['name']}: {tenants} est. tenants (need >= 100). Verify."
        }
        max_existing = 1
        tier = f'{tenants} est. tenants'
    else:
        checks['100_tenants'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{sc['name']}: only {tenants} tenants (need >= 100 for 134A)"
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Too few tenants'}
    
    if tenants >= 100:
        checks['100_tenants'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{sc['name']}: {tenants} tenants ({tier})"
        }
    
    # Large SC requirements (same as 134)
    if gla >= 5000:
        checks['gla_5000'] = {'status': 'PASS', 'auto': True, 'detail': f"GLA = {gla:,.0f}m²"}
    elif gla >= 4000:
        checks['gla_5000'] = {'status': 'UNVERIFIED', 'auto': False, 'detail': f"GLA = {gla:,.0f}m². Verify >= 5,000."}
    else:
        checks['gla_5000'] = {'status': 'FAIL', 'auto': True, 'detail': f"GLA only {gla:,.0f}m²"}
    
    # Count existing pharmacies in centre
    ph_in_sc = grid.within_km(sc['lat'], sc['lng'], 0.2)
    count = len(ph_in_sc)
    
    if count == 0:
        checks['existing_pharmacy'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'No existing pharmacy in centre — use Item 134 instead'
        }
    elif count <= max_existing:
        names = ', '.join(p['name'] for _, p in ph_in_sc)
        checks['existing_pharmacy'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{count} existing ({names}). {tier}: max {max_existing} existing → room for {max_existing + 1 - count} more"
        }
    else:
        names = ', '.join(p['name'] for _, p in ph_in_sc)
        checks['existing_pharmacy'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{count} existing ({names}). {tier}: max {max_existing} — already full"
        }
    
    checks['no_relocation_12mo'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': 'Verify no pharmacy relocated out of centre in last 12 months'
    }
    
    checks['single_mgmt_parking'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': f"Verify {sc['name']} has single management and customer parking"
    }
    
    # Supermarket
    sc_supers = [(hav(sc['lat'], sc['lng'], s['lat'], s['lng']), s)
                 for s in supermarkets if hav(sc['lat'], sc['lng'], s['lat'], s['lng']) < 0.5]
    if sc_supers:
        best = max(sc_supers, key=lambda x: x[1]['gla'])
        if best[1]['gla'] >= 2500:
            checks['supermarket_2500'] = {'status': 'PASS', 'auto': True, 'detail': f"{best[1]['name']}: {best[1]['gla']:.0f}m²"}
        else:
            checks['supermarket_2500'] = {'status': 'UNVERIFIED', 'auto': False, 'detail': f"Largest: {best[1]['name']} ({best[1]['gla']:.0f}m²). Verify >= 2,500."}
    else:
        checks['supermarket_2500'] = {'status': 'UNVERIFIED', 'auto': False, 'detail': 'No supermarket data. Verify >= 2,500m².'}
    
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Needs verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All pass'}


def check_item_135(opp, grid, pharmacies, hospitals):
    """
    Item 135: New pharmacy in large private hospital.
    (a) Hospital can admit >= 150 patients at any time (private)
    (b) No pharmacy in the hospital
    """
    checks = {}
    
    # Find nearby hospitals (within 300m = "in")
    nearby_h = [(hav(opp['lat'], opp['lng'], h['lat'], h['lng']), h)
                for h in hospitals
                if hav(opp['lat'], opp['lng'], h['lat'], h['lng']) < 0.3]
    nearby_h.sort(key=lambda x: x[0])
    
    if not nearby_h:
        checks['in_hospital'] = {
            'status': 'FAIL', 'auto': True,
            'detail': 'Not within 300m of any hospital in database'
        }
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Not in a hospital'}
    
    h_dist, hosp = nearby_h[0]
    
    # (a) Private hospital with >= 150 beds
    h_type = (hosp['type'] or '').lower()
    is_private = 'private' in h_type
    
    if is_private and hosp['beds'] >= 150:
        checks['private_150beds'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{hosp['name']}: {hosp['beds']} beds, {hosp['type']}"
        }
    elif is_private and hosp['beds'] >= 100:
        checks['private_150beds'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{hosp['name']}: {hosp['beds']} beds (need >= 150 admittable). Verify capacity."
        }
    elif hosp['beds'] >= 150 and h_type == 'unknown':
        checks['private_150beds'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{hosp['name']}: {hosp['beds']} beds, type unknown. Verify it's a PRIVATE hospital."
        }
    elif h_type == 'unknown' and hosp['beds'] >= 100:
        checks['private_150beds'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{hosp['name']}: {hosp['beds']} beds, type unknown. Verify private + >= 150 beds."
        }
    elif not is_private:
        checks['private_150beds'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{hosp['name']} is {hosp['type']}, not private"
        }
    else:
        checks['private_150beds'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{hosp['name']}: only {hosp['beds']} beds (need >= 150)"
        }
    
    # Admittable patients check (beds != admittable patients necessarily)
    checks['admit_150'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': f"Verify {hosp['name']} can admit >= 150 patients at any time ({hosp['beds']} beds listed)"
    }
    
    # (b) No pharmacy in the hospital
    ph_in_h = grid.within_km(hosp['lat'], hosp['lng'], 0.15)
    if ph_in_h:
        checks['no_pharmacy_in_hospital'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{len(ph_in_h)} pharmacy(ies) within 150m: {', '.join(p['name'] for _, p in ph_in_h[:3])}"
        }
    else:
        checks['no_pharmacy_in_hospital'] = {
            'status': 'PASS', 'auto': True,
            'detail': 'No pharmacy within 150m of hospital'
        }
    
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Needs verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All pass'}


def check_item_136(opp, grid, pharmacies, med_centres, shopping_centres, hospitals):
    """
    Item 136: New pharmacy in large medical centre.
    (a) Single mgmt, operates >= 70 hrs/wk, >= 1 GP available >= 70 hrs/wk
    (b) No pharmacy in the medical centre
    (c) >= 300m SL from nearest pharmacy (excl different large SC/hospital pharmacies)
    (d) >= 8 FTE PBS prescribers (>= 7 medical practitioners)
    (e) Match medical centre hours (manual check)
    """
    checks = {}
    
    # Find nearby medical centres (within 300m)
    nearby_mc = [(hav(opp['lat'], opp['lng'], mc['lat'], mc['lng']), mc)
                 for mc in med_centres
                 if hav(opp['lat'], opp['lng'], mc['lat'], mc['lng']) < 0.3]
    nearby_mc.sort(key=lambda x: x[0])
    
    if not nearby_mc:
        # Check if the opportunity itself IS a medical centre based on poi_type
        if opp.get('poi_type') == 'medical_centre' and opp.get('poi_name'):
            checks['in_medical_centre'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"Opportunity references '{opp['poi_name']}' but no match in medical_centres DB. Verify."
            }
        else:
            checks['in_medical_centre'] = {
                'status': 'FAIL', 'auto': True,
                'detail': 'Not within 300m of any medical centre in database'
            }
            return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Not in a medical centre'}
    
    if nearby_mc:
        mc_dist, mc = nearby_mc[0]
    else:
        # Dummy for unverified case
        mc = {'name': opp.get('poi_name', 'Unknown'), 'gps': 0, 'fte': 0.0, 'hrs': 0.0}
    
    # (a) Operates >= 70hrs/wk
    if mc['hrs'] >= 70:
        checks['a_70hrs'] = {
            'status': 'PASS', 'auto': True,
            'detail': f"{mc['name']}: {mc['hrs']:.0f} hrs/wk"
        }
    elif mc['hrs'] >= 55:
        checks['a_70hrs'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{mc['name']}: {mc['hrs']:.0f} hrs/wk (need >= 70). Verify actual hours."
        }
    elif mc['hrs'] > 0:
        checks['a_70hrs'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"{mc['name']}: only {mc['hrs']:.0f} hrs/wk (need >= 70)"
        }
    else:
        checks['a_70hrs'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"No hours data for {mc['name']}. Verify >= 70hrs/wk."
        }
    
    # GP available >= 70hrs/wk
    if mc['gps'] >= 3:
        checks['a_gp_70hrs'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{mc['gps']} GPs — plausible for 70hrs/wk coverage. Verify rostering."
        }
    elif mc['gps'] >= 1:
        checks['a_gp_70hrs'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"Only {mc['gps']} GPs. Verify GP available >= 70hrs/wk."
        }
    else:
        checks['a_gp_70hrs'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"No GP count data for {mc['name']}. Verify GP available >= 70hrs/wk."
        }
    
    checks['a_single_mgmt'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': f"Verify {mc['name']} has single management"
    }
    
    # (b) No pharmacy inside the medical centre
    if nearby_mc:
        ph_in_mc = grid.within_km(mc['lat'], mc['lng'], 0.1)
        if ph_in_mc:
            names = ', '.join(p['name'] for _, p in ph_in_mc[:3])
            checks['b_no_pharmacy_inside'] = {
                'status': 'UNVERIFIED', 'auto': False,
                'detail': f"{len(ph_in_mc)} pharmacy(ies) within 100m: {names}. Verify none INSIDE the centre."
            }
        else:
            checks['b_no_pharmacy_inside'] = {
                'status': 'PASS', 'auto': True,
                'detail': 'No pharmacy within 100m'
            }
    else:
        checks['b_no_pharmacy_inside'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': 'Cannot verify — no medical centre coords. Check manually.'
        }
    
    # (c) >= 300m from nearest pharmacy (excl those in a DIFFERENT large SC or hospital)
    def in_large_sc_or_hospital(p):
        for sc2 in shopping_centres:
            if sc2['tenants'] >= 50 and sc2['gla'] >= 5000:
                if hav(p['lat'], p['lng'], sc2['lat'], sc2['lng']) < 0.2:
                    return True
        for h in hospitals:
            if hav(p['lat'], p['lng'], h['lat'], h['lng']) < 0.2:
                return True
        return False
    
    relevant_phs = [(hav(opp['lat'], opp['lng'], p['lat'], p['lng']), p) 
                     for p in pharmacies
                     if hav(opp['lat'], opp['lng'], p['lat'], p['lng']) < 1.0 
                     and not in_large_sc_or_hospital(p)]
    relevant_phs.sort(key=lambda x: x[0])
    
    if relevant_phs:
        dist_m = relevant_phs[0][0] * 1000
        if dist_m >= 300:
            checks['c_300m'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"Nearest relevant pharmacy ({relevant_phs[0][1]['name']}) is {dist_m:.0f}m"
            }
        else:
            checks['c_300m'] = {
                'status': 'FAIL', 'auto': True,
                'detail': f"Nearest pharmacy ({relevant_phs[0][1]['name']}) only {dist_m:.0f}m (need >= 300m)"
            }
    else:
        # Check all pharmacies within wider range
        all_nearby = grid.within_km(opp['lat'], opp['lng'], 1.0)
        if all_nearby:
            checks['c_300m'] = {
                'status': 'PASS', 'auto': True,
                'detail': f"All {len(all_nearby)} pharmacies within 1km are in large SC/hospital (excluded)"
            }
        else:
            checks['c_300m'] = {
                'status': 'PASS', 'auto': True,
                'detail': 'No pharmacies within 1km'
            }
    
    # (d) >= 8 FTE PBS prescribers
    if mc['fte'] >= 8:
        checks['d_8fte'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"Est {mc['fte']:.1f} FTE ({mc['gps']} GPs). Likely meets 8 FTE — verify at 38hrs/wk."
        }
    elif mc['gps'] >= 8:
        checks['d_8fte'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{mc['gps']} GPs ({mc['fte']:.1f} est FTE). Could meet 8 FTE if near full-time."
        }
    elif mc['gps'] >= 6:
        checks['d_8fte'] = {
            'status': 'UNVERIFIED', 'auto': False,
            'detail': f"{mc['gps']} GPs / {mc['fte']:.1f} FTE. Borderline — verify actual prescriber hours."
        }
    elif mc['gps'] >= 4:
        checks['d_8fte'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"Only {mc['gps']} GPs / {mc['fte']:.1f} FTE (need >= 8 FTE)"
        }
    else:
        checks['d_8fte'] = {
            'status': 'FAIL', 'auto': True,
            'detail': f"Only {mc['gps']} GPs / {mc['fte']:.1f} FTE (need >= 8 FTE prescribers)"
        }
    
    checks['e_hours_match'] = {
        'status': 'UNVERIFIED', 'auto': False,
        'detail': 'Applicant must make reasonable attempts to match medical centre hours'
    }
    
    statuses = [c['status'] for c in checks.values()]
    if 'FAIL' in statuses:
        return {'verdict': 'FAIL', 'checks': checks, 'reason': 'Criteria fail'}
    elif 'UNVERIFIED' in statuses:
        return {'verdict': 'UNVERIFIED', 'checks': checks, 'reason': 'Needs verification'}
    else:
        return {'verdict': 'PASS', 'checks': checks, 'reason': 'All pass'}


# =====================================================================
# Main scorer: check EVERY rule for EVERY opportunity
# =====================================================================

def compute_score(rules_checked):
    """
    Score = rule compliance score (0-100).
    Based on: best rule's pass/unverified/fail status + number of passing rules.
    """
    verdicts = {rule: data['verdict'] for rule, data in rules_checked.items()}
    
    passes = sum(1 for v in verdicts.values() if v == 'PASS')
    unverified = sum(1 for v in verdicts.values() if v == 'UNVERIFIED')
    fails = sum(1 for v in verdicts.values() if v == 'FAIL')
    
    # Best rule determines base score
    if passes > 0:
        base = 70
    elif unverified > 0:
        base = 40
    else:
        base = 5
    
    # Bonus for multiple passing rules
    base += passes * 5
    
    # Bonus for unverified rules (potential)
    base += unverified * 2
    
    return min(100, base)


def determine_best_rule(rules_checked):
    """Pick the best rule: PASS > UNVERIFIED > FAIL. Break ties by fewest unverified checks."""
    priority = {'PASS': 0, 'UNVERIFIED': 1, 'FAIL': 2}
    
    best_rule = None
    best_score = (3, 999)
    
    for rule, data in rules_checked.items():
        v = data['verdict']
        unv_count = sum(1 for c in data.get('checks', {}).values() if c.get('status') == 'UNVERIFIED')
        score = (priority.get(v, 3), unv_count)
        if score < best_score:
            best_score = score
            best_rule = rule
    
    return best_rule


def determine_overall_verdict(rules_checked):
    """Overall verdict based on best rule across all 7."""
    verdicts = [data['verdict'] for data in rules_checked.values()]
    if 'PASS' in verdicts:
        return 'PASS'
    elif 'UNVERIFIED' in verdicts:
        return 'LIKELY'
    else:
        return 'FAIL'


def score_all_opportunities():
    """Score every opportunity against every rule."""
    _load_osrm_cache()
    conn = sqlite3.connect(DB_PATH)
    
    print("=" * 80)
    print("PharmacyFinder v2 — MULTI-RULE CHECKER")
    print("=" * 80)
    print("\nLoading data...")
    
    pharmacies, opportunities, supermarkets, med_centres, shopping_centres, gps_list, hospitals = load_all(conn)
    conn.close()
    
    print(f"  {len(pharmacies)} pharmacies | {len(opportunities)} opportunities")
    print(f"  {len(supermarkets)} supermarkets | {len(med_centres)} med centres | {len(shopping_centres)} shopping centres")
    print(f"  {len(gps_list)} GPs | {len(hospitals)} hospitals")
    
    # Build spatial index
    print("\nBuilding spatial index...")
    grid = PharmacyGrid(pharmacies, cell_deg=0.5)
    
    print(f"\nScoring {len(opportunities)} opportunities against all 7 rules...\n")
    
    scored = []
    for i, opp in enumerate(opportunities):
        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(opportunities)}] processing...")
            _save_osrm_cache()
        
        # Check state
        detected_state = detect_state(opp['lat'], opp['lng'])
        stated_state = (opp.get('region') or '').upper().strip()
        geocoding_flag = None
        if stated_state and detected_state != 'UNKNOWN' and stated_state != detected_state:
            geocoding_flag = f"Stated {stated_state}, detected {detected_state}"
        state = detected_state if detected_state != 'UNKNOWN' else stated_state
        
        # Nearest pharmacy (fresh calculation)
        sl_km, nearest_ph = grid.nearest(opp['lat'], opp['lng'])
        
        # Skip INVALID entries (manually verified as invalid)
        if opp.get('verification', '').upper() == 'INVALID':
            record = {
                'id': opp['id'],
                'name': opp.get('poi_name') or opp.get('address', '')[:60] or f"Opp #{opp['id']}",
                'state': state, 'lat': opp['lat'], 'lng': opp['lng'],
                'address': opp.get('address', ''), 'poi_type': opp.get('poi_type', ''),
                'pop_5km': opp.get('pop_5km', 0), 'pop_10km': opp.get('pop_10km', 0), 'pop_15km': opp.get('pop_15km', 0),
                'pharmacy_5km': 0, 'pharmacy_10km': 0, 'pharmacy_15km': 0,
                'nearest_pharmacy_km': sl_km, 'nearest_pharmacy': nearest_ph['name'] if nearest_ph else '',
                'nearest_town': opp.get('nearest_town', ''), 'evidence': opp.get('evidence', ''),
                'growth_indicator': opp.get('growth_indicator', ''), 'growth_details': opp.get('growth_details', ''),
                'geocoding_flag': geocoding_flag, 'original_rules': opp.get('original_rules', ''),
                'rules_checked': {}, 'best_rule': '', 'overall_verdict': 'FAIL', 'score': 0,
                'verdict': 'FAIL', 'rules': '', 'best_rule_display': '',
                'auto_pass': 0, 'auto_fail': 0, 'manual_checks': 0,
                'status_summary': 'INVALID', 'checks': {},
                'auto_pass_list': [], 'auto_fail_list': [], 'manual_check_list': [],
                'ratio': 0,
            }
            scored.append(record)
            continue

        # Check ALL rules
        rules_checked = {}
        rules_checked['item_131'] = check_item_131(opp, grid, pharmacies)
        rules_checked['item_132'] = check_item_132(opp, grid, pharmacies, supermarkets, med_centres, gps_list)
        rules_checked['item_133'] = check_item_133(opp, grid, pharmacies, shopping_centres, supermarkets)
        rules_checked['item_134'] = check_item_134(opp, grid, pharmacies, shopping_centres, supermarkets)
        rules_checked['item_134a'] = check_item_134a(opp, grid, pharmacies, shopping_centres, supermarkets)
        rules_checked['item_135'] = check_item_135(opp, grid, pharmacies, hospitals)
        rules_checked['item_136'] = check_item_136(opp, grid, pharmacies, med_centres, shopping_centres, hospitals)
        
        best_rule = determine_best_rule(rules_checked)
        overall_verdict = determine_overall_verdict(rules_checked)
        score = compute_score(rules_checked)
        
        # Build flat checks dict for dashboard (from best rule)
        best_checks = rules_checked[best_rule]['checks'] if best_rule else {}
        
        # Status counts across ALL rules
        all_statuses = []
        for r_data in rules_checked.values():
            for c in r_data.get('checks', {}).values():
                all_statuses.append(c.get('status', 'FAIL'))
        
        total_pass = sum(1 for s in all_statuses if s == 'PASS')
        total_fail = sum(1 for s in all_statuses if s == 'FAIL')
        total_unv = sum(1 for s in all_statuses if s == 'UNVERIFIED')
        
        record = {
            'id': opp['id'],
            'name': opp.get('poi_name') or opp.get('address', '')[:60] or f"Opp #{opp['id']}",
            'state': state,
            'lat': opp['lat'],
            'lng': opp['lng'],
            'address': opp.get('address', ''),
            'poi_type': opp.get('poi_type', ''),
            'pop_5km': opp.get('pop_5km', 0),
            'pop_10km': opp.get('pop_10km', 0),
            'pop_15km': opp.get('pop_15km', 0),
            'pharmacy_5km': opp.get('pharmacy_5km', 0),
            'pharmacy_10km': opp.get('pharmacy_10km', 0),
            'pharmacy_15km': opp.get('pharmacy_15km', 0),
            'nearest_pharmacy_km': round(sl_km, 2) if nearest_ph else 999,
            'nearest_pharmacy': nearest_ph['name'] if nearest_ph else '',
            'nearest_town': opp.get('nearest_town', ''),
            'original_rules': opp.get('original_rules', ''),
            'evidence': opp.get('evidence', ''),
            'growth_indicator': opp.get('growth_indicator', ''),
            'growth_details': opp.get('growth_details', ''),
            'geocoding_flag': geocoding_flag,
            
            # Multi-rule results
            'rules_checked': rules_checked,
            'best_rule': best_rule,
            'overall_verdict': overall_verdict,
            'score': score,
            
            # Summary for dashboard cards
            'verdict': overall_verdict,
            'rules': best_rule.replace('item_', 'Item ').replace('a', 'A') if best_rule else 'None',
            'best_rule_display': best_rule.replace('item_', 'Item ').replace('a', 'A') if best_rule else 'None',
            'auto_pass': total_pass,
            'auto_fail': total_fail,
            'manual_checks': total_unv,
            'status_summary': f"{total_pass}P {total_fail}F {total_unv}M",
            
            # Best rule's flat checks for card display
            'checks': best_checks,
            'auto_pass_list': [k for k, v in best_checks.items() if v.get('status') == 'PASS'],
            'auto_fail_list': [k for k, v in best_checks.items() if v.get('status') == 'FAIL'],
            'manual_check_list': [k for k, v in best_checks.items() if v.get('status') == 'UNVERIFIED'],
            
            # Ratio (for backward compat)
            'ratio': round(opp.get('pop_10km', 0) / max(opp.get('pharmacy_10km', 1), 1)),
        }
        
        scored.append(record)
    
    _save_osrm_cache()
    
    # Sort: PASS > LIKELY > FAIL, then by score desc
    verdict_order = {'PASS': 0, 'LIKELY': 1, 'FAIL': 2}
    scored.sort(key=lambda x: (verdict_order.get(x['verdict'], 3), -x['score']))
    
    # Print summary
    by_verdict = defaultdict(int)
    by_rule = defaultdict(int)
    for o in scored:
        by_verdict[o['verdict']] += 1
        if o['best_rule']:
            by_rule[o['best_rule']] += 1
    
    print(f"\n{'='*80}")
    print(f"SCORING COMPLETE — {len(scored)} opportunities scored")
    print(f"\nBy overall verdict:")
    for v in ['PASS', 'LIKELY', 'FAIL']:
        print(f"  {v}: {by_verdict.get(v, 0)}")
    print(f"\nBy best rule:")
    for r in sorted(by_rule.keys()):
        label = r.replace('item_', 'Item ').replace('a', 'A')
        print(f"  {label}: {by_rule[r]}")
    print(f"\nOSRM API calls this session: {_osrm_calls_made}")
    print(f"OSRM cache total: {len(_osrm_cache)}")
    print(f"{'='*80}")
    
    return scored


def print_results(scored):
    """Print top results."""
    print()
    print("=" * 150)
    print("TOP OPPORTUNITIES (PASS + LIKELY)")
    print("=" * 150)
    print(f"{'#':<4} {'Verdict':<8} {'Score':<6} {'Best Rule':<12} {'Name':<45} {'ST':<4} {'Near km':<9} {'Checks':<10} {'Reason'}")
    print("-" * 150)
    
    shown = 0
    for o in scored:
        if o['verdict'] == 'FAIL':
            continue
        shown += 1
        if shown > 80:
            break
        
        best = o.get('best_rule', '')
        label = best.replace('item_', 'Item ').replace('a', 'A') if best else '?'
        reason = o['rules_checked'].get(best, {}).get('reason', '')[:50] if best else ''
        
        print(f"{shown:<4} {o['verdict']:<8} {o['score']:<6} {label:<12} "
              f"{o['name'][:44]:<45} {o['state']:<4} {o['nearest_pharmacy_km']:<9.1f} "
              f"{o['status_summary']:<10} {reason}")
    
    print(f"\n  Showing {shown} non-FAIL opportunities out of {len(scored)} total")


def update_db(scored):
    """Update opportunity scores in database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE opportunities SET composite_score = 0, opp_score = 0")
    updated = 0
    for o in scored:
        cur.execute("UPDATE opportunities SET composite_score = ?, opp_score = ? WHERE id = ?",
                    (o['score'], o['ratio'], o['id']))
        if cur.rowcount > 0:
            updated += 1
    conn.commit()
    conn.close()
    print(f"Updated {updated} opportunity scores in database.")


# Backwards-compatible aliases used by build_dashboard_v3.py
def score_opportunities():
    return score_all_opportunities()


if __name__ == '__main__':
    sys.stdout.reconfigure(encoding='utf-8')
    
    scored = score_all_opportunities()
    print_results(scored)
    
    # Save JSON
    with open(os.path.join(OUTPUT_DIR, 'scored_v2.json'), 'w', encoding='utf-8') as f:
        json.dump(scored, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nSaved {len(scored)} results to output/scored_v2.json")
    
    update_db(scored)
