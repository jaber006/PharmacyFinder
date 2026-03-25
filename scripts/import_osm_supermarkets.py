"""
Import OSM-extracted supermarkets and shopping centres from commercial_sites_v4.json
into pharmacy_finder.db. Deduplicates by 100m haversine distance.
"""

import json
import sqlite3
import math
from datetime import datetime

DB_PATH = 'pharmacy_finder.db'
JSON_PATH = 'data/commercial_sites_v4.json'
DEDUP_RADIUS_M = 100.0
DATE_SCRAPED = datetime.now().isoformat()

# Known brand normalization map
BRAND_MAP = {
    'coles': 'coles',
    'woolworths': 'woolworths',
    'aldi': 'aldi',
    'iga': 'iga',
    'iga x-press': 'iga_xpress',
    'iga xpress': 'iga_xpress',
    'iga everyday': 'iga_everyday',
    'foodworks': 'foodworks',
    'drakes': 'drakes',
    'costco': 'costco',
    'harris farm': 'harris_farm',
    'harris farm markets': 'harris_farm',
    'nqr': 'nqr',
    'spar': 'spar',
    'friendly grocer': 'friendly_grocer',
    'the friendly grocer': 'friendly_grocer',
    'ritchies': 'ritchies',
    "ritchie's": 'ritchies',
    'supabarn': 'supabarn',
    'metcash': 'metcash',
    'foodland': 'foodland',
    'romeo\'s': 'romeos',
    'romeos': 'romeos',
}


def haversine(lat1, lon1, lat2, lon2):
    """Distance in meters between two lat/lon points."""
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlam/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))


def normalize_brand(brand_str):
    """Normalize brand string to standard brand code."""
    if not brand_str:
        return None
    lower = brand_str.strip().lower()
    # Direct lookup
    if lower in BRAND_MAP:
        return BRAND_MAP[lower]
    # Partial match
    for key, val in BRAND_MAP.items():
        if key in lower or lower in key:
            return val
    # Return cleaned version
    return lower.replace(' ', '_')


def is_nearby(lat, lon, existing_coords, radius=DEDUP_RADIUS_M):
    """Check if (lat, lon) is within radius of any existing coordinate."""
    for elat, elon in existing_coords:
        if haversine(lat, lon, elat, elon) <= radius:
            return True
    return False


def build_spatial_index(coords, cell_size=0.002):
    """Build a grid-based spatial index for fast proximity checks.
    cell_size ~0.002 degrees ≈ 200m at Australian latitudes."""
    grid = {}
    for lat, lon in coords:
        key = (round(lat / cell_size), round(lon / cell_size))
        grid.setdefault(key, []).append((lat, lon))
    return grid, cell_size


def is_nearby_indexed(lat, lon, grid, cell_size, radius=DEDUP_RADIUS_M):
    """Check proximity using spatial grid index."""
    key_lat = round(lat / cell_size)
    key_lon = round(lon / cell_size)
    # Check surrounding cells
    for dlat in (-1, 0, 1):
        for dlon in (-1, 0, 1):
            cell = grid.get((key_lat + dlat, key_lon + dlon))
            if cell:
                for elat, elon in cell:
                    if haversine(lat, lon, elat, elon) <= radius:
                        return True
    return False


def add_to_index(lat, lon, grid, cell_size):
    """Add a new point to the spatial index."""
    key = (round(lat / cell_size), round(lon / cell_size))
    grid.setdefault(key, []).append((lat, lon))


def main():
    # Load source data
    print(f"Loading {JSON_PATH}...")
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
    sites = data['sites']
    
    supermarkets = [s for s in sites if s.get('t') == 'supermarket']
    centres = [s for s in sites if s.get('t') == 'shopping_centre']
    print(f"Source: {len(supermarkets)} supermarkets, {len(centres)} shopping centres")
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # --- SUPERMARKETS ---
    print("\n--- Importing Supermarkets ---")
    cur.execute("SELECT latitude, longitude FROM supermarkets")
    existing_sm = cur.fetchall()
    print(f"Existing supermarkets in DB: {len(existing_sm)}")
    
    grid_sm, cell_sm = build_spatial_index(existing_sm)
    
    sm_inserted = 0
    sm_skipped_dupe = 0
    sm_skipped_err = 0
    
    for s in supermarkets:
        name = s.get('n', '')
        lat = s.get('la')
        lon = s.get('ln')
        addr = s.get('a', '')
        brand = normalize_brand(s.get('b', ''))
        state = s.get('s', '')
        
        if lat is None or lon is None:
            sm_skipped_err += 1
            continue
        
        # Build address string
        full_addr = addr if addr else name
        if state:
            state_map = {'n': 'NSW', 'v': 'VIC', 'q': 'QLD', 'w': 'WA', 's': 'SA', 't': 'TAS', 'a': 'ACT', 'd': 'NT'}
            state_name = state_map.get(state, state.upper())
            if state_name not in full_addr:
                full_addr = f"{full_addr}, {state_name}, Australia"
        
        # Dedup check
        if is_nearby_indexed(lat, lon, grid_sm, cell_sm):
            sm_skipped_dupe += 1
            continue
        
        try:
            cur.execute("""
                INSERT INTO supermarkets (name, address, latitude, longitude, brand, date_scraped, gla_confidence, coord_verified)
                VALUES (?, ?, ?, ?, ?, ?, 'estimated', 0)
            """, (name, full_addr, lat, lon, brand, DATE_SCRAPED))
            add_to_index(lat, lon, grid_sm, cell_sm)
            sm_inserted += 1
        except sqlite3.IntegrityError:
            # Address uniqueness constraint
            sm_skipped_dupe += 1
    
    print(f"Supermarkets: {sm_inserted} inserted, {sm_skipped_dupe} skipped (dupes), {sm_skipped_err} skipped (errors)")
    
    # --- SHOPPING CENTRES ---
    print("\n--- Importing Shopping Centres ---")
    cur.execute("SELECT latitude, longitude FROM shopping_centres")
    existing_sc = cur.fetchall()
    print(f"Existing shopping centres in DB: {len(existing_sc)}")
    
    grid_sc, cell_sc = build_spatial_index(existing_sc)
    
    # Build supermarket lookup for detecting major supermarkets near centres
    # Include both existing DB supermarkets and newly imported ones
    cur.execute("SELECT name, latitude, longitude, brand FROM supermarkets")
    all_supermarkets = cur.fetchall()
    
    sc_inserted = 0
    sc_skipped_dupe = 0
    sc_skipped_err = 0
    
    for s in centres:
        name = s.get('n', '')
        lat = s.get('la')
        lon = s.get('ln')
        addr = s.get('a', '')
        state = s.get('s', '')
        
        if lat is None or lon is None or not name:
            sc_skipped_err += 1
            continue
        
        full_addr = addr if addr else name
        if state:
            state_map = {'n': 'NSW', 'v': 'VIC', 'q': 'QLD', 'w': 'WA', 's': 'SA', 't': 'TAS', 'a': 'ACT', 'd': 'NT'}
            state_name = state_map.get(state, state.upper())
            if state_name not in full_addr:
                full_addr = f"{full_addr}, {state_name}, Australia"
        
        # Dedup check
        if is_nearby_indexed(lat, lon, grid_sc, cell_sc):
            sc_skipped_dupe += 1
            continue
        
        # Detect nearby major supermarkets (within 500m of centre)
        nearby_brands = set()
        major_brands = {'woolworths', 'coles', 'aldi', 'iga', 'costco'}
        for sm_name, sm_lat, sm_lon, sm_brand in all_supermarkets:
            if haversine(lat, lon, sm_lat, sm_lon) <= 500:
                if sm_brand and sm_brand.lower() in major_brands:
                    nearby_brands.add(sm_name or sm_brand.title())
        
        major_supermarkets = json.dumps(sorted(nearby_brands)) if nearby_brands else None
        
        try:
            cur.execute("""
                INSERT INTO shopping_centres (name, address, latitude, longitude, major_supermarkets, date_scraped, centre_class, coord_verified)
                VALUES (?, ?, ?, ?, ?, ?, 'unknown', 0)
            """, (name, full_addr, lat, lon, major_supermarkets, DATE_SCRAPED))
            add_to_index(lat, lon, grid_sc, cell_sc)
            sc_inserted += 1
        except sqlite3.IntegrityError:
            sc_skipped_dupe += 1
    
    print(f"Shopping Centres: {sc_inserted} inserted, {sc_skipped_dupe} skipped (dupes), {sc_skipped_err} skipped (errors)")
    
    conn.commit()
    
    # Final counts
    cur.execute('SELECT COUNT(*) FROM supermarkets')
    total_sm = cur.fetchone()[0]
    cur.execute('SELECT COUNT(*) FROM shopping_centres')
    total_sc = cur.fetchone()[0]
    
    print(f"\n=== FINAL COUNTS ===")
    print(f"Supermarkets: {total_sm} (was {len(existing_sm)}, +{sm_inserted})")
    print(f"Shopping Centres: {total_sc} (was {len(existing_sc)}, +{sc_inserted})")
    
    conn.close()
    print("\nDone!")


if __name__ == '__main__':
    main()
