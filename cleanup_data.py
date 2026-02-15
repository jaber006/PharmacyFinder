"""
PharmacyFinder QA — Data Cleanup Script
========================================
Fixes bad records in the opportunities table:
1. Cygnet-coords junk (generic IGA etc with default Cygnet TAS coords)
2. Generic names with no town + score=0 (bad scrapes)  
3. Zero-population Item 131 with Unknown town
4. Bad coordinates in top opportunities
5. Duplicates near Cygnet

Run: python cleanup_data.py
"""

import sqlite3
import sys
import io
import math
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = 'pharmacy_finder.db'
TIMESTAMP = datetime.now().strftime('%Y-%m-%d %H:%M')

# ── State lat/lng bounding boxes (rough) ──
STATE_BOUNDS = {
    'NSW': {'lat': (-37.5, -28.0), 'lng': (140.9, 153.7)},
    'VIC': {'lat': (-39.2, -33.9), 'lng': (140.9, 150.1)},
    'QLD': {'lat': (-29.2, -10.0), 'lng': (137.9, 153.6)},
    'SA':  {'lat': (-38.1, -26.0), 'lng': (129.0, 141.0)},
    'WA':  {'lat': (-35.2, -13.5), 'lng': (112.9, 129.0)},
    'TAS': {'lat': (-43.7, -39.5), 'lng': (143.5, 148.5)},
    'NT':  {'lat': (-26.1, -10.9), 'lng': (129.0, 138.0)},
    'ACT': {'lat': (-36.0, -35.0), 'lng': (148.7, 149.4)},
}

CYGNET_LAT = -43.1585571
CYGNET_LNG = 147.0750318

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def coords_in_state(lat, lng, state):
    """Check if coords are plausibly within a state's bounding box."""
    bounds = STATE_BOUNDS.get(state)
    if not bounds:
        return True
    lat_ok = bounds['lat'][0] <= lat <= bounds['lat'][1]
    lng_ok = bounds['lng'][0] <= lng <= bounds['lng'][1]
    return lat_ok and lng_ok

def update_note(c, ids, note_suffix):
    """Update verification_notes for a list of IDs."""
    note = f' | QA cleanup {TIMESTAMP} - {note_suffix}'
    if isinstance(ids, int):
        ids = [ids]
    placeholders = ','.join(['?'] * len(ids))
    c.execute(f"""
        UPDATE opportunities
        SET verification_notes = COALESCE(verification_notes, '') || ?
        WHERE id IN ({placeholders})
    """, [note] + list(ids))

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    results = {
        'cygnet_coords_junk': 0,
        'cygnet_coords_with_town': 0,
        'generic_no_town': 0,
        'zero_pop_item131': 0,
        'bad_coords_top20': 0,
        'duplicates': 0,
    }
    
    print(f"{'='*60}")
    print(f"  PHARMACY FINDER — DATA CLEANUP")
    print(f"  {TIMESTAMP}")
    print(f"{'='*60}\n")
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 1: Cygnet-coords junk records  
    # ═══════════════════════════════════════════════════════════════
    print("STEP 1: Cygnet-coords junk (default fallback coords)")
    print("-" * 56)
    
    # 1a: Cygnet coords, empty town, score=0
    c.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules
        FROM opportunities
        WHERE ABS(latitude - ?) < 0.0001 AND ABS(longitude - ?) < 0.0001
        AND (nearest_town IS NULL OR nearest_town = '' OR nearest_town = 'Unknown')
        AND opp_score = 0
    """, (CYGNET_LAT, CYGNET_LNG))
    rows = c.fetchall()
    ids_1a = [r['id'] for r in rows]
    for r in rows:
        print(f"  [{r['id']}] {r['poi_name']} ({r['region']}) town='{r['nearest_town']}' -> DATA_QUALITY_FAIL")
    
    if ids_1a:
        placeholders = ','.join(['?'] * len(ids_1a))
        c.execute(f"""
            UPDATE opportunities
            SET qualifying_rules = 'NONE', verification = 'DATA_QUALITY_FAIL'
            WHERE id IN ({placeholders})
        """, ids_1a)
        update_note(c, ids_1a, 'Cygnet default coords, no town, score=0')
        results['cygnet_coords_junk'] = len(ids_1a)
    print(f"  -> Updated {len(ids_1a)} records\n")
    
    # 1b: Cygnet coords, HAS a town but wrong state (not TAS)
    c.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules, opp_score
        FROM opportunities
        WHERE ABS(latitude - ?) < 0.0001 AND ABS(longitude - ?) < 0.0001
        AND nearest_town IS NOT NULL AND nearest_town != '' AND nearest_town != 'Unknown'
        AND region != 'TAS'
    """, (CYGNET_LAT, CYGNET_LNG))
    rows = c.fetchall()
    ids_1b = [r['id'] for r in rows]
    for r in rows:
        print(f"  [{r['id']}] {r['poi_name']} ({r['nearest_town']}, {r['region']}) score={r['opp_score']} -> BAD_COORDS")
    
    if ids_1b:
        placeholders = ','.join(['?'] * len(ids_1b))
        c.execute(f"""
            UPDATE opportunities
            SET verification = 'BAD_COORDS', qualifying_rules = 'NONE'
            WHERE id IN ({placeholders})
        """, ids_1b)
        update_note(c, ids_1b, 'Cygnet default coords but claimed non-TAS region')
        results['cygnet_coords_with_town'] = len(ids_1b)
    print(f"  -> Updated {len(ids_1b)} records with towns but wrong coords\n")
    
    # 1c: Cygnet coords, TAS, duplicates — keep one, mark rest
    c.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules, opp_score
        FROM opportunities
        WHERE ABS(latitude - ?) < 0.0001 AND ABS(longitude - ?) < 0.0001
        AND region = 'TAS'
        AND poi_name = 'IGA'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS')
        ORDER BY 
            CASE WHEN nearest_town != '' AND nearest_town IS NOT NULL THEN 0 ELSE 1 END,
            opp_score DESC,
            id ASC
    """, (CYGNET_LAT, CYGNET_LNG))
    tas_igas = c.fetchall()
    if len(tas_igas) > 1:
        keep_id = tas_igas[0]['id']
        dup_ids = [r['id'] for r in tas_igas[1:]]
        print(f"  TAS IGA duplicates at Cygnet: keeping #{keep_id}, marking {len(dup_ids)} as DUPLICATE")
        for r in tas_igas[1:]:
            print(f"    [{r['id']}] {r['poi_name']} ('{r['nearest_town']}') -> DUPLICATE")
        
        placeholders = ','.join(['?'] * len(dup_ids))
        c.execute(f"""
            UPDATE opportunities
            SET qualifying_rules = 'NONE', verification = 'DUPLICATE'
            WHERE id IN ({placeholders})
        """, dup_ids)
        update_note(c, dup_ids, f'Duplicate IGA at Cygnet coords, kept #{keep_id}')
        results['duplicates'] = len(dup_ids)
        print(f"  -> Marked {len(dup_ids)} duplicates\n")
    else:
        print("  No TAS IGA duplicates found\n")
    
    conn.commit()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 2: Remaining generic names with no town + score=0
    # ═══════════════════════════════════════════════════════════════
    print("STEP 2: Remaining generic names with no town + score=0")
    print("-" * 56)
    
    c.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules, latitude, longitude
        FROM opportunities
        WHERE (nearest_town IS NULL OR nearest_town = '' OR nearest_town = 'Unknown')
        AND opp_score = 0
        AND qualifying_rules IS NOT NULL AND qualifying_rules != 'NONE'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
    """)
    rows = c.fetchall()
    ids_2 = [r['id'] for r in rows]
    for r in rows:
        print(f"  [{r['id']}] {r['poi_name']} ({r['region']}) lat={r['latitude']:.4f} -> DATA_QUALITY_FAIL")
    
    if ids_2:
        placeholders = ','.join(['?'] * len(ids_2))
        c.execute(f"""
            UPDATE opportunities
            SET qualifying_rules = 'NONE', verification = 'DATA_QUALITY_FAIL'
            WHERE id IN ({placeholders})
        """, ids_2)
        update_note(c, ids_2, 'No town, score=0, bad scrape data')
        results['generic_no_town'] = len(ids_2)
    print(f"  -> Updated {len(ids_2)} records\n")
    
    conn.commit()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 3: Zero-population Item 131 with Unknown town
    # ═══════════════════════════════════════════════════════════════
    print("STEP 3: Zero-population Item 131 with Unknown town")
    print("-" * 56)
    
    c.execute("""
        SELECT id, poi_name, nearest_town, region, pop_5km, nearest_pharmacy_km, qualifying_rules
        FROM opportunities
        WHERE qualifying_rules LIKE '%131%'
        AND pop_5km = 0
        AND nearest_town = 'Unknown'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE', 'NEEDS_POP_DATA')
    """)
    rows = c.fetchall()
    ids_3 = [r['id'] for r in rows]
    for r in rows:
        print(f"  [{r['id']}] {r['poi_name']} ({r['region']}) nearest_pharm={r['nearest_pharmacy_km']:.1f}km -> NEEDS_POP_DATA")
    
    if ids_3:
        placeholders = ','.join(['?'] * len(ids_3))
        c.execute(f"""
            UPDATE opportunities
            SET verification = 'NEEDS_POP_DATA'
            WHERE id IN ({placeholders})
        """, ids_3)
        update_note(c, ids_3, 'pop_5km=0, town=Unknown, needs population data')
        results['zero_pop_item131'] = len(ids_3)
    print(f"  -> Flagged {len(ids_3)} records\n")
    
    conn.commit()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 4: Bad coordinates in top opportunities
    # ═══════════════════════════════════════════════════════════════
    print("STEP 4: Bad coordinates check (top 20 + manual checks)")
    print("-" * 56)
    
    c.execute("""
        SELECT id, poi_name, nearest_town, region, composite_score, opp_score,
               pop_5km, nearest_pharmacy_km, qualifying_rules, latitude, longitude, verification
        FROM opportunities
        WHERE qualifying_rules IS NOT NULL AND qualifying_rules != 'NONE'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        ORDER BY composite_score DESC
        LIMIT 20
    """)
    top20 = c.fetchall()
    
    bad_coord_ids = []
    for r in top20:
        lat, lng, state = r['latitude'], r['longitude'], r['region']
        name = r['poi_name']
        town = r['nearest_town']
        pop5 = r['pop_5km']
        dist = r['nearest_pharmacy_km']
        rid = r['id']
        
        issues = []
        
        if not coords_in_state(lat, lng, state):
            issues.append(f"coords ({lat:.4f}, {lng:.4f}) outside {state} bounds")
        
        if pop5 > 500000 and dist > 15:
            issues.append(f"pop_5km={pop5:,} but nearest pharmacy={dist:.1f}km (impossible for urban)")
        
        if issues:
            bad_coord_ids.append(rid)
            print(f"  [{rid}] {name} ({town}, {state}) composite={r['composite_score']}")
            for issue in issues:
                print(f"       >> {issue}")
    
    if bad_coord_ids:
        placeholders = ','.join(['?'] * len(bad_coord_ids))
        c.execute(f"""
            UPDATE opportunities
            SET verification = 'BAD_COORDS'
            WHERE id IN ({placeholders})
            AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        """, bad_coord_ids)
        update_note(c, bad_coord_ids, 'Coords do not match claimed location')
        results['bad_coords_top20'] = len(bad_coord_ids)
    else:
        print("  No bad coords found in automated top-20 check")
    
    # Manual checks for specific known-bad records
    print("\n  Manual coordinate checks:")
    
    manual_bad = []
    
    # #8505: IGA Friendly Grocer — claims Docklands but coords near Skipton VIC
    c.execute("SELECT id, poi_name, nearest_town, latitude, longitude, verification FROM opportunities WHERE id = 8505")
    r = c.fetchone()
    if r and r['verification'] not in ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE'):
        dist_to_docklands = haversine_km(r['latitude'], r['longitude'], -37.815, 144.943)
        if dist_to_docklands > 20:
            print(f"  [{r['id']}] {r['poi_name']} claims Docklands but coords {dist_to_docklands:.0f}km away -> BAD_COORDS")
            manual_bad.append(r['id'])
    
    # #8705: SmartClinics Toowoomba — coords near Moranbah, not Toowoomba
    c.execute("SELECT id, poi_name, nearest_town, latitude, longitude, verification FROM opportunities WHERE id = 8705")
    r = c.fetchone()
    if r and r['verification'] not in ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE'):
        dist_to_toowoomba = haversine_km(r['latitude'], r['longitude'], -27.56, 151.95)
        if dist_to_toowoomba > 50:
            print(f"  [{r['id']}] {r['poi_name']} claims Toowoomba but coords {dist_to_toowoomba:.0f}km away -> BAD_COORDS")
            manual_bad.append(r['id'])
    
    # #8716: Emerald Medical Centre — coords in NSW, not Emerald QLD
    c.execute("SELECT id, poi_name, nearest_town, latitude, longitude, verification FROM opportunities WHERE id = 8716")
    r = c.fetchone()
    if r and r['verification'] not in ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE'):
        dist_to_emerald = haversine_km(r['latitude'], r['longitude'], -23.43, 148.16)
        if dist_to_emerald > 50:
            print(f"  [{r['id']}] {r['poi_name']} claims Emerald QLD but coords {dist_to_emerald:.0f}km away -> BAD_COORDS")
            manual_bad.append(r['id'])
    
    if manual_bad:
        placeholders = ','.join(['?'] * len(manual_bad))
        c.execute(f"""
            UPDATE opportunities
            SET verification = 'BAD_COORDS'
            WHERE id IN ({placeholders})
        """, manual_bad)
        update_note(c, manual_bad, 'Manual check - coords far from claimed location')
        results['bad_coords_top20'] += len(manual_bad)
    
    print(f"  -> Total bad coords flagged: {results['bad_coords_top20']}\n")
    
    conn.commit()
    
    # ═══════════════════════════════════════════════════════════════
    # STEP 5: Additional duplicate check
    # ═══════════════════════════════════════════════════════════════
    print("STEP 5: Additional duplicate check")
    print("-" * 56)
    
    c.execute("""
        SELECT poi_name, ROUND(latitude, 4) as rlat, ROUND(longitude, 4) as rlng, 
               COUNT(*) as cnt, GROUP_CONCAT(id) as ids
        FROM opportunities
        WHERE qualifying_rules != 'NONE'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        GROUP BY poi_name, rlat, rlng
        HAVING COUNT(*) > 1
    """)
    dup_groups = c.fetchall()
    extra_dups = 0
    for g in dup_groups:
        ids = [int(x) for x in g['ids'].split(',')]
        placeholders = ','.join(['?'] * len(ids))
        c.execute(f"""
            SELECT id, poi_name, nearest_town, opp_score, qualifying_rules
            FROM opportunities
            WHERE id IN ({placeholders})
            ORDER BY opp_score DESC, id ASC
        """, ids)
        dups = c.fetchall()
        keep = dups[0]
        to_mark = [d['id'] for d in dups[1:]]
        if to_mark:
            print(f"  Duplicate: '{g['poi_name']}' at ({g['rlat']}, {g['rlng']})")
            print(f"    Keep: #{keep['id']} (score={keep['opp_score']})")
            for d in dups[1:]:
                print(f"    Mark: #{d['id']} (score={d['opp_score']}) -> DUPLICATE")
            
            dp = ','.join(['?'] * len(to_mark))
            c.execute(f"""
                UPDATE opportunities
                SET verification = 'DUPLICATE', qualifying_rules = 'NONE'
                WHERE id IN ({dp})
            """, to_mark)
            update_note(c, to_mark, f'Duplicate of #{keep["id"]}')
            extra_dups += len(to_mark)
    
    results['duplicates'] += extra_dups
    print(f"  -> Marked {extra_dups} additional duplicates\n")
    
    conn.commit()
    
    # ═══════════════════════════════════════════════════════════════
    # SUMMARY
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'='*60}")
    print("  CLEANUP SUMMARY")
    print(f"{'='*60}")
    print(f"  Cygnet-coords junk (no town):       {results['cygnet_coords_junk']}")
    print(f"  Cygnet-coords (wrong state+town):   {results['cygnet_coords_with_town']}")
    print(f"  Generic no-town (other):            {results['generic_no_town']}")
    print(f"  Zero-pop Item 131 (flagged):        {results['zero_pop_item131']}")
    print(f"  Bad coordinates (top opps):         {results['bad_coords_top20']}")
    print(f"  Duplicates:                         {results['duplicates']}")
    total = sum(results.values())
    print(f"  {'_'*40}")
    print(f"  TOTAL RECORDS UPDATED:              {total}")
    
    # Post-cleanup stats
    print(f"\n{'='*60}")
    print("  POST-CLEANUP STATS")
    print(f"{'='*60}")
    
    c.execute("SELECT COUNT(*) FROM opportunities WHERE qualifying_rules != 'NONE'")
    clean_qualifying = c.fetchone()[0]
    
    c.execute("""
        SELECT COUNT(*) FROM opportunities 
        WHERE qualifying_rules != 'NONE'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
    """)
    truly_clean = c.fetchone()[0]
    
    print(f"  Remaining qualifying opportunities: {clean_qualifying}")
    print(f"  Truly clean (excl BAD_COORDS etc):  {truly_clean}")
    
    c.execute("""
        SELECT region, qualifying_rules, COUNT(*) 
        FROM opportunities 
        WHERE qualifying_rules != 'NONE'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        GROUP BY region, qualifying_rules
        ORDER BY region, qualifying_rules
    """)
    print(f"\n  By State & Rule (clean only):")
    current_state = None
    state_total = 0
    for row in c.fetchall():
        if row[0] != current_state:
            if current_state:
                print(f"    {'':5s} {'TOTAL':30s} {state_total:4d}")
            current_state = row[0]
            state_total = 0
        state_total += row[2]
        print(f"    {row[0]:5s} {row[1]:30s} {row[2]:4d}")
    if current_state:
        print(f"    {'':5s} {'TOTAL':30s} {state_total:4d}")
    
    c.execute("""
        SELECT verification, COUNT(*) 
        FROM opportunities 
        GROUP BY verification 
        ORDER BY COUNT(*) DESC
    """)
    print(f"\n  Verification Status Distribution:")
    for row in c.fetchall():
        print(f"    {str(row[0]):30s} {row[1]:4d}")
    
    conn.close()
    print(f"\n  Done. Database saved.")


if __name__ == '__main__':
    main()
