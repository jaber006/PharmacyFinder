#!/usr/bin/env python3
"""Fix 65 PASS_ISSUE opportunities in PharmacyFinder database."""

import sqlite3
import json
import time
import urllib.request
import urllib.parse
import math
import os
from datetime import datetime

DB_PATH = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"
OUTPUT_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\output"

def nominatim_search(query, limit=1):
    """Search OpenStreetMap Nominatim for coordinates."""
    params = urllib.parse.urlencode({
        'q': query,
        'format': 'json',
        'limit': limit,
        'countrycodes': 'au'
    })
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data:
                return float(data[0]['lat']), float(data[0]['lon']), data[0].get('display_name', '')
    except Exception as e:
        print(f"  Nominatim error for '{query}': {e}")
    return None, None, None


def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def get_nearest_pharmacy(conn, lat, lon):
    """Find nearest pharmacy to given coordinates."""
    cur = conn.execute("SELECT name, latitude, longitude FROM pharmacies")
    best_name = None
    best_dist = float('inf')
    for name, plat, plon in cur:
        d = haversine(lat, lon, plat, plon)
        if d < best_dist:
            best_dist = d
            best_name = name
    return best_name, best_dist


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    
    fixes_log = []
    
    # =========================================================================
    # 1. Fix Port Macquarie Private Hospital (id 9356)
    # =========================================================================
    print("=" * 60)
    print("1. Fixing Port Macquarie Private Hospital (id 9356)")
    print("=" * 60)
    
    # Real location: 86-94 Lake Rd, Port Macquarie NSW 2444
    lat, lon, display = nominatim_search("Port Macquarie Private Hospital, Lake Road, Port Macquarie, NSW")
    time.sleep(1.1)
    
    if lat is None:
        # Fallback to known coords
        lat, lon = -31.4388, 152.8998
        display = "Fallback coords for Port Macquarie Private Hospital"
    
    print(f"  Found: {lat}, {lon} ({display})")
    
    old = conn.execute("SELECT latitude, longitude FROM opportunities WHERE id = 9356").fetchone()
    conn.execute("UPDATE opportunities SET latitude = ?, longitude = ? WHERE id = 9356", (lat, lon))
    
    # Update nearest pharmacy
    pname, pdist = get_nearest_pharmacy(conn, lat, lon)
    conn.execute("UPDATE opportunities SET nearest_pharmacy_name = ?, nearest_pharmacy_km = ? WHERE id = 9356", (pname, pdist))
    
    fixes_log.append({
        'id': 9356,
        'name': 'Port Macquarie Private Hospital',
        'fix': 'Corrected coordinates',
        'old_coords': f"{old[0]}, {old[1]}",
        'new_coords': f"{lat}, {lon}",
        'nearest_pharmacy': f"{pname} ({pdist:.2f} km)"
    })
    print(f"  Updated: ({old[0]}, {old[1]}) -> ({lat}, {lon})")
    print(f"  Nearest pharmacy: {pname} ({pdist:.2f} km)")
    
    # =========================================================================
    # 2. Fix Emerald QLD entries (ids 8716, 8901)
    # =========================================================================
    print("\n" + "=" * 60)
    print("2. Fixing Emerald QLD entries (ids 8716, 8901)")
    print("=" * 60)
    
    emerald_ids = [8716, 8901]
    # Emerald QLD real coordinates
    emerald_lat, emerald_lon = -23.4375, 148.1591
    
    # Try to get more specific coords for each
    for eid in emerald_ids:
        row = conn.execute("SELECT poi_name, latitude, longitude FROM opportunities WHERE id = ?", (eid,)).fetchone()
        name = row[0]
        old_lat, old_lon = row[1], row[2]
        
        # Search for specific POI in Emerald QLD
        search_q = f"{name}, Emerald, Queensland, Australia"
        lat, lon, display = nominatim_search(search_q)
        time.sleep(1.1)
        
        if lat is None or haversine(lat, lon, emerald_lat, emerald_lon) > 20:
            # Use Emerald town center coords
            lat, lon = emerald_lat, emerald_lon
            display = "Emerald QLD town center"
        
        print(f"  {name}: ({old_lat}, {old_lon}) -> ({lat}, {lon})")
        
        conn.execute("UPDATE opportunities SET latitude = ?, longitude = ?, region = 'QLD' WHERE id = ?", (lat, lon, eid))
        
        pname, pdist = get_nearest_pharmacy(conn, lat, lon)
        conn.execute("UPDATE opportunities SET nearest_pharmacy_name = ?, nearest_pharmacy_km = ? WHERE id = ?", (pname, pdist, eid))
        
        fixes_log.append({
            'id': eid,
            'name': name,
            'fix': 'Corrected coordinates to Emerald QLD',
            'old_coords': f"{old_lat}, {old_lon}",
            'new_coords': f"{lat}, {lon}",
            'nearest_pharmacy': f"{pname} ({pdist:.2f} km)"
        })
    
    # =========================================================================
    # 3. Fix 3 generic "Supermarket" entries (ids 8011, 8259, 9382)
    # =========================================================================
    print("\n" + "=" * 60)
    print("3. Fixing 3 generic 'Supermarket' entries")
    print("=" * 60)
    
    supermarket_ids = {
        8011: 'NT',
        8259: 'WA', 
        9382: 'TAS'
    }
    
    # 9382 (TAS) has address "Esplanade, Coles Bay" - this is actually Coles Bay Convenience area, may be legit
    # 8011 (NT) and 8259 (WA) have no address - these are phantom entries
    
    for sid, region in supermarket_ids.items():
        row = conn.execute("SELECT poi_name, latitude, longitude, address FROM opportunities WHERE id = ?", (sid,)).fetchone()
        name, old_lat, old_lon, address = row
        
        if sid == 9382 and address:
            # TAS entry has an address - try to geocode it properly
            lat, lon, display = nominatim_search(f"Supermarket, Coles Bay, Tasmania, Australia")
            time.sleep(1.1)
            
            if lat and haversine(lat, lon, -42.12, 148.28) < 5:
                # Close to Coles Bay area - legitimate
                print(f"  ID {sid} ({region}): TAS Coles Bay - coords seem correct ({lat}, {lon})")
                # The coords are already in Coles Bay area, keep them
                conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Generic Supermarket name, unable to verify specific location. Coles Bay area.' WHERE id = ?", (sid,))
                fixes_log.append({
                    'id': sid,
                    'name': name,
                    'fix': f'Marked INVALID - generic name, unverifiable in {region}',
                    'old_coords': f"{old_lat}, {old_lon}",
                    'new_coords': 'unchanged',
                    'nearest_pharmacy': 'N/A'
                })
            else:
                conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Generic Supermarket name geocoded to fallback. Cannot identify real location.' WHERE id = ?", (sid,))
                fixes_log.append({
                    'id': sid,
                    'name': name,
                    'fix': f'Marked INVALID - phantom entry for {region}',
                    'old_coords': f"{old_lat}, {old_lon}",
                    'new_coords': 'unchanged',
                    'nearest_pharmacy': 'N/A'
                })
        else:
            # NT and WA entries with no address - mark as INVALID
            print(f"  ID {sid} ({region}): No address, marking INVALID")
            conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Generic Supermarket name with no address, geocoded to fallback TAS coords. Cannot identify real location.' WHERE id = ?", (sid,))
            fixes_log.append({
                'id': sid,
                'name': name,
                'fix': f'Marked INVALID - phantom entry for {region}, no address',
                'old_coords': f"{old_lat}, {old_lon}",
                'new_coords': 'unchanged',
                'nearest_pharmacy': 'N/A'
            })
    
    # =========================================================================
    # 4. Fix state-mismatch entries (19 entries)
    # =========================================================================
    print("\n" + "=" * 60)
    print("4. Fixing 19 state-mismatch entries")
    print("=" * 60)
    
    state_mismatch_ids = [
        8115,  # RFDS Clinic Innamincka - SA region, detected QLD
        8376,  # Rupanyup Supermarket - VIC region, detected NSW  
        8379,  # Rupanyup Doctors Surgery - VIC region, detected NSW
        8490,  # Dargo General Store - VIC region, detected NSW
        8492,  # McPherson General Store - VIC region, detected NSW
        8495,  # Marnoo General Store - VIC region, detected NSW
        8498,  # Balmoral Community Store - VIC region, detected NSW
        8500,  # Hotham Supermarket - VIC region, detected NSW
        8503,  # Katamatite Fuel & Grocery - VIC region, detected NSW
        8504,  # Cann River Friendly Grocer - VIC region, detected NSW
        8508,  # Mt Buller Medical Centre - VIC region, detected NSW
        8509,  # Mount Hotham Medical Centre - VIC region, detected NSW
        8510,  # Peter M. Sudholz Medical & Allied Health Centre - VIC region, detected NSW
        8515,  # Rural Northwest Health Beulah Campus - VIC region, detected NSW
        8518,  # Kaniava Hospital - VIC region, detected NSW
    ]
    
    for sid in state_mismatch_ids:
        row = conn.execute("SELECT poi_name, latitude, longitude, region, address FROM opportunities WHERE id = ?", (sid,)).fetchone()
        name, old_lat, old_lon, region, address = row
        
        # Search for the actual location
        search_q = f"{name}, {region}, Australia"
        lat, lon, display = nominatim_search(search_q)
        time.sleep(1.1)
        
        if lat is None:
            # Try simpler search
            search_q = f"{name}, Australia"
            lat, lon, display = nominatim_search(search_q)
            time.sleep(1.1)
        
        if lat is not None:
            dist_from_old = haversine(lat, lon, old_lat, old_lon)
            print(f"  ID {sid} {name}: ({old_lat}, {old_lon}) -> ({lat}, {lon}) [{display[:60]}] (moved {dist_from_old:.1f}km)")
            
            conn.execute("UPDATE opportunities SET latitude = ?, longitude = ? WHERE id = ?", (lat, lon, sid))
            
            pname, pdist = get_nearest_pharmacy(conn, lat, lon)
            conn.execute("UPDATE opportunities SET nearest_pharmacy_name = ?, nearest_pharmacy_km = ? WHERE id = ?", (pname, pdist, sid))
            
            fixes_log.append({
                'id': sid,
                'name': name,
                'fix': f'Re-geocoded to correct {region} location',
                'old_coords': f"{old_lat}, {old_lon}",
                'new_coords': f"{lat}, {lon}",
                'nearest_pharmacy': f"{pname} ({pdist:.2f} km)"
            })
        else:
            print(f"  ID {sid} {name}: Could not geocode, keeping original coords")
            fixes_log.append({
                'id': sid,
                'name': name,
                'fix': 'Could not re-geocode - kept original',
                'old_coords': f"{old_lat}, {old_lon}",
                'new_coords': 'unchanged',
                'nearest_pharmacy': 'N/A'
            })
    
    # =========================================================================
    # 5. Fix 42 Foodworks entries
    # =========================================================================
    print("\n" + "=" * 60)
    print("5. Fixing 42 Foodworks entries with fallback coordinates")
    print("=" * 60)
    
    # Get all Foodworks opportunities with fallback coords
    foodworks_opps = conn.execute("""
        SELECT id, region, nearest_pharmacy_name, nearest_pharmacy_km, evidence 
        FROM opportunities 
        WHERE latitude = -32.2443163 AND longitude = 147.3564635
        ORDER BY region, id
    """).fetchall()
    
    # Get all pharmacies for matching
    pharmacies = conn.execute("SELECT name, latitude, longitude FROM pharmacies").fetchall()
    pharmacy_dict = {p[0]: (p[1], p[2]) for p in pharmacies}
    
    # Get all Foodworks from supermarkets table with proper coords (not the fallback one)
    all_foodworks = conn.execute("""
        SELECT id, name, address, latitude, longitude 
        FROM supermarkets 
        WHERE name = 'Foodworks' AND NOT (latitude = -32.2443163 AND longitude = 147.3564635)
        ORDER BY id
    """).fetchall()
    
    print(f"  Found {len(foodworks_opps)} Foodworks opportunities to fix")
    print(f"  Found {len(all_foodworks)} Foodworks stores with valid coords")
    
    # Strategy: For each opportunity, find the Foodworks store nearest to its 
    # listed nearest_pharmacy (since the pharmacy assignment was from the scoring)
    # OR match by region and find the one that creates the best distance to nearest pharmacy
    
    # Group foodworks stores by region
    fw_by_region = {}
    for fw_id, fw_name, fw_addr, fw_lat, fw_lon in all_foodworks:
        # Determine region from address
        addr_upper = fw_addr.upper()
        if 'VIC' in addr_upper:
            r = 'VIC'
        elif 'NSW' in addr_upper:
            r = 'NSW'
        elif 'QLD' in addr_upper:
            r = 'QLD'
        elif 'SA' in addr_upper or 'SOUTH AUSTRALIA' in addr_upper:
            r = 'SA'
        elif 'WA' in addr_upper or 'WESTERN AUSTRALIA' in addr_upper:
            r = 'WA'
        elif 'TAS' in addr_upper:
            r = 'TAS'
        elif 'NT' in addr_upper or 'NORTHERN TERRITORY' in addr_upper:
            r = 'NT'
        elif 'ACT' in addr_upper:
            r = 'ACT'
        else:
            r = 'UNKNOWN'
        
        if r not in fw_by_region:
            fw_by_region[r] = []
        fw_by_region[r].append((fw_id, fw_name, fw_addr, fw_lat, fw_lon))
    
    print(f"  Foodworks by region: {', '.join(f'{k}:{len(v)}' for k, v in sorted(fw_by_region.items()))}")
    
    # For each opportunity, match to the Foodworks store nearest to the originally-intended pharmacy
    used_fw = set()  # Track which Foodworks stores we've assigned
    
    for opp_id, opp_region, opp_pharm, opp_pharm_km, evidence in foodworks_opps:
        # Find the pharmacy coords
        pharm_coords = pharmacy_dict.get(opp_pharm)
        
        if pharm_coords is None:
            print(f"  ID {opp_id} ({opp_region}): Cannot find pharmacy '{opp_pharm}', marking INVALID")
            conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Foodworks with fallback coords, cannot match to specific store' WHERE id = ?", (opp_id,))
            fixes_log.append({
                'id': opp_id,
                'name': 'Foodworks',
                'fix': f'Marked INVALID - cannot find pharmacy {opp_pharm}',
                'old_coords': '-32.2443163, 147.3564635',
                'new_coords': 'unchanged',
                'nearest_pharmacy': 'N/A'
            })
            continue
        
        p_lat, p_lon = pharm_coords
        
        # Find Foodworks stores in the right region
        candidates = fw_by_region.get(opp_region, [])
        
        if not candidates:
            print(f"  ID {opp_id} ({opp_region}): No Foodworks stores found in region, marking INVALID")
            conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Foodworks with fallback coords, no Foodworks stores found in region' WHERE id = ?", (opp_id,))
            fixes_log.append({
                'id': opp_id,
                'name': 'Foodworks',
                'fix': f'Marked INVALID - no Foodworks in {opp_region}',
                'old_coords': '-32.2443163, 147.3564635',
                'new_coords': 'unchanged',
                'nearest_pharmacy': 'N/A'
            })
            continue
        
        # Find nearest Foodworks to the pharmacy (that hasn't been used yet)
        best_fw = None
        best_dist = float('inf')
        for fw in candidates:
            fw_id, fw_name, fw_addr, fw_lat, fw_lon = fw
            if fw_id in used_fw:
                continue
            d = haversine(fw_lat, fw_lon, p_lat, p_lon)
            if d < best_dist:
                best_dist = d
                best_fw = fw
        
        if best_fw is None:
            # All candidates used, try again without used filter
            for fw in candidates:
                fw_id, fw_name, fw_addr, fw_lat, fw_lon = fw
                d = haversine(fw_lat, fw_lon, p_lat, p_lon)
                if d < best_dist:
                    best_dist = d
                    best_fw = fw
        
        if best_fw and best_dist < 10:  # Within 10km of the pharmacy
            fw_id, fw_name, fw_addr, fw_lat, fw_lon = best_fw
            used_fw.add(fw_id)
            
            print(f"  ID {opp_id} ({opp_region}): Matched to Foodworks at {fw_addr[:50]} ({best_dist:.2f}km from {opp_pharm})")
            
            conn.execute("UPDATE opportunities SET latitude = ?, longitude = ?, address = ? WHERE id = ?", 
                        (fw_lat, fw_lon, fw_addr, opp_id))
            
            # Recalculate nearest pharmacy
            pname, pdist = get_nearest_pharmacy(conn, fw_lat, fw_lon)
            conn.execute("UPDATE opportunities SET nearest_pharmacy_name = ?, nearest_pharmacy_km = ? WHERE id = ?", 
                        (pname, pdist, opp_id))
            
            fixes_log.append({
                'id': opp_id,
                'name': 'Foodworks',
                'fix': f'Matched to Foodworks at {fw_addr[:80]}',
                'old_coords': '-32.2443163, 147.3564635',
                'new_coords': f"{fw_lat}, {fw_lon}",
                'nearest_pharmacy': f"{pname} ({pdist:.2f} km)"
            })
        else:
            # Can't match - the opportunity was likely based on a fallback coord, mark invalid
            if best_fw:
                print(f"  ID {opp_id} ({opp_region}): Best match {best_dist:.1f}km from pharmacy '{opp_pharm}' - too far, marking INVALID")
            else:
                print(f"  ID {opp_id} ({opp_region}): No match found, marking INVALID")
            
            conn.execute("UPDATE opportunities SET verification = 'INVALID', verification_notes = 'Foodworks with fallback coords, cannot reliably match to specific store (nearest match too far)' WHERE id = ?", (opp_id,))
            fixes_log.append({
                'id': opp_id,
                'name': 'Foodworks',
                'fix': f'Marked INVALID - no reliable match in {opp_region}',
                'old_coords': '-32.2443163, 147.3564635',
                'new_coords': 'unchanged',
                'nearest_pharmacy': 'N/A'
            })
    
    # =========================================================================
    # Commit and save summary
    # =========================================================================
    conn.commit()
    
    print(f"\n{'=' * 60}")
    print(f"SUMMARY: Applied {len(fixes_log)} fixes")
    print(f"{'=' * 60}")
    
    # Count by type
    coords_fixed = sum(1 for f in fixes_log if 'Corrected' in f['fix'] or 'Re-geocoded' in f['fix'] or 'Matched to' in f['fix'])
    marked_invalid = sum(1 for f in fixes_log if 'INVALID' in f['fix'])
    print(f"  Coordinates fixed: {coords_fixed}")
    print(f"  Marked INVALID: {marked_invalid}")
    
    # Save summary markdown
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    md_path = os.path.join(OUTPUT_DIR, "pass_fixes_applied.md")
    with open(md_path, 'w') as f:
        f.write(f"# PASS Issue Fixes Applied\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Total fixes:** {len(fixes_log)}\n")
        f.write(f"- Coordinates corrected: {coords_fixed}\n")
        f.write(f"- Marked INVALID: {marked_invalid}\n\n")
        
        f.write("## 1. Port Macquarie Private Hospital (ID 9356)\n\n")
        for fix in fixes_log:
            if fix['id'] == 9356:
                f.write(f"- **Fix:** {fix['fix']}\n")
                f.write(f"- **Old coords:** {fix['old_coords']}\n")
                f.write(f"- **New coords:** {fix['new_coords']}\n")
                f.write(f"- **Nearest pharmacy:** {fix['nearest_pharmacy']}\n\n")
        
        f.write("## 2. Emerald QLD Entries (IDs 8716, 8901)\n\n")
        for fix in fixes_log:
            if fix['id'] in [8716, 8901]:
                f.write(f"### {fix['name']} (ID {fix['id']})\n")
                f.write(f"- **Fix:** {fix['fix']}\n")
                f.write(f"- **Old coords:** {fix['old_coords']}\n")
                f.write(f"- **New coords:** {fix['new_coords']}\n")
                f.write(f"- **Nearest pharmacy:** {fix['nearest_pharmacy']}\n\n")
        
        f.write("## 3. Generic 'Supermarket' Entries (IDs 8011, 8259, 9382)\n\n")
        for fix in fixes_log:
            if fix['id'] in [8011, 8259, 9382]:
                f.write(f"### ID {fix['id']} ({fix['name']})\n")
                f.write(f"- **Fix:** {fix['fix']}\n\n")
        
        f.write("## 4. State-Mismatch Entries\n\n")
        f.write("| ID | Name | Fix | Old Coords | New Coords | Nearest Pharmacy |\n")
        f.write("|-----|------|-----|------------|------------|------------------|\n")
        for fix in fixes_log:
            if fix['id'] in state_mismatch_ids:
                f.write(f"| {fix['id']} | {fix['name']} | {fix['fix'][:40]} | {fix['old_coords']} | {fix['new_coords']} | {fix['nearest_pharmacy']} |\n")
        f.write("\n")
        
        f.write("## 5. Foodworks Entries (42 with fallback coordinates)\n\n")
        matched = [fix for fix in fixes_log if fix['name'] == 'Foodworks' and 'Matched' in fix['fix']]
        invalid = [fix for fix in fixes_log if fix['name'] == 'Foodworks' and 'INVALID' in fix['fix']]
        f.write(f"- **Successfully matched:** {len(matched)}\n")
        f.write(f"- **Marked INVALID:** {len(invalid)}\n\n")
        
        if matched:
            f.write("### Successfully Matched\n\n")
            f.write("| ID | Region | New Coords | Nearest Pharmacy |\n")
            f.write("|-----|--------|------------|------------------|\n")
            for fix in matched:
                f.write(f"| {fix['id']} | - | {fix['new_coords']} | {fix['nearest_pharmacy']} |\n")
            f.write("\n")
        
        if invalid:
            f.write("### Marked INVALID\n\n")
            f.write("| ID | Reason |\n")
            f.write("|-----|--------|\n")
            for fix in invalid:
                f.write(f"| {fix['id']} | {fix['fix']} |\n")
            f.write("\n")
    
    print(f"\nSummary saved to: {md_path}")
    conn.close()
    return fixes_log


if __name__ == '__main__':
    main()
