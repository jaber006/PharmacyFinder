#!/usr/bin/env python3
"""Fix the last 5 INVALID Foodworks by matching supermarket→pharmacy proximity."""
import sqlite3, math

DB = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

conn = sqlite3.connect(DB)

# Load all pharmacies
pharmacies = {}
for name, lat, lon in conn.execute("SELECT name, latitude, longitude FROM pharmacies"):
    pharmacies[name] = (lat, lon)

def get_nearest_pharmacy(lat, lon):
    best_n, best_d = None, float('inf')
    for n, (la, lo) in pharmacies.items():
        d = haversine(lat, lon, la, lo)
        if d < best_d: best_d, best_n = d, n
    return best_n, best_d

# Load all valid VIC Foodworks from supermarkets
vic_fw = conn.execute("""
    SELECT id, address, latitude, longitude FROM supermarkets 
    WHERE name='Foodworks' AND address LIKE '%VIC%' AND latitude != -32.2443163
""").fetchall()

qld_fw = conn.execute("""
    SELECT id, address, latitude, longitude FROM supermarkets 
    WHERE name='Foodworks' AND address LIKE '%QLD%' AND latitude != -32.2443163
""").fetchall()

nsw_fw = conn.execute("""
    SELECT id, address, latitude, longitude FROM supermarkets 
    WHERE name='Foodworks' AND address LIKE '%NSW%' AND latitude != -32.2443163
""").fetchall()

# Already-assigned Foodworks coordinates (from previous fixes)
already_used = set()
for lat, lon in conn.execute("""
    SELECT latitude, longitude FROM opportunities 
    WHERE poi_name='Foodworks' AND verification != 'INVALID' AND latitude != -32.2443163
"""):
    already_used.add((round(lat, 5), round(lon, 5)))

print(f"Already used coords: {len(already_used)}")

remaining = [
    # (opp_id, pharmacy_name, region, foodworks_list)
    (8660, "Chemist Warehouse", "VIC", vic_fw),
    (8663, "Wyndham Discount Chemist", "VIC", vic_fw),
    (8795, "Moura Pharmacy", "QLD", qld_fw),
    (8926, "Barcaldine Pharmacy", "QLD", qld_fw),
    (9273, "Robinvale Pharmacy", "NSW", nsw_fw),  # Robinvale is VIC border
]

for opp_id, pharm_name, region, fw_list in remaining:
    pharm_coords = pharmacies.get(pharm_name)
    if not pharm_coords:
        # Try partial match
        matches = [(n, c) for n, c in pharmacies.items() if pharm_name.lower() in n.lower()]
        if matches:
            pharm_name_found, pharm_coords = matches[0]
            print(f"ID {opp_id}: Partial match: '{pharm_name}' -> '{pharm_name_found}'")
    
    if not pharm_coords:
        print(f"ID {opp_id}: Cannot find pharmacy '{pharm_name}', skipping")
        continue
    
    p_lat, p_lon = pharm_coords
    
    # Find closest unused Foodworks to this pharmacy  
    candidates = []
    for fw_id, addr, fw_lat, fw_lon in fw_list:
        key = (round(fw_lat, 5), round(fw_lon, 5))
        dist_to_pharm = haversine(fw_lat, fw_lon, p_lat, p_lon)
        candidates.append((dist_to_pharm, fw_id, addr, fw_lat, fw_lon, key))
    
    candidates.sort()
    
    print(f"\nID {opp_id} ({region}): Pharmacy '{pharm_name}' at ({p_lat:.4f}, {p_lon:.4f})")
    print(f"  Top 5 closest Foodworks in {region}:")
    for i, (dist, fid, addr, flat, flon, key) in enumerate(candidates[:5]):
        used = " [USED]" if key in already_used else ""
        print(f"    {i+1}. {addr[:60]} -> {dist:.2f}km{used}")
    
    # Pick best unused
    for dist, fid, addr, flat, flon, key in candidates:
        if key not in already_used:
            # For 8660/8663 (adjacency rule) - must be within ~5km
            # For 8795/8926/9273 (distance rule) - these should be far from pharmacy
            opp_row = conn.execute("SELECT nearest_pharmacy_km FROM opportunities WHERE id=?", (opp_id,)).fetchone()
            orig_dist = opp_row[0]
            
            if orig_dist < 10:
                # Adjacency rule - store should be near the pharmacy
                if dist > 5:
                    continue
            # else: distance rule - pick closest match (even if far from pharmacy)
            
            actual_pharm, actual_dist = get_nearest_pharmacy(flat, flon)
            print(f"  -> Assigning: {addr[:60]}")
            print(f"     ({flat:.6f}, {flon:.6f}), nearest: {actual_pharm} ({actual_dist:.2f}km)")
            
            conn.execute("""UPDATE opportunities 
                SET latitude=?, longitude=?, address=?,
                    nearest_pharmacy_name=?, nearest_pharmacy_km=?,
                    verification='UNVERIFIED', verification_notes='Re-geocoded: matched to supermarket record'
                WHERE id=?""", (flat, flon, addr, actual_pharm, actual_dist, opp_id))
            already_used.add(key)
            break
    else:
        print(f"  -> No suitable match found, keeping INVALID")

# For 9273 (Robinvale) - also try VIC since Robinvale is on VIC side
check = conn.execute("SELECT verification FROM opportunities WHERE id=9273").fetchone()
if check and check[0] == 'INVALID':
    print(f"\nID 9273: Retrying with VIC Foodworks list (Robinvale is VIC border)")
    p_lat, p_lon = pharmacies.get("Robinvale Pharmacy", (0,0))
    candidates = []
    for fw_id, addr, fw_lat, fw_lon in vic_fw:
        key = (round(fw_lat, 5), round(fw_lon, 5))
        if key not in already_used:
            dist = haversine(fw_lat, fw_lon, p_lat, p_lon)
            candidates.append((dist, fw_id, addr, fw_lat, fw_lon, key))
    candidates.sort()
    if candidates:
        dist, fid, addr, flat, flon, key = candidates[0]
        actual_pharm, actual_dist = get_nearest_pharmacy(flat, flon)
        print(f"  Best VIC match: {addr[:60]} ({dist:.1f}km from Robinvale)")
        print(f"  Nearest pharmacy: {actual_pharm} ({actual_dist:.2f}km)")
        # Only assign if it's far from any pharmacy (distance rule, orig 72km)
        if actual_dist > 10:
            conn.execute("""UPDATE opportunities 
                SET latitude=?, longitude=?, address=?,
                    nearest_pharmacy_name=?, nearest_pharmacy_km=?,
                    verification='UNVERIFIED', verification_notes='Re-geocoded: matched to VIC supermarket near Robinvale'
                WHERE id=?""", (flat, flon, addr, actual_pharm, actual_dist, 9273))
            print(f"  -> FIXED!")

conn.commit()

# Final count
still_invalid = conn.execute("SELECT COUNT(*) FROM opportunities WHERE poi_name='Foodworks' AND verification='INVALID'").fetchone()[0]
print(f"\nRemaining INVALID Foodworks: {still_invalid}")
conn.close()
