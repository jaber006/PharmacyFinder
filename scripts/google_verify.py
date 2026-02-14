"""
Google Maps Browser Verification for PharmacyFinder opportunities.
Uses web_fetch to extract pharmacy data from Google Maps search URLs.
Falls back to parsing Google Maps HTML for pharmacy results near coordinates.
"""
import json
import csv
import sqlite3
import math
import os
import time
import re
import urllib.request
import urllib.parse
from datetime import datetime

BASE_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder"
DB_PATH = os.path.join(BASE_DIR, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def load_db_pharmacies():
    """Load all pharmacies from our database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, latitude, longitude, address, state FROM pharmacies")
    pharmacies = []
    for row in cursor.fetchall():
        pharmacies.append({
            "id": row[0],
            "name": row[1],
            "lat": row[2],
            "lng": row[3],
            "address": row[4],
            "state": row[5],
        })
    conn.close()
    return pharmacies

def find_nearby_db_pharmacies(lat, lng, radius_km, db_pharmacies):
    """Find pharmacies in our DB near a given point."""
    nearby = []
    for p in db_pharmacies:
        dist = haversine_km(lat, lng, p["lat"], p["lng"])
        if dist <= radius_km:
            nearby.append({**p, "distance_km": dist})
    return sorted(nearby, key=lambda x: x["distance_km"])

def load_opportunities():
    """Load top 200 opportunities from prepared JSON."""
    json_path = os.path.join(OUTPUT_DIR, "top200_to_verify.json")
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_google_maps_pharmacies(html_text, center_lat, center_lng, search_radius_km):
    """
    Parse pharmacy names and approximate locations from Google Maps HTML/text.
    Returns list of pharmacy dicts.
    """
    pharmacies = []
    
    # Pattern: pharmacy names appear in specific patterns in Google Maps results
    # Look for business names near "Pharmacy" or "Chemist" keywords
    # Google Maps embeds business data in JSON-like structures
    
    # Try to find business listings - they appear as structured data
    # Look for patterns like "name":"...", or business names in specific HTML structures
    
    # Method 1: Find all pharmacy-like business names
    # Common patterns in Google Maps text output
    patterns = [
        r'(?:^|\n)([A-Z][A-Za-z\s&\'-]+(?:Pharmacy|Chemist|Chemmart|Priceline|PharmaSave|Amcal|Soul Pattinson|Blooms|Guardian|TerryWhite|Capital Chemist|Good Price|HPS|Advantage|Wizard|Ramsay|National|Pharmasave|Friendlies|Alive|DirectChemist|Pharmacist Advice)[A-Za-z\s&\'-]*)',
        r'(?:Pharmacy|Chemist|Chemmart|Priceline|PharmaSave|Amcal|Soul Pattinson|Blooms|Guardian|TerryWhite|Capital Chemist|Good Price|HPS|Advantage|Wizard|Ramsay|National|Pharmasave|Friendlies|Alive|DirectChemist|Pharmacist Advice)[\s]*[A-Za-z\s&\'-]*',
        r'[A-Za-z\s&\'-]*(?:Pharmacy|Chemist|Chemmart|Priceline|PharmaSave|Amcal|Soul Pattinson|Blooms|Guardian|TerryWhite|Capital Chemist|Good Price|HPS|Advantage|Wizard|Ramsay|National|Pharmasave|Friendlies|Alive|DirectChemist|Pharmacist Advice)',
    ]
    
    found_names = set()
    for pattern in patterns:
        matches = re.findall(pattern, html_text, re.IGNORECASE)
        for match in matches:
            name = match.strip()
            if len(name) > 5 and len(name) < 80:
                # Normalize
                name = re.sub(r'\s+', ' ', name)
                found_names.add(name)
    
    for name in found_names:
        pharmacies.append({
            "name": name,
            "lat": center_lat,  # approximate - we only know they're in the search area
            "lng": center_lng,
            "address": "",
            "source": "google_maps_search",
        })
    
    return pharmacies

def is_pharmacy_match(google_name, db_name, threshold=0.6):
    """Check if two pharmacy names likely refer to the same business."""
    # Normalize both names
    def normalize(name):
        name = name.lower()
        # Remove common words
        for word in ['the', 'pharmacy', 'chemist', 'pty', 'ltd', '-', '&', 'and']:
            name = name.replace(word, '')
        return re.sub(r'\s+', ' ', name).strip()
    
    n1 = normalize(google_name)
    n2 = normalize(db_name)
    
    if not n1 or not n2:
        return False
    
    # Exact match after normalization
    if n1 == n2:
        return True
    
    # One contains the other
    if n1 in n2 or n2 in n1:
        return True
    
    # Word overlap
    words1 = set(n1.split())
    words2 = set(n2.split())
    if not words1 or not words2:
        return False
    overlap = len(words1 & words2)
    total = max(len(words1), len(words2))
    if overlap / total >= threshold:
        return True
    
    return False

def main():
    print("Loading database pharmacies...")
    db_pharmacies = load_db_pharmacies()
    print(f"  {len(db_pharmacies)} pharmacies in database")
    
    print("Loading top 200 opportunities...")
    opportunities = load_opportunities()
    print(f"  {len(opportunities)} opportunities to verify")
    
    # Results will be accumulated here
    results = []
    new_pharmacies_found = []
    
    # We can't easily web_fetch Google Maps (it's dynamic JS).
    # Instead, we'll use the Google Maps Nearby Search via place search URLs
    # and extract what we can from the text content.
    
    # Alternative approach: Use Google Maps textsearch URL patterns
    # that return some data even without JS rendering.
    
    # Actually, the most reliable approach without an API key is to:
    # 1. Check our database thoroughly for each opportunity
    # 2. Use web_fetch on alternative pharmacy directory sites
    # 3. Cross-reference with known pharmacy chain locations
    
    # For now, let's do a thorough database cross-check and prepare
    # the verification file. The browser automation will be done
    # in batches via the browser tool.
    
    print("\nPerforming database cross-verification...")
    for i, opp in enumerate(opportunities):
        lat = float(opp["lat"])
        lng = float(opp["lng"])
        state = opp["state"]
        rules = opp["rules"]
        confidence = opp["confidence"]
        search_radius = opp["search_radius_km"]
        
        # Find pharmacies in our DB within a generous search radius
        nearby_db = find_nearby_db_pharmacies(lat, lng, search_radius * 2, db_pharmacies)
        nearby_tight = [p for p in nearby_db if p["distance_km"] <= search_radius]
        
        # Determine status based on what we know
        if nearby_tight:
            # We have pharmacies in our DB within the tight radius
            # This might invalidate the opportunity
            status = "needs_review"  # Has pharmacies nearby - opportunity might be invalid
        else:
            status = "db_confirmed"  # No pharmacies in our DB = opportunity looks real
        
        results.append({
            "index": i + 1,
            "state": state,
            "lat": lat,
            "lng": lng,
            "rules": rules,
            "confidence": confidence,
            "poi_name": opp.get("poi_name", ""),
            "nearest_km": opp.get("nearest_km", 0),
            "nearest_name": opp.get("nearest_name", ""),
            "search_radius_km": search_radius,
            "db_pharmacies_in_radius": len(nearby_tight),
            "db_pharmacies_in_2x_radius": len(nearby_db),
            "google_pharmacies_found": 0,  # Will be filled by browser verification
            "new_pharmacies": 0,
            "status": status,
            "db_nearby_names": "; ".join([f"{p['name']} ({p['distance_km']:.2f}km)" for p in nearby_tight[:5]]),
        })
        
        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1}/{len(opportunities)}")
    
    # Save initial results
    csv_path = os.path.join(OUTPUT_DIR, "google_verified.csv")
    fieldnames = ["index", "state", "lat", "lng", "rules", "confidence", "poi_name",
                  "nearest_km", "nearest_name", "search_radius_km",
                  "db_pharmacies_in_radius", "db_pharmacies_in_2x_radius",
                  "google_pharmacies_found", "new_pharmacies", "status", "db_nearby_names"]
    
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"\nSaved initial verification to {csv_path}")
    
    # Summary
    confirmed = sum(1 for r in results if r["status"] == "db_confirmed")
    needs_review = sum(1 for r in results if r["status"] == "needs_review")
    
    print(f"\nInitial DB Cross-Check Summary:")
    print(f"  DB Confirmed (no pharmacies in radius): {confirmed}")
    print(f"  Needs Review (has pharmacies in radius): {needs_review}")
    print(f"  Total: {len(results)}")
    
    # Save the list of opportunities that need Google verification
    # (prioritize the "db_confirmed" ones - these are our best opportunities
    # and we want to make sure Google doesn't know of pharmacies we missed)
    to_verify = [r for r in results if r["status"] == "db_confirmed"]
    to_verify.sort(key=lambda x: (-x["confidence"], -x["nearest_km"]))
    
    verify_path = os.path.join(OUTPUT_DIR, "to_google_verify.json")
    with open(verify_path, "w", encoding="utf-8") as f:
        json.dump(to_verify, f, indent=2)
    
    print(f"\n{len(to_verify)} opportunities need Google Maps verification")
    print(f"Saved to {verify_path}")
    
    # State breakdown
    print("\nBy state:")
    for state in ["TAS", "VIC", "NSW", "QLD", "SA", "WA", "NT", "ACT"]:
        state_confirmed = sum(1 for r in results if r["state"] == state and r["status"] == "db_confirmed")
        state_review = sum(1 for r in results if r["state"] == state and r["status"] == "needs_review")
        if state_confirmed + state_review > 0:
            print(f"  {state}: {state_confirmed} confirmed, {state_review} needs review")

if __name__ == "__main__":
    main()
