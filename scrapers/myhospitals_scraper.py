#!/usr/bin/env python3
"""
MyHospitals Scraper — Enrich hospitals table using AIHW MyHospitals API.

API: https://myhospitalsapi.aihw.gov.au/api/v1/reporting-units
Docs: https://www.aihw.gov.au/hospitals/other-resources/myhospitals-api

Updates: hospital_type (public/private), bed_count_verified, coordinates cross-check
"""

import argparse
import json
import logging
import math
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests

DB_PATH = Path(__file__).resolve().parent.parent / "pharmacy_finder.db"
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "myhospitals"
API_BASE = "https://myhospitalsapi.aihw.gov.au"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.aihw.gov.au/",
}

log = logging.getLogger("myhospitals_scraper")


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def normalise(name):
    import re
    name = name.lower().strip()
    for term in ["hospital", "the ", "st ", "st. ", "saint "]:
        name = name.replace(term, "")
    return re.sub(r"\s+", " ", name).strip()


def name_similarity(a, b):
    wa = set(normalise(a).split())
    wb = set(normalise(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def ensure_columns(conn):
    cursor = conn.cursor()
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(hospitals)").fetchall()}
    for col, sql in [
        ("bed_count_verified", "ALTER TABLE hospitals ADD COLUMN bed_count_verified INTEGER DEFAULT 0"),
        ("aihw_code", "ALTER TABLE hospitals ADD COLUMN aihw_code TEXT DEFAULT ''"),
        ("aihw_matched", "ALTER TABLE hospitals ADD COLUMN aihw_matched INTEGER DEFAULT 0"),
    ]:
        if col not in existing:
            cursor.execute(sql)
            log.info(f"Added column {col}")
    conn.commit()


def fetch_aihw_hospitals(session):
    """Fetch all hospitals from AIHW API."""
    cache_file = CACHE_DIR / "aihw_hospitals.json"
    
    # Use cache if less than 24h old
    if cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_h < 24:
            log.info(f"Using cached AIHW data ({age_h:.1f}h old)")
            with open(cache_file) as f:
                return json.load(f)
    
    log.info("Fetching hospitals from AIHW API...")
    r = session.get(f"{API_BASE}/api/v1/reporting-units", headers=HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    hospitals = data.get("result", data) if isinstance(data, dict) else data
    
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(cache_file, "w") as f:
        json.dump(hospitals, f, indent=2)
    
    log.info(f"Fetched {len(hospitals)} hospitals from AIHW")
    return hospitals


def match_hospital(our_hosp, aihw_list):
    """Find best AIHW match for one of our hospitals."""
    our_name = our_hosp["name"]
    our_lat = our_hosp["latitude"]
    our_lon = our_hosp["longitude"]
    
    best_match = None
    best_score = 0
    
    for aihw in aihw_list:
        aihw_lat = aihw.get("latitude")
        aihw_lon = aihw.get("longitude")
        if aihw_lat is None or aihw_lon is None:
            continue
        
        dist = haversine_km(our_lat, our_lon, aihw_lat, aihw_lon)
        if dist > 5:  # Skip if more than 5km away
            continue
        
        # Check name similarity against main name + alternatives
        aihw_name = aihw.get("reporting_unit_name", aihw.get("name", ""))
        alt_names = aihw.get("alternative_names", [])
        all_names = [aihw_name] + (alt_names if isinstance(alt_names, list) else [])
        
        best_name_sim = max(name_similarity(our_name, n) for n in all_names if n)
        
        # Score = name similarity + proximity bonus
        proximity_bonus = max(0, 1 - dist/5) * 0.3  # 0-0.3 bonus for proximity
        score = best_name_sim * 0.7 + proximity_bonus
        
        if score > best_score and score > 0.2:
            best_score = score
            best_match = {
                "aihw": aihw,
                "score": score,
                "distance_km": dist,
                "name_sim": best_name_sim,
                "aihw_name": aihw_name,
            }
    
    return best_match


def run(state=None, limit=None, dry_run=False, verbose=False, **kwargs):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Fetch AIHW data
    try:
        aihw_hospitals = fetch_aihw_hospitals(session)
    except Exception as e:
        log.error(f"Failed to fetch AIHW data: {e}")
        return {"error": str(e)}
    
    # Get our hospitals
    sql = "SELECT id, name, address, latitude, longitude, bed_count, hospital_type FROM hospitals WHERE 1=1"
    params = []
    if state:
        sql += " AND address LIKE ?"
        params.append(f"%{state.upper()}%")
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    our_hospitals = [dict(row) for row in conn.execute(sql, params).fetchall()]
    log.info(f"Processing {len(our_hospitals)} hospitals")
    
    stats = {"updated": 0, "skipped": 0, "failed": 0, "matched": 0}
    
    for hosp in our_hospitals:
        try:
            match = match_hospital(hosp, aihw_hospitals)
            
            if not match:
                log.info(f"[{hosp['id']}] {hosp['name']}: no AIHW match")
                stats["skipped"] += 1
                continue
            
            aihw = match["aihw"]
            is_private = aihw.get("private", None)
            is_closed = aihw.get("closed", False)
            aihw_code = aihw.get("reporting_unit_code", "")
            aihw_name = match["aihw_name"]
            
            new_type = "private" if is_private else "public" if is_private is not None else hosp["hospital_type"]
            
            log.info(f"[{hosp['id']}] {hosp['name']} -> {aihw_name} "
                     f"(score={match['score']:.2f}, dist={match['distance_km']:.1f}km, "
                     f"type={new_type}, closed={is_closed})")
            
            if not dry_run:
                conn.execute("""
                    UPDATE hospitals 
                    SET hospital_type = ?, aihw_code = ?, aihw_matched = 1, bed_count_verified = 0
                    WHERE id = ?
                """, (new_type, aihw_code, hosp["id"]))
                conn.commit()
            
            stats["updated"] += 1
            stats["matched"] += 1
            
        except Exception as e:
            log.error(f"[{hosp['id']}] {hosp['name']}: error - {e}")
            stats["failed"] += 1
    
    conn.close()
    
    log.info("=" * 50)
    log.info("MYHOSPITALS SCRAPER — SUMMARY")
    log.info(f"  Total processed: {len(our_hospitals)}")
    log.info(f"  Matched:         {stats['matched']}")
    log.info(f"  Updated:         {stats['updated']}")
    log.info(f"  Skipped:         {stats['skipped']}")
    log.info(f"  Failed:          {stats['failed']}")
    log.info("=" * 50)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Enrich hospitals via AIHW MyHospitals API")
    parser.add_argument("--state", help="Filter by state (e.g. TAS)")
    parser.add_argument("--limit", type=int, help="Max records to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    
    run(state=args.state, limit=args.limit, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
