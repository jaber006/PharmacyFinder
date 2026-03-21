"""
matcher.py - Score and rank pharmacy site + commercial property combinations.

Scoring:
  - Distance: closer = better (max 400 pts at 0m, 0 at 1500m)
  - Rent: cheaper per sqm = better (max 200 pts)  
  - Size: 80-200 sqm ideal for pharmacy (max 200 pts)
  - Lease type: medical/retail preferred (max 100 pts)
  - Site quality: based on commercial_score from v2 (max 100 pts)

Total max score: 1000
"""

import sqlite3
import csv
import json
from pathlib import Path
from datetime import datetime
from typing import Optional


DB_PATH = Path(__file__).parent.parent / "pharmacy_finder.db"
OUTPUT_DIR = Path(__file__).parent.parent / "output"

# Scoring constants
IDEAL_MIN_AREA = 80    # sqm
IDEAL_MAX_AREA = 200   # sqm
MAX_DISTANCE_M = 1500  # beyond this, no score


def _score_distance(distance_m: Optional[float]) -> float:
    """Score distance: 0m = 400, 1500m = 0. Linear decay."""
    if distance_m is None:
        return 200  # Unknown distance gets middle score
    if distance_m <= 0:
        return 400
    if distance_m >= MAX_DISTANCE_M:
        return 0
    return 400 * (1 - distance_m / MAX_DISTANCE_M)


def _score_rent(rent_per_sqm: Optional[float]) -> float:
    """Score rent: cheaper is better. 
    $0-200/sqm = 200pts, $200-500 = linear decay, >$500 = 50pts.
    """
    if rent_per_sqm is None:
        return 100  # Unknown rent gets middle score
    if rent_per_sqm <= 200:
        return 200
    if rent_per_sqm <= 500:
        return 200 - (150 * (rent_per_sqm - 200) / 300)
    return 50


def _score_size(floor_area_sqm: Optional[float]) -> float:
    """Score size: 80-200sqm ideal for pharmacy.
    In range = 200pts, outside = decay.
    """
    if floor_area_sqm is None:
        return 100  # Unknown size gets middle score
    if IDEAL_MIN_AREA <= floor_area_sqm <= IDEAL_MAX_AREA:
        return 200
    if floor_area_sqm < IDEAL_MIN_AREA:
        # Too small - linear decay
        if floor_area_sqm < 30:
            return 20
        return 200 * (floor_area_sqm - 30) / (IDEAL_MIN_AREA - 30)
    # Too big - gentler decay
    if floor_area_sqm > 500:
        return 50
    return 200 - (150 * (floor_area_sqm - IDEAL_MAX_AREA) / 300)


def _score_lease_type(lease_type: Optional[str]) -> float:
    """Score lease type: medical > retail > office."""
    if not lease_type:
        return 50
    lt = lease_type.lower()
    if "medical" in lt or "health" in lt:
        return 100
    if "retail" in lt or "shop" in lt:
        return 80
    if "office" in lt:
        return 40
    return 50


def _score_site_quality(commercial_score: Optional[float]) -> float:
    """Score based on the site's commercial_score from v2_results.
    Maps 0-1 to 0-100 points.
    """
    if commercial_score is None:
        return 50
    return min(100, max(0, commercial_score * 100))


def score_match(match: dict, site: dict) -> dict:
    """Calculate composite score for a site + property match.
    
    Args:
        match: Row from commercial_matches table
        site: Row from v2_results table
        
    Returns:
        match dict with added score fields
    """
    scores = {
        "distance_score": _score_distance(match.get("distance_to_site_m")),
        "rent_score": _score_rent(match.get("rent_per_sqm")),
        "size_score": _score_size(match.get("floor_area_sqm")),
        "type_score": _score_lease_type(match.get("lease_type")),
        "site_score": _score_site_quality(site.get("commercial_score")),
    }
    scores["total_score"] = sum(scores.values())

    result = {**match, **scores}
    result["site_name"] = site.get("name", "")
    result["site_address"] = site.get("address", "")
    result["site_state"] = site.get("state", "")
    result["site_commercial_score"] = site.get("commercial_score")
    result["site_primary_rule"] = site.get("primary_rule", "")
    return result


def match_sites_to_properties(db_path: str = None, state: str = None, top_n: int = None) -> list[dict]:
    """Main entry point: find and score all site + property combinations.
    
    Args:
        db_path: Path to pharmacy_finder.db
        state: Filter by state
        top_n: Only process top N sites
        
    Returns:
        Ranked list of scored matches (highest first)
    """
    db = db_path or str(DB_PATH)
    conn = sqlite3.connect(db, timeout=30)
    conn.row_factory = sqlite3.Row

    # Get qualifying sites
    site_query = "SELECT * FROM v2_results WHERE passed_any = 1"
    params = []
    if state:
        site_query += " AND UPPER(state) = UPPER(?)"
        params.append(state)
    site_query += " ORDER BY commercial_score DESC"
    if top_n:
        site_query += " LIMIT ?"
        params.append(top_n)

    sites = {row["id"]: dict(row) for row in conn.execute(site_query, params).fetchall()}

    if not sites:
        print("No qualifying sites found")
        conn.close()
        return []

    # Get all active matches for these sites
    site_ids = list(sites.keys())
    placeholders = ",".join("?" * len(site_ids))
    match_query = f"""
        SELECT * FROM commercial_matches 
        WHERE site_id IN ({placeholders}) 
        AND status = 'active'
    """
    
    try:
        matches = conn.execute(match_query, site_ids).fetchall()
    except sqlite3.OperationalError:
        print("No commercial_matches table found. Run the property scan first.")
        conn.close()
        return []

    conn.close()

    if not matches:
        print(f"No commercial matches found for {len(sites)} sites")
        return []

    # Score each match
    scored = []
    for match in matches:
        match_dict = dict(match)
        site = sites.get(match_dict["site_id"])
        if site:
            scored_match = score_match(match_dict, site)
            scored.append(scored_match)

    # Sort by total score descending
    scored.sort(key=lambda x: x.get("total_score", 0), reverse=True)

    print(f"\nRanked {len(scored)} site+property combinations")
    return scored


def print_ranked_results(results: list[dict], limit: int = 50):
    """Print ranked results to console in a readable format."""
    if not results:
        print("No results to display.")
        return

    print(f"\n{'='*80}")
    print(f"TOP PHARMACY SITE + COMMERCIAL LEASE MATCHES")
    print(f"{'='*80}")
    print(f"{'Rank':<5} {'Score':<7} {'Site':<30} {'Property Address':<35} {'Rent':<15} {'Area':<10} {'Dist':<8}")
    print(f"{'-'*5} {'-'*7} {'-'*30} {'-'*35} {'-'*15} {'-'*10} {'-'*8}")

    for i, r in enumerate(results[:limit], 1):
        site_name = (r.get("site_name") or "")[:28]
        address = (r.get("address") or "")[:33]
        
        rent = ""
        if r.get("rent_per_sqm"):
            rent = f"${r['rent_per_sqm']:.0f}/sqm"
        elif r.get("rent_annual"):
            rent = f"${r['rent_annual']:,.0f}pa"
        
        area = f"{r['floor_area_sqm']:.0f}sqm" if r.get("floor_area_sqm") else "?"
        
        dist = ""
        if r.get("distance_to_site_m") is not None:
            dist = f"{r['distance_to_site_m']:.0f}m"
        
        score = r.get("total_score", 0)
        
        print(f"{i:<5} {score:<7.0f} {site_name:<30} {address:<35} {rent:<15} {area:<10} {dist:<8}")

    print(f"\n{'='*80}")

    # Score breakdown for top 5
    print(f"\nScore Breakdown (Top 5):")
    print(f"{'Site':<25} {'Dist':<8} {'Rent':<8} {'Size':<8} {'Type':<8} {'Site Q':<8} {'TOTAL':<8}")
    print(f"{'-'*25} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for r in results[:5]:
        name = (r.get("site_name") or "")[:23]
        print(f"{name:<25} "
              f"{r.get('distance_score',0):<8.0f} "
              f"{r.get('rent_score',0):<8.0f} "
              f"{r.get('size_score',0):<8.0f} "
              f"{r.get('type_score',0):<8.0f} "
              f"{r.get('site_score',0):<8.0f} "
              f"{r.get('total_score',0):<8.0f}")


def export_csv(results: list[dict], output_path: str = None) -> str:
    """Export ranked results to CSV.
    
    Returns the path to the CSV file.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = output_path or str(OUTPUT_DIR / "commercial_matches.csv")

    fields = [
        "rank", "total_score",
        "site_id", "site_name", "site_address", "site_state",
        "site_commercial_score", "site_primary_rule",
        "listing_url", "address", "latitude", "longitude",
        "rent_annual", "rent_per_sqm", "floor_area_sqm",
        "lease_type", "property_type",
        "distance_to_site_m", "agent_name", "agent_phone",
        "distance_score", "rent_score", "size_score", "type_score", "site_score",
        "discovered_date", "last_checked", "status",
    ]

    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for i, r in enumerate(results, 1):
            r["rank"] = i
            writer.writerow(r)

    print(f"\nExported {len(results)} matches to {out}")
    return out


if __name__ == "__main__":
    results = match_sites_to_properties()
    print_ranked_results(results)
    if results:
        export_csv(results)
