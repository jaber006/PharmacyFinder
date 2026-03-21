"""
CLI entry point for generating evidence package reports.

Usage:
    py -3.12 -m evidence.report_runner --site-id supermarket_28018
    py -3.12 -m evidence.report_runner --top 10
    py -3.12 -m evidence.report_runner --all
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import List, Optional, Tuple

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from evidence.checklist import generate_checklist
from evidence.risk_report import generate_risk_report
from evidence.pdf_generator import generate_site_report
from evidence.distance_maps import generate_distance_map


DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output", "reports")


def get_db_connection() -> sqlite3.Connection:
    """Get database connection with row factory."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_site(conn: sqlite3.Connection, site_id: str) -> Optional[dict]:
    """Fetch a single site from v2_results."""
    cursor = conn.execute("SELECT * FROM v2_results WHERE id = ?", (site_id,))
    row = cursor.fetchone()
    if row:
        return dict(row)
    return None


def fetch_top_sites(conn: sqlite3.Connection, limit: int = 10) -> List[dict]:
    """Fetch top N sites by commercial score (passing only)."""
    cursor = conn.execute(
        """SELECT * FROM v2_results
           WHERE passed_any = 1
           ORDER BY commercial_score DESC
           LIMIT ?""",
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def fetch_all_passing(conn: sqlite3.Connection) -> List[dict]:
    """Fetch all passing sites."""
    cursor = conn.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 ORDER BY commercial_score DESC"
    )
    return [dict(row) for row in cursor.fetchall()]


def fetch_nearby_pharmacies(
    conn: sqlite3.Connection,
    lat: float,
    lon: float,
    radius_km: float = 5.0,
) -> List[dict]:
    """Fetch pharmacies near a location from the pharmacies table."""
    # Simple bounding box filter then haversine
    deg_margin = radius_km / 111.0  # ~111 km per degree
    cursor = conn.execute(
        """SELECT id, name, address, latitude, longitude
           FROM pharmacies
           WHERE latitude BETWEEN ? AND ?
             AND longitude BETWEEN ? AND ?""",
        (lat - deg_margin, lat + deg_margin, lon - deg_margin, lon + deg_margin),
    )
    return [dict(row) for row in cursor.fetchall()]


def generate_report_for_site(conn: sqlite3.Connection, site: dict) -> str:
    """
    Generate a full evidence package for a single site.

    Returns the path to the generated PDF.
    """
    site_id = site["id"]
    name = site.get("name", site_id)
    lat = site["latitude"]
    lon = site["longitude"]

    print(f"  Generating report for: {name} ({site_id})")

    # Parse rules
    rules_json = site.get("rules_json", "[]")
    if isinstance(rules_json, str):
        passing_rules = json.loads(rules_json)
    else:
        passing_rules = rules_json

    # 1. Generate evidence checklist
    print(f"    - Building checklist...")
    checklist = generate_checklist(site)

    # 2. Generate risk report
    print(f"    - Assessing risks...")
    risk_report = generate_risk_report(site)

    # 3. Generate distance maps for each passing rule
    print(f"    - Creating distance maps...")
    pharmacies = fetch_nearby_pharmacies(conn, lat, lon, radius_km=5.0)
    map_paths = {}

    for rule in passing_rules:
        item = rule.get("item", "")
        safe_id = site_id.replace("/", "_").replace("\\", "_")
        map_output = os.path.join(
            PROJECT_ROOT, "output", "maps",
            f"{safe_id}_{item.replace(' ', '_')}.html"
        )
        try:
            map_path = generate_distance_map(
                candidate_lat=lat,
                candidate_lon=lon,
                pharmacies=pharmacies,
                radius_km=5.0,
                rule_item=item,
                candidate_name=name,
                output_path=map_output,
            )
            map_paths[item] = map_path
            print(f"    - Map: {os.path.basename(map_path)}")
        except Exception as e:
            print(f"    - Map error for {item}: {e}")

    # 4. Generate PDF
    print(f"    - Generating PDF...")
    candidate = {
        "id": site_id,
        "name": name,
        "address": site.get("address", ""),
        "latitude": lat,
        "longitude": lon,
        "state": site.get("state", ""),
    }

    context = {
        "checklist": checklist,
        "risk_report": risk_report,
        "map_paths": map_paths,
    }

    try:
        pdf_bytes = generate_site_report(candidate, site, context)
    except Exception as e:
        print(f"    [ERR] PDF generation error: {e}")
        raise

    # Save PDF
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in name)[:60]
    safe_id = site_id.replace("/", "_").replace("\\", "_")
    pdf_filename = f"{safe_id}_{safe_name}.pdf"
    pdf_path = os.path.join(OUTPUT_DIR, pdf_filename)

    with open(pdf_path, "wb") as f:
        f.write(pdf_bytes)

    print(f"    [OK] Saved: {pdf_path}")

    # Also save JSON evidence package
    json_path = pdf_path.replace(".pdf", "_evidence.json")
    evidence_package = {
        "site": site,
        "checklist": checklist,
        "risk_report": risk_report,
        "map_paths": map_paths,
        "generated_at": datetime.now().isoformat(),
    }
    with open(json_path, "w") as f:
        json.dump(evidence_package, f, indent=2, default=str)

    return pdf_path


def main():
    parser = argparse.ArgumentParser(
        description="Generate evidence package reports for pharmacy sites",
        prog="python -m evidence.report_runner",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--site-id", help="Generate report for a specific site ID")
    group.add_argument("--top", type=int, help="Generate reports for top N sites by commercial score")
    group.add_argument("--all", action="store_true", help="Generate reports for all passing sites")

    args = parser.parse_args()

    conn = get_db_connection()

    try:
        if args.site_id:
            site = fetch_site(conn, args.site_id)
            if not site:
                print(f"Error: Site '{args.site_id}' not found in v2_results")
                sys.exit(1)
            if not site.get("passed_any"):
                print(f"Warning: Site '{args.site_id}' did not pass any rules")
            path = generate_report_for_site(conn, site)
            print(f"\nReport generated: {path}")

        elif args.top:
            sites = fetch_top_sites(conn, args.top)
            if not sites:
                print("No passing sites found in v2_results")
                sys.exit(1)
            print(f"Generating reports for top {len(sites)} sites...\n")
            paths = []
            for i, site in enumerate(sites, 1):
                print(f"[{i}/{len(sites)}]")
                try:
                    path = generate_report_for_site(conn, site)
                    paths.append(path)
                except Exception as e:
                    print(f"  [ERR] Failed: {e}")
            print(f"\n{'='*60}")
            print(f"Generated {len(paths)}/{len(sites)} reports in {OUTPUT_DIR}")

        elif args.all:
            sites = fetch_all_passing(conn)
            if not sites:
                print("No passing sites found in v2_results")
                sys.exit(1)
            print(f"Generating reports for all {len(sites)} passing sites...\n")
            paths = []
            for i, site in enumerate(sites, 1):
                print(f"[{i}/{len(sites)}]")
                try:
                    path = generate_report_for_site(conn, site)
                    paths.append(path)
                except Exception as e:
                    print(f"  [ERR] Failed: {e}")
            print(f"\n{'='*60}")
            print(f"Generated {len(paths)}/{len(sites)} reports in {OUTPUT_DIR}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
