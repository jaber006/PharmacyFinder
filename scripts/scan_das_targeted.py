#!/usr/bin/env python3
"""
Targeted DA Scanner — queries PlanningAlerts around each qualifying site.

The national scan via postcodes only found 2 results because it queried
~10 postcodes per state. This script instead queries PlanningAlerts with
a lat/lon + 2 km radius around every one of our 619 qualifying sites.

Usage:
    py -3.12 scripts/scan_das_targeted.py
    py -3.12 scripts/scan_das_targeted.py --radius 3000       # 3 km radius
    py -3.12 scripts/scan_das_targeted.py --state TAS          # single state
    py -3.12 scripts/scan_das_targeted.py --dry-run            # show plan only
    py -3.12 scripts/scan_das_targeted.py --cached             # skip API, re-filter cache

Requires PlanningAlerts API key:
    - env var PLANNING_ALERTS_KEY, or
    - file ~/.config/planningalerts/api_key
"""

import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "data" / "sources" / "_cache" / "planningalerts"

API_BASE = "https://api.planningalerts.org.au/applications.json"
DEFAULT_RADIUS_M = 2000
RATE_LIMIT_S = 1.0  # 1 request per second (PlanningAlerts TOS)
MAX_PER_PAGE = 100  # API max

# Keywords that signal commercial / retail / medical DAs
INCLUDE_KEYWORDS = re.compile(
    r"(?i)"
    r"(?:shopping\s*cent(?:re|er))"
    r"|(?:medical\s*cent(?:re|er))"
    r"|(?:health\s*cent(?:re|er))"
    r"|(?:retail)"
    r"|(?:commercial)"
    r"|(?:pharmacy|chemist)"
    r"|(?:mixed\s*use)"
    r"|(?:supermarket)"
    r"|(?:child\s*care|childcare)"
    r"|(?:shop(?:s|front)?)"
    r"|(?:office(?:s)?)"
    r"|(?:food\s*(?:and|&)\s*drink)"
    r"|(?:restaurant|cafe|takeaway)"
    r"|(?:bulky\s*goods)"
    r"|(?:service\s*station)"
    r"|(?:neighbourhood\s*cent(?:re|er))"
    r"|(?:community\s*cent(?:re|er))"
    r"|(?:aged\s*care)"
    r"|(?:hospital)"
    r"|(?:clinic)"
    r"|(?:(?:sub)?division.*(?:lot|commercial|retail))"
    r"|(?:change\s*of\s*use)"
    r"|(?:tenancy)"
)

# Exclude purely residential / trivial DAs
EXCLUDE_KEYWORDS = re.compile(
    r"(?i)"
    r"(?:single\s*(?:storey\s*)?(?:dwelling|residence|house))"
    r"|(?:swimming\s*pool)"
    r"|(?:carport)"
    r"|(?:pergola)"
    r"|(?:fence|fencing)"
    r"|(?:tree\s*removal)"
    r"|(?:shed)"
    r"|(?:garage)"
    r"|(?:granny\s*flat)"
    r"|(?:verandah)"
    r"|(?:patio)"
    r"|(?:deck\b)"
    r"|(?:retaining\s*wall)"
    r"|(?:awning)"
    r"|(?:demolition\s*(?:of\s*)?(?:existing\s*)?(?:dwelling|residence|house))"
)


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    key = os.environ.get("PLANNING_ALERTS_KEY", "").strip()
    if key:
        return key
    key_file = Path.home() / ".config" / "planningalerts" / "api_key"
    if key_file.exists():
        key = key_file.read_text().strip()
        if key:
            return key
    print("ERROR: No PlanningAlerts API key found.")
    print("  Set PLANNING_ALERTS_KEY env var, or create ~/.config/planningalerts/api_key")
    print("  Free key: https://www.planningalerts.org.au/api/howto")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def cache_key(lat: float, lon: float, radius: int) -> str:
    """Deterministic cache key for a query."""
    raw = f"{lat:.6f},{lon:.6f},{radius}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_cached(lat: float, lon: float, radius: int) -> Optional[list]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{cache_key(lat, lon, radius)}.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def save_cached(lat: float, lon: float, radius: int, apps: list):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{cache_key(lat, lon, radius)}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(apps, f)


# ---------------------------------------------------------------------------
# PlanningAlerts API
# ---------------------------------------------------------------------------

def query_planningalerts(api_key: str, lat: float, lon: float,
                         radius: int = DEFAULT_RADIUS_M) -> list:
    """Query PlanningAlerts for DAs near a point. Returns raw application list."""
    all_apps = []
    page = 1

    while True:
        params = {
            "key": api_key,
            "lat": f"{lat:.6f}",
            "lng": f"{lon:.6f}",
            "radius": radius,
            "count": MAX_PER_PAGE,
            "page": page,
        }

        try:
            resp = requests.get(API_BASE, params=params, timeout=30)
            if resp.status_code == 429:
                print("    Rate limited, waiting 5s ...")
                time.sleep(5)
                continue
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"    API error: {exc}")
            break

        data = resp.json()
        # API wraps each app in {"application": {...}}
        batch = []
        if isinstance(data, list):
            for item in data:
                app = item.get("application", item) if isinstance(item, dict) else item
                batch.append(app)
        elif isinstance(data, dict):
            for item in data.get("applications", data.get("application", [])):
                app = item.get("application", item) if isinstance(item, dict) else item
                batch.append(app)

        if not batch:
            break

        all_apps.extend(batch)

        if len(batch) < MAX_PER_PAGE:
            break
        page += 1
        time.sleep(RATE_LIMIT_S)

    return all_apps


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def classify_da(description: str) -> tuple[str, float]:
    """Classify a DA description into development type and pharmacy potential.

    Returns (dev_type, potential_score).
    """
    desc = (description or "").lower()

    if not desc:
        return ("unknown", 0.0)

    # Exclude purely residential / trivial
    if EXCLUDE_KEYWORDS.search(desc) and not INCLUDE_KEYWORDS.search(desc):
        return ("residential", 0.0)

    if not INCLUDE_KEYWORDS.search(desc):
        return ("other", 0.0)

    # Classify
    if re.search(r"(?i)shopping\s*cent(?:re|er)", desc):
        gla = _extract_sqm(desc)
        if gla and gla >= 5000:
            return ("shopping_centre_large", 0.95)
        return ("shopping_centre", 0.85)

    if re.search(r"(?i)medical\s*cent(?:re|er)|health\s*cent(?:re|er)|clinic", desc):
        return ("medical_centre", 0.90)

    if re.search(r"(?i)hospital", desc):
        return ("hospital", 0.80)

    if re.search(r"(?i)pharmacy|chemist", desc):
        return ("pharmacy", 0.70)

    if re.search(r"(?i)supermarket", desc):
        return ("supermarket", 0.75)

    if re.search(r"(?i)mixed\s*use", desc):
        return ("mixed_use", 0.60)

    if re.search(r"(?i)aged\s*care", desc):
        return ("aged_care", 0.50)

    if re.search(r"(?i)change\s*of\s*use", desc):
        return ("change_of_use", 0.55)

    if re.search(r"(?i)retail|shop|tenancy|commercial", desc):
        return ("retail", 0.50)

    return ("commercial", 0.40)


def _extract_sqm(text: str) -> Optional[float]:
    m = re.search(r"([\d,]+)\s*(?:sqm|sq\.?\s*m|m2|m²)", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


def is_relevant(app: dict) -> bool:
    """Check if a DA is relevant (commercial/retail/medical)."""
    desc = app.get("description", "")
    dev_type, score = classify_da(desc)
    return score > 0


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def ensure_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS council_da (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            da_number        TEXT,
            council          TEXT,
            state            TEXT,
            address          TEXT,
            lat              REAL,
            lon              REAL,
            description      TEXT,
            applicant        TEXT,
            status           TEXT,
            lodged_date      TEXT,
            determined_date  TEXT,
            development_type TEXT,
            estimated_gla_sqm REAL,
            pharmacy_potential REAL,
            source_url       TEXT,
            date_scraped     TEXT
        )
    """)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_council_da_number "
        "ON council_da(da_number)"
    )
    conn.commit()


def upsert_da(conn: sqlite3.Connection, app: dict,
              site_state: str) -> bool:
    """Insert or skip a DA. Returns True if newly inserted."""
    da_number = (
        app.get("council_reference")
        or app.get("id")
        or ""
    )
    if not da_number:
        return False

    da_number = str(da_number).strip()

    # Skip if already exists
    existing = conn.execute(
        "SELECT 1 FROM council_da WHERE da_number = ?", (da_number,)
    ).fetchone()
    if existing:
        return False

    desc = app.get("description", "")
    dev_type, potential = classify_da(desc)
    gla = _extract_sqm(desc)

    authority = app.get("authority", {})
    council = authority.get("full_name", "") if isinstance(authority, dict) else str(authority)

    lat = app.get("lat")
    lon = app.get("lng") or app.get("lon")

    address = app.get("address", "")
    # Infer state from address or use site state
    state = site_state
    for abbr in ("NSW", "VIC", "QLD", "TAS", "WA", "SA", "NT", "ACT"):
        if abbr in (address or "").upper():
            state = abbr
            break

    lodged = app.get("date_received") or app.get("date_lodged") or ""
    determined = app.get("date_decided") or app.get("date_determined") or ""

    conn.execute(
        "INSERT INTO council_da "
        "(da_number, council, state, address, lat, lon, description, "
        " applicant, status, lodged_date, determined_date, "
        " development_type, estimated_gla_sqm, pharmacy_potential, "
        " source_url, date_scraped) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            da_number,
            council,
            state,
            address,
            lat,
            lon,
            desc,
            None,  # PlanningAlerts doesn't expose applicant
            app.get("status", ""),
            lodged,
            determined,
            dev_type,
            gla,
            potential,
            app.get("info_url", ""),
            datetime.now().isoformat(),
        ),
    )
    return True


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def load_sites(conn: sqlite3.Connection,
               state: Optional[str] = None) -> list[dict]:
    """Load qualifying sites from v2_results."""
    query = "SELECT id, name, address, latitude, longitude, state FROM v2_results"
    params = []
    if state:
        query += " WHERE state = ?"
        params.append(state.upper())
    query += " ORDER BY state, id"

    rows = conn.execute(query, params).fetchall()
    return [
        {"id": r[0], "name": r[1], "address": r[2],
         "lat": r[3], "lon": r[4], "state": r[5]}
        for r in rows
    ]


def deduplicate_queries(sites: list[dict],
                        radius_m: int) -> list[dict]:
    """Merge sites whose query circles overlap to reduce API calls.

    Two sites within `radius_m` of each other share the same circle, so
    we only need to query once for the cluster centroid.
    """
    from geopy.distance import geodesic

    # Round to ~100m grid to find nearby clusters
    used = set()
    queries = []

    for site in sites:
        grid_key = (round(site["lat"], 3), round(site["lon"], 3))
        if grid_key in used:
            continue
        used.add(grid_key)

        # Collect all sites in this grid cell
        cluster_sites = [
            s for s in sites
            if (round(s["lat"], 3), round(s["lon"], 3)) == grid_key
        ]
        # Use centroid
        avg_lat = sum(s["lat"] for s in cluster_sites) / len(cluster_sites)
        avg_lon = sum(s["lon"] for s in cluster_sites) / len(cluster_sites)

        queries.append({
            "lat": avg_lat,
            "lon": avg_lon,
            "state": cluster_sites[0]["state"],
            "site_ids": [s["id"] for s in cluster_sites],
            "label": cluster_sites[0].get("address", cluster_sites[0]["id"]),
        })

    return queries


def run_scan(state: Optional[str] = None, radius: int = DEFAULT_RADIUS_M,
             dry_run: bool = False, cached_only: bool = False):
    api_key = get_api_key()
    conn = sqlite3.connect(str(DB_PATH))
    ensure_table(conn)

    sites = load_sites(conn, state)
    if not sites:
        print(f"No qualifying sites found{f' for {state}' if state else ''}.")
        return

    queries = deduplicate_queries(sites, radius)
    site_count = len(sites)
    query_count = len(queries)
    print(f"Sites: {site_count} qualifying locations"
          f"{f' in {state}' if state else ''}")
    print(f"Queries: {query_count} unique locations (after dedup)")
    print(f"Radius: {radius}m")
    print(f"Rate: 1 req/sec -> ~{query_count} seconds estimated")
    print()

    if dry_run:
        print("DRY RUN — showing first 20 queries:")
        for q in queries[:20]:
            print(f"  ({q['lat']:.4f}, {q['lon']:.4f}) {q['state']} "
                  f"[{len(q['site_ids'])} sites] {q['label'][:60]}")
        return

    total_apps = 0
    total_relevant = 0
    total_inserted = 0
    cache_hits = 0
    api_calls = 0
    errors = 0

    for i, q in enumerate(queries, 1):
        label = q["label"][:50]
        prefix = f"[{i}/{query_count}]"

        # Check cache first
        cached = load_cached(q["lat"], q["lon"], radius)
        if cached is not None:
            apps = cached
            cache_hits += 1
            src = "cache"
        elif cached_only:
            continue
        else:
            apps = query_planningalerts(api_key, q["lat"], q["lon"], radius)
            save_cached(q["lat"], q["lon"], radius, apps)
            api_calls += 1
            src = "API"
            time.sleep(RATE_LIMIT_S)

        total_apps += len(apps)

        relevant = [a for a in apps if is_relevant(a)]
        total_relevant += len(relevant)

        inserted = 0
        for app in relevant:
            try:
                if upsert_da(conn, app, q["state"]):
                    inserted += 1
            except sqlite3.IntegrityError:
                pass
            except Exception as exc:
                errors += 1
                if errors <= 5:
                    print(f"  {prefix} Error inserting DA: {exc}")

        total_inserted += inserted
        conn.commit()

        if len(apps) > 0:
            print(f"  {prefix} {q['state']} {label} "
                  f"-> {len(apps)} DAs, {len(relevant)} relevant, "
                  f"{inserted} new ({src})")
        elif i % 50 == 0:
            print(f"  {prefix} ... {api_calls} API calls, "
                  f"{cache_hits} cache hits so far")

    conn.close()

    # Summary
    print()
    print("=" * 60)
    print("SCAN COMPLETE")
    print("=" * 60)
    print(f"  Queries:          {query_count}")
    print(f"  API calls:        {api_calls}")
    print(f"  Cache hits:       {cache_hits}")
    print(f"  Total DAs found:  {total_apps}")
    print(f"  Relevant DAs:     {total_relevant}")
    print(f"  New DAs inserted: {total_inserted}")
    if errors:
        print(f"  Errors:           {errors}")

    # Show top DAs by potential
    conn2 = sqlite3.connect(str(DB_PATH))
    print()
    print("Top DAs by pharmacy potential:")
    for row in conn2.execute(
        "SELECT da_number, council, address, development_type, "
        "       pharmacy_potential "
        "FROM council_da "
        "WHERE pharmacy_potential >= 0.5 "
        "ORDER BY pharmacy_potential DESC "
        "LIMIT 20"
    ).fetchall():
        print(f"  [{row[3]}] {row[4]:.2f} | {row[0]} | "
              f"{(row[2] or '')[:60]} ({row[1]})")

    total = conn2.execute("SELECT COUNT(*) FROM council_da").fetchone()[0]
    high = conn2.execute(
        "SELECT COUNT(*) FROM council_da WHERE pharmacy_potential >= 0.7"
    ).fetchone()[0]
    print(f"\nTotal in council_da: {total} ({high} with potential >= 0.7)")
    conn2.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Targeted DA scan around qualifying pharmacy sites")
    parser.add_argument("--state", default=None,
                        help="Filter to a single state (NSW, VIC, QLD, etc)")
    parser.add_argument("--radius", type=int, default=DEFAULT_RADIUS_M,
                        help=f"Search radius in metres (default: {DEFAULT_RADIUS_M})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show query plan without calling API")
    parser.add_argument("--cached", action="store_true",
                        help="Only process cached responses, skip API calls")
    args = parser.parse_args()

    run_scan(
        state=args.state,
        radius=args.radius,
        dry_run=args.dry_run,
        cached_only=args.cached,
    )


if __name__ == "__main__":
    main()
