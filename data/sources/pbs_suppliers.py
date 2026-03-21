"""
PBS Approved Suppliers — Authoritative source of Australian pharmacy locations.

Data Source Strategy
====================
The official PBS Approved Suppliers list is maintained by the Department of
Health via pbsapprovedsuppliers.health.gov.au (login required — no public
download). The best publicly accessible proxy is **findapharmacy.com.au**
(Pharmacy Guild of Australia), which lists all PBS-approved community
pharmacies via its Funnelback search API.

This script:
  1. Fetches pharmacy data from findapharmacy.com.au (Funnelback API)
  2. Caches results locally as NDJSON for offline re-runs
  3. Cross-references with the existing `pharmacies` table in pharmacy_finder.db
  4. Reports: missing from our DB, in DB but not in PBS source, coordinate mismatches

Usage:
    py -3.12 data/sources/pbs_suppliers.py                     # full run
    py -3.12 data/sources/pbs_suppliers.py --cached             # re-use cached data
    py -3.12 data/sources/pbs_suppliers.py --state TAS          # single state
    py -3.12 data/sources/pbs_suppliers.py --report-only        # skip fetch, just report
"""

import argparse
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # PharmacyFinder/
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "data" / "sources" / "_cache"
CACHE_FILE = CACHE_DIR / "pbs_guild_pharmacies.ndjson"
REPORT_DIR = PROJECT_ROOT / "data" / "sources" / "_reports"

FUNNELBACK_URL = "https://tpgoa-search.funnelback.squiz.cloud/s/search.html"
FUNNELBACK_PARAMS = {
    "collection": "tpgoa~sp-locations",
    "profile": "react-data",
    "query": "!null",
    "num_ranks": 500,
    "sort": "prox",
    "serviceKeyword": "!null",
}

# Rate limit: be a good citizen
RATE_LIMIT_SECONDS = 1.0

# State query grid — multiple origin points to stay under the 500-result cap.
# Format: (lat, lon, radius_km)
STATE_QUERY_POINTS = {
    "TAS": [
        (-42.0, 146.5, 300),
    ],
    "ACT": [
        (-35.28, 149.13, 50),
    ],
    "NT": [
        (-12.46, 130.84, 200),
        (-23.70, 133.87, 500),
    ],
    "SA": [
        (-34.93, 138.60, 50),
        (-35.10, 138.55, 50),
        (-34.70, 138.70, 60),
        (-34.85, 138.35, 60),
        (-35.50, 138.50, 150),
        (-33.50, 138.00, 300),
        (-32.00, 137.00, 500),
    ],
    "WA": [
        (-31.95, 115.86, 30),
        (-31.75, 115.85, 40),
        (-32.10, 115.85, 40),
        (-31.95, 116.10, 50),
        (-31.95, 115.60, 50),
        (-33.30, 115.65, 100),
        (-28.77, 114.62, 300),
        (-21.00, 119.00, 600),
        (-34.00, 117.50, 200),
        (-31.50, 121.50, 400),
    ],
    "VIC": [
        (-37.81, 144.96, 8),
        (-37.84, 145.00, 8),
        (-37.78, 145.02, 8),
        (-37.80, 144.88, 8),
        (-37.75, 145.15, 12),
        (-37.90, 145.05, 12),
        (-37.88, 144.75, 12),
        (-37.73, 144.75, 15),
        (-37.95, 145.20, 15),
        (-37.68, 145.00, 15),
        (-37.80, 145.35, 20),
        (-37.60, 145.10, 20),
        (-38.05, 145.10, 15),
        (-37.60, 144.70, 20),
        (-37.55, 145.30, 25),
        (-38.00, 145.40, 20),
        (-37.70, 144.55, 25),
        (-37.50, 144.85, 25),
        (-38.15, 144.35, 40),
        (-37.55, 143.85, 50),
        (-36.76, 144.28, 50),
        (-36.36, 145.39, 50),
        (-38.10, 145.50, 80),
        (-37.80, 147.00, 100),
        (-36.00, 146.50, 100),
        (-36.50, 142.00, 200),
        (-38.35, 145.80, 80),
        (-37.30, 145.80, 80),
    ],
    "QLD": [
        (-27.47, 153.03, 10),
        (-27.40, 153.05, 12),
        (-27.55, 153.05, 12),
        (-27.47, 152.95, 12),
        (-27.35, 153.15, 15),
        (-27.55, 153.15, 15),
        (-27.35, 152.95, 15),
        (-27.60, 152.90, 15),
        (-27.25, 153.10, 20),
        (-27.20, 153.00, 20),
        (-27.70, 153.15, 20),
        (-27.65, 153.35, 20),
        (-27.95, 153.35, 25),
        (-28.05, 153.45, 20),
        (-28.20, 153.50, 20),
        (-26.65, 153.07, 30),
        (-26.40, 153.05, 30),
        (-27.60, 152.70, 30),
        (-27.50, 152.40, 50),
        (-26.00, 152.50, 80),
        (-24.85, 152.35, 50),
        (-23.38, 150.50, 60),
        (-21.14, 149.19, 60),
        (-19.26, 146.80, 50),
        (-16.92, 145.77, 50),
        (-20.00, 148.00, 150),
        (-25.00, 147.00, 300),
        (-18.00, 146.00, 100),
    ],
    "NSW": [
        (-33.87, 151.21, 6),
        (-33.84, 151.25, 8),
        (-33.90, 151.18, 8),
        (-33.87, 151.10, 8),
        (-33.80, 151.18, 8),
        (-33.80, 151.30, 12),
        (-33.75, 151.15, 12),
        (-33.93, 151.25, 10),
        (-33.85, 150.98, 12),
        (-33.92, 151.05, 12),
        (-33.70, 151.05, 15),
        (-33.95, 151.35, 15),
        (-34.05, 151.10, 15),
        (-33.92, 150.85, 15),
        (-33.75, 150.80, 15),
        (-33.70, 150.95, 15),
        (-33.65, 151.20, 15),
        (-33.75, 150.65, 20),
        (-33.85, 150.65, 20),
        (-33.55, 150.70, 20),
        (-34.05, 150.85, 20),
        (-33.40, 151.30, 25),
        (-33.30, 151.50, 25),
        (-32.93, 151.78, 20),
        (-32.75, 151.70, 25),
        (-33.05, 151.65, 25),
        (-32.50, 151.50, 40),
        (-34.42, 150.90, 25),
        (-34.75, 150.65, 50),
        (-35.30, 149.50, 50),
        (-35.85, 150.10, 80),
        (-33.28, 149.10, 60),
        (-33.50, 148.00, 80),
        (-32.25, 148.60, 80),
        (-31.00, 150.80, 80),
        (-30.50, 153.00, 60),
        (-29.50, 153.25, 60),
        (-28.80, 153.30, 60),
        (-31.90, 152.50, 60),
        (-31.40, 152.90, 40),
        (-30.00, 151.50, 100),
        (-33.00, 146.00, 200),
        (-31.50, 145.50, 300),
    ],
}

STATE_NAME_MAP = {
    "VICTORIA": "VIC",
    "NEW SOUTH WALES": "NSW",
    "QUEENSLAND": "QLD",
    "SOUTH AUSTRALIA": "SA",
    "WESTERN AUSTRALIA": "WA",
    "TASMANIA": "TAS",
    "NORTHERN TERRITORY": "NT",
    "AUSTRALIAN CAPITAL TERRITORY": "ACT",
}


# ---------------------------------------------------------------------------
# Funnelback API fetcher
# ---------------------------------------------------------------------------


class PBSSupplierFetcher:
    """Fetch pharmacy data from the Pharmacy Guild's Funnelback API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-AU,en;q=0.9",
                "Referer": "https://findapharmacy.com.au/",
                "Origin": "https://findapharmacy.com.au",
            }
        )
        self._seen_ids: Set[str] = set()

    def fetch_state(self, state: str) -> List[Dict]:
        """Fetch all pharmacies for a given state. Returns list of parsed dicts."""
        query_points = STATE_QUERY_POINTS.get(state)
        if not query_points:
            print(f"  [!] No query points for {state}")
            return []

        results: List[Dict] = []
        for i, (lat, lon, radius_km) in enumerate(query_points, 1):
            print(
                f"  [{state}] Query {i}/{len(query_points)}: "
                f"({lat:.2f}, {lon:.2f}) r={radius_km}km ... ",
                end="",
                flush=True,
            )
            raw = self._query_api(lat, lon, radius_km)
            if raw is None:
                print("FAILED")
                continue

            new = 0
            for entry in raw:
                parsed = self._parse_entry(entry, state)
                if parsed and parsed["fap_id"] not in self._seen_ids:
                    self._seen_ids.add(parsed["fap_id"])
                    results.append(parsed)
                    new += 1

            hit_cap = len(raw) >= 500
            print(f"{len(raw)} raw, {new} new" + (" [!] CAP" if hit_cap else ""))
            time.sleep(RATE_LIMIT_SECONDS)

        return results

    def fetch_all(self, states: Optional[List[str]] = None) -> List[Dict]:
        """Fetch pharmacies for all (or specified) states."""
        target_states = states or list(STATE_QUERY_POINTS.keys())
        all_results: List[Dict] = []
        for state in target_states:
            print(f"\n{'='*60}")
            print(f"  Fetching {state}")
            print(f"{'='*60}")
            state_results = self.fetch_state(state)
            all_results.extend(state_results)
            print(f"  {state} total: {len(state_results)} pharmacies")
        return all_results

    def _query_api(self, lat: float, lon: float, radius_km: float) -> Optional[List]:
        """Query Funnelback and return raw result list."""
        params = dict(FUNNELBACK_PARAMS)
        params["origin"] = f"{lat},{lon}"
        params["maxdist"] = str(radius_km)

        try:
            resp = self.session.get(FUNNELBACK_URL, params=params, timeout=60)
            if resp.status_code == 403:
                # Cloudflare or WAF block
                print("[Cloudflare blocked] ", end="")
                return None
            resp.raise_for_status()
            data = resp.json()
            # Funnelback can return results at different paths
            results = data.get("results", [])
            if not results:
                rp = data.get("response", {}).get("resultPacket", {})
                results = rp.get("results", [])
            return results
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"[Error: {e}] ", end="")
            return None

    def _parse_entry(self, entry: Dict, fallback_state: str) -> Optional[Dict]:
        """Parse a Funnelback result into a normalised pharmacy dict."""
        try:
            # Handle both top-level and nested metadata formats
            meta = entry.get("listMetadata", entry)
            name = (
                entry.get("title", "")
                or entry.get("name", "")
                or meta.get("pharmacyName", [""])[0]
            ).strip()
            if not name:
                return None

            fap_id = str(
                entry.get("id", "")
                or meta.get("pharmacyId", [""])[0]
                or name
            )

            # Coordinates
            geometry = entry.get("geometry", {})
            coords = geometry.get("coordinates", [])
            lat = lon = None
            if len(coords) >= 2:
                lon, lat = float(coords[0]), float(coords[1])
            else:
                lat_list = meta.get("pharmacyLatitude", meta.get("latitude", []))
                lon_list = meta.get("pharmacyLongitude", meta.get("longitude", []))
                if lat_list and lon_list:
                    lat = float(lat_list[0] if isinstance(lat_list, list) else lat_list)
                    lon = float(lon_list[0] if isinstance(lon_list, list) else lon_list)

            if lat is None or lon is None:
                return None

            # Sanity check AU bounds
            if not (-45 <= lat <= -10 and 110 <= lon <= 155):
                return None

            # Address fields
            address_parts = []
            for f in ("address", "address2", "address3"):
                val = entry.get(f) or (meta.get(f, [""])[0] if isinstance(meta.get(f), list) else meta.get(f, ""))
                if val and str(val).strip():
                    address_parts.append(str(val).strip())

            city = (
                entry.get("city", "")
                or (meta.get("pharmacySuburb", [""])[0] if isinstance(meta.get("pharmacySuburb"), list) else meta.get("pharmacySuburb", ""))
            ).strip()
            raw_state = (
                entry.get("state", "")
                or (meta.get("pharmacyState", [""])[0] if isinstance(meta.get("pharmacyState"), list) else meta.get("pharmacyState", ""))
            ).strip().upper()
            postcode = (
                entry.get("postcode", "")
                or (meta.get("pharmacyPostCode", [""])[0] if isinstance(meta.get("pharmacyPostCode"), list) else meta.get("pharmacyPostCode", ""))
            ).strip()

            state = STATE_NAME_MAP.get(raw_state, raw_state) or fallback_state

            if city:
                address_parts.append(city.upper())
            if state:
                address_parts.append(state)
            if postcode:
                address_parts.append(postcode)

            address = ", ".join(address_parts) if address_parts else ""

            return {
                "fap_id": fap_id,
                "name": name,
                "address": address,
                "latitude": lat,
                "longitude": lon,
                "suburb": city.upper(),
                "state": state,
                "postcode": postcode,
            }
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def save_cache(pharmacies: List[Dict]):
    """Write pharmacies to NDJSON cache file."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        for p in pharmacies:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")
    print(f"\n  Cached {len(pharmacies)} pharmacies -> {CACHE_FILE}")


def load_cache() -> List[Dict]:
    """Load pharmacies from NDJSON cache file."""
    if not CACHE_FILE.exists():
        return []
    results = []
    with open(CACHE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                results.append(json.loads(line))
    print(f"  Loaded {len(results)} pharmacies from cache ({CACHE_FILE.name})")
    return results


# ---------------------------------------------------------------------------
# Cross-reference with existing DB
# ---------------------------------------------------------------------------


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalise_name(name: str) -> str:
    """Lowercase, strip common suffixes, collapse whitespace."""
    n = name.lower().strip()
    # Remove common suffixes
    for suffix in [
        " pharmacy",
        " chemist",
        " discount drug store",
        " discount drugstore",
        " dds",
    ]:
        if n.endswith(suffix):
            n = n[: -len(suffix)]
    # Remove punctuation
    n = re.sub(r"[^\w\s]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def cross_reference(
    pbs_pharmacies: List[Dict],
    db_path: Path = DB_PATH,
    coord_threshold_km: float = 2.0,
) -> Dict:
    """
    Cross-reference PBS source pharmacies against the existing DB.

    Returns a dict with keys:
        matched         — list of (pbs, db_row) tuples that matched
        missing_from_db — PBS entries NOT found in our DB
        not_in_pbs      — DB entries NOT found in PBS source
        coord_mismatches — matched but coordinates differ > threshold
        stats           — summary counts
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Load all DB pharmacies
    cur.execute(
        "SELECT id, name, address, latitude, longitude, source, suburb, state, postcode "
        "FROM pharmacies"
    )
    db_rows = cur.fetchall()
    conn.close()

    # Build lookup structures for DB
    # Key: (normalised_name, postcode)
    db_by_name_pc: Dict[Tuple[str, str], List] = {}
    # Key: (suburb_upper, state)
    db_by_location: Dict[Tuple[str, str], List] = {}
    db_all_ids: Set[int] = set()

    for row in db_rows:
        db_all_ids.add(row["id"])
        key = (normalise_name(row["name"]), (row["postcode"] or "").strip())
        db_by_name_pc.setdefault(key, []).append(row)
        loc_key = ((row["suburb"] or "").upper().strip(), (row["state"] or "").upper().strip())
        db_by_location.setdefault(loc_key, []).append(row)

    matched = []
    missing_from_db = []
    coord_mismatches = []
    matched_db_ids: Set[int] = set()

    for pbs in pbs_pharmacies:
        pbs_norm = normalise_name(pbs["name"])
        pbs_pc = pbs.get("postcode", "").strip()
        pbs_suburb = pbs.get("suburb", "").upper().strip()
        pbs_state = pbs.get("state", "").upper().strip()

        best_match = None
        best_dist = float("inf")

        # Strategy 1: exact name + postcode match
        candidates = db_by_name_pc.get((pbs_norm, pbs_pc), [])

        # Strategy 2: name match within same suburb/state
        if not candidates:
            loc_candidates = db_by_location.get((pbs_suburb, pbs_state), [])
            for row in loc_candidates:
                if normalise_name(row["name"]) == pbs_norm:
                    candidates.append(row)

        # Strategy 3: proximity match — closest pharmacy within threshold
        if not candidates and pbs.get("latitude") and pbs.get("longitude"):
            loc_candidates = db_by_location.get((pbs_suburb, pbs_state), [])
            for row in loc_candidates:
                if row["latitude"] and row["longitude"]:
                    d = haversine_km(
                        pbs["latitude"],
                        pbs["longitude"],
                        row["latitude"],
                        row["longitude"],
                    )
                    if d < coord_threshold_km and d < best_dist:
                        best_dist = d
                        best_match = row

        if candidates:
            # Pick closest by coords if multiple matches
            if pbs.get("latitude") and pbs.get("longitude"):
                for c in candidates:
                    if c["latitude"] and c["longitude"]:
                        d = haversine_km(
                            pbs["latitude"],
                            pbs["longitude"],
                            c["latitude"],
                            c["longitude"],
                        )
                        if d < best_dist:
                            best_dist = d
                            best_match = c
                    elif best_match is None:
                        best_match = c
            else:
                best_match = candidates[0]
                best_dist = 0

        if best_match is not None:
            matched.append((pbs, dict(best_match)))
            matched_db_ids.add(best_match["id"])

            # Check coordinate mismatch
            if (
                pbs.get("latitude")
                and pbs.get("longitude")
                and best_match["latitude"]
                and best_match["longitude"]
            ):
                dist = haversine_km(
                    pbs["latitude"],
                    pbs["longitude"],
                    best_match["latitude"],
                    best_match["longitude"],
                )
                if dist > coord_threshold_km:
                    coord_mismatches.append(
                        {
                            "pbs_name": pbs["name"],
                            "db_name": best_match["name"],
                            "distance_km": round(dist, 2),
                            "pbs_coords": (pbs["latitude"], pbs["longitude"]),
                            "db_coords": (
                                best_match["latitude"],
                                best_match["longitude"],
                            ),
                            "db_id": best_match["id"],
                        }
                    )
        else:
            missing_from_db.append(pbs)

    # DB entries not found in PBS source
    not_in_pbs = [
        dict(row) for row in db_rows if row["id"] not in matched_db_ids
    ]

    stats = {
        "pbs_total": len(pbs_pharmacies),
        "db_total": len(db_rows),
        "matched": len(matched),
        "missing_from_db": len(missing_from_db),
        "not_in_pbs": len(not_in_pbs),
        "coord_mismatches": len(coord_mismatches),
    }

    return {
        "matched": matched,
        "missing_from_db": missing_from_db,
        "not_in_pbs": not_in_pbs,
        "coord_mismatches": coord_mismatches,
        "stats": stats,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_report(xref: Dict, output_dir: Path = REPORT_DIR):
    """Write cross-reference report files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    stats = xref["stats"]

    # Summary report
    summary_path = output_dir / f"pbs_xref_summary_{ts}.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("PBS Approved Suppliers Cross-Reference Report\n")
        f.write(f"Generated: {datetime.now().isoformat()}\n")
        f.write(f"Source: findapharmacy.com.au (Pharmacy Guild / PBS proxy)\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"PBS source pharmacies:        {stats['pbs_total']:>6}\n")
        f.write(f"Database pharmacies:          {stats['db_total']:>6}\n")
        f.write(f"Matched:                      {stats['matched']:>6}\n")
        f.write(f"Missing from DB (PBS only):   {stats['missing_from_db']:>6}\n")
        f.write(f"Not in PBS (DB only):         {stats['not_in_pbs']:>6}\n")
        f.write(f"Coordinate mismatches (>2km): {stats['coord_mismatches']:>6}\n")

        # State breakdown of missing
        if xref["missing_from_db"]:
            f.write("\n\nMISSING FROM DATABASE (by state):\n")
            f.write("-" * 60 + "\n")
            by_state: Dict[str, List] = {}
            for p in xref["missing_from_db"]:
                s = p.get("state", "??")
                by_state.setdefault(s, []).append(p)
            for state in sorted(by_state):
                f.write(f"\n  {state} ({len(by_state[state])} pharmacies):\n")
                for p in sorted(by_state[state], key=lambda x: x.get("suburb", "")):
                    f.write(
                        f"    - {p['name']}, {p.get('suburb','')}, "
                        f"{p.get('postcode','')} "
                        f"({p.get('latitude','?')}, {p.get('longitude','?')})\n"
                    )

        # Coordinate mismatches
        if xref["coord_mismatches"]:
            f.write("\n\nCOORDINATE MISMATCHES (>2km):\n")
            f.write("-" * 60 + "\n")
            for m in sorted(
                xref["coord_mismatches"], key=lambda x: -x["distance_km"]
            ):
                f.write(
                    f"  {m['pbs_name']} (DB id={m['db_id']})\n"
                    f"    PBS:  {m['pbs_coords']}\n"
                    f"    DB:   {m['db_coords']}\n"
                    f"    Dist: {m['distance_km']} km\n\n"
                )

        # DB-only entries by source
        if xref["not_in_pbs"]:
            f.write("\n\nIN DATABASE BUT NOT IN PBS SOURCE (by source):\n")
            f.write("-" * 60 + "\n")
            by_source: Dict[str, int] = {}
            for row in xref["not_in_pbs"]:
                src = row.get("source", "unknown")
                by_source[src] = by_source.get(src, 0) + 1
            for src in sorted(by_source, key=lambda x: -by_source[x]):
                f.write(f"  {src}: {by_source[src]}\n")

    print(f"\n  Report written -> {summary_path}")

    # Also write missing pharmacies as JSON for easy import
    if xref["missing_from_db"]:
        missing_path = output_dir / f"pbs_missing_from_db_{ts}.json"
        with open(missing_path, "w", encoding="utf-8") as f:
            json.dump(xref["missing_from_db"], f, indent=2, ensure_ascii=False)
        print(f"  Missing pharmacies JSON -> {missing_path}")

    # Write coordinate mismatches as JSON
    if xref["coord_mismatches"]:
        mismatch_path = output_dir / f"pbs_coord_mismatches_{ts}.json"
        with open(mismatch_path, "w", encoding="utf-8") as f:
            json.dump(xref["coord_mismatches"], f, indent=2, ensure_ascii=False)
        print(f"  Coord mismatches JSON -> {mismatch_path}")

    return summary_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="PBS Approved Suppliers — fetch & cross-reference"
    )
    parser.add_argument(
        "--cached",
        action="store_true",
        help="Use cached data instead of fetching fresh",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Skip fetching, use cache, just generate report",
    )
    parser.add_argument(
        "--state",
        type=str,
        help="Fetch a single state only (e.g. TAS, NSW)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=2.0,
        help="Coordinate mismatch threshold in km (default: 2.0)",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip API fetch entirely, use DB fallback directly",
    )
    parser.add_argument(
        "--audit-only",
        action="store_true",
        help="Only run the DB coverage audit (no cross-reference)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  PBS Approved Suppliers -- Pharmacy Cross-Reference")
    print("=" * 60)
    print(f"  DB: {DB_PATH}")
    print(f"  Cache: {CACHE_FILE}")
    print(f"  Threshold: {args.threshold} km")
    print()

    # Audit-only mode
    if args.audit_only:
        db_coverage_audit(DB_PATH, args.threshold)
        return

    # Step 1: Get PBS data
    pbs_pharmacies = []

    if args.report_only or args.cached:
        pbs_pharmacies = load_cache()
        if not pbs_pharmacies:
            print("  [!] No cached data found. Run without --cached first.")
            sys.exit(1)
    elif args.skip_fetch:
        print("  Skipping API fetch (--skip-fetch), using DB fallback...")
        pbs_pharmacies = _extract_pbs_from_db(DB_PATH)
        if pbs_pharmacies:
            print(f"  Extracted {len(pbs_pharmacies)} findapharmacy.com.au records from DB")
            save_cache(pbs_pharmacies)
    else:
        print("  Fetching from findapharmacy.com.au (PBS proxy)...")
        fetcher = PBSSupplierFetcher()
        states = [args.state.upper()] if args.state else None
        pbs_pharmacies = fetcher.fetch_all(states)

        if pbs_pharmacies:
            save_cache(pbs_pharmacies)
        else:
            print("\n  [!] No data fetched (API may be blocked by Cloudflare).")
            print("  [!] Falling back to cached data...")
            pbs_pharmacies = load_cache()
            if not pbs_pharmacies:
                print(
                    "  [!] No cache available either. The Funnelback API is behind"
                    " Cloudflare and cannot be accessed via requests."
                )
                print(
                    "  [!] Options:\n"
                    "      1. Run the existing scrapers/findapharmacy.py scraper\n"
                    "         (which may handle Cloudflare via browser)\n"
                    "      2. Export existing findapharmacy.com.au data from the DB\n"
                    "         for cross-reference analysis"
                )
                print("\n  Generating report from DB-only analysis...")
                # Fall back to using existing DB findapharmacy data as PBS proxy
                pbs_pharmacies = _extract_pbs_from_db(DB_PATH)
                if pbs_pharmacies:
                    print(
                        f"  Extracted {len(pbs_pharmacies)} findapharmacy.com.au"
                        " records from DB as PBS proxy"
                    )
                else:
                    print("  [!] No data available. Exiting.")
                    sys.exit(1)

    # Filter by state if requested
    if args.state and not args.report_only:
        state_upper = args.state.upper()
        before = len(pbs_pharmacies)
        pbs_pharmacies = [
            p for p in pbs_pharmacies if p.get("state", "").upper() == state_upper
        ]
        print(f"  Filtered to {state_upper}: {len(pbs_pharmacies)} (from {before})")

    # Step 2: Cross-reference
    print(f"\n  Cross-referencing {len(pbs_pharmacies)} PBS pharmacies against DB...")
    xref = cross_reference(pbs_pharmacies, DB_PATH, args.threshold)

    # Step 3: Print summary
    stats = xref["stats"]
    print(f"\n  {'='*50}")
    print(f"  RESULTS")
    print(f"  {'='*50}")
    print(f"  PBS source:              {stats['pbs_total']:>6}")
    print(f"  Database:                {stats['db_total']:>6}")
    print(f"  Matched:                 {stats['matched']:>6}")
    print(f"  Missing from DB:         {stats['missing_from_db']:>6}")
    print(f"  Not in PBS source:       {stats['not_in_pbs']:>6}")
    print(f"  Coord mismatches (>{args.threshold}km): {stats['coord_mismatches']:>6}")

    # Step 4: Generate report
    generate_report(xref)

    print(f"\n  Done.")


def _extract_pbs_from_db(db_path: Path) -> List[Dict]:
    """
    Extract findapharmacy.com.au records from the existing DB
    as a proxy for PBS approved suppliers.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, address, latitude, longitude, suburb, state, postcode "
        "FROM pharmacies WHERE source = 'findapharmacy.com.au'"
    )
    rows = cur.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append(
            {
                "fap_id": str(row["id"]),
                "name": row["name"],
                "address": row["address"] or "",
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "suburb": (row["suburb"] or "").upper(),
                "state": (row["state"] or "").upper(),
                "postcode": row["postcode"] or "",
            }
        )
    return results


def db_coverage_audit(db_path: Path = DB_PATH, coord_threshold_km: float = 0.5):
    """
    Audit: check which non-findapharmacy DB entries are duplicates vs. unique.

    This compares chain-website-sourced and OSM-sourced pharmacies against
    the findapharmacy.com.au baseline to identify:
      - Duplicates (same pharmacy, different source)
      - Genuinely additional pharmacies not covered by findapharmacy
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute(
        "SELECT id, name, address, latitude, longitude, source, suburb, state, postcode "
        "FROM pharmacies WHERE source = 'findapharmacy.com.au'"
    )
    fap_rows = cur.fetchall()

    cur.execute(
        "SELECT id, name, address, latitude, longitude, source, suburb, state, postcode "
        "FROM pharmacies WHERE source != 'findapharmacy.com.au'"
    )
    other_rows = cur.fetchall()
    conn.close()

    print(f"\n  {'='*60}")
    print(f"  DB COVERAGE AUDIT")
    print(f"  {'='*60}")
    print(f"  findapharmacy.com.au records:  {len(fap_rows)}")
    print(f"  Other-source records:          {len(other_rows)}")

    # Build spatial index of FAP records: group by (suburb, state)
    fap_by_loc: Dict[Tuple[str, str], List] = {}
    for row in fap_rows:
        key = ((row["suburb"] or "").upper().strip(), (row["state"] or "").upper().strip())
        fap_by_loc.setdefault(key, []).append(row)

    duplicates = []  # likely same pharmacy, different source
    unique_additions = []  # genuinely additional pharmacies

    for row in other_rows:
        suburb = (row["suburb"] or "").upper().strip()
        state = (row["state"] or "").upper().strip()
        name_norm = normalise_name(row["name"])
        lat, lon = row["latitude"], row["longitude"]

        matched = False

        # Check same location
        candidates = fap_by_loc.get((suburb, state), [])

        # Also check nearby suburbs (in case suburb spelling differs)
        if not candidates and lat and lon:
            for (s, st), rows_list in fap_by_loc.items():
                if st != state:
                    continue
                for fap in rows_list:
                    if fap["latitude"] and fap["longitude"]:
                        d = haversine_km(lat, lon, fap["latitude"], fap["longitude"])
                        if d < coord_threshold_km:
                            candidates.append(fap)

        for fap in candidates:
            # Name match
            if normalise_name(fap["name"]) == name_norm:
                matched = True
                break
            # Proximity match
            if lat and lon and fap["latitude"] and fap["longitude"]:
                d = haversine_km(lat, lon, fap["latitude"], fap["longitude"])
                if d < coord_threshold_km:
                    matched = True
                    break

        if matched:
            duplicates.append(dict(row))
        else:
            unique_additions.append(dict(row))

    # Report
    print(f"\n  Likely duplicates (in findapharmacy):  {len(duplicates)}")
    print(f"  Unique additions (not in findapharmacy): {len(unique_additions)}")

    # Breakdown of unique additions by source
    by_source: Dict[str, int] = {}
    for row in unique_additions:
        src = row.get("source", "unknown")
        by_source[src] = by_source.get(src, 0) + 1

    if by_source:
        print(f"\n  Unique additions by source:")
        for src in sorted(by_source, key=lambda x: -by_source[x]):
            print(f"    {src}: {by_source[src]}")

    # Breakdown by state
    by_state: Dict[str, int] = {}
    for row in unique_additions:
        st = row.get("state", "??")
        by_state[st] = by_state.get(st, 0) + 1

    if by_state:
        print(f"\n  Unique additions by state:")
        for st in sorted(by_state, key=lambda x: -by_state[x]):
            print(f"    {st}: {by_state[st]}")

    # Save unique additions to report
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if unique_additions:
        additions_path = REPORT_DIR / f"unique_non_fap_pharmacies_{ts}.json"
        with open(additions_path, "w", encoding="utf-8") as f:
            json.dump(unique_additions, f, indent=2, ensure_ascii=False)
        print(f"\n  Unique additions saved -> {additions_path}")

    return {
        "fap_count": len(fap_rows),
        "other_count": len(other_rows),
        "duplicates": len(duplicates),
        "unique_additions": len(unique_additions),
        "unique_by_source": by_source,
        "unique_by_state": by_state,
    }


if __name__ == "__main__":
    main()
