#!/usr/bin/env python3
"""
Targeted DA Scanner — queries PlanningAlerts around each qualifying site.

Instead of broad state-level postcode searches (which return limited results),
this queries a 2km radius around each of the 619 qualifying sites from v2_results.

Usage:
  py -3.12 scripts/scan_das_targeted.py --all
  py -3.12 scripts/scan_das_targeted.py --state TAS
  py -3.12 scripts/scan_das_targeted.py --top 50
  py -3.12 scripts/scan_das_targeted.py --resume
  py -3.12 scripts/scan_das_targeted.py --state NSW --resume
"""
import argparse
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "cache" / "planning_alerts"

PLANNING_ALERTS_URL = "https://api.planningalerts.org.au/applications.json"
USER_AGENT = "PharmacyFinder-TargetedDA/1.0 (pharmacy-location-finder)"
RATE_LIMIT_SEC = 1.0
CACHE_TTL_HOURS = 24
DEFAULT_RADIUS = 2000  # metres

KEYWORDS = [
    "shopping centre", "shopping center",
    "medical centre", "medical center",
    "retail", "commercial", "pharmacy",
    "mixed use", "mixed-use",
    "supermarket", "health", "clinic",
]


def _get_api_key() -> str:
    """Read API key from env or file."""
    key = os.environ.get("PLANNING_ALERTS_KEY", "").strip()
    if key:
        return key
    key_file = Path.home() / ".config" / "planningalerts" / "api_key"
    if key_file.exists():
        key = key_file.read_text().strip()
    if not key:
        print("ERROR: No PlanningAlerts API key found.")
        print("  Set PLANNING_ALERTS_KEY env var or create ~/.config/planningalerts/api_key")
        sys.exit(1)
    return key


# ── Caching ──────────────────────────────────────────────────────────────────

def _cache_key(lat: float, lon: float, radius: int) -> str:
    """Deterministic cache key from query params."""
    raw = f"{lat:.6f}_{lon:.6f}_{radius}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_path(lat: float, lon: float, radius: int) -> Path:
    return CACHE_DIR / f"{_cache_key(lat, lon, radius)}.json"


def _read_cache(lat: float, lon: float, radius: int) -> Optional[List[Dict]]:
    """Return cached response if within TTL, else None."""
    p = _cache_path(lat, lon, radius)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        cached_at = datetime.fromisoformat(data["cached_at"])
        if datetime.now() - cached_at > timedelta(hours=CACHE_TTL_HOURS):
            return None  # expired
        return data["applications"]
    except (json.JSONDecodeError, KeyError, ValueError):
        return None


def _write_cache(lat: float, lon: float, radius: int, applications: List[Dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _cache_path(lat, lon, radius)
    p.write_text(json.dumps({
        "cached_at": datetime.now().isoformat(),
        "lat": lat, "lon": lon, "radius": radius,
        "applications": applications,
    }, default=str), encoding="utf-8")


def _is_cached(lat: float, lon: float, radius: int) -> bool:
    """Check if valid (non-expired) cache exists."""
    return _read_cache(lat, lon, radius) is not None


# ── PlanningAlerts API ───────────────────────────────────────────────────────

def _fetch_radius(api_key: str, lat: float, lon: float, radius: int = DEFAULT_RADIUS) -> List[Dict]:
    """Fetch DAs within radius of a point. Returns raw application dicts."""
    cached = _read_cache(lat, lon, radius)
    if cached is not None:
        return cached

    params = {
        "key": api_key,
        "lat": f"{lat:.6f}",
        "lng": f"{lon:.6f}",
        "radius": radius,
        "count": 100,
    }

    apps = []
    should_cache = True
    try:
        resp = requests.get(
            PLANNING_ALERTS_URL,
            params=params,
            headers={"User-Agent": USER_AGENT},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            # API returns list of {application: {...}} wrappers
            items = data if isinstance(data, list) else data.get("applications", data.get("application", []))
            if isinstance(items, dict):
                items = [items]
            for item in (items or []):
                app = item.get("application", item) if isinstance(item, dict) else item
                if isinstance(app, dict):
                    apps.append(app)
        elif resp.status_code == 429:
            print("\n  WARNING: Rate limited (429). Waiting 60s...")
            should_cache = False  # don't cache rate-limited responses
            time.sleep(60)
        else:
            should_cache = False
    except requests.RequestException:
        should_cache = False

    if should_cache:
        _write_cache(lat, lon, radius, apps)
    time.sleep(RATE_LIMIT_SEC)
    return apps


# ── Classification & Scoring ────────────────────────────────────────────────

def _matches_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def _classify_development(desc: str) -> str:
    t = (desc or "").lower()
    if any(k in t for k in ["shopping centre", "shopping center", "neighbourhood centre"]):
        return "shopping_centre"
    if any(k in t for k in ["medical centre", "medical center", "health precinct",
                             "gp ", "general practice", "clinic"]):
        return "medical_centre"
    if any(k in t for k in ["mixed use", "mixed-use"]):
        return "mixed_use"
    if any(k in t for k in ["supermarket"]):
        return "retail"
    if any(k in t for k in ["retail", "commercial", "pharmacy"]):
        return "commercial"
    return "commercial"


def _extract_gla(text: str) -> Optional[float]:
    if not text:
        return None
    patterns = [
        r"([\d,]+)\s*(?:sq\s*m|sqm|m²|m2)\s*(?:gla|gross\s*lettable|floor\s*area)?",
        r"(?:gla|gross\s*lettable|floor\s*area)\s*(?:of\s*)?([\d,]+)\s*(?:sq\s*m|sqm|m²)?",
        r"([\d,]+)\s*(?:sq\s*m|sqm)\s*(?:retail|commercial)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = m.group(1).replace(",", "")
            try:
                return float(val)
            except ValueError:
                pass
    m = re.search(r"([\d.]+)\s*ha(?:ectares?)?", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1)) * 10000
        except ValueError:
            pass
    return None


def _score_pharmacy_potential(dev_type: str, gla: Optional[float], desc: str) -> float:
    score = 0.0
    t = (desc or "").lower()

    if dev_type == "shopping_centre":
        score = 0.8
        if gla and gla >= 5000:
            score = min(1.0, score + 0.15)
        if gla and gla >= 10000:
            score = 1.0
    elif dev_type == "medical_centre":
        score = 0.85
        if any(k in t for k in ["8 fte", "8fte", "eight fte", "pharmacy"]):
            score = 1.0
    elif dev_type == "mixed_use":
        score = 0.5
        if "retail" in t or "commercial" in t:
            score = 0.6
        if "pharmacy" in t or "medical" in t:
            score = 0.75
    elif dev_type == "retail":
        score = 0.5
        if "supermarket" in t:
            score = 0.65
        if "pharmacy" in t:
            score = 0.8
    elif dev_type == "commercial":
        score = 0.35
        if "pharmacy" in t:
            score = 0.8
        elif "health" in t or "clinic" in t:
            score = 0.55

    if gla and gla >= 2500 and score < 0.7:
        score = min(1.0, score + 0.2)

    return round(min(1.0, score), 2)


def _parse_date(s: Any) -> Optional[str]:
    if not s:
        return None
    return str(s)[:10]


# ── Database ─────────────────────────────────────────────────────────────────

def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS council_da (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            da_number TEXT UNIQUE,
            council TEXT,
            state TEXT,
            address TEXT,
            lat REAL,
            lon REAL,
            description TEXT,
            applicant TEXT,
            status TEXT,
            lodged_date TEXT,
            determined_date TEXT,
            development_type TEXT,
            estimated_gla_sqm REAL,
            pharmacy_potential REAL,
            source_url TEXT,
            date_scraped TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_council_da_state ON council_da(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_council_da_potential ON council_da(pharmacy_potential)")
    conn.commit()


def _upsert_da(cur: sqlite3.Cursor, da: Dict) -> bool:
    """Insert or update a DA. Returns True if new row inserted."""
    # Try insert first
    try:
        cur.execute("""
            INSERT INTO council_da
            (da_number, council, state, address, lat, lon, description, applicant, status,
             lodged_date, determined_date, development_type, estimated_gla_sqm,
             pharmacy_potential, source_url, date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            da["da_number"], da["council"], da["state"], da["address"],
            da["lat"], da["lon"], da["description"], da["applicant"], da["status"],
            da["lodged_date"], da["determined_date"], da["development_type"],
            da["estimated_gla_sqm"], da["pharmacy_potential"],
            da["source_url"], da["date_scraped"],
        ))
        return True
    except sqlite3.IntegrityError:
        # DA already exists — update if we have better data
        cur.execute("""
            UPDATE council_da SET
                council = COALESCE(?, council),
                state = COALESCE(?, state),
                address = COALESCE(?, address),
                lat = COALESCE(?, lat),
                lon = COALESCE(?, lon),
                description = COALESCE(?, description),
                status = COALESCE(?, status),
                development_type = COALESCE(?, development_type),
                estimated_gla_sqm = COALESCE(?, estimated_gla_sqm),
                pharmacy_potential = MAX(COALESCE(pharmacy_potential, 0), ?),
                date_scraped = ?
            WHERE da_number = ?
        """, (
            da["council"], da["state"], da["address"],
            da["lat"], da["lon"], da["description"],
            da["status"], da["development_type"],
            da["estimated_gla_sqm"], da["pharmacy_potential"],
            da["date_scraped"], da["da_number"],
        ))
        return False


# ── Site Loading ─────────────────────────────────────────────────────────────

def _load_sites(db_path: Path, state: Optional[str] = None,
                top_n: Optional[int] = None) -> List[Dict]:
    """Load qualifying sites from v2_results."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    query = """
        SELECT id, name, address, latitude, longitude, state, profitability_score
        FROM v2_results
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """
    params: list = []

    if state:
        query += " AND state = ?"
        params.append(state.upper())

    query += " ORDER BY profitability_score DESC"

    if top_n:
        query += " LIMIT ?"
        params.append(top_n)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── State Detection ──────────────────────────────────────────────────────────

def _detect_state(lat: float, lon: float, fallback: str = "UNK") -> str:
    """Rough state detection from coordinates."""
    if lat > -29 and lon > 141:
        return "QLD"
    if lat < -39 and lon > 144 and lon < 149:
        return "TAS"
    if lon < 129:
        return "WA"
    if lon < 141:
        return "SA"
    if lat > -33.5 and lon > 141:
        return "NSW"  # rough
    if lat <= -33.5 and lon > 141 and lon < 150:
        return "VIC"  # rough
    if lon >= 150:
        return "NSW"
    return fallback


# ── Main Scanner ─────────────────────────────────────────────────────────────

def scan_targeted(
    sites: List[Dict],
    api_key: str,
    db_path: Path,
    resume: bool = False,
    radius: int = DEFAULT_RADIUS,
) -> Dict[str, Any]:
    """Scan PlanningAlerts around each site. Returns summary stats."""
    conn = sqlite3.connect(str(db_path))
    _ensure_table(conn)
    cur = conn.cursor()

    total_sites = len(sites)
    total_das_found = 0
    total_inserted = 0
    total_updated = 0
    total_skipped_cache = 0
    das_by_state: Dict[str, int] = {}
    high_potential = 0
    seen_da_numbers: set = set()

    start_time = time.time()
    bar_width = 40

    for i, site in enumerate(sites):
        lat = site["latitude"]
        lon = site["longitude"]
        site_state = site.get("state", _detect_state(lat, lon))

        # Resume mode: skip if already cached
        if resume and _is_cached(lat, lon, radius):
            total_skipped_cache += 1
            _show_progress(i + 1, total_sites, bar_width, site["name"], "cached", start_time)
            continue

        apps = _fetch_radius(api_key, lat, lon, radius)
        matched = 0

        for app in apps:
            desc = app.get("description") or ""
            if not _matches_keywords(desc):
                continue

            da_id = str(app.get("id", ""))
            da_number = app.get("council_reference") or da_id or f"PA-{da_id}"

            if da_number in seen_da_numbers:
                continue
            seen_da_numbers.add(da_number)

            app_lat = app.get("lat")
            app_lon = app.get("lng")
            if app_lat is not None and app_lon is not None:
                try:
                    app_lat, app_lon = float(app_lat), float(app_lon)
                except (ValueError, TypeError):
                    app_lat, app_lon = lat, lon
            else:
                app_lat, app_lon = lat, lon

            da_state = _detect_state(app_lat, app_lon, site_state)

            authority = app.get("authority") or {}
            council = authority.get("full_name", "") if isinstance(authority, dict) else str(authority)

            dev_type = _classify_development(desc)
            gla = _extract_gla(desc)
            potential = _score_pharmacy_potential(dev_type, gla, desc)

            da_record = {
                "da_number": da_number,
                "council": council,
                "state": da_state,
                "address": app.get("address", ""),
                "lat": app_lat,
                "lon": app_lon,
                "description": desc[:2000] if desc else None,
                "applicant": None,
                "status": app.get("status") or "Unknown",
                "lodged_date": _parse_date(app.get("date_received") or app.get("date_lodged")),
                "determined_date": _parse_date(app.get("date_decided")),
                "development_type": dev_type,
                "estimated_gla_sqm": gla,
                "pharmacy_potential": potential,
                "source_url": app.get("info_url") or app.get("comment_url"),
                "date_scraped": datetime.now().isoformat(),
            }

            is_new = _upsert_da(cur, da_record)
            if is_new:
                total_inserted += 1
            else:
                total_updated += 1

            total_das_found += 1
            matched += 1
            das_by_state[da_state] = das_by_state.get(da_state, 0) + 1

            if potential >= 0.7:
                high_potential += 1

        conn.commit()
        _show_progress(i + 1, total_sites, bar_width, site["name"],
                       f"{matched} DAs" if matched else "0", start_time)

    conn.close()
    elapsed = time.time() - start_time

    return {
        "total_sites": total_sites,
        "total_das_found": total_das_found,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "skipped_cached": total_skipped_cache,
        "high_potential": high_potential,
        "das_by_state": das_by_state,
        "elapsed_seconds": round(elapsed, 1),
    }


def _show_progress(current: int, total: int, bar_width: int, name: str,
                   status: str, start_time: float) -> None:
    """Print a progress bar to stderr."""
    pct = current / total if total else 1
    filled = int(bar_width * pct)
    bar = "#" * filled + "-" * (bar_width - filled)

    elapsed = time.time() - start_time
    if current > 0 and elapsed > 0:
        rate = current / elapsed
        eta = (total - current) / rate if rate > 0 else 0
        eta_str = f"ETA {int(eta)}s"
    else:
        eta_str = ""

    short_name = (name[:25] + "..") if len(name) > 27 else name
    line = f"\r  [{bar}] {current}/{total} ({pct:.0%}) {short_name:<28} {status:<10} {eta_str}"
    sys.stderr.write(line)
    sys.stderr.flush()
    if current == total:
        sys.stderr.write("\n")


# ── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Targeted DA scanner — queries PlanningAlerts around each qualifying site"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Scan all 619 qualifying sites")
    group.add_argument("--state", type=str, help="Scan sites in a specific state (NSW, VIC, QLD, TAS, WA, SA)")
    group.add_argument("--top", type=int, help="Scan top N sites by profitability score")

    parser.add_argument("--resume", action="store_true",
                        help="Skip sites that already have valid cached responses")
    parser.add_argument("--radius", type=int, default=DEFAULT_RADIUS,
                        help=f"Search radius in metres (default: {DEFAULT_RADIUS})")
    parser.add_argument("--db", type=str, default=None,
                        help="Database path (default: pharmacy_finder.db in project root)")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH

    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}")
        return 1

    api_key = _get_api_key()

    # Load sites
    state_filter = args.state.upper() if args.state else None
    top_n = args.top if args.top else None

    if state_filter and state_filter not in ("NSW", "VIC", "QLD", "TAS", "WA", "SA", "NT", "ACT"):
        print(f"ERROR: Invalid state: {state_filter}")
        return 1

    sites = _load_sites(db_path, state=state_filter, top_n=top_n)

    if not sites:
        print("No qualifying sites found matching criteria.")
        return 0

    # Banner
    print("=" * 60)
    print("  PharmacyFinder — Targeted DA Scanner")
    print("=" * 60)
    scope = f"state={state_filter}" if state_filter else f"top {top_n}" if top_n else "ALL"
    print(f"  Sites to scan: {len(sites)} ({scope})")
    print(f"  Radius: {args.radius}m")
    print(f"  Resume mode: {'ON' if args.resume else 'OFF'}")
    print(f"  Cache dir: {CACHE_DIR}")
    print(f"  Database: {db_path}")
    print("-" * 60)

    results = scan_targeted(
        sites=sites,
        api_key=api_key,
        db_path=db_path,
        resume=args.resume,
        radius=args.radius,
    )

    # Summary
    print("\n" + "=" * 60)
    print("  SCAN COMPLETE")
    print("=" * 60)
    print(f"  Sites scanned:     {results['total_sites']}")
    if results["skipped_cached"]:
        print(f"  Skipped (cached):  {results['skipped_cached']}")
    print(f"  Total DAs found:   {results['total_das_found']}")
    print(f"  New DAs inserted:  {results['total_inserted']}")
    print(f"  DAs updated:       {results['total_updated']}")
    print(f"  High potential:    {results['high_potential']}  (score >= 0.7)")
    print(f"  Time elapsed:      {results['elapsed_seconds']}s")

    if results["das_by_state"]:
        print("\n  DAs by state:")
        for st in sorted(results["das_by_state"].keys()):
            print(f"    {st}: {results['das_by_state'][st]}")

    if results["total_das_found"] == 0:
        print("\n  TIP: No matching DAs found. This could mean:")
        print("     - PlanningAlerts has limited coverage in these areas")
        print("     - No recent commercial/medical DAs near these sites")
        print("     - Try a larger --radius (e.g. 4000)")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
