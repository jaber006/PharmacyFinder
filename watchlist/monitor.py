"""
Change detection engine for the Watchlist system.

Compares scan results, detects pharmacy closures, new GP clinics,
and threshold crossings. Writes alerts to watchlist_alerts.
"""
import json
import os
import sqlite3
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__("sys").path:
    __import__("sys").path.insert(0, PROJECT_ROOT)

from watchlist.models import WatchlistItem, WatchlistAlert, WATCH_REASONS

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")


def _geodesic_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Geodesic distance in km. Uses geopy if available."""
    try:
        from geopy.distance import geodesic
        return geodesic((lat1, lon1), (lat2, lon2)).kilometers
    except ImportError:
        import math
        # Haversine fallback
        R = 6371  # Earth radius km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        return R * c

# Radius in km to find watchlist items affected by a pharmacy closure
PHARMACY_CLOSURE_ALERT_RADIUS_KM = 2.0


def _get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Get DB connection with row factory."""
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_scan_snapshots(conn: sqlite3.Connection):
    """Create scan_snapshots table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS scan_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_type TEXT NOT NULL,
            entity_ids_json TEXT NOT NULL,
            scan_date TEXT NOT NULL,
            UNIQUE(scan_type)
        )
    """)
    conn.commit()


def _insert_alert(
    conn: sqlite3.Connection,
    item_id: Optional[int],
    alert_type: str,
    message: str,
    severity: str,
):
    """Insert an alert into watchlist_alerts."""
    conn.execute("""
        INSERT INTO watchlist_alerts (item_id, alert_type, message, severity, triggered_date, acknowledged)
        VALUES (?, ?, ?, ?, ?, 0)
    """, (item_id, alert_type, message, severity, date.today().isoformat()))
    conn.commit()


def compare_scan_results(
    old_results: List[Dict[str, Any]],
    new_results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Compare two evaluation result sets and return a list of changes.

    Each change dict has: candidate_id, change_type (new_qualification | lost_qualification),
    old_passed, new_passed, primary_rule (if applicable).
    """
    old_by_id = {r.get("id") or r.get("candidate_id"): r for r in old_results if r.get("id") or r.get("candidate_id")}
    new_by_id = {r.get("id") or r.get("candidate_id"): r for r in new_results if r.get("id") or r.get("candidate_id")}

    changes = []

    for cid, new_r in new_by_id.items():
        if cid is None:
            continue
        old_passed = (old_by_id.get(cid) or {}).get("passed_any", False)
        new_passed = new_r.get("passed_any", False)

        if old_passed and not new_passed:
            changes.append({
                "candidate_id": cid,
                "change_type": "lost_qualification",
                "old_passed": True,
                "new_passed": False,
                "primary_rule": new_r.get("primary_rule", ""),
            })
        elif not old_passed and new_passed:
            changes.append({
                "candidate_id": cid,
                "change_type": "new_qualification",
                "old_passed": False,
                "new_passed": True,
                "primary_rule": new_r.get("primary_rule", ""),
            })

    # Sites that were in old but not in new (dropped from scan)
    for cid in set(old_by_id) - set(new_by_id):
        if cid and old_by_id[cid].get("passed_any"):
            changes.append({
                "candidate_id": cid,
                "change_type": "lost_qualification",
                "old_passed": True,
                "new_passed": False,
                "primary_rule": old_by_id[cid].get("primary_rule", ""),
            })

    return changes


def detect_pharmacy_closures(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Detect pharmacies that disappeared between scans.
    Uses scan_snapshots to store/compare previous pharmacy details.
    Writes alerts to watchlist_alerts for affected watchlist items.
    Returns list of closed pharmacy dicts (id, name, address, lat, lon).
    """
    from watchlist.db import ensure_watchlist_tables
    ensure_watchlist_tables(db_path)
    conn = _get_conn(db_path)
    _ensure_scan_snapshots(conn)

    cur = conn.cursor()

    # Current pharmacies with full details
    cur.execute("SELECT id, name, address, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
    current = {r["id"]: dict(r) for r in cur.fetchall()}
    current_ids = set(current.keys())

    # Previous full snapshot (id -> details)
    cur.execute("SELECT entity_ids_json FROM scan_snapshots WHERE scan_type = 'pharmacies_full'")
    row = cur.fetchone()
    if row is None:
        # First run: store current as snapshot
        full_snapshot = {str(p["id"]): {k: p[k] for k in ("id", "name", "address", "latitude", "longitude")} for p in current.values()}
        cur.execute(
            "INSERT OR REPLACE INTO scan_snapshots (scan_type, entity_ids_json, scan_date) VALUES (?, ?, ?)",
            ("pharmacies_full", json.dumps(full_snapshot), date.today().isoformat())
        )
        conn.commit()
        conn.close()
        return []

    prev_details = json.loads(row["entity_ids_json"])
    prev_ids = set(int(k) for k in prev_details.keys())
    closed_ids = prev_ids - current_ids

    closed_pharmacies = []
    for pid in closed_ids:
        details = prev_details.get(str(pid), {"id": pid, "name": "Unknown", "address": "", "latitude": None, "longitude": None})
        closed_pharmacies.append(details)

        lat = details.get("latitude")
        lon = details.get("longitude")
        if lat is None or lon is None:
            continue

        # Find watchlist items near closed pharmacy (v2_results has candidate coords)
        try:
            cur.execute("""
                SELECT wi.id, wi.candidate_id, wi.trigger_condition, v.latitude, v.longitude
                FROM watchlist_items wi
                LEFT JOIN v2_results v ON v.id = wi.candidate_id
                WHERE wi.status = 'watching'
            """)
            for wrow in cur.fetchall():
                vlat, vlon = wrow.get("latitude"), wrow.get("longitude")
                if vlat is not None and vlon is not None:
                    dist_km = _geodesic_km(lat, lon, vlat, vlon)
                    if dist_km <= PHARMACY_CLOSURE_ALERT_RADIUS_KM:
                        _insert_alert(
                            conn, wrow["id"],
                            "new_qualification",
                            f"Pharmacy closure nearby ({details.get('name', 'Unknown')}) may allow this site to newly qualify. "
                            f"Distance: {dist_km:.1f}km. {wrow['trigger_condition']}",
                            "high",
                        )
        except sqlite3.OperationalError:
            pass

    # Update full snapshot for next run
    full_snapshot = {str(p["id"]): {k: p[k] for k in ("id", "name", "address", "latitude", "longitude")} for p in current.values()}
    cur.execute(
        "INSERT OR REPLACE INTO scan_snapshots (scan_type, entity_ids_json, scan_date) VALUES (?, ?, ?)",
        ("pharmacies_full", json.dumps(full_snapshot), date.today().isoformat())
    )

    conn.commit()
    conn.close()
    return closed_pharmacies


def detect_new_gp_clinics(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Detect new GP practices added since last scan.
    Writes alerts for watchlist items with gp_growth reason.
    Returns list of new GP dicts.
    """
    from watchlist.db import ensure_watchlist_tables
    ensure_watchlist_tables(db_path)
    conn = _get_conn(db_path)
    _ensure_scan_snapshots(conn)

    cur = conn.cursor()
    cur.execute("SELECT id, name, address, latitude, longitude FROM gps WHERE latitude IS NOT NULL")
    current = {r["id"]: dict(r) for r in cur.fetchall()}
    current_ids = set(current.keys())

    cur.execute("SELECT entity_ids_json FROM scan_snapshots WHERE scan_type = 'gps'")
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT OR REPLACE INTO scan_snapshots (scan_type, entity_ids_json, scan_date) VALUES (?, ?, ?)",
            ("gps", json.dumps(list(current_ids)), date.today().isoformat())
        )
        conn.commit()
        conn.close()
        return []

    prev_ids = set(json.loads(row["entity_ids_json"]))
    new_ids = current_ids - prev_ids

    cur.execute(
        "INSERT OR REPLACE INTO scan_snapshots (scan_type, entity_ids_json, scan_date) VALUES (?, ?, ?)",
        ("gps", json.dumps(list(current_ids)), date.today().isoformat())
    )

    new_gps = [current[pid] for pid in new_ids]

    # Alert watchlist items with gp_growth reason
    cur.execute("""
        SELECT id, candidate_id, trigger_condition FROM watchlist_items
        WHERE status = 'watching' AND watch_reason = 'gp_growth'
    """)
    for wrow in cur.fetchall():
        _insert_alert(
            conn, wrow["id"],
            "new_qualification",
            f"New GP clinic(s) detected ({len(new_gps)} total). Re-evaluate for Item 130/136. {wrow['trigger_condition']}",
            "medium",
        )

    conn.commit()
    conn.close()
    return new_gps


def detect_threshold_crossings(
    candidate_id: str,
    old_eval: Dict[str, Any],
    new_eval: Dict[str, Any],
    db_path: Optional[str] = None,
    watchlist_item_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Detect if a candidate newly qualifies or lost qualification.
    Writes to watchlist_alerts. Returns list of change dicts.
    """
    old_passed = bool(old_eval.get("passed_any", False))
    new_passed = bool(new_eval.get("passed_any", False))

    changes = []

    if not old_passed and new_passed:
        changes.append({
            "candidate_id": candidate_id,
            "change_type": "new_qualification",
            "primary_rule": new_eval.get("primary_rule", ""),
        })
        if db_path is not None and watchlist_item_id is not None:
            conn = _get_conn(db_path)
            _insert_alert(
                conn, watchlist_item_id,
                "new_qualification",
                f"Site {candidate_id} now qualifies under {new_eval.get('primary_rule', 'rule')}.",
                "high",
            )
            conn.close()

    elif old_passed and not new_passed:
        changes.append({
            "candidate_id": candidate_id,
            "change_type": "lost_qualification",
            "primary_rule": old_eval.get("primary_rule", ""),
        })
        if db_path is not None and watchlist_item_id is not None:
            conn = _get_conn(db_path)
            _insert_alert(
                conn, watchlist_item_id,
                "lost_qualification",
                f"Site {candidate_id} no longer qualifies (was {old_eval.get('primary_rule', 'rule')}).",
                "medium",
            )
            conn.close()

    return changes
