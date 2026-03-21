"""
Sites routes — list, filter, and get stats on qualifying sites.
"""
import json
import math
import sqlite3
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "pharmacy_finder.db",
)

router = APIRouter(tags=["sites"])


def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _format_site(row: dict) -> dict:
    """Format a v2_results row for API response."""
    rules_json = json.loads(row.get("rules_json", "[]") or "[]")
    return {
        "id": row["id"],
        "name": row["name"],
        "address": row["address"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "state": row["state"],
        "primary_rule": row["primary_rule"],
        "commercial_score": row["commercial_score"],
        "best_confidence": row["best_confidence"],
        "passed_any": bool(row["passed_any"]),
        "rules": rules_json,
    }


@router.get("/sites/stats")
async def get_stats():
    """Summary statistics: count by state, by rule, average score."""
    conn = _get_db()
    cur = conn.cursor()

    # Total
    cur.execute("SELECT COUNT(*) FROM v2_results WHERE passed_any = 1")
    total = cur.fetchone()[0]

    # By state
    cur.execute(
        "SELECT state, COUNT(*) as cnt FROM v2_results WHERE passed_any = 1 GROUP BY state ORDER BY cnt DESC"
    )
    by_state = {row["state"]: row["cnt"] for row in cur.fetchall()}

    # By rule
    cur.execute(
        "SELECT primary_rule, COUNT(*) as cnt FROM v2_results WHERE passed_any = 1 GROUP BY primary_rule ORDER BY cnt DESC"
    )
    by_rule = {row["primary_rule"]: row["cnt"] for row in cur.fetchall()}

    # Avg score
    cur.execute("SELECT AVG(commercial_score) as avg_score FROM v2_results WHERE passed_any = 1")
    avg_score = round(cur.fetchone()["avg_score"] or 0, 4)

    # Top scoring
    cur.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 ORDER BY commercial_score DESC LIMIT 5"
    )
    top = [_format_site(dict(row)) for row in cur.fetchall()]

    conn.close()
    return {
        "total_qualifying": total,
        "by_state": by_state,
        "by_rule": by_rule,
        "avg_commercial_score": avg_score,
        "top_sites": top,
    }


@router.get("/sites")
async def list_sites(
    state: Optional[str] = Query(None, description="Filter by state (e.g. NSW, VIC, TAS)"),
    rule: Optional[str] = Query(None, description="Filter by primary rule (e.g. Item 130)"),
    min_score: Optional[float] = Query(None, description="Minimum commercial score"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """List all qualifying sites with optional filters."""
    conn = _get_db()
    cur = conn.cursor()

    where_clauses = ["passed_any = 1"]
    params = []

    if state:
        where_clauses.append("state = ?")
        params.append(state.upper())
    if rule:
        where_clauses.append("primary_rule = ?")
        params.append(rule)
    if min_score is not None:
        where_clauses.append("commercial_score >= ?")
        params.append(min_score)

    where = " AND ".join(where_clauses)

    # Count
    cur.execute(f"SELECT COUNT(*) FROM v2_results WHERE {where}", params)
    total = cur.fetchone()[0]

    # Fetch
    cur.execute(
        f"SELECT * FROM v2_results WHERE {where} ORDER BY commercial_score DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    )
    sites = [_format_site(dict(row)) for row in cur.fetchall()]

    conn.close()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "sites": sites,
    }


@router.get("/sites/{site_id}")
async def get_site(site_id: str):
    """Get full details for a single site."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM v2_results WHERE id = ?", (site_id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    row = dict(row)
    result = _format_site(row)

    # Include all rules detail
    all_rules = json.loads(row.get("all_rules_json", "[]") or "[]")
    result["all_rules"] = all_rules

    # Include profitability score if available
    if row.get("profitability_score") is not None:
        result["profitability_score"] = row["profitability_score"]

    return result


# Rule-specific compliance radii in km
# None = no circle (rule is boundary/centre based, not distance-based)
RULE_RADII = {
    "Item 130": 1.5,
    "Item 131": 10.0,
    "Item 132": None,   # town boundary based
    "Item 133": None,   # shopping centre based
    "Item 134": None,   # shopping centre based
    "Item 134A": None,  # shopping centre based
    "Item 135": 2.0,
    "Item 136": 0.5,
}


def _nearby_query(conn, table, lat, lon, radius_km, columns, deg_buffer=None):
    """
    Query nearby entities within radius_km using bounding-box pre-filter.
    Returns list of dicts with entity fields + distance_km.
    """
    if deg_buffer is None:
        deg_buffer = radius_km / 111.0 + 0.01
    cur = conn.cursor()
    cur.execute(
        f"SELECT {columns} FROM {table} "
        f"WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat - deg_buffer, lat + deg_buffer,
         lon - deg_buffer, lon + deg_buffer),
    )
    results = []
    for row in cur.fetchall():
        d = dict(row)
        d["distance_km"] = round(
            _haversine_km(lat, lon, d["latitude"], d["longitude"]), 3
        )
        if d["distance_km"] <= radius_km:
            results.append(d)
    results.sort(key=lambda x: x["distance_km"])
    return results


@router.get("/sites/{site_id}/nearby")
async def get_nearby(site_id: str):
    """
    Return all nearby entities for a qualifying site:
    pharmacies (15km), GPs (2km), supermarkets (2km),
    hospitals (5km), medical centres (2km).
    """
    conn = _get_db()
    cur = conn.cursor()

    # Get site coordinates
    cur.execute(
        "SELECT latitude, longitude, primary_rule, profitability_score "
        "FROM v2_results WHERE id = ?",
        (site_id,),
    )
    site_row = cur.fetchone()
    if not site_row:
        conn.close()
        raise HTTPException(status_code=404, detail=f"Site not found: {site_id}")

    lat, lon = site_row["latitude"], site_row["longitude"]
    primary_rule = site_row["primary_rule"]
    compliance_radius = RULE_RADII.get(primary_rule, 1.5)

    pharmacies = _nearby_query(
        conn, "pharmacies", lat, lon, 15.0,
        "name, address, latitude, longitude, suburb, state",
        deg_buffer=15.0 / 111.0 + 0.02,
    )

    gps = _nearby_query(
        conn, "gps", lat, lon, 2.0,
        "name, address, latitude, longitude, fte",
    )

    supermarkets = _nearby_query(
        conn, "supermarkets", lat, lon, 2.0,
        "name, address, latitude, longitude, estimated_gla, brand",
    )

    hospitals = _nearby_query(
        conn, "hospitals", lat, lon, 5.0,
        "name, address, latitude, longitude, bed_count, hospital_type",
        deg_buffer=5.0 / 111.0 + 0.01,
    )

    medical_centres = _nearby_query(
        conn, "medical_centres", lat, lon, 2.0,
        "name, address, latitude, longitude, num_gps, total_fte",
    )

    conn.close()

    return {
        "site_id": site_id,
        "latitude": lat,
        "longitude": lon,
        "primary_rule": primary_rule,
        "compliance_radius_km": compliance_radius,
        "profitability_score": site_row["profitability_score"],
        "pharmacies": pharmacies,
        "gps": gps,
        "supermarkets": supermarkets,
        "hospitals": hospitals,
        "medical_centres": medical_centres,
    }
