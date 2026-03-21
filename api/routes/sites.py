"""
Sites routes — list, filter, and get stats on qualifying sites.
"""
import json
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

    return result
