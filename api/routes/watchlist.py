"""
Watchlist routes — manage watched sites and alerts.
"""
import json
import sqlite3
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "pharmacy_finder.db",
)

router = APIRouter(tags=["watchlist"])


def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class WatchlistAddRequest(BaseModel):
    candidate_id: str
    watch_reason: str = "manual"
    trigger_condition: str = ""
    check_frequency: str = "weekly"
    notes: str = ""


@router.get("/watchlist")
async def list_watchlist():
    """List all watchlist items."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM watchlist_items ORDER BY created_date DESC")
    items = []
    for row in cur.fetchall():
        item = dict(row)
        # Try to enrich with site data
        cur.execute("SELECT name, address, state, primary_rule, commercial_score FROM v2_results WHERE id = ?",
                     (item["candidate_id"],))
        site = cur.fetchone()
        if site:
            item["site"] = dict(site)
        items.append(item)
    conn.close()
    return {"items": items}


@router.post("/watchlist")
async def add_to_watchlist(req: WatchlistAddRequest):
    """Add a site to the watchlist."""
    conn = _get_db()
    cur = conn.cursor()

    # Check if already watching
    cur.execute("SELECT id FROM watchlist_items WHERE candidate_id = ? AND status = 'watching'",
                (req.candidate_id,))
    existing = cur.fetchone()
    if existing:
        conn.close()
        raise HTTPException(status_code=409, detail="Already watching this site")

    cur.execute(
        """INSERT INTO watchlist_items 
           (candidate_id, watch_reason, trigger_condition, check_frequency, status, created_date, notes)
           VALUES (?, ?, ?, ?, 'watching', ?, ?)""",
        (req.candidate_id, req.watch_reason, req.trigger_condition,
         req.check_frequency, datetime.now().isoformat(), req.notes),
    )
    conn.commit()
    item_id = cur.lastrowid
    conn.close()
    return {"id": item_id, "status": "added"}


@router.delete("/watchlist/{item_id}")
async def remove_from_watchlist(item_id: int):
    """Remove an item from the watchlist."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM watchlist_items WHERE id = ?", (item_id,))
    if not cur.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Watchlist item not found")

    cur.execute("DELETE FROM watchlist_items WHERE id = ?", (item_id,))
    conn.commit()
    conn.close()
    return {"status": "removed"}


@router.get("/watchlist/alerts")
async def get_alerts():
    """Get pending (unacknowledged) alerts."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute(
        """SELECT a.*, w.candidate_id, w.watch_reason
           FROM watchlist_alerts a
           JOIN watchlist_items w ON a.item_id = w.id
           WHERE a.acknowledged = 0
           ORDER BY a.triggered_date DESC"""
    )
    alerts = [dict(row) for row in cur.fetchall()]
    conn.close()
    return {"alerts": alerts}
