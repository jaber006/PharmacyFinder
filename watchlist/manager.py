"""
CRUD operations for the Watchlist system.
"""
import json
import os
import sqlite3
from datetime import date
from typing import Any, Dict, List, Optional

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__("sys").path:
    __import__("sys").path.insert(0, PROJECT_ROOT)

from watchlist.models import WatchlistItem, WATCH_REASONS, CHECK_FREQUENCIES, ITEM_STATUSES
from watchlist.db import ensure_watchlist_tables

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

# Near-miss threshold: sites within 15% of any rule threshold get auto-added
NEAR_MISS_MARGIN_PCT = 0.15


def _get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_item(row: sqlite3.Row) -> WatchlistItem:
    """Convert DB row to WatchlistItem."""
    last = row["last_checked"]
    created = row["created_date"]
    return WatchlistItem(
        id=row["id"],
        candidate_id=row["candidate_id"],
        watch_reason=row["watch_reason"] or "near_miss",
        trigger_condition=row["trigger_condition"] or "",
        check_frequency=row["check_frequency"] or "weekly",
        last_checked=date.fromisoformat(last) if isinstance(last, str) and last else date.today(),
        status=row["status"] or "watching",
        created_date=date.fromisoformat(created) if isinstance(created, str) and created else date.today(),
        notes=row["notes"] or "",
    )


def add_to_watchlist(
    candidate_id: str,
    reason: str,
    trigger: str,
    frequency: str = "weekly",
    db_path: Optional[str] = None,
    notes: str = "",
) -> int:
    """
    Add a candidate to the watchlist. Returns the new item's id.
    """
    ensure_watchlist_tables(db_path)
    if reason not in WATCH_REASONS:
        reason = "near_miss"
    if frequency not in CHECK_FREQUENCIES:
        frequency = "weekly"

    conn = _get_conn(db_path)
    cur = conn.execute("""
        INSERT INTO watchlist_items
        (candidate_id, watch_reason, trigger_condition, check_frequency, last_checked, status, created_date, notes)
        VALUES (?, ?, ?, ?, ?, 'watching', ?, ?)
    """, (candidate_id, reason, trigger, frequency, date.today().isoformat(), date.today().isoformat(), notes))
    item_id = cur.lastrowid
    conn.commit()
    conn.close()
    return item_id


def remove_from_watchlist(item_id: int, db_path: Optional[str] = None) -> bool:
    """Remove a watchlist item by id. Returns True if deleted."""
    conn = _get_conn(db_path)
    cur = conn.execute("DELETE FROM watchlist_items WHERE id = ?", (item_id,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    return deleted


def get_watchlist(
    filters: Optional[Dict[str, Any]] = None,
    db_path: Optional[str] = None,
) -> List[WatchlistItem]:
    """
    Get watchlist items, optionally filtered by status, watch_reason, etc.
    """
    ensure_watchlist_tables(db_path)
    conn = _get_conn(db_path)
    cur = conn.cursor()
    sql = "SELECT * FROM watchlist_items WHERE 1=1"
    params: List[Any] = []

    if filters:
        if filters.get("status"):
            sql += " AND status = ?"
            params.append(filters["status"])
        if filters.get("watch_reason"):
            sql += " AND watch_reason = ?"
            params.append(filters["watch_reason"])
        if filters.get("candidate_id"):
            sql += " AND candidate_id = ?"
            params.append(filters["candidate_id"])

    sql += " ORDER BY created_date DESC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return [_row_to_item(r) for r in rows]


def auto_populate_watchlist(
    evaluation_results: List[Dict[str, Any]],
    db_path: Optional[str] = None,
) -> int:
    """
    Automatically add near-miss sites (within 15% of any rule threshold) to the watchlist.
    Returns count of new items added.
    """
    ensure_watchlist_tables(db_path)
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("SELECT candidate_id FROM watchlist_items WHERE status = 'watching'")
    already_watching = {r["candidate_id"] for r in cur.fetchall()}

    added = 0
    for r in evaluation_results:
        if r.get("passed_any"):
            continue  # Skip passing sites
        cid = r.get("id") or r.get("candidate_id")
        if not cid or cid in already_watching:
            continue

        # Check if any rule is a near-miss (margin_pct or confidence close to threshold)
        all_rules = r.get("all_rules", r.get("rule_results", []))
        if isinstance(all_rules, str):
            try:
                all_rules = json.loads(all_rules)
            except (json.JSONDecodeError, TypeError):
                all_rules = []

        trigger_parts = []
        for rr in all_rules:
            conf = rr.get("confidence", 0) or 0
            margin_pct = rr.get("margin_pct")
            # Near miss: confidence within 15% of passing, or margin_pct within 15% of threshold
            is_near = conf >= (1.0 - NEAR_MISS_MARGIN_PCT)
            if margin_pct is not None and margin_pct >= -NEAR_MISS_MARGIN_PCT:
                is_near = True
            if is_near:
                item = rr.get("item", "")
                trigger_parts.append(f"{item}: confidence {conf:.2f}")

        if not trigger_parts:
            continue

        trigger = "; ".join(trigger_parts) or "Within 15% of rule threshold"
        try:
            conn.execute("""
                INSERT INTO watchlist_items
                (candidate_id, watch_reason, trigger_condition, check_frequency, last_checked, status, created_date, notes)
                VALUES (?, 'near_miss', ?, 'weekly', ?, 'watching', ?, '')
            """, (cid, trigger, date.today().isoformat(), date.today().isoformat()))
            added += 1
            already_watching.add(cid)
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return added
