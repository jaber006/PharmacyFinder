"""
Notification system for watchlist alerts.

Format alerts for display, get pending alerts, acknowledge, and export.
"""
import csv
import json
import os
from datetime import date
from io import StringIO
from typing import Any, Dict, List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")


def _get_conn(db_path: Optional[str] = None):
    import sqlite3
    from watchlist.db import ensure_watchlist_tables
    ensure_watchlist_tables(db_path)
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def format_alert(alert: Dict[str, Any]) -> str:
    """Return a human-readable message for an alert."""
    severity = alert.get("severity", "low").upper()
    alert_type = (alert.get("alert_type") or "").replace("_", " ").title()
    msg = alert.get("message", "")
    triggered = alert.get("triggered_date", "")

    lines = [
        f"[{severity}] {alert_type}",
        msg,
    ]
    if triggered:
        lines.append(f"Triggered: {triggered}")

    return "\n".join(lines)


def get_pending_alerts(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return unacknowledged alerts, optionally with watchlist item details."""
    conn = _get_conn(db_path)
    cur = conn.cursor()
    cur.execute("""
        SELECT a.id, a.item_id, a.alert_type, a.message, a.severity,
               a.triggered_date, a.acknowledged
        FROM watchlist_alerts a
        WHERE a.acknowledged = 0
        ORDER BY a.triggered_date DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def acknowledge_alert(alert_id: int, db_path: Optional[str] = None) -> bool:
    """Mark an alert as acknowledged. Returns True if updated."""
    conn = _get_conn(db_path)
    cur = conn.execute("UPDATE watchlist_alerts SET acknowledged = 1 WHERE id = ?", (alert_id,))
    updated = cur.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def alerts_to_json(alerts: Optional[List[Dict[str, Any]]] = None, db_path: Optional[str] = None) -> str:
    """
    Export alerts to JSON string.
    If alerts is None, exports all (including acknowledged).
    """
    if alerts is None:
        conn = _get_conn(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT a.id, a.item_id, a.alert_type, a.message, a.severity,
                   a.triggered_date, a.acknowledged
            FROM watchlist_alerts a
            ORDER BY a.triggered_date DESC
        """)
        alerts = [dict(r) for r in cur.fetchall()]
        conn.close()

    return json.dumps(alerts, indent=2)


def alerts_to_csv(alerts: Optional[List[Dict[str, Any]]] = None, db_path: Optional[str] = None) -> str:
    """
    Export alerts to CSV string.
    If alerts is None, exports all (including acknowledged).
    """
    if alerts is None:
        conn = _get_conn(db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT a.id, a.item_id, a.alert_type, a.message, a.severity,
                   a.triggered_date, a.acknowledged
            FROM watchlist_alerts a
            ORDER BY a.triggered_date DESC
        """)
        alerts = [dict(r) for r in cur.fetchall()]
        conn.close()

    if not alerts:
        return "id,item_id,alert_type,message,severity,triggered_date,acknowledged"

    out = StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=["id", "item_id", "alert_type", "message", "severity", "triggered_date", "acknowledged"],
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(alerts)
    return out.getvalue()
