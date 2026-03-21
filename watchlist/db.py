"""
Database setup for watchlist tables.
Ensures watchlist_items, watchlist_alerts, and scan_snapshots exist.
"""
import os
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")


def ensure_watchlist_tables(db_path: str = None) -> None:
    """Create watchlist tables if they don't exist."""
    path = db_path or DB_PATH
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            candidate_id TEXT NOT NULL,
            watch_reason TEXT NOT NULL,
            trigger_condition TEXT NOT NULL,
            check_frequency TEXT DEFAULT 'weekly',
            last_checked TEXT,
            status TEXT DEFAULT 'watching',
            created_date TEXT NOT NULL,
            notes TEXT DEFAULT '',
            last_eval_json TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER,
            alert_type TEXT NOT NULL,
            message TEXT NOT NULL,
            severity TEXT DEFAULT 'medium',
            triggered_date TEXT NOT NULL,
            acknowledged INTEGER DEFAULT 0,
            FOREIGN KEY (item_id) REFERENCES watchlist_items(id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_type TEXT NOT NULL UNIQUE,
            entity_ids_json TEXT NOT NULL,
            scan_date TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
