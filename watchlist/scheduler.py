"""
Automated re-checks for watchlist items using APScheduler.

Gets items due for check, re-evaluates them via the rules engine,
and updates last_checked. Uses cron-like scheduling.
"""
import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in __import__("sys").path:
    __import__("sys").path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

# Frequency in days
FREQUENCY_DAYS = {"daily": 1, "weekly": 7, "monthly": 30}


def _get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_items_due_for_check(db_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Return watchlist items where last_checked + frequency < now.
    Items with status 'watching' only.
    """
    from watchlist.db import ensure_watchlist_tables
    ensure_watchlist_tables(db_path)
    conn = _get_conn(db_path)
    cur = conn.cursor()
    today = date.today()

    cur.execute("""
        SELECT id, candidate_id, watch_reason, trigger_condition, check_frequency,
               last_checked, status, created_date, notes, last_eval_json
        FROM watchlist_items
        WHERE status = 'watching'
    """)
    rows = cur.fetchall()
    conn.close()

    due = []
    for r in rows:
        last = r["last_checked"]
        if isinstance(last, str):
            last = date.fromisoformat(last) if last else date.min
        freq_days = FREQUENCY_DAYS.get(r["check_frequency"] or "weekly", 7)
        if last + timedelta(days=freq_days) <= today:
            due.append(dict(r))

    return due


def _candidate_from_v2_or_opportunities(
    conn: sqlite3.Connection,
    candidate_id: str,
) -> Optional[Dict[str, Any]]:
    """Build a candidate dict for evaluation from v2_results or opportunities."""
    cur = conn.cursor()

    # Try v2_results first
    cur.execute("SELECT * FROM v2_results WHERE id = ?", (candidate_id,))
    row = cur.fetchone()
    if row is not None:
        r = dict(row)
        return {
            "id": r.get("id") or candidate_id,
            "latitude": r.get("latitude"),
            "longitude": r.get("longitude"),
            "name": r.get("name", ""),
            "address": r.get("address", ""),
            "source_type": r.get("source_type", ""),
            "source_id": None,
            "state": r.get("state", ""),
            "pop_10km": 0,
            "growth_indicator": "",
        }

    # Reconstruct from source tables by parsing candidate_id (e.g. supermarket_123, medical_centre_5)
    parts = candidate_id.rpartition("_")
    if not parts[0] or not parts[2]:
        return None
    prefix, sid = parts[0], parts[2]
    try:
        source_id = int(sid)
    except ValueError:
        return None

    if prefix == "supermarket":
        cur.execute("SELECT * FROM supermarkets WHERE id = ?", (source_id,))
    elif prefix == "medical_centre":
        cur.execute("SELECT * FROM medical_centres WHERE id = ?", (source_id,))
    elif prefix == "shopping_centre":
        cur.execute("SELECT * FROM shopping_centres WHERE id = ?", (source_id,))
    elif prefix == "hospital":
        cur.execute("SELECT * FROM hospitals WHERE id = ?", (source_id,))
    elif prefix == "gap":
        return None  # Gap candidates need special handling
    else:
        return None

    row = cur.fetchone()
    if row is None:
        return None
    r = dict(row)
    return {
        "id": candidate_id,
        "latitude": r.get("latitude"),
        "longitude": r.get("longitude"),
        "name": r.get("name", ""),
        "address": r.get("address", ""),
        "source_type": prefix,
        "source_id": source_id,
        "state": r.get("state", ""),
        "pop_10km": 0,
        "growth_indicator": "",
    }


def re_evaluate_item(
    item: Dict[str, Any],
    context: Any,
    db_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Run rules engine on watched candidate. Returns new EvaluationResult as dict,
    or None if candidate could not be loaded. Updates last_checked and last_eval_json.
    """
    from engine.models import Candidate, EvaluationResult
    from engine.evaluator import evaluate_candidate
    from engine.scoring import score_commercial
    from watchlist.monitor import detect_threshold_crossings

    conn = _get_conn(db_path)
    cand_dict = _candidate_from_v2_or_opportunities(conn, item["candidate_id"])
    conn.close()

    if cand_dict is None or cand_dict.get("latitude") is None or cand_dict.get("longitude") is None:
        return None

    candidate = Candidate(
        id=cand_dict["id"],
        latitude=cand_dict["latitude"],
        longitude=cand_dict["longitude"],
        name=cand_dict.get("name", ""),
        address=cand_dict.get("address", ""),
        source_type=cand_dict.get("source_type", ""),
        source_id=cand_dict.get("source_id"),
        state=cand_dict.get("state", ""),
        pop_10km=cand_dict.get("pop_10km", 0),
        growth_indicator=cand_dict.get("growth_indicator", ""),
    )

    result = evaluate_candidate(candidate, context)
    if result.passed_any:
        result.commercial_score = score_commercial(result, context)

    new_eval = result.to_dict()

    # Compare with previous evaluation
    old_eval_json = item.get("last_eval_json")
    if old_eval_json:
        try:
            old_eval = json.loads(old_eval_json)
            detect_threshold_crossings(
                item["candidate_id"],
                old_eval,
                new_eval,
                db_path=db_path,
                watchlist_item_id=item.get("id"),
            )
        except (json.JSONDecodeError, TypeError):
            pass

    # Update watchlist_items: last_checked, last_eval_json
    conn = _get_conn(db_path)
    conn.execute(
        "UPDATE watchlist_items SET last_checked = ?, last_eval_json = ? WHERE id = ?",
        (date.today().isoformat(), json.dumps(new_eval), item["id"]),
    )
    conn.commit()
    conn.close()

    return new_eval


def start_scheduler(db_path: Optional[str] = None, interval_hours: int = 6):
    """
    Start APScheduler to run watchlist checks at interval_hours.
    Call this from main app to run in background.
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from engine.context import EvaluationContext
    except ImportError:
        raise ImportError("pip install apscheduler")

    def _job():
        context = EvaluationContext(db_path=db_path or DB_PATH)
        items = get_items_due_for_check(db_path)
        for item in items:
            try:
                re_evaluate_item(item, context, db_path)
            except Exception as e:
                # Log but continue
                print(f"[Watchlist] Error re-evaluating {item.get('candidate_id')}: {e}")

    scheduler = BackgroundScheduler()
    scheduler.add_job(_job, "interval", hours=interval_hours, id="watchlist_checks")
    scheduler.start()
    return scheduler
