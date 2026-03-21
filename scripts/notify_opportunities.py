#!/usr/bin/env python3
"""
PharmacyFinder Daily Notification Script
Generates a daily summary of opportunities, watchlist alerts, and commercial matches.
Saves to output/daily_summary.md and output/daily_summary.json

Usage: py -3.12 scripts/notify_opportunities.py [--since YYYY-MM-DD]
"""

import sqlite3
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Project root
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "pharmacy_finder.db"
OUTPUT_DIR = ROOT / "output"
STATE_FILE = OUTPUT_DIR / "notify_state.json"
MINISTERIAL_FILE = OUTPUT_DIR / "ministerial_candidates.json"
DAILY_MD = OUTPUT_DIR / "daily_summary.md"
DAILY_JSON = OUTPUT_DIR / "daily_summary.json"


def load_state() -> dict:
    """Load last run state."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}


def save_state(state: dict):
    """Save run state."""
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def fetch_v2_results(conn: sqlite3.Connection, since: str | None = None) -> list[dict]:
    """Fetch qualifying v2_results, optionally filtered by date."""
    query = "SELECT * FROM v2_results WHERE passed_any = 1"
    params = []
    if since:
        query += " AND date_evaluated >= ?"
        params.append(since)
    query += " ORDER BY commercial_score DESC"
    rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def fetch_all_v2(conn: sqlite3.Connection) -> list[dict]:
    """Fetch ALL qualifying v2_results for ranking."""
    rows = conn.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 ORDER BY commercial_score DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_watchlist(conn: sqlite3.Connection) -> tuple[list[dict], list[dict]]:
    """Fetch watchlist items and any alerts."""
    items = [dict(r) for r in conn.execute("SELECT * FROM watchlist_items").fetchall()]
    alerts = [dict(r) for r in conn.execute(
        "SELECT * FROM watchlist_alerts ORDER BY triggered_date DESC"
    ).fetchall()]
    return items, alerts


def fetch_commercial_matches(conn: sqlite3.Connection) -> list[dict]:
    """Fetch commercial property matches if table has data."""
    try:
        rows = conn.execute(
            "SELECT * FROM commercial_matches ORDER BY distance_to_site_m ASC"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def fetch_commercial_properties(conn: sqlite3.Connection) -> list[dict]:
    """Fetch from commercial_properties as fallback."""
    try:
        rows = conn.execute(
            "SELECT * FROM commercial_properties ORDER BY suitability_score DESC LIMIT 20"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def load_ministerial_candidates() -> dict | None:
    """Load ministerial candidates JSON if it exists."""
    if MINISTERIAL_FILE.exists():
        return json.loads(MINISTERIAL_FILE.read_text(encoding="utf-8"))
    return None


def parse_rules(rules_json: str) -> list[dict]:
    """Parse rules JSON safely."""
    try:
        return json.loads(rules_json) if rules_json else []
    except (json.JSONDecodeError, TypeError):
        return []


def get_next_steps(result: dict) -> list[str]:
    """Generate actionable next steps for an opportunity."""
    steps = []
    rules = parse_rules(result.get("rules_json", "[]"))

    for rule in rules:
        if rule.get("passed"):
            evidence = rule.get("evidence_needed", [])
            for e in evidence[:2]:  # Top 2 evidence items
                steps.append(e)

    if result.get("commercial_score", 0) > 0.4:
        steps.append("Search for commercial leases near this site")

    if not steps:
        steps.append("Verify site eligibility with on-ground assessment")

    return steps[:3]  # Max 3 steps


def build_summary(
    new_sites: list[dict],
    all_sites: list[dict],
    watchlist_items: list[dict],
    watchlist_alerts: list[dict],
    commercial_matches: list[dict],
    commercial_properties: list[dict],
    ministerial: dict | None,
    since: str | None,
) -> dict:
    """Build the summary data structure."""
    now = datetime.now().isoformat()

    # State breakdown
    state_counts = {}
    for s in all_sites:
        st = s.get("state", "Unknown")
        state_counts[st] = state_counts.get(st, 0) + 1

    # Rule breakdown
    rule_counts = {}
    for s in all_sites:
        rule = s.get("primary_rule", "Unknown")
        rule_counts[rule] = rule_counts.get(rule, 0) + 1

    # Top 5 opportunities (from all sites, ranked by commercial_score)
    top5 = []
    for s in all_sites[:5]:
        rules = parse_rules(s.get("rules_json", "[]"))
        confidence = max((r.get("confidence", 0) for r in rules), default=0)
        top5.append({
            "rank": len(top5) + 1,
            "id": s["id"],
            "name": s["name"],
            "address": s["address"],
            "state": s["state"],
            "primary_rule": s["primary_rule"],
            "commercial_score": s["commercial_score"],
            "confidence": confidence,
            "next_steps": get_next_steps(s),
        })

    # Unacknowledged alerts
    active_alerts = [a for a in watchlist_alerts if not a.get("acknowledged")]

    # Ministerial summary
    ministerial_summary = None
    if ministerial:
        candidates = ministerial.get("ministerial_candidates", [])
        ministerial_summary = {
            "total_candidates": len(candidates),
            "scan_date": ministerial.get("scan_date"),
            "top_candidates": [
                {
                    "name": c.get("name", ""),
                    "address": c.get("address", ""),
                    "state": c.get("state", ""),
                    "primary_rule": c.get("primary_rule", ""),
                    "confidence": c.get("best_confidence", 0),
                }
                for c in candidates[:5]
            ],
        }

    return {
        "generated_at": now,
        "since": since,
        "total_qualifying_sites": len(all_sites),
        "new_sites_since_last": len(new_sites),
        "state_breakdown": state_counts,
        "rule_breakdown": rule_counts,
        "top_5_opportunities": top5,
        "watchlist": {
            "items_watching": len(watchlist_items),
            "active_alerts": len(active_alerts),
            "alerts": active_alerts[:10],
        },
        "commercial": {
            "direct_matches": len(commercial_matches),
            "property_listings": len(commercial_properties),
            "top_matches": [
                {
                    "site_id": m.get("site_id") or m.get("opportunity_id"),
                    "address": m.get("address") or m.get("property_address"),
                    "rent": m.get("rent_annual") or m.get("rent_display"),
                    "area_sqm": m.get("floor_area_sqm"),
                    "distance_m": m.get("distance_to_site_m") or m.get("distance_km"),
                }
                for m in (commercial_matches or commercial_properties)[:5]
            ],
        },
        "ministerial": ministerial_summary,
    }


def render_markdown(summary: dict) -> str:
    """Render summary as clean markdown."""
    lines = []
    now_str = datetime.now().strftime("%A %d %B %Y")
    lines.append(f"# PharmacyFinder Daily Summary — {now_str}")
    lines.append("")

    # Overview
    lines.append("## Overview")
    lines.append(f"- **Total qualifying sites:** {summary['total_qualifying_sites']}")
    lines.append(f"- **New since last run:** {summary['new_sites_since_last']}")
    if summary["since"]:
        lines.append(f"- **Period:** since {summary['since']}")
    lines.append("")

    # State breakdown
    lines.append("### By State")
    for state, count in sorted(summary["state_breakdown"].items(), key=lambda x: -x[1]):
        lines.append(f"- **{state}:** {count}")
    lines.append("")

    # Rule breakdown
    lines.append("### By Rule")
    for rule, count in sorted(summary["rule_breakdown"].items(), key=lambda x: -x[1]):
        lines.append(f"- **{rule}:** {count}")
    lines.append("")

    # Top 5
    lines.append("## Top 5 Opportunities")
    lines.append("")
    for opp in summary["top_5_opportunities"]:
        conf_pct = f"{opp['confidence'] * 100:.0f}%" if opp['confidence'] else "N/A"
        lines.append(f"### {opp['rank']}. {opp['name']}")
        lines.append(f"- **Address:** {opp['address']}")
        lines.append(f"- **State:** {opp['state']}")
        lines.append(f"- **Rule:** {opp['primary_rule']}")
        lines.append(f"- **Commercial Score:** {opp['commercial_score']:.4f}")
        lines.append(f"- **Confidence:** {conf_pct}")
        lines.append("- **Next Steps:**")
        for step in opp["next_steps"]:
            lines.append(f"  - {step}")
        lines.append("")

    # Watchlist
    wl = summary["watchlist"]
    lines.append("## Watchlist")
    lines.append(f"- **Items watching:** {wl['items_watching']}")
    lines.append(f"- **Active alerts:** {wl['active_alerts']}")
    if wl["alerts"]:
        lines.append("")
        for alert in wl["alerts"]:
            sev = alert.get("severity", "info").upper()
            lines.append(f"- [{sev}] {alert.get('message', 'No message')} ({alert.get('triggered_date', '')})")
    else:
        lines.append("- No new alerts triggered.")
    lines.append("")

    # Commercial matches
    cm = summary["commercial"]
    lines.append("## Commercial Properties")
    lines.append(f"- **Direct matches:** {cm['direct_matches']}")
    lines.append(f"- **Listed properties near sites:** {cm['property_listings']}")
    if cm["top_matches"]:
        lines.append("")
        for m in cm["top_matches"]:
            rent_str = m.get("rent") or "Contact agent"
            area_str = f"{m['area_sqm']}sqm" if m.get("area_sqm") else "N/A"
            lines.append(f"- **{m.get('address', 'Unknown')}** — {rent_str}, {area_str}")
    lines.append("")

    # Ministerial
    if summary.get("ministerial"):
        ms = summary["ministerial"]
        lines.append("## Ministerial Pathway Candidates")
        lines.append(f"- **Total candidates:** {ms['total_candidates']}")
        lines.append(f"- **Last scan:** {ms.get('scan_date', 'Unknown')}")
        if ms.get("top_candidates"):
            lines.append("")
            for c in ms["top_candidates"]:
                conf_pct = f"{c['confidence'] * 100:.0f}%" if c.get("confidence") else "N/A"
                lines.append(f"- **{c['name']}** ({c['state']}) — {c['primary_rule']}, confidence {conf_pct}")
        lines.append("")

    # Footer
    lines.append("---")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by PharmacyFinder notify_opportunities.py*")

    return "\n".join(lines)


def main():
    # Parse --since argument
    since = None
    if "--since" in sys.argv:
        idx = sys.argv.index("--since")
        if idx + 1 < len(sys.argv):
            since = sys.argv[idx + 1]

    # Load previous state for delta detection
    state = load_state()
    if not since:
        since = state.get("last_run")

    print(f"[notify] Connecting to {DB_PATH}")
    conn = get_connection()

    # Fetch data
    print("[notify] Fetching v2_results...")
    new_sites = fetch_v2_results(conn, since) if since else []
    all_sites = fetch_all_v2(conn)
    print(f"[notify] {len(all_sites)} total qualifying sites, {len(new_sites)} new since {since or 'beginning'}")

    print("[notify] Fetching watchlist...")
    watchlist_items, watchlist_alerts = fetch_watchlist(conn)
    print(f"[notify] {len(watchlist_items)} watchlist items, {len(watchlist_alerts)} alerts")

    print("[notify] Fetching commercial matches...")
    commercial_matches = fetch_commercial_matches(conn)
    commercial_properties = fetch_commercial_properties(conn)
    print(f"[notify] {len(commercial_matches)} direct matches, {len(commercial_properties)} property listings")

    print("[notify] Loading ministerial candidates...")
    ministerial = load_ministerial_candidates()
    if ministerial:
        mc = ministerial.get("ministerial_candidates", [])
        print(f"[notify] {len(mc)} ministerial candidates found")

    conn.close()

    # Build summary
    summary = build_summary(
        new_sites, all_sites, watchlist_items, watchlist_alerts,
        commercial_matches, commercial_properties, ministerial, since,
    )

    # Render markdown
    md = render_markdown(summary)

    # Save outputs
    OUTPUT_DIR.mkdir(exist_ok=True)
    DAILY_MD.write_text(md, encoding="utf-8")
    DAILY_JSON.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    print(f"[notify] Saved {DAILY_MD}")
    print(f"[notify] Saved {DAILY_JSON}")

    # Update state
    save_state({
        "last_run": datetime.now().isoformat(),
        "total_sites": len(all_sites),
        "new_sites": len(new_sites),
    })

    # Print quick summary to stdout
    print()
    print(f"=== PharmacyFinder Daily Summary ===")
    print(f"Qualifying sites: {len(all_sites)}")
    print(f"New since last run: {len(new_sites)}")
    print(f"Watchlist alerts: {len(watchlist_alerts)}")
    print(f"Commercial matches: {len(commercial_matches) + len(commercial_properties)}")
    if summary.get("ministerial"):
        print(f"Ministerial candidates: {summary['ministerial']['total_candidates']}")
    print()
    print("Top 5:")
    for opp in summary["top_5_opportunities"]:
        print(f"  {opp['rank']}. {opp['name']} ({opp['state']}) — {opp['primary_rule']}, score {opp['commercial_score']:.4f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
