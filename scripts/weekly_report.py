#!/usr/bin/env python3
"""
PharmacyFinder Weekly Executive Report
Aggregates the week's daily summaries and generates a weekly report.
Saves to output/weekly_report.md

Usage: py -3.12 scripts/weekly_report.py [--week YYYY-MM-DD]
       (date = any day in the target week; defaults to current week)
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
DAILY_JSON = OUTPUT_DIR / "daily_summary.json"
WEEKLY_MD = OUTPUT_DIR / "weekly_report.md"
WEEKLY_JSON = OUTPUT_DIR / "weekly_report.json"
SNAPSHOTS_DIR = OUTPUT_DIR / "_snapshots"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_week_range(ref_date: datetime) -> tuple[datetime, datetime]:
    """Get Monday-Sunday range for the week containing ref_date."""
    monday = ref_date - timedelta(days=ref_date.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday


def load_daily_snapshots(week_start: datetime, week_end: datetime) -> list[dict]:
    """Load any daily summary snapshots from the week."""
    snapshots = []

    # Check _snapshots dir for dated files
    if SNAPSHOTS_DIR.exists():
        for f in sorted(SNAPSHOTS_DIR.glob("daily_summary_*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                gen = data.get("generated_at", "")
                if gen:
                    gen_dt = datetime.fromisoformat(gen.split("T")[0] if "T" in gen else gen[:10])
                    if week_start <= gen_dt <= week_end:
                        snapshots.append(data)
            except (json.JSONDecodeError, ValueError):
                continue

    # Also load current daily_summary.json if it falls within the week
    if DAILY_JSON.exists():
        try:
            data = json.loads(DAILY_JSON.read_text(encoding="utf-8"))
            gen = data.get("generated_at", "")
            if gen:
                gen_dt = datetime.fromisoformat(gen.split("T")[0] if "T" in gen else gen[:10])
                if week_start <= gen_dt <= week_end:
                    # Avoid duplicates
                    if not any(s.get("generated_at") == data.get("generated_at") for s in snapshots):
                        snapshots.append(data)
        except (json.JSONDecodeError, ValueError):
            pass

    return snapshots


def fetch_sites_by_date(conn: sqlite3.Connection, start: str, end: str) -> list[dict]:
    """Fetch v2_results evaluated within the date range."""
    rows = conn.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 AND date_evaluated >= ? AND date_evaluated <= ?",
        (start, end + "T23:59:59"),
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_all_qualifying(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all qualifying sites for current state."""
    rows = conn.execute(
        "SELECT * FROM v2_results WHERE passed_any = 1 ORDER BY commercial_score DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_scan_snapshots(conn: sqlite3.Connection) -> list[dict]:
    """Fetch scan snapshots for delta analysis."""
    rows = conn.execute(
        "SELECT * FROM scan_snapshots ORDER BY scan_date DESC LIMIT 10"
    ).fetchall()
    return [dict(r) for r in rows]


def parse_rules(rules_json: str) -> list[dict]:
    try:
        return json.loads(rules_json) if rules_json else []
    except (json.JSONDecodeError, TypeError):
        return []


def build_weekly_report(
    all_sites: list[dict],
    new_this_week: list[dict],
    daily_snapshots: list[dict],
    scan_snapshots: list[dict],
    week_start: datetime,
    week_end: datetime,
) -> dict:
    """Build weekly report data."""
    now = datetime.now()

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

    # Confidence distribution
    high_conf = [s for s in all_sites if s.get("best_confidence", 0) >= 0.85]
    med_conf = [s for s in all_sites if 0.5 <= s.get("best_confidence", 0) < 0.85]
    low_conf = [s for s in all_sites if s.get("best_confidence", 0) < 0.5]

    # Best new opportunities this week
    best_new = sorted(new_this_week, key=lambda x: x.get("commercial_score", 0), reverse=True)[:10]

    # Data quality: count sites with various data completeness
    has_address = sum(1 for s in all_sites if s.get("address"))
    has_coords = sum(1 for s in all_sites if s.get("latitude") and s.get("longitude"))
    has_rules = sum(1 for s in all_sites if s.get("rules_json"))

    # Recommended actions
    actions = []
    if best_new:
        top = best_new[0]
        actions.append(f"Investigate top new site: {top['name']} ({top['state']}) — {top['primary_rule']}")

    if high_conf:
        actions.append(f"Prioritise {len(high_conf)} high-confidence sites (>=85%) for on-ground verification")

    if len(new_this_week) == 0:
        actions.append("No new sites this week — consider expanding search parameters or running fresh scans")
    elif len(new_this_week) > 10:
        actions.append(f"{len(new_this_week)} new sites found — review and triage before next scan")

    actions.append("Review commercial property listings for top 5 sites")
    actions.append("Check watchlist items for any near-miss sites that may now qualify")

    return {
        "generated_at": now.isoformat(),
        "week": {
            "start": week_start.strftime("%Y-%m-%d"),
            "end": week_end.strftime("%Y-%m-%d"),
        },
        "summary": {
            "total_qualifying_sites": len(all_sites),
            "new_this_week": len(new_this_week),
            "daily_reports_generated": len(daily_snapshots),
        },
        "state_breakdown": state_counts,
        "rule_breakdown": rule_counts,
        "confidence_distribution": {
            "high_85plus": len(high_conf),
            "medium_50_85": len(med_conf),
            "low_below_50": len(low_conf),
        },
        "best_new_opportunities": [
            {
                "id": s["id"],
                "name": s["name"],
                "address": s["address"],
                "state": s["state"],
                "primary_rule": s["primary_rule"],
                "commercial_score": s["commercial_score"],
                "confidence": s.get("best_confidence", 0),
                "date_evaluated": s.get("date_evaluated"),
            }
            for s in best_new
        ],
        "data_quality": {
            "total_sites": len(all_sites),
            "with_address": has_address,
            "with_coordinates": has_coords,
            "with_rules_evaluated": has_rules,
            "completeness_pct": round(has_coords / max(len(all_sites), 1) * 100, 1),
        },
        "recommended_actions": actions,
    }


def render_weekly_markdown(report: dict) -> str:
    """Render weekly report as markdown."""
    lines = []
    week = report["week"]
    ws = datetime.strptime(week["start"], "%Y-%m-%d").strftime("%d %b")
    we = datetime.strptime(week["end"], "%Y-%m-%d").strftime("%d %b %Y")

    lines.append(f"# PharmacyFinder Weekly Report — {ws} to {we}")
    lines.append("")

    # Executive summary
    s = report["summary"]
    lines.append("## Executive Summary")
    lines.append(f"- **Total qualifying sites:** {s['total_qualifying_sites']}")
    lines.append(f"- **New sites this week:** {s['new_this_week']}")
    lines.append(f"- **Daily reports generated:** {s['daily_reports_generated']}")
    lines.append("")

    # State breakdown
    lines.append("## Sites by State")
    for state, count in sorted(report["state_breakdown"].items(), key=lambda x: -x[1]):
        bar = "█" * min(count, 40)
        lines.append(f"- **{state}:** {count} {bar}")
    lines.append("")

    # Rule breakdown
    lines.append("## Sites by Rule")
    for rule, count in sorted(report["rule_breakdown"].items(), key=lambda x: -x[1]):
        lines.append(f"- **{rule}:** {count}")
    lines.append("")

    # Confidence
    cd = report["confidence_distribution"]
    lines.append("## Confidence Distribution")
    lines.append(f"- **High (>=85%):** {cd['high_85plus']} -- ready for verification")
    lines.append(f"- **Medium (50-84%):** {cd['medium_50_85']} -- needs more data")
    lines.append(f"- **Low (<50%):** {cd['low_below_50']} -- early stage / speculative")
    lines.append("")

    # Best new opportunities
    best = report["best_new_opportunities"]
    if best:
        lines.append("## Best New Opportunities This Week")
        lines.append("")
        for i, opp in enumerate(best, 1):
            conf_pct = f"{opp['confidence'] * 100:.0f}%" if opp.get("confidence") else "N/A"
            lines.append(f"### {i}. {opp['name']}")
            lines.append(f"- **Address:** {opp['address']}")
            lines.append(f"- **State:** {opp['state']}")
            lines.append(f"- **Rule:** {opp['primary_rule']}")
            lines.append(f"- **Commercial Score:** {opp['commercial_score']:.4f}")
            lines.append(f"- **Confidence:** {conf_pct}")
            lines.append(f"- **Evaluated:** {opp.get('date_evaluated', 'N/A')}")
            lines.append("")
    else:
        lines.append("## Best New Opportunities This Week")
        lines.append("No new qualifying sites found this week.")
        lines.append("")

    # Data quality
    dq = report["data_quality"]
    lines.append("## Data Quality")
    lines.append(f"- **Sites with coordinates:** {dq['with_coordinates']}/{dq['total_sites']} ({dq['completeness_pct']}%)")
    lines.append(f"- **Sites with addresses:** {dq['with_address']}/{dq['total_sites']}")
    lines.append(f"- **Sites with rules evaluated:** {dq['with_rules_evaluated']}/{dq['total_sites']}")
    lines.append("")

    # Recommended actions
    lines.append("## Recommended Actions for MJ")
    lines.append("")
    for i, action in enumerate(report["recommended_actions"], 1):
        lines.append(f"{i}. {action}")
    lines.append("")

    # Footer
    lines.append("---")
    lines.append(f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} by PharmacyFinder weekly_report.py*")

    return "\n".join(lines)


def main():
    # Parse --week argument (any date in the target week)
    ref_date = datetime.now()
    if "--week" in sys.argv:
        idx = sys.argv.index("--week")
        if idx + 1 < len(sys.argv):
            ref_date = datetime.strptime(sys.argv[idx + 1], "%Y-%m-%d")

    week_start, week_end = get_week_range(ref_date)
    print(f"[weekly] Generating report for week {week_start.strftime('%Y-%m-%d')} to {week_end.strftime('%Y-%m-%d')}")

    conn = get_connection()

    # Fetch data
    print("[weekly] Fetching all qualifying sites...")
    all_sites = fetch_all_qualifying(conn)
    print(f"[weekly] {len(all_sites)} total qualifying sites")

    print("[weekly] Fetching sites evaluated this week...")
    new_this_week = fetch_sites_by_date(
        conn, week_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")
    )
    print(f"[weekly] {len(new_this_week)} sites evaluated this week")

    print("[weekly] Loading scan snapshots...")
    scan_snapshots = fetch_scan_snapshots(conn)

    conn.close()

    # Load daily snapshots
    print("[weekly] Loading daily summaries...")
    daily_snapshots = load_daily_snapshots(week_start, week_end)
    print(f"[weekly] {len(daily_snapshots)} daily summaries found")

    # Build report
    report = build_weekly_report(
        all_sites, new_this_week, daily_snapshots, scan_snapshots,
        week_start, week_end,
    )

    # Render
    md = render_weekly_markdown(report)

    # Save
    OUTPUT_DIR.mkdir(exist_ok=True)
    WEEKLY_MD.write_text(md, encoding="utf-8")
    WEEKLY_JSON.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print(f"[weekly] Saved {WEEKLY_MD}")
    print(f"[weekly] Saved {WEEKLY_JSON}")

    # Archive daily summary as snapshot for future weeks
    if DAILY_JSON.exists():
        SNAPSHOTS_DIR.mkdir(exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        snapshot_path = SNAPSHOTS_DIR / f"daily_summary_{today}.json"
        if not snapshot_path.exists():
            import shutil
            shutil.copy2(str(DAILY_JSON), str(snapshot_path))
            print(f"[weekly] Archived daily snapshot to {snapshot_path}")

    # Print summary
    print()
    print(f"=== Weekly Report: {week_start.strftime('%d %b')} – {week_end.strftime('%d %b %Y')} ===")
    print(f"Total qualifying sites: {len(all_sites)}")
    print(f"New this week: {len(new_this_week)}")
    print()
    print("Recommended actions:")
    for i, a in enumerate(report["recommended_actions"], 1):
        print(f"  {i}. {a}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
