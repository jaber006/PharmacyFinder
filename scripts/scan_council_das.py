#!/usr/bin/env python3
"""
Council DA Scanner CLI.

Scan council development applications for a single state and optionally
evaluate them against pharmacy rules.

Usage:
  py -3.12 scripts/scan_council_das.py --state TAS
  py -3.12 scripts/scan_council_das.py --state NSW --evaluate

Requires PLANNING_ALERTS_KEY env var for PlanningAlerts API (free at planningalerts.org.au).
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from candidates.council_da import scan_state, get_das_for_evaluation
from candidates.da_evaluator import evaluate_all_das


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan council DAs for pharmacy opportunities")
    parser.add_argument("--state", required=True, help="State to scan (NSW, VIC, QLD, TAS, WA, SA)")
    parser.add_argument("--evaluate", action="store_true", help="Run rules evaluation on DAs with pharmacy_potential > 0.5")
    parser.add_argument("--db", default=None, help="Database path (default: pharmacy_finder.db)")
    args = parser.parse_args()

    state = args.state.upper()
    if state not in ("NSW", "VIC", "QLD", "TAS", "WA", "SA"):
        print(f"Error: State must be one of NSW, VIC, QLD, TAS, WA, SA (got {state})")
        return 1

    db_path = args.db
    if not db_path:
        db_path = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

    if not os.environ.get("PLANNING_ALERTS_KEY"):
        print("Warning: PLANNING_ALERTS_KEY not set. Get free key at https://www.planningalerts.org.au/api/howto")
        print("Continuing anyway (may get no results from PlanningAlerts)...")

    print(f"Scanning council DAs for {state}...")
    inserted = scan_state(state, db_path=db_path)
    print(f"  Stored {inserted} new DAs")

    das = get_das_for_evaluation(min_potential=0.5, state=state, db_path=db_path)
    print(f"  DAs with pharmacy_potential >= 0.5 (with coords): {len(das)}")

    if args.evaluate and das:
        print("\nEvaluating DAs against pharmacy rules (Items 130, 133, 134, 135, 136)...")
        results = evaluate_all_das(min_potential=0.5, state=state, db_path=db_path)
        for r in results:
            qual = ", ".join(r.get("qualifying_items", [])) or "none"
            conf = r.get("confidence", 0)
            print(f"  {r.get('da_number', '?')}: {qual} (confidence={conf:.2f})")
            if r.get("notes"):
                n = r["notes"].encode("ascii", "replace").decode("ascii")
                print(f"    -> {(n[:120] + '...') if len(n) > 120 else n}")
        passing = sum(1 for r in results if r.get("passed_any"))
        print(f"\n  {passing}/{len(results)} DAs would qualify under at least one rule (hypothetically)")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
