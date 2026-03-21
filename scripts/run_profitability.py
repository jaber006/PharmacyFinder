#!/usr/bin/env python3
"""
Pharmacy Profitability Analysis CLI.

Estimates revenue, GP, setup costs, payback, and flip profit for each
qualifying v2_results site. Outputs ranked table and saves to CSV.

Usage:
  py -3.12 scripts/run_profitability.py --top 20
  py -3.12 scripts/run_profitability.py --top 50 --db pharmacy_finder.db
"""
import argparse
import csv
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from analysis.profitability import analyze_all_sites, update_v2_results_profitability

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "profitability_analysis.csv")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pharmacy Profitability Estimator")
    parser.add_argument("--top", type=int, default=20, help="Number of top sites to display (default: 20)")
    parser.add_argument("--db", default=None, help="Database path (default: pharmacy_finder.db)")
    parser.add_argument("--no-update", action="store_true", help="Do not update v2_results with profitability_score")
    args = parser.parse_args()

    db_path = args.db or os.path.join(PROJECT_ROOT, "pharmacy_finder.db")
    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        return 1

    print("Running profitability analysis...")
    results = analyze_all_sites(db_path=db_path)

    if not results:
        print("No v2_results sites found.")
        return 0

    if not args.no_update:
        update_v2_results_profitability(results, db_path=db_path)
        print(f"Updated profitability_score for {len(results)} sites in v2_results")

    top = results[: args.top]
    print(f"\nTop {len(top)} sites by profitability score:")
    print("-" * 100)
    print(f"{'#':<3} {'Site':<35} {'Revenue':>12} {'GP':>10} {'Setup':>10} {'Payback':>8} {'Flip':>10} {'Score':>6}")
    print("-" * 100)

    for i, r in enumerate(top, 1):
        name = (r.get("name") or r.get("address") or r.get("id") or "?")[:34]
        name = name.encode("ascii", "replace").decode("ascii")
        rev = r.get("annual_revenue", 0)
        gp = r.get("annual_gp", 0)
        setup = r.get("setup_cost", 0)
        payback = r.get("payback_years", 0)
        flip = r.get("flip_profit", 0)
        score = r.get("profitability_score", 0)
        print(f"{i:<3} {name:<35} {rev:>12,.0f} {gp:>10,.0f} {setup:>10,.0f} {payback:>8.1f}y {flip:>10,.0f} {score:>6.1f}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    cols = [
        "id", "name", "address", "state", "primary_rule", "population_5km", "pharmacies_5km", "gps_2km",
        "estimated_scripts", "annual_revenue", "annual_gp", "setup_cost", "payback_years",
        "exit_value_12mo", "flip_profit", "profitability_score",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in results:
            row = {k: r.get(k) for k in cols}
            w.writerow(row)

    print("-" * 100)
    print(f"Saved to {OUTPUT_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
