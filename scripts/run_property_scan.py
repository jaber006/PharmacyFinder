"""
run_property_scan.py - CLI entry point for commercial property scanning.

Usage:
    py -3.12 scripts/run_property_scan.py                  # All qualifying sites
    py -3.12 scripts/run_property_scan.py --state TAS       # Single state
    py -3.12 scripts/run_property_scan.py --top 20          # Top 20 sites only
    py -3.12 scripts/run_property_scan.py --match-only      # Skip scraping, just re-score
    py -3.12 scripts/run_property_scan.py --state TAS --top 5 --match-only
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from candidates.commercial_re import run_scan, init_db, DB_PATH
from candidates.matcher import match_sites_to_properties, print_ranked_results, export_csv


def main():
    parser = argparse.ArgumentParser(
        description="Scan for commercial leases near qualifying pharmacy sites"
    )
    parser.add_argument(
        "--state", type=str, default=None,
        help="Filter by state (e.g., TAS, NSW, VIC)"
    )
    parser.add_argument(
        "--top", type=int, default=None,
        help="Only process top N sites by commercial score"
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="Path to pharmacy_finder.db (default: auto-detect)"
    )
    parser.add_argument(
        "--match-only", action="store_true",
        help="Skip scraping, only re-score existing matches"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output CSV path (default: output/commercial_matches.csv)"
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Number of results to display (default: 50)"
    )

    args = parser.parse_args()
    db_path = args.db or str(DB_PATH)

    print(f"PharmacyFinder - Commercial Property Scanner")
    print(f"{'='*50}")
    print(f"Database: {db_path}")
    if args.state:
        print(f"State filter: {args.state}")
    if args.top:
        print(f"Top sites: {args.top}")
    print()

    # Step 1: Scrape (unless --match-only)
    if not args.match_only:
        print("STEP 1: Scanning for commercial leases...")
        print("-" * 50)
        init_db(db_path)
        total = run_scan(db_path, state=args.state, top_n=args.top)
        print(f"\nScan complete: {total} listings found")
    else:
        print("STEP 1: Skipped (--match-only)")

    # Step 2: Score and rank
    print(f"\nSTEP 2: Scoring and ranking matches...")
    print("-" * 50)
    results = match_sites_to_properties(db_path, state=args.state, top_n=args.top)

    if results:
        # Print to console
        print_ranked_results(results, limit=args.limit)

        # Export CSV
        csv_path = export_csv(results, output_path=args.output)
        print(f"\nDone! Results saved to: {csv_path}")
    else:
        print("\nNo matches found. Run without --match-only to scrape listings first.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
