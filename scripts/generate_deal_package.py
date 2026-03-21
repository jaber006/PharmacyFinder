#!/usr/bin/env python3
"""
CLI for generating Deal Package PDFs.

Usage:
    py -3.12 scripts/generate_deal_package.py --site-id supermarket_27601
    py -3.12 scripts/generate_deal_package.py --top 10
    py -3.12 scripts/generate_deal_package.py --site-id supermarket_27601 --db pharmacy_finder.db
"""
import argparse
import os
import sys
import time

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evidence.deal_package import generate_deal_package, get_top_sites


def main():
    parser = argparse.ArgumentParser(
        description="Generate Deal Package PDFs for pharmacy site opportunities",
    )
    parser.add_argument("--site-id", type=str, help="Site ID from v2_results table")
    parser.add_argument("--top", type=int, help="Generate for top N sites by profitability")
    parser.add_argument("--db", type=str, default="pharmacy_finder.db",
                        help="Path to pharmacy_finder.db (default: pharmacy_finder.db)")
    parser.add_argument("--output", type=str, default="output/deal_packages",
                        help="Output directory (default: output/deal_packages)")
    args = parser.parse_args()

    if not args.site_id and not args.top:
        parser.error("Specify either --site-id or --top")

    db_path = args.db
    if not os.path.exists(db_path):
        print(f"ERROR: Database not found: {db_path}")
        sys.exit(1)

    site_ids = []
    if args.site_id:
        site_ids = [args.site_id]
    elif args.top:
        site_ids = get_top_sites(db_path, args.top)
        print(f"Found {len(site_ids)} top sites")

    if not site_ids:
        print("No sites to process.")
        sys.exit(0)

    success = 0
    errors = 0
    for i, sid in enumerate(site_ids, 1):
        try:
            t0 = time.time()
            print(f"[{i}/{len(site_ids)}] Generating deal package for {sid}...", end=" ", flush=True)
            path = generate_deal_package(sid, db_path, args.output)
            elapsed = time.time() - t0
            print(f"OK ({elapsed:.1f}s) -> {path}")
            success += 1
        except Exception as e:
            print(f"FAILED: {e}")
            errors += 1

    print(f"\nDone: {success} generated, {errors} errors")
    if success > 0:
        print(f"PDFs saved to: {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
