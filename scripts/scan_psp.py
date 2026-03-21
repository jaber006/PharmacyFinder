#!/usr/bin/env python3
"""
scan_psp.py - CLI wrapper for the PSP Scanner

Scans Australian Precinct Structure Plans and growth area precincts to
identify planned town centres where a pharmacy could be needed.

Usage:
    py -3.12 scripts/scan_psp.py --national
    py -3.12 scripts/scan_psp.py --state VIC
    py -3.12 scripts/scan_psp.py --state NSW --top 20
    py -3.12 scripts/scan_psp.py --national --no-scrape
    py -3.12 scripts/scan_psp.py --report-only --top 50
"""

import sys
import os
import argparse

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from candidates.psp_scanner import run_scan, get_db, init_tables, generate_outputs


def main():
    parser = argparse.ArgumentParser(
        description="PSP Scanner — Find pharmacy opportunities in planned town centres",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  py -3.12 scripts/scan_psp.py --national          Scan all 6 states
  py -3.12 scripts/scan_psp.py --state VIC          Scan Victoria only
  py -3.12 scripts/scan_psp.py --state QLD --top 10 Scan QLD, report top 10
  py -3.12 scripts/scan_psp.py --national --no-scrape  Use curated data only
  py -3.12 scripts/scan_psp.py --report-only --top 50  Re-generate reports
        """,
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--national", action="store_true",
        help="Scan all states (VIC, NSW, QLD, WA, SA, TAS)",
    )
    group.add_argument(
        "--state", type=str, metavar="STATE",
        help="Scan a single state (e.g. VIC, NSW, QLD, WA, SA, TAS)",
    )
    group.add_argument(
        "--report-only", action="store_true",
        help="Skip scanning, just regenerate output files from existing DB data",
    )

    parser.add_argument(
        "--top", type=int, default=30, metavar="N",
        help="Number of top opportunities to feature in report (default: 30)",
    )
    parser.add_argument(
        "--no-scrape", action="store_true",
        help="Skip live web scraping (use curated data only — faster)",
    )

    args = parser.parse_args()

    if args.report_only:
        conn = get_db()
        init_tables(conn)
        generate_outputs(conn, top_n=args.top)
        conn.close()
        print(f"Reports regenerated with top {args.top} opportunities.")
        return

    if not args.national and not args.state:
        parser.print_help()
        print("\nError: specify --national, --state <STATE>, or --report-only")
        sys.exit(1)

    valid_states = {"VIC", "NSW", "QLD", "WA", "SA", "TAS"}
    if args.state and args.state.upper() not in valid_states:
        print(f"Error: invalid state '{args.state}'. Choose from: {', '.join(sorted(valid_states))}")
        sys.exit(1)

    run_scan(
        national=args.national,
        state=args.state,
        top_n=args.top,
        live_scrape=not args.no_scrape,
    )


if __name__ == "__main__":
    main()
