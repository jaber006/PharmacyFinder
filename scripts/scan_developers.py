#!/usr/bin/env python3
"""
scan_developers.py - CLI for the Developer Pipeline Tracker

Scans major Australian shopping centre developers for new projects
that could include pharmacy tenancies.

Usage:
    py -3.12 scripts/scan_developers.py --all
    py -3.12 scripts/scan_developers.py --developer oreana
    py -3.12 scripts/scan_developers.py --developer stockland
    py -3.12 scripts/scan_developers.py --top 20
    py -3.12 scripts/scan_developers.py --list-developers

Available developers:
    oreana, stockland, lendlease, frasers, vicinity,
    mab, villawood, scentre, qic, charterhall
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Fix Windows encoding for emoji/unicode output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from candidates.developer_pipeline import (
    run_pipeline,
    DEVELOPERS,
    DB_PATH,
)


def main():
    parser = argparse.ArgumentParser(
        description="Developer Pipeline Tracker — scan Australian shopping centre developers for pharmacy opportunities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  py -3.12 scripts/scan_developers.py --all              Scan all developers
  py -3.12 scripts/scan_developers.py --developer oreana  Scan Oreana only
  py -3.12 scripts/scan_developers.py --top 20            Show top 20 by score
  py -3.12 scripts/scan_developers.py --list-developers   List available developers
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Scan all developers")
    group.add_argument("--developer", type=str, help="Scan a specific developer (e.g. oreana, stockland)")
    group.add_argument("--list-developers", action="store_true", help="List available developer scrapers")

    parser.add_argument("--top", type=int, default=None, help="Show only top N results by score")
    parser.add_argument("--db", type=str, default=None, help="Path to pharmacy_finder.db (default: auto)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--quiet", "-q", action="store_true", help="Minimal output")

    args = parser.parse_args()

    # Setup logging
    if args.quiet:
        level = logging.WARNING
    elif args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # List developers
    if args.list_developers:
        print("\nAvailable developers:")
        print("=" * 40)
        for key in sorted(DEVELOPERS.keys()):
            print(f"  {key}")
        print(f"\nTotal: {len(DEVELOPERS)} developers")
        return

    # Resolve DB path
    db_path = Path(args.db) if args.db else DB_PATH

    # Run pipeline
    developer_filter = args.developer if args.developer else None
    top_n = args.top

    if args.all:
        print(f"\n🔍 Scanning ALL {len(DEVELOPERS)} developers...")
    else:
        print(f"\n🔍 Scanning developer: {args.developer}")

    projects = run_pipeline(
        developer_filter=developer_filter,
        top_n=top_n,
        db_path=db_path,
    )

    if not projects:
        print("\n⚠️  No projects found. Sites may have changed structure or be blocking requests.")
        print("   Try --verbose for details, or check network connectivity.")
        return

    # Print summary
    print(f"\n{'=' * 70}")
    print(f"📊 DEVELOPER PIPELINE RESULTS")
    print(f"{'=' * 70}")
    print(f"Total projects: {len(projects)}")
    print(f"Developers:     {len(set(p['developer'] for p in projects))}")

    scored = [p for p in projects if p.get("score", 0) > 0]
    print(f"Scored > 0:     {len(scored)}")

    high = [p for p in projects if p.get("score", 0) >= 50]
    if high:
        print(f"Score >= 50:    {len(high)} ⭐")

    # Print top results
    display = projects[:top_n] if top_n else projects[:15]
    if display:
        print(f"\n{'─' * 70}")
        print(f"{'#':>3} {'Score':>5}  {'Developer':<20} {'Project':<25} {'State':<5}")
        print(f"{'─' * 70}")
        for i, p in enumerate(display, 1):
            score = p.get("score", 0)
            star = "⭐" if score >= 50 else "  "
            print(f"{i:>3} {score:>5}  {p['developer']:<20} {p['project_name'][:24]:<25} "
                  f"{p.get('state', '?'):<5} {star}")

    print(f"\n📁 Output files:")
    print(f"   output/developer_pipeline.json")
    print(f"   output/developer_pipeline.csv")
    print(f"   output/developer_pipeline_report.md")
    print(f"   Database: developer_pipeline table")
    print()


if __name__ == "__main__":
    main()
