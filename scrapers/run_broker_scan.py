#!/usr/bin/env python3
"""
Run Broker Scan
================
Simple runner script for the pharmacy broker scanner.
Executes a full scan, prints results, and saves a report.

Usage:
    python scrapers/run_broker_scan.py
    python scrapers/run_broker_scan.py --quiet
    python scrapers/run_broker_scan.py --db path/to/db.sqlite

Can also be imported:
    from scrapers.run_broker_scan import run_scan
    results = run_scan()
"""

import sys
import os
import argparse
from pathlib import Path
from datetime import datetime

# Ensure project root is on path
PROJECT_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from scrapers.broker_scanner import BrokerScanner, DB_PATH


def run_scan(db_path: Path = DB_PATH, verbose: bool = True) -> dict:
    """
    Run a full broker scan and return results.

    Args:
        db_path: Path to the SQLite database
        verbose: Whether to print progress/summary to console

    Returns:
        Dict with stats, new_listings, source_summary, report_path, errors
    """
    scanner = BrokerScanner(db_path=db_path)
    results = scanner.run_full_scan(verbose=verbose)
    return results


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Pharmacy Broker Scanner — Scan 12 Australian pharmacy broker sites for listings'
    )
    parser.add_argument(
        '--quiet', '-q',
        action='store_true',
        help='Suppress console output (still saves report)'
    )
    parser.add_argument(
        '--db',
        type=str,
        default=str(DB_PATH),
        help=f'Path to SQLite database (default: {DB_PATH})'
    )
    parser.add_argument(
        '--json',
        action='store_true',
        help='Output results as JSON'
    )

    args = parser.parse_args()

    verbose = not args.quiet
    db_path = Path(args.db)

    try:
        results = run_scan(db_path=db_path, verbose=verbose)

        if args.json:
            import json
            # Serialize results (convert Listing objects to dicts)
            output = {
                'scan_date': datetime.now().isoformat(),
                'stats': results['stats'],
                'new_listings': [
                    {
                        'source': l.source,
                        'title': l.title,
                        'price': l.price,
                        'price_bucket': l.price_bucket,
                        'location': l.location,
                        'state': l.state,
                        'url': l.url,
                        'description': l.description[:200],
                        'tags': l.tags,
                    }
                    for l in results.get('new_listings', [])
                ],
                'source_summary': results.get('source_summary', {}),
                'report_path': results.get('report_path', ''),
                'errors': results.get('errors', {}),
            }
            print(json.dumps(output, indent=2))

        # Exit code: 0 if successful, 1 if errors occurred
        if results.get('errors'):
            sys.exit(0)  # Still exit 0 — errors in individual scrapers are expected
        else:
            sys.exit(0)

    except Exception as e:
        print(f"\nFATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
