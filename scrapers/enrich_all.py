#!/usr/bin/env python3
"""
Master enrichment script — runs all 3 data scrapers in sequence.

Usage:
    python scrapers/enrich_all.py
    python scrapers/enrich_all.py --state TAS --dry-run
    python scrapers/enrich_all.py --limit 10 --verbose
"""

import argparse
import logging
import sys
import time
from datetime import datetime

log = logging.getLogger("enrich_all")


def run_scraper(name: str, run_fn, **kwargs) -> dict:
    """Run a single scraper with error handling."""
    log.info(f"\n{'='*60}")
    log.info(f"STARTING: {name}")
    log.info(f"{'='*60}")
    
    start = time.time()
    try:
        stats = run_fn(**kwargs)
        elapsed = time.time() - start
        log.info(f"✓ {name} completed in {elapsed:.1f}s")
        return stats or {}
    except Exception as e:
        elapsed = time.time() - start
        log.error(f"✗ {name} FAILED after {elapsed:.1f}s: {e}", exc_info=True)
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Run all data enrichment scrapers in sequence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrapers/enrich_all.py                    # Run all scrapers
  python scrapers/enrich_all.py --state TAS        # Only TAS records
  python scrapers/enrich_all.py --dry-run          # Preview without writing
  python scrapers/enrich_all.py --limit 5 -v       # Test with 5 records each
  python scrapers/enrich_all.py --only gp          # Run only GP scraper
  python scrapers/enrich_all.py --only hospitals    # Run only hospitals scraper
        """,
    )
    parser.add_argument("--state", type=str, help="Filter by state (e.g. TAS, VIC)")
    parser.add_argument("--limit", type=int, help="Max records per scraper")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    parser.add_argument("--only", type=str, choices=["gp", "shopping", "hospitals"],
                        help="Run only one scraper")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    kwargs = {
        "state": args.state,
        "limit": args.limit,
        "dry_run": args.dry_run,
    }

    overall_start = time.time()
    all_stats = {}

    log.info(f"PharmacyFinder Data Enrichment — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info(f"Options: state={args.state or 'ALL'}, limit={args.limit or 'ALL'}, "
             f"dry_run={args.dry_run}")

    # 1. Hotdoc GP Scraper
    if not args.only or args.only == "gp":
        from hotdoc_gp_scraper import run as gp_run
        all_stats["gp"] = run_scraper("Hotdoc GP Scraper", gp_run, **kwargs)

    # 2. Shopping Centre Scraper
    if not args.only or args.only == "shopping":
        from shopping_centre_scraper import run as shopping_run
        all_stats["shopping"] = run_scraper("Shopping Centre Scraper", shopping_run, **kwargs)

    # 3. MyHospitals Scraper
    if not args.only or args.only == "hospitals":
        from myhospitals_scraper import run as hospitals_run
        all_stats["hospitals"] = run_scraper("MyHospitals Scraper", hospitals_run, **kwargs)

    # Final summary
    total_elapsed = time.time() - overall_start
    
    log.info(f"\n{'='*60}")
    log.info(f"ENRICHMENT COMPLETE — {total_elapsed:.1f}s total")
    log.info(f"{'='*60}")
    
    for name, stats in all_stats.items():
        if "error" in stats:
            log.info(f"  {name:12s}: FAILED — {stats['error']}")
        else:
            updated = stats.get("updated", 0)
            skipped = stats.get("skipped", 0)
            failed = stats.get("failed", 0)
            log.info(f"  {name:12s}: {updated} updated, {skipped} skipped, {failed} failed")
    
    log.info(f"{'='*60}")

    # Exit with error if any scraper failed
    if any("error" in s for s in all_stats.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
