"""
Run opportunity zone scans for all Australian states/territories.

Prerequisites: pharmacy data should already be in the DB
(run scrape_all_states.py first, or the scan will scrape per-state).

Usage:
    python run_all_scans.py               # scan all states
    python run_all_scans.py TAS VIC NSW   # scan specific states
"""
import sys
import time
from main import PharmacyLocationFinder


def run_all_scans(states=None):
    """Run scans for all (or specified) states."""
    all_states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'VIC', 'QLD', 'NSW']

    if states:
        for s in states:
            if s not in all_states:
                print(f"Unknown state: {s}")
                sys.exit(1)
    else:
        states = all_states

    finder = PharmacyLocationFinder()

    try:
        all_results = {}
        for state in states:
            print(f"\n\n{'#'*60}")
            print(f"  STARTING SCAN: {state}")
            print(f"{'#'*60}")

            finder.collect_reference_data(state)
            opps = finder.scan_opportunities(state)
            all_results[state] = len(opps)

            time.sleep(2)  # Be nice to OSM Overpass API

        # Final summary
        print(f"\n\n{'='*60}")
        print("  ALL SCANS COMPLETE")
        print(f"{'='*60}")
        total_opps = 0
        for state, count in all_results.items():
            print(f"  {state:.<20} {count:>5} opportunities")
            total_opps += count
        print(f"  {'TOTAL':.<20} {total_opps:>5}")
        print(f"{'='*60}")

        finder.show_stats()

    finally:
        finder.close()


if __name__ == '__main__':
    states = [s.upper() for s in sys.argv[1:]] if len(sys.argv) > 1 else None
    run_all_scans(states)
