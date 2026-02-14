"""
Scrape ALL Australian pharmacies using the FindAPharmacy scraper.

Phase 1 of the pipeline - comprehensive pharmacy data collection.
Run run_all_scans.py after this for the full opportunity scan.

Usage:
    python scrape_all_states.py                # scrape all states
    python scrape_all_states.py TAS VIC        # scrape specific states only
"""
import sys
import time
from utils.database import Database
from scrapers.findapharmacy import FindAPharmacyScraper
import config


def scrape_all_pharmacies(states=None):
    """Scrape all pharmacies across all (or specified) states."""
    all_states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'VIC', 'QLD', 'NSW']
    
    if states:
        for s in states:
            if s not in all_states:
                print(f"Unknown state: {s}")
                sys.exit(1)
    else:
        states = all_states

    db = Database(config.DATABASE_PATH)
    db.connect()

    if not states or states == all_states:
        # Full scrape: clear old pharmacy data first
        cursor = db.connection.cursor()
        cursor.execute("DELETE FROM pharmacies")
        db.connection.commit()
        print("Cleared old pharmacy data\n")

    scraper = FindAPharmacyScraper(db)

    for state in states:
        print(f"\n{'='*60}")
        print(f"  SCRAPING {state}")
        print(f"{'='*60}")
        scraper.scrape_all(state)
        time.sleep(1)

    # Summary
    cursor = db.connection.cursor()
    print(f"\n\n{'='*60}")
    print("  PHARMACY SCRAPING COMPLETE")
    print(f"{'='*60}")

    cursor.execute("SELECT COUNT(*) FROM pharmacies")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT state, COUNT(*) as cnt FROM pharmacies GROUP BY state ORDER BY cnt DESC")
    for row in cursor.fetchall():
        print(f"  {row[0] or 'Unknown':.<20} {row[1]:>5}")

    print(f"  {'TOTAL':.<20} {total:>5}")
    print(f"{'='*60}")

    db.close()
    return total


if __name__ == '__main__':
    states = [s.upper() for s in sys.argv[1:]] if len(sys.argv) > 1 else None
    total = scrape_all_pharmacies(states)
    print(f"\nDone. {total} pharmacies in database.")
