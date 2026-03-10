"""
Development Scanner - Cron / Scheduled Run
==========================================
Wrapper around development_scanner.py for daily/regular runs.
It executes a full scan and reports only NEW rows inserted into the DB.

Exit codes:
  0 = nothing new found
  1 = new developments found
  2 = error during scan
"""

import json
import os
import sys
import sqlite3
import logging
from datetime import datetime

from development_scanner import DevelopmentScanner, DB_PATH, OUTPUT_DIR, init_db, log as scanner_log

CRON_NEW_JSON = os.path.join(OUTPUT_DIR, "development_new_findings.json")
CRON_LOG = os.path.join(OUTPUT_DIR, "development_scanner_cron.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dev_scanner_cron")


def get_db_totals():
    """Return (total_developments, total_opportunities)."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM developments")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM developments WHERE is_opportunity = 1")
        opps = cur.fetchone()[0]
        conn.close()
        return total, opps
    except Exception:
        return 0, 0


def format_location(dev):
    parts = []
    if dev.get("location_suburb"):
        parts.append(dev["location_suburb"])
    if dev.get("location_state"):
        parts.append(dev["location_state"])
    return ", ".join(parts)


def main():
    notify_mode = "--notify" in sys.argv
    quiet_mode = "--quiet" in sys.argv

    if quiet_mode:
        logging.getLogger().setLevel(logging.WARNING)
        scanner_log.setLevel(logging.WARNING)

    init_db()
    before_total, before_opps = get_db_totals()
    log.info("Development Scanner (cron) start: %s", datetime.now().isoformat())
    log.info("DB before run: total=%d opportunities=%d", before_total, before_opps)

    try:
        scanner = DevelopmentScanner()
        scanner.run_all_scans()

        save_stats = scanner.save_to_db()
        scanner.save_to_files()

        new_devs = [d for d in scanner.developments if d.get("_db_action") == "inserted"]
        if not new_devs:
            # Fallback in case DB tracking field is not present
            new_devs = [d for d in scanner.developments if d.get("_is_new_to_db", False)]
        new_opps = [d for d in new_devs if d.get("is_opportunity")]

        after_total, after_opps = get_db_totals()

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        clean_new = [{k: v for k, v in d.items() if not k.startswith("_")} for d in new_devs]
        clean_new.sort(key=lambda x: x.get("relevance_score", 0), reverse=True)

        with open(CRON_NEW_JSON, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "scan_date": datetime.now().isoformat(),
                    "db_before_total": before_total,
                    "db_after_total": after_total,
                    "db_before_opportunities": before_opps,
                    "db_after_opportunities": after_opps,
                    "scanner_total_in_run": len(scanner.developments),
                    "db_inserted": save_stats.get("inserted", 0),
                    "db_updated": save_stats.get("updated", 0),
                    "db_errors": save_stats.get("errors", 0),
                    "new_count": len(clean_new),
                    "new_opportunities_count": len(new_opps),
                    "new_developments": clean_new,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )

        with open(CRON_LOG, "a", encoding="utf-8") as f:
            f.write(
                "%s | run_total=%d | db_inserted=%d | db_updated=%d | new_opps=%d | db_errors=%d\n"
                % (
                    datetime.now().isoformat(),
                    len(scanner.developments),
                    save_stats.get("inserted", 0),
                    save_stats.get("updated", 0),
                    len(new_opps),
                    save_stats.get("errors", 0),
                )
            )

        if clean_new:
            if notify_mode:
                print("PHARMACY FINDER - New Retail Developments")
                print("")
                print("%d new development(s), %d opportunities:" % (len(clean_new), len(new_opps)))
                print("")
                for d in clean_new[:10]:
                    score = d.get("relevance_score", 0)
                    suffix = " [OPPORTUNITY]" if d.get("is_opportunity") else ""
                    print("- [%.0f] %s%s" % (score, d["name"][:80], suffix))
                    loc = format_location(d)
                    if loc:
                        print("  Location: %s" % loc)
                    if d.get("nearest_pharmacy_km") is not None:
                        print("  Nearest pharmacy: %.1f km" % d["nearest_pharmacy_km"])
                if len(clean_new) > 10:
                    print("")
                    print("... and %d more (%s)" % (len(clean_new) - 10, CRON_NEW_JSON))
            elif not quiet_mode:
                print("")
                print("=" * 60)
                print("  CRON SCAN COMPLETE - %s" % datetime.now().strftime("%Y-%m-%d %H:%M"))
                print("=" * 60)
                print("  DB before:          %d developments (%d opportunities)" % (before_total, before_opps))
                print("  Scanned this run:   %d" % len(scanner.developments))
                print("  DB inserted new:    %d" % save_stats.get("inserted", 0))
                print("  DB updated:         %d" % save_stats.get("updated", 0))
                print("  NEW opportunities:  %d" % len(new_opps))
                print("  DB after:           %d developments (%d opportunities)" % (after_total, after_opps))
                print("  New findings JSON:  %s" % CRON_NEW_JSON)
                print("=" * 60)
                print("")
            return 1

        if notify_mode:
            print("No new retail developments found.")
        elif not quiet_mode:
            print("")
            print("Cron scan complete - no new developments.")
            print("Total in DB: %d developments, %d opportunities" % (after_total, after_opps))
            print("")
        return 0

    except Exception as exc:
        log.error("Scan failed: %s", exc, exc_info=True)
        if notify_mode:
            print("Development scanner error: %s" % str(exc))
        return 2


if __name__ == "__main__":
    sys.exit(main())
