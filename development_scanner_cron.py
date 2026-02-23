"""
Development Scanner - Cron / Daily Version
============================================
Lightweight version of development_scanner.py designed to run daily.
Compares against previous results and only outputs NEW findings.

Exit codes:
  0 = nothing new found
  1 = new opportunities found (suitable for notification trigger)

Usage:
  python development_scanner_cron.py
  python development_scanner_cron.py --notify   (print notification-friendly output)
"""

import json
import os
import sys
import logging
from datetime import datetime

# Import the main scanner
from development_scanner import (
    DevelopmentScanner, OUTPUT_JSON, OUTPUT_DIR, OUTPUT_CSV,
    log as scanner_log,
)

# Output paths for cron
CRON_NEW_JSON = os.path.join(OUTPUT_DIR, "development_new_findings.json")
CRON_LOG = os.path.join(OUTPUT_DIR, "development_scanner_cron.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dev_scanner_cron")


def load_previous_ids():
    """Load IDs from the previous scan results."""
    if not os.path.exists(OUTPUT_JSON):
        return set()
    try:
        with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {d["id"] for d in data.get("developments", [])}
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log.warning("Could not load previous results: %s", e)
        return set()


def main():
    notify_mode = "--notify" in sys.argv

    log.info("Development Scanner (cron) starting...")
    log.info("Timestamp: %s", datetime.now().isoformat())

    # Load previous IDs
    previous_ids = load_previous_ids()
    log.info("Previous scan had %d developments", len(previous_ids))

    # Run full scan
    scanner = DevelopmentScanner()
    scanner.run_all_scans()

    # Find new developments
    new_devs = [d for d in scanner.developments if d["id"] not in previous_ids]
    new_opps = [d for d in new_devs if d["is_opportunity"]]

    log.info("Current scan: %d total, %d new, %d new opportunities",
             len(scanner.developments), len(new_devs), len(new_opps))

    # Save full results (overwrite previous)
    scanner.save_results()

    # Save new-only findings
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(CRON_NEW_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "scan_date": datetime.now().isoformat(),
            "previous_count": len(previous_ids),
            "current_count": len(scanner.developments),
            "new_count": len(new_devs),
            "new_opportunities_count": len(new_opps),
            "new_developments": new_devs,
        }, f, indent=2, ensure_ascii=False)

    # Append to cron log
    with open(CRON_LOG, "a", encoding="utf-8") as f:
        f.write("%s | total=%d | new=%d | new_opps=%d\n" % (
            datetime.now().isoformat(), len(scanner.developments),
            len(new_devs), len(new_opps)))

    # Output
    if new_devs:
        if notify_mode:
            # Notification-friendly output
            print("PHARMACY FINDER - New Retail Developments Detected")
            print("")
            print("%d new development(s) found, %d are greenfield opportunities:" % (
                len(new_devs), len(new_opps)))
            print("")
            for d in sorted(new_devs, key=lambda x: -x["priority_score"])[:10]:
                flag = " [OPPORTUNITY]" if d["is_opportunity"] else ""
                print("  * %s (%s)%s" % (d["name"][:60], d["type"], flag))
                if d["address"]:
                    print("    Location: %s" % d["address"][:60])
                if d["nearest_pharmacy_km"] is not None:
                    print("    Nearest pharmacy: %.1f km" % d["nearest_pharmacy_km"])
            if len(new_devs) > 10:
                print("")
                print("  ... and %d more. See full report." % (len(new_devs) - 10))
        else:
            print("")
            print("=" * 60)
            print("  CRON SCAN COMPLETE - %s" % datetime.now().strftime("%Y-%m-%d %H:%M"))
            print("=" * 60)
            print("")
            print("  Previous developments: %d" % len(previous_ids))
            print("  Current developments:  %d" % len(scanner.developments))
            print("  NEW developments:      %d" % len(new_devs))
            print("  NEW opportunities:     %d" % len(new_opps))
            print("")

            if new_opps:
                print("  New Greenfield Opportunities:")
                for i, d in enumerate(new_opps[:10], 1):
                    print("    %d. [Score %d] %s" % (
                        i, d["priority_score"], d["name"][:60]))
                    if d["nearest_pharmacy_km"] is not None:
                        print("       %.1f km from nearest pharmacy" % d["nearest_pharmacy_km"])
            elif new_devs:
                print("  New Developments (no greenfield opportunities):")
                for i, d in enumerate(new_devs[:5], 1):
                    print("    %d. %s (%s)" % (i, d["name"][:60], d["type"]))

            print("")
            print("  Full results: %s" % OUTPUT_JSON)
            print("  New findings: %s" % CRON_NEW_JSON)
            print("=" * 60)

        # Exit 1 = new findings
        return 1
    else:
        if notify_mode:
            print("No new retail developments found.")
        else:
            print("")
            print("Cron scan complete - no new developments found.")
            print("Total tracked: %d" % len(scanner.developments))
            print("")

        # Exit 0 = nothing new
        return 0


if __name__ == "__main__":
    sys.exit(main())
