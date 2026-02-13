"""
Pharmacy Location Finder — Find where in Australia new pharmacies can be
opened under the Pharmacy Location Rules.

Two modes:
  1. **scan** (NEW — default) — proactively discover opportunity zones by
     running the Location Rules in reverse across the whole map.
  2. **check** (legacy) — check a set of commercial properties against the
     rules.

Usage examples:
    # Scan Tasmania for opportunity zones (the new way)
    python main.py scan --region TAS

    # Scan and also cross-reference with commercial RE listings
    python main.py scan --region TAS --with-properties

    # Just update reference data without scanning
    python main.py collect --region TAS

    # Legacy: check specific properties
    python main.py check --region TAS

    # Show database stats
    python main.py stats
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
import folium

from utils.database import Database
from utils.geocoding import Geocoder

from scrapers.pharmacies import PharmacyScraper
from scrapers.gps import GPScraper
from scrapers.supermarkets import SupermarketScraper
from scrapers.hospitals import HospitalScraper
from scrapers.shopping_centres import ShoppingCentreScraper
from scrapers.commercial_re import CommercialREScraper

from scanner.zone_scanner import ZoneScanner
from scanner.output import generate_csv, generate_map, print_summary

from rules.item_130 import Item130Rule
from rules.item_131 import Item131Rule
from rules.item_132 import Item132Rule
from rules.item_133 import Item133Rule
from rules.item_134 import Item134Rule
from rules.item_134a import Item134ARule
from rules.item_135 import Item135Rule
from rules.item_136 import Item136Rule

import config


# ==================================================================
#  Core orchestrator
# ==================================================================

class PharmacyLocationFinder:
    def __init__(self, db_path: str = None):
        self.db = Database(db_path or config.DATABASE_PATH)
        self.db.connect()
        self.geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, self.db)

        # Scrapers
        self.pharmacy_scraper = PharmacyScraper(self.db, self.geocoder)
        self.gp_scraper = GPScraper(self.db, self.geocoder)
        self.supermarket_scraper = SupermarketScraper(self.db, self.geocoder)
        self.hospital_scraper = HospitalScraper(self.db, self.geocoder)
        self.shopping_centre_scraper = ShoppingCentreScraper(self.db, self.geocoder)
        self.property_scraper = CommercialREScraper(self.db, self.geocoder)

        # Zone scanner (new)
        self.scanner = ZoneScanner(self.db)

        # Legacy rules (for property checking)
        self.rules = [
            Item130Rule(self.db),
            Item131Rule(self.db),
            Item132Rule(self.db),
            Item133Rule(self.db),
            Item134Rule(self.db),
            Item134ARule(self.db),
            Item135Rule(self.db),
            Item136Rule(self.db),
        ]

    # -- Data collection -------------------------------------------

    def collect_reference_data(self, region: str = 'TAS'):
        """Scrape all reference data (pharmacies, GPs, supermarkets, hospitals, shopping centres)."""
        print(f"\n{'='*60}")
        print(f"  COLLECTING REFERENCE DATA — {config.AUSTRALIAN_STATES.get(region, region)}")
        print(f"{'='*60}\n")

        self.db.clear_reference_data()

        print(f"  [1/5] Pharmacies...")
        pharmacy_count = self.pharmacy_scraper.scrape_all(region)
        print(f"         [OK] {pharmacy_count} pharmacies\n")

        print(f"  [2/5] GP practices...")
        gp_count = self.gp_scraper.scrape_all(region)
        print(f"         [OK] {gp_count} GP practices\n")

        print(f"  [3/5] Supermarkets...")
        supermarket_count = self.supermarket_scraper.scrape_all(region)
        print(f"         [OK] {supermarket_count} supermarkets\n")

        print(f"  [4/5] Hospitals...")
        hospital_count = self.hospital_scraper.scrape_all(region)
        print(f"         [OK] {hospital_count} hospitals\n")

        print(f"  [5/5] Shopping centres...")
        shopping_centre_count = self.shopping_centre_scraper.scrape_all(region)
        print(f"         [OK] {shopping_centre_count} shopping centres\n")

        print(f"  {'-'*56}")
        print(f"  Pharmacies:        {pharmacy_count}")
        print(f"  GPs:               {gp_count}")
        print(f"  Supermarkets:      {supermarket_count}")
        print(f"  Hospitals:         {hospital_count}")
        print(f"  Shopping centres:  {shopping_centre_count}")
        print(f"  {'-'*56}\n")

        return {
            'pharmacies': pharmacy_count,
            'gps': gp_count,
            'supermarkets': supermarket_count,
            'hospitals': hospital_count,
            'shopping_centres': shopping_centre_count,
        }

    # -- Zone scanning (NEW) ---------------------------------------

    def scan_opportunities(self, region: str = 'TAS', output_dir: str = None):
        """
        Run the zone scanner to find opportunity zones.
        Returns list of opportunity dicts.
        """
        if output_dir is None:
            output_dir = config.OUTPUT_DIR
        os.makedirs(output_dir, exist_ok=True)

        print(f"\n{'='*60}")
        print(f"  SCANNING FOR OPPORTUNITY ZONES — {config.AUSTRALIAN_STATES.get(region, region)}")
        print(f"{'='*60}")

        opportunities = self.scanner.scan(region=region, verbose=True)

        # Fetch from DB (has date_scanned, etc.)
        opp_dicts = self.db.get_all_opportunities(region)

        # Outputs
        csv_path = os.path.join(output_dir, f'opportunity_zones_{region}.csv')
        generate_csv(opp_dicts, csv_path)
        print(f"\n  [OK] CSV saved: {csv_path}")

        map_path = os.path.join(output_dir, f'opportunity_zones_{region}.html')
        generate_map(opp_dicts, self.db, map_path, region=region)
        print(f"  [OK] Map saved: {map_path}")

        print_summary(opp_dicts, region)

        return opp_dicts

    # -- Property cross-reference (optional add-on to scan) --------

    def cross_reference_properties(self, region: str = 'TAS', limit: int = 100,
                                    output_dir: str = None):
        """
        After scanning opportunity zones, scrape commercial RE listings and
        find properties that fall inside (or near) those zones.
        """
        if output_dir is None:
            output_dir = config.OUTPUT_DIR

        print(f"\n{'='*60}")
        print(f"  CROSS-REFERENCING WITH COMMERCIAL PROPERTIES")
        print(f"{'='*60}\n")

        self.db.clear_properties()

        count = self.property_scraper.scrape_all(region, limit)
        if count == 0:
            print("  No properties found via web scraping — generating samples...")
            count = self.property_scraper.generate_sample_properties(region, min(limit, 25))

        print(f"\n  Properties collected: {count}")

        # Check each property against rules (legacy flow)
        self._check_properties_against_rules()

        # Generate legacy outputs
        self._generate_property_outputs(output_dir)

    # -- Legacy property checking ----------------------------------

    def _check_properties_against_rules(self):
        """Check all properties in the DB against eligibility rules."""
        properties = self.db.get_all_properties()
        if not properties:
            print("  No properties to check.")
            return

        print(f"\n  Checking {len(properties)} properties against {len(self.rules)} rules...")

        eligible_count = 0
        for i, prop in enumerate(properties, 1):
            matched = False
            for rule in self.rules:
                try:
                    ok, evidence = rule.check_eligibility(prop)
                    if ok:
                        self.db.insert_eligible_property(prop['id'], rule.item_number, evidence)
                        matched = True
                except Exception:
                    pass
            if matched:
                eligible_count += 1

        print(f"  Eligible properties: {eligible_count}/{len(properties)}")

    def _generate_property_outputs(self, output_dir: str):
        """Generate legacy CSV + map for property-level results."""
        eligible = self.db.get_eligible_properties()
        if not eligible:
            print("  No eligible properties to output.")
            return

        csv_path = os.path.join(output_dir, config.CSV_OUTPUT_FILE)
        rows = []
        for prop in eligible:
            rows.append({
                'Address': prop['address'],
                'Latitude': prop['latitude'],
                'Longitude': prop['longitude'],
                'Listing URL': prop.get('listing_url') or '',
                'Qualifying Rules': prop['qualifying_rules'],
                'Evidence': prop['evidence'],
                'Agent Name': prop.get('agent_name') or '',
                'Agent Phone': prop.get('agent_phone') or '',
                'Agent Email': prop.get('agent_email') or '',
                'Date Checked': datetime.now().strftime('%Y-%m-%d'),
            })
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        print(f"  [OK] Property CSV: {csv_path}")

    # -- Stats -----------------------------------------------------

    def show_stats(self):
        stats = self.db.get_reference_data_stats()
        print(f"\n{'='*60}")
        print("  DATABASE STATISTICS")
        print(f"{'='*60}")
        for table, count in stats.items():
            label = table.replace('_', ' ').title()
            print(f"  {label:.<30} {count}")
        props = len(self.db.get_all_properties())
        eligible = len(self.db.get_eligible_properties())
        print(f"  {'Properties':.<30} {props}")
        print(f"  {'Eligible Properties':.<30} {eligible}")
        print(f"{'='*60}\n")

    def close(self):
        self.db.close()


# ==================================================================
#  CLI
# ==================================================================

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Pharmacy Location Finder — discover where new pharmacies '
                    'can be opened under Australian Pharmacy Location Rules',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python main.py scan --region TAS          # scan Tasmania for opportunities
  python main.py scan --region TAS --with-properties  # also find RE listings
  python main.py collect --region VIC       # just collect reference data
  python main.py check --region TAS         # legacy property-checking mode
  python main.py stats                      # show DB stats
        """,
    )

    sub = parser.add_subparsers(dest='command', help='Command to run')

    # -- scan ------------------------------------------------------
    p_scan = sub.add_parser(
        'scan',
        help='Collect reference data then scan for opportunity zones (recommended)',
    )
    p_scan.add_argument('--region', default='TAS',
                        help='State/territory code (default: TAS)')
    p_scan.add_argument('--output-dir', default=config.OUTPUT_DIR,
                        help='Output directory')
    p_scan.add_argument('--skip-collect', action='store_true',
                        help='Skip data collection (use existing DB)')
    p_scan.add_argument('--with-properties', action='store_true',
                        help='Also scrape commercial RE and cross-reference')
    p_scan.add_argument('--property-limit', type=int, default=100,
                        help='Max properties to scrape (default: 100)')

    # -- collect ---------------------------------------------------
    p_collect = sub.add_parser(
        'collect',
        help='Collect reference data only (pharmacies, GPs, supermarkets, hospitals)',
    )
    p_collect.add_argument('--region', default='TAS')

    # -- check (legacy) --------------------------------------------
    p_check = sub.add_parser(
        'check',
        help='Legacy: collect data, scrape properties, check eligibility',
    )
    p_check.add_argument('--region', default='TAS')
    p_check.add_argument('--output-dir', default=config.OUTPUT_DIR)
    p_check.add_argument('--limit', type=int, default=100)
    p_check.add_argument('--use-samples', action='store_true')

    # -- stats -----------------------------------------------------
    sub.add_parser('stats', help='Show database statistics')

    # -- Backward-compatible flat flags ----------------------------
    parser.add_argument('--all', action='store_true',
                        help='(Legacy) Run scan with default settings')
    parser.add_argument('--region', default=None,
                        help='(Legacy) Region override')
    parser.add_argument('--stats', action='store_true',
                        help='(Legacy) Show stats')
    parser.add_argument('--update-reference-data', action='store_true',
                        help='(Legacy) Collect reference data')
    parser.add_argument('--scrape-properties', action='store_true',
                        help='(Legacy) Scrape properties')
    parser.add_argument('--check-eligibility', action='store_true',
                        help='(Legacy) Check eligibility')
    parser.add_argument('--generate-outputs', action='store_true',
                        help='(Legacy) Generate outputs')
    parser.add_argument('--output-dir', default=None)
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--use-samples', action='store_true')

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    # Validate config
    missing, optional = config.validate_config()
    if missing:
        print("ERROR: Missing required configuration:")
        for item in missing:
            print(f"  - {item}")
        sys.exit(1)

    # Resolve legacy flat flags to subcommands
    command = args.command
    if not command:
        if args.all:
            command = 'scan'
        elif getattr(args, 'stats', False):
            command = 'stats'
        elif args.update_reference_data:
            command = 'collect'
        elif args.scrape_properties or args.check_eligibility or args.generate_outputs:
            command = 'check'

    if not command:
        parser.print_help()
        return

    # Determine region (subcommand arg or legacy flag)
    region = getattr(args, 'region', None) or 'TAS'

    # Header
    print("\n" + "=" * 60)
    print("  PHARMACY LOCATION FINDER")
    print(f"  Region: {config.AUSTRALIAN_STATES.get(region, region)}")
    print(f"  Mode:   {command}")
    print("=" * 60)

    if optional:
        print("\n  Optional keys not configured (using free alternatives):")
        for item in optional:
            print(f"    - {item}")

    finder = PharmacyLocationFinder()

    try:
        if command == 'scan':
            skip = getattr(args, 'skip_collect', False)
            if not skip:
                finder.collect_reference_data(region)
            output_dir = getattr(args, 'output_dir', None) or config.OUTPUT_DIR
            finder.scan_opportunities(region, output_dir)
            if getattr(args, 'with_properties', False):
                limit = getattr(args, 'property_limit', 100) or 100
                finder.cross_reference_properties(region, limit, output_dir)
            finder.show_stats()

        elif command == 'collect':
            finder.collect_reference_data(region)
            finder.show_stats()

        elif command == 'check':
            output_dir = getattr(args, 'output_dir', None) or config.OUTPUT_DIR
            limit = getattr(args, 'limit', None) or 100
            use_samples = getattr(args, 'use_samples', False)
            finder.collect_reference_data(region)
            finder.cross_reference_properties(region, limit, output_dir)
            finder.show_stats()

        elif command == 'stats':
            finder.show_stats()

    finally:
        finder.close()


if __name__ == '__main__':
    main()
