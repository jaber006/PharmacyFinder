"""
Pharmacy Location Finder - Main orchestration and CLI.

Finds commercial properties eligible for new PBS-approved pharmacies
under Australian Pharmacy Location Rules.
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
from scrapers.commercial_re import CommercialREScraper
from rules.item_130 import Item130Rule
from rules.item_131 import Item131Rule
from rules.item_132 import Item132Rule
from rules.item_133 import Item133Rule
from rules.item_134 import Item134Rule
from rules.item_134a import Item134ARule
from rules.item_135 import Item135Rule
from rules.item_136 import Item136Rule
import config


class PharmacyLocationFinder:
    def __init__(self):
        self.db = Database(config.DATABASE_PATH)
        self.db.connect()
        self.geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, self.db)

        # Initialize scrapers
        self.pharmacy_scraper = PharmacyScraper(self.db, self.geocoder)
        self.gp_scraper = GPScraper(self.db, self.geocoder)
        self.supermarket_scraper = SupermarketScraper(self.db, self.geocoder)
        self.hospital_scraper = HospitalScraper(self.db, self.geocoder)
        self.property_scraper = CommercialREScraper(self.db, self.geocoder)

        # Initialize rules
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

    def update_reference_data(self, region: str = 'NSW'):
        """
        Update reference data (pharmacies, GPs, supermarkets, hospitals).

        Args:
            region: State/territory to update
        """
        print(f"\n{'='*60}")
        print("UPDATING REFERENCE DATA")
        print(f"{'='*60}\n")

        # Clear existing reference data
        print("Clearing existing reference data...")
        self.db.clear_reference_data()

        # Scrape pharmacies
        print(f"\n--- Scraping Pharmacies ---")
        pharmacy_count = self.pharmacy_scraper.scrape_all(region)
        print(f"Collected {pharmacy_count} pharmacies")

        # Scrape GPs
        print(f"\n--- Scraping GP Practices ---")
        gp_count = self.gp_scraper.scrape_all(region)
        print(f"Collected {gp_count} GP practices")

        # Scrape supermarkets
        print(f"\n--- Scraping Supermarkets ---")
        supermarket_count = self.supermarket_scraper.scrape_all(region)
        print(f"Collected {supermarket_count} supermarkets")

        # Scrape hospitals
        print(f"\n--- Scraping Hospitals ---")
        hospital_count = self.hospital_scraper.scrape_all(region)
        print(f"Collected {hospital_count} hospitals")

        print(f"\n{'='*60}")
        print("REFERENCE DATA UPDATE COMPLETE")
        print(f"{'='*60}\n")

    def scrape_properties(self, region: str = 'NSW', limit: int = 100):
        """
        Scrape commercial property listings.

        Args:
            region: State/territory to search
            limit: Maximum properties to scrape
        """
        print(f"\n{'='*60}")
        print("SCRAPING COMMERCIAL PROPERTIES")
        print(f"{'='*60}\n")

        count = self.property_scraper.scrape_all(region, limit)
        print(f"\nCollected {count} properties")

    def check_eligibility(self):
        """
        Check all properties against eligibility rules.
        """
        print(f"\n{'='*60}")
        print("CHECKING PROPERTY ELIGIBILITY")
        print(f"{'='*60}\n")

        properties = self.db.get_all_properties()
        print(f"Checking {len(properties)} properties against {len(self.rules)} rules...")

        eligible_count = 0

        for i, property_data in enumerate(properties, 1):
            property_id = property_data['id']
            address = property_data['address']

            print(f"\n[{i}/{len(properties)}] {address}")

            any_rule_matched = False

            for rule in self.rules:
                is_eligible, evidence = rule.check_eligibility(property_data)

                if is_eligible:
                    print(f"  ✓ {rule.item_number}: {evidence}")
                    self.db.insert_eligible_property(
                        property_id,
                        rule.item_number,
                        evidence
                    )
                    any_rule_matched = True

            if any_rule_matched:
                eligible_count += 1
            else:
                print(f"  ✗ No rules matched")

        print(f"\n{'='*60}")
        print(f"ELIGIBILITY CHECK COMPLETE")
        print(f"Found {eligible_count} eligible properties")
        print(f"{'='*60}\n")

    def generate_outputs(self, output_dir: str = None):
        """
        Generate CSV and HTML map outputs.

        Args:
            output_dir: Output directory path
        """
        if output_dir is None:
            output_dir = config.OUTPUT_DIR

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        print(f"\n{'='*60}")
        print("GENERATING OUTPUTS")
        print(f"{'='*60}\n")

        # Get eligible properties
        eligible_properties = self.db.get_eligible_properties()

        if not eligible_properties:
            print("No eligible properties found.")
            return

        # Generate CSV
        csv_path = os.path.join(output_dir, config.CSV_OUTPUT_FILE)
        self._generate_csv(eligible_properties, csv_path)
        print(f"Generated CSV: {csv_path}")

        # Generate HTML map
        map_path = os.path.join(output_dir, config.MAP_OUTPUT_FILE)
        self._generate_map(eligible_properties, map_path)
        print(f"Generated Map: {map_path}")

        print(f"\n{'='*60}")
        print(f"OUTPUT GENERATION COMPLETE")
        print(f"{'='*60}\n")

    def _generate_csv(self, properties: list, csv_path: str):
        """
        Generate CSV output file.

        Args:
            properties: List of eligible property dicts
            csv_path: Output CSV file path
        """
        rows = []

        for prop in properties:
            rows.append({
                'Address': prop['address'],
                'Latitude': prop['latitude'],
                'Longitude': prop['longitude'],
                'Listing URL': prop['listing_url'] or '',
                'Qualifying Rules': prop['qualifying_rules'],
                'Evidence': prop['evidence'],
                'Agent Name': prop['agent_name'] or '',
                'Agent Phone': prop['agent_phone'] or '',
                'Agent Email': prop['agent_email'] or '',
                'Date Checked': datetime.now().strftime('%Y-%m-%d')
            })

        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False)

    def _generate_map(self, properties: list, map_path: str):
        """
        Generate interactive HTML map with Folium.

        Args:
            properties: List of eligible property dicts
            map_path: Output HTML file path
        """
        # Calculate map center
        if properties:
            center_lat = sum(p['latitude'] for p in properties) / len(properties)
            center_lng = sum(p['longitude'] for p in properties) / len(properties)
        else:
            center_lat = config.MAP_CONFIG['center_lat']
            center_lng = config.MAP_CONFIG['center_lng']

        # Create map
        m = folium.Map(
            location=[center_lat, center_lng],
            zoom_start=config.MAP_CONFIG['zoom_start'],
            tiles=config.MAP_CONFIG['tile_layer']
        )

        # Add markers for each property
        for prop in properties:
            rules = prop['qualifying_rules'].split(',')
            primary_rule = rules[0] if rules else 'Unknown'

            color = config.RULE_COLORS.get(primary_rule, 'gray')

            popup_html = f"""
            <b>{prop['address']}</b><br><br>
            <b>Qualifying Rules:</b> {prop['qualifying_rules']}<br><br>
            <b>Evidence:</b><br>{prop['evidence']}<br><br>
            """

            if prop['listing_url']:
                popup_html += f'<a href="{prop["listing_url"]}" target="_blank">View Listing</a><br>'

            if prop['agent_name']:
                popup_html += f"<br><b>Agent:</b> {prop['agent_name']}<br>"
            if prop['agent_phone']:
                popup_html += f"<b>Phone:</b> {prop['agent_phone']}<br>"
            if prop['agent_email']:
                popup_html += f"<b>Email:</b> {prop['agent_email']}<br>"

            folium.Marker(
                location=[prop['latitude'], prop['longitude']],
                popup=folium.Popup(popup_html, max_width=300),
                icon=folium.Icon(color=color, icon='info-sign')
            ).add_to(m)

        # Add legend
        legend_html = '''
        <div style="position: fixed; bottom: 50px; left: 50px; width: 250px; height: auto;
                    background-color: white; border:2px solid grey; z-index:9999; font-size:14px;
                    padding: 10px">
        <b>Pharmacy Location Rules</b><br>
        '''

        for rule_name, color in config.RULE_COLORS.items():
            legend_html += f'<i class="fa fa-map-marker" style="color:{color}"></i> {rule_name}<br>'

        legend_html += '</div>'

        m.get_root().html.add_child(folium.Element(legend_html))

        # Save map
        m.save(map_path)

    def close(self):
        """Close database connection."""
        self.db.close()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Pharmacy Location Finder - Find eligible properties for new PBS-approved pharmacies'
    )

    parser.add_argument(
        '--update-reference-data',
        action='store_true',
        help='Update reference data (pharmacies, GPs, supermarkets, hospitals)'
    )

    parser.add_argument(
        '--scrape-properties',
        action='store_true',
        help='Scrape commercial property listings'
    )

    parser.add_argument(
        '--check-eligibility',
        action='store_true',
        help='Check properties against eligibility rules'
    )

    parser.add_argument(
        '--generate-outputs',
        action='store_true',
        help='Generate CSV and HTML map outputs'
    )

    parser.add_argument(
        '--region',
        type=str,
        default=config.DEFAULT_REGION,
        help=f'Australian state/territory (default: {config.DEFAULT_REGION})'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default=config.OUTPUT_DIR,
        help=f'Output directory (default: {config.OUTPUT_DIR})'
    )

    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum properties to scrape (default: 100)'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Run complete workflow (update data, scrape, check, generate outputs)'
    )

    args = parser.parse_args()

    # Validate configuration
    missing, optional = config.validate_config()

    if missing:
        print("ERROR: Missing required configuration:")
        for item in missing:
            print(f"  - {item}")
        sys.exit(1)

    # Show info about geocoding
    print("=" * 60)
    print("GEOCODING: Using Nominatim (OpenStreetMap) - Free, no API key!")
    print("=" * 60)

    if optional:
        print("\nOptional API keys not configured:")
        for item in optional:
            print(f"  - {item}")
        print()

    # Initialize finder
    finder = PharmacyLocationFinder()

    try:
        if args.all:
            # Run complete workflow
            finder.update_reference_data(args.region)
            finder.scrape_properties(args.region, args.limit)
            finder.check_eligibility()
            finder.generate_outputs(args.output_dir)

        else:
            # Run individual steps
            if args.update_reference_data:
                finder.update_reference_data(args.region)

            if args.scrape_properties:
                finder.scrape_properties(args.region, args.limit)

            if args.check_eligibility:
                finder.check_eligibility()

            if args.generate_outputs:
                finder.generate_outputs(args.output_dir)

            # If no arguments provided, show help
            if not any([
                args.update_reference_data,
                args.scrape_properties,
                args.check_eligibility,
                args.generate_outputs
            ]):
                parser.print_help()

    finally:
        finder.close()


if __name__ == '__main__':
    main()
