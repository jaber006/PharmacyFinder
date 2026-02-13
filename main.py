"""
Pharmacy Location Finder - Main orchestration and CLI.

Finds commercial properties eligible for new PBS-approved pharmacies
under Australian Pharmacy Location Rules.

Usage:
    python main.py --all --region TAS
    python main.py --update-reference-data --region TAS
    python main.py --scrape-properties --region TAS
    python main.py --check-eligibility
    python main.py --generate-outputs
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
    def __init__(self, db_path: str = None):
        self.db = Database(db_path or config.DATABASE_PATH)
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

    def update_reference_data(self, region: str = 'TAS'):
        """
        Update reference data (pharmacies, GPs, supermarkets, hospitals).
        """
        print(f"\n{'='*60}")
        print(f"UPDATING REFERENCE DATA FOR {region}")
        print(f"{'='*60}\n")

        # Clear existing reference data
        print("Clearing existing reference data...")
        self.db.clear_reference_data()

        # Scrape pharmacies
        print(f"\n--- Scraping Pharmacies ({region}) ---")
        pharmacy_count = self.pharmacy_scraper.scrape_all(region)
        print(f"[OK] Collected {pharmacy_count} pharmacies\n")

        # Scrape GPs
        print(f"--- Scraping GP Practices ({region}) ---")
        gp_count = self.gp_scraper.scrape_all(region)
        print(f"[OK] Collected {gp_count} GP practices\n")

        # Scrape supermarkets
        print(f"--- Scraping Supermarkets ({region}) ---")
        supermarket_count = self.supermarket_scraper.scrape_all(region)
        print(f"[OK] Collected {supermarket_count} supermarkets\n")

        # Scrape hospitals
        print(f"--- Scraping Hospitals ({region}) ---")
        hospital_count = self.hospital_scraper.scrape_all(region)
        print(f"[OK] Collected {hospital_count} hospitals\n")

        print(f"{'='*60}")
        print("REFERENCE DATA SUMMARY")
        print(f"{'='*60}")
        print(f"  Pharmacies:   {pharmacy_count}")
        print(f"  GP Practices: {gp_count}")
        print(f"  Supermarkets: {supermarket_count}")
        print(f"  Hospitals:    {hospital_count}")
        print(f"{'='*60}\n")

    def scrape_properties(self, region: str = 'TAS', limit: int = 100, 
                          use_samples: bool = False):
        """
        Scrape commercial property listings.
        """
        print(f"\n{'='*60}")
        print(f"SCRAPING COMMERCIAL PROPERTIES ({region})")
        print(f"{'='*60}\n")

        # Clear existing properties for fresh scrape
        self.db.clear_properties()

        if use_samples:
            print("Generating sample properties for testing...")
            count = self.property_scraper.generate_sample_properties(region, limit)
            print(f"[OK] Generated {count} sample properties\n")
        else:
            count = self.property_scraper.scrape_all(region, limit)
            
            # If web scraping got nothing, fall back to samples
            if count == 0:
                print("\nWeb scraping returned 0 results. Generating sample properties...")
                count = self.property_scraper.generate_sample_properties(region, min(limit, 25))
                print(f"[OK] Generated {count} sample properties\n")

        print(f"Total properties: {count}")

    def check_eligibility(self):
        """
        Check all properties against eligibility rules.
        """
        print(f"\n{'='*60}")
        print("CHECKING PROPERTY ELIGIBILITY")
        print(f"{'='*60}\n")

        properties = self.db.get_all_properties()
        
        if not properties:
            print("No properties in database. Run --scrape-properties first.")
            return

        print(f"Checking {len(properties)} properties against {len(self.rules)} rules...\n")

        eligible_count = 0
        total_matches = 0

        for i, property_data in enumerate(properties, 1):
            address = property_data['address']
            print(f"[{i}/{len(properties)}] {address}")

            any_rule_matched = False

            for rule in self.rules:
                try:
                    is_eligible, evidence = rule.check_eligibility(property_data)

                    if is_eligible:
                        print(f"  [OK] {rule.item_number}: {evidence[:100]}...")
                        self.db.insert_eligible_property(
                            property_data['id'],
                            rule.item_number,
                            evidence
                        )
                        any_rule_matched = True
                        total_matches += 1
                except Exception as e:
                    print(f"  [!] Error checking {rule.item_number}: {e}")

            if any_rule_matched:
                eligible_count += 1
            else:
                print(f"  [X] No rules matched")

        print(f"\n{'='*60}")
        print(f"ELIGIBILITY CHECK COMPLETE")
        print(f"{'='*60}")
        print(f"  Properties checked:  {len(properties)}")
        print(f"  Eligible properties: {eligible_count}")
        print(f"  Total rule matches:  {total_matches}")
        print(f"{'='*60}\n")

    def generate_outputs(self, output_dir: str = None):
        """
        Generate CSV and HTML map outputs.
        """
        if output_dir is None:
            output_dir = config.OUTPUT_DIR

        os.makedirs(output_dir, exist_ok=True)

        print(f"\n{'='*60}")
        print("GENERATING OUTPUTS")
        print(f"{'='*60}\n")

        # Get eligible properties
        eligible_properties = self.db.get_eligible_properties()

        if not eligible_properties:
            print("No eligible properties found.")
            # Still generate a summary map showing reference data
            self._generate_reference_map(output_dir)
            return

        # Generate CSV
        csv_path = os.path.join(output_dir, config.CSV_OUTPUT_FILE)
        self._generate_csv(eligible_properties, csv_path)
        print(f"[OK] Generated CSV: {csv_path}")

        # Generate HTML map
        map_path = os.path.join(output_dir, config.MAP_OUTPUT_FILE)
        self._generate_map(eligible_properties, map_path)
        print(f"[OK] Generated Map: {map_path}")

        # Print summary
        print(f"\n--- Eligible Properties ---")
        for prop in eligible_properties:
            print(f"  [PIN] {prop['address']}")
            print(f"     Rules: {prop['qualifying_rules']}")
            print(f"     Evidence: {prop['evidence'][:120]}...")
            print()

        print(f"{'='*60}")
        print(f"OUTPUT COMPLETE - {len(eligible_properties)} eligible properties")
        print(f"{'='*60}\n")

    def _generate_csv(self, properties: list, csv_path: str):
        """Generate CSV output file."""
        rows = []

        for prop in properties:
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
                'Date Checked': datetime.now().strftime('%Y-%m-%d')
            })

        df = pd.DataFrame(rows)
        df.to_csv(csv_path, index=False)

    def _generate_map(self, properties: list, map_path: str):
        """Generate interactive HTML map with Folium."""
        # Calculate map center
        if properties:
            center_lat = sum(p['latitude'] for p in properties) / len(properties)
            center_lng = sum(p['longitude'] for p in properties) / len(properties)
        else:
            center_lat = config.MAP_CONFIG['center_lat']
            center_lng = config.MAP_CONFIG['center_lng']

        m = folium.Map(
            location=[center_lat, center_lng],
            zoom_start=8,
            tiles='OpenStreetMap'
        )

        # Feature groups for layer control
        eligible_group = folium.FeatureGroup(name='Eligible Properties')
        pharmacy_group = folium.FeatureGroup(name='Existing Pharmacies')
        gp_group = folium.FeatureGroup(name='GP Practices')

        # Add eligible properties
        for prop in properties:
            rules = prop['qualifying_rules'].split(',')
            primary_rule = rules[0].strip() if rules else 'Unknown'
            color = config.RULE_COLORS.get(primary_rule, 'gray')

            popup_html = f"""
            <div style="min-width: 250px">
            <b>[PIN] {prop['address']}</b><br><br>
            <b>Qualifying Rules:</b> {prop['qualifying_rules']}<br><br>
            <b>Evidence:</b><br>{prop['evidence'][:200]}<br><br>
            """

            if prop.get('listing_url'):
                popup_html += f'<a href="{prop["listing_url"]}" target="_blank">View Listing</a><br>'

            if prop.get('agent_name'):
                popup_html += f"<br><b>Agent:</b> {prop['agent_name']}<br>"
            if prop.get('agent_phone'):
                popup_html += f"<b>Phone:</b> {prop['agent_phone']}<br>"

            popup_html += "</div>"

            folium.Marker(
                location=[prop['latitude'], prop['longitude']],
                popup=folium.Popup(popup_html, max_width=350),
                icon=folium.Icon(color=color, icon='star', prefix='fa'),
                tooltip=f"ELIGIBLE: {prop['address'][:50]}"
            ).add_to(eligible_group)

        # Add existing pharmacies as small markers
        pharmacies = self.db.get_all_pharmacies()
        for pharm in pharmacies:
            folium.CircleMarker(
                location=[pharm['latitude'], pharm['longitude']],
                radius=4,
                color='red',
                fill=True,
                fill_color='red',
                fill_opacity=0.7,
                popup=f"[RX] {pharm.get('name', 'Pharmacy')}",
                tooltip=f"Pharmacy: {pharm.get('name', '')[:30]}"
            ).add_to(pharmacy_group)

        # Add GPs as small markers
        gps = self.db.get_all_gps()
        for gp in gps:
            folium.CircleMarker(
                location=[gp['latitude'], gp['longitude']],
                radius=3,
                color='green',
                fill=True,
                fill_color='green',
                fill_opacity=0.5,
                popup=f"[HOSP] {gp.get('name', 'GP')} ({gp.get('fte', 1.0):.1f} FTE)",
                tooltip=f"GP: {gp.get('name', '')[:30]}"
            ).add_to(gp_group)

        # Add groups to map
        eligible_group.add_to(m)
        pharmacy_group.add_to(m)
        gp_group.add_to(m)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Add legend
        legend_html = '''
        <div style="position: fixed; bottom: 50px; left: 50px; width: 280px; 
                    background-color: white; border: 2px solid grey; z-index: 9999; 
                    font-size: 13px; padding: 12px; border-radius: 5px;">
        <b>[MAP] Pharmacy Location Rules</b><br><hr style="margin: 5px 0">
        <span style="color: red">o</span> Existing Pharmacies<br>
        <span style="color: green">o</span> GP Practices<br>
        <span style="color: gold">*</span> Eligible Properties<br>
        <hr style="margin: 5px 0">
        '''

        for rule_name, color in config.RULE_COLORS.items():
            legend_html += f'<span style="color: {color}">#</span> {rule_name}<br>'

        legend_html += '</div>'
        m.get_root().html.add_child(folium.Element(legend_html))

        m.save(map_path)

    def _generate_reference_map(self, output_dir: str):
        """Generate a map showing just reference data (when no eligible properties)."""
        pharmacies = self.db.get_all_pharmacies()
        gps = self.db.get_all_gps()
        supermarkets = self.db.get_all_supermarkets()
        hospitals = self.db.get_all_hospitals()

        all_points = pharmacies + gps + supermarkets + hospitals
        if not all_points:
            print("No reference data to map.")
            return

        center_lat = sum(p['latitude'] for p in all_points) / len(all_points)
        center_lng = sum(p['longitude'] for p in all_points) / len(all_points)

        m = folium.Map(location=[center_lat, center_lng], zoom_start=8)

        for pharm in pharmacies:
            folium.CircleMarker(
                location=[pharm['latitude'], pharm['longitude']],
                radius=4, color='red', fill=True, fill_color='red',
                popup=f"[RX] {pharm.get('name', 'Pharmacy')}"
            ).add_to(m)

        for gp in gps:
            folium.CircleMarker(
                location=[gp['latitude'], gp['longitude']],
                radius=3, color='green', fill=True, fill_color='green',
                popup=f"[HOSP] {gp.get('name', 'GP')}"
            ).add_to(m)

        for sm in supermarkets:
            folium.CircleMarker(
                location=[sm['latitude'], sm['longitude']],
                radius=3, color='blue', fill=True, fill_color='blue',
                popup=f"[CART] {sm.get('name', 'Supermarket')}"
            ).add_to(m)

        for hosp in hospitals:
            folium.Marker(
                location=[hosp['latitude'], hosp['longitude']],
                popup=f"[H] {hosp.get('name', 'Hospital')} ({hosp.get('bed_count', '?')} beds)",
                icon=folium.Icon(color='red', icon='plus', prefix='fa')
            ).add_to(m)

        map_path = os.path.join(output_dir, 'reference_data_map.html')
        m.save(map_path)
        print(f"[OK] Generated reference data map: {map_path}")

    def show_stats(self):
        """Display current database statistics."""
        print(f"\n{'='*60}")
        print("DATABASE STATISTICS")
        print(f"{'='*60}")
        print(f"  Pharmacies:   {len(self.db.get_all_pharmacies())}")
        print(f"  GP Practices: {len(self.db.get_all_gps())}")
        print(f"  Supermarkets: {len(self.db.get_all_supermarkets())}")
        print(f"  Hospitals:    {len(self.db.get_all_hospitals())}")
        print(f"  Properties:   {len(self.db.get_all_properties())}")
        print(f"  Eligible:     {len(self.db.get_eligible_properties())}")
        print(f"{'='*60}\n")

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
        default='TAS',
        help='Australian state/territory (default: TAS)'
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
        '--use-samples',
        action='store_true',
        help='Use sample properties instead of web scraping (for testing)'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Run complete workflow (update data, scrape, check, generate outputs)'
    )

    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show database statistics'
    )

    args = parser.parse_args()

    # Validate configuration
    missing, optional = config.validate_config()

    if missing:
        print("ERROR: Missing required configuration:")
        for item in missing:
            print(f"  - {item}")
        sys.exit(1)

    # Header
    print("\n" + "=" * 60)
    print("PHARMACY LOCATION FINDER")
    print("  Finding eligible properties for new PBS-approved pharmacies")
    print("  Region: " + config.AUSTRALIAN_STATES.get(args.region, args.region))
    print("  Geocoding: Nominatim (OpenStreetMap) - Free, no API key!")
    print("=" * 60)

    if optional:
        print("\nOptional API keys not configured (using free alternatives):")
        for item in optional:
            print(f"  - {item}")

    # Initialize finder
    finder = PharmacyLocationFinder()

    try:
        if args.stats:
            finder.show_stats()
            return

        if args.all:
            # Run complete workflow
            finder.update_reference_data(args.region)
            finder.scrape_properties(args.region, args.limit, args.use_samples)
            finder.check_eligibility()
            finder.generate_outputs(args.output_dir)
            finder.show_stats()

        else:
            # Run individual steps
            if args.update_reference_data:
                finder.update_reference_data(args.region)

            if args.scrape_properties:
                finder.scrape_properties(args.region, args.limit, args.use_samples)

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
