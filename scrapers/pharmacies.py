"""
Scraper for pharmacy locations from NSW Pharmacy Register and Healthdirect API.
"""
import requests
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


class PharmacyScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'NSW') -> int:
        """
        Scrape all pharmacy locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of pharmacies scraped
        """
        count = 0

        print("Scraping NSW Pharmacy Register...")
        count += self.scrape_nsw_register()

        print("Scraping Healthdirect API...")
        count += self.scrape_healthdirect(region)

        print(f"Total pharmacies scraped: {count}")
        return count

    def scrape_nsw_register(self) -> int:
        """
        Scrape pharmacy locations from NSW Pharmacy Register.

        Note: This is a placeholder implementation. The actual NSW Pharmacy Register
        may require different scraping logic depending on their website structure.

        Returns:
            Number of pharmacies scraped
        """
        count = 0

        # NSW Pharmacy Register URL (placeholder - actual URL may differ)
        # The register might be available at: https://www.pharmacy.nsw.gov.au/
        # This would need to be updated with the actual URL and scraping logic

        print("Note: NSW Pharmacy Register scraping requires manual configuration.")
        print("Please visit the NSW Pharmacy Guild or Health Department website")
        print("to obtain the current pharmacy register.")

        # Placeholder for actual implementation
        # When implemented, this would:
        # 1. Navigate to the register page
        # 2. Parse the list of approved pharmacies
        # 3. Extract addresses
        # 4. Geocode and store in database

        return count

    def scrape_healthdirect(self, region: str = 'NSW') -> int:
        """
        Scrape pharmacy locations using Healthdirect API.

        Args:
            region: State/territory to search

        Returns:
            Number of pharmacies scraped
        """
        count = 0

        if not config.HEALTHDIRECT_API_KEY:
            print("WARNING: HEALTHDIRECT_API_KEY not configured. Skipping Healthdirect scraping.")
            return 0

        try:
            # Healthdirect Service Finder API
            # Note: This is a simplified implementation. The actual API structure
            # may differ and require authentication or different endpoints.

            base_url = f"{config.API_ENDPOINTS['healthdirect']}/service-finder"

            # Search for pharmacies in the region
            # This would typically involve pagination and multiple requests

            params = {
                'api_key': config.HEALTHDIRECT_API_KEY,
                'service_type': 'pharmacy',
                'state': region,
                'limit': 100,
            }

            offset = 0
            while True:
                params['offset'] = offset

                response = self.session.get(base_url, params=params, timeout=config.SCRAPER_CONFIG['timeout'])

                if response.status_code != 200:
                    print(f"Error fetching pharmacies: HTTP {response.status_code}")
                    break

                data = response.json()

                # Process results (structure depends on actual API)
                results = data.get('results', [])
                if not results:
                    break

                for pharmacy in results:
                    pharmacy_data = self._parse_healthdirect_pharmacy(pharmacy)
                    if pharmacy_data:
                        self.db.insert_pharmacy(pharmacy_data)
                        count += 1

                # Check if there are more results
                if len(results) < params['limit']:
                    break

                offset += params['limit']
                time.sleep(config.SCRAPER_CONFIG['rate_limit_delay'])

        except Exception as e:
            print(f"Error scraping Healthdirect: {e}")

        return count

    def _parse_healthdirect_pharmacy(self, data: Dict) -> Optional[Dict]:
        """
        Parse pharmacy data from Healthdirect API response.

        Args:
            data: Raw API response data

        Returns:
            Parsed pharmacy data or None
        """
        try:
            # Extract address
            address = data.get('address', {})
            full_address = self._format_address(address)

            if not full_address:
                return None

            # Get or geocode coordinates
            latitude = data.get('latitude')
            longitude = data.get('longitude')

            if not latitude or not longitude:
                coords = self.geocoder.geocode(full_address)
                if coords:
                    latitude, longitude = coords
                else:
                    return None

            return {
                'name': data.get('name', 'Unknown Pharmacy'),
                'address': full_address,
                'latitude': latitude,
                'longitude': longitude,
                'source': 'Healthdirect API'
            }

        except Exception as e:
            print(f"Error parsing pharmacy data: {e}")
            return None

    def _format_address(self, address_data: Dict) -> str:
        """
        Format address components into a full address string.

        Args:
            address_data: Dict with address components

        Returns:
            Formatted address string
        """
        components = []

        if 'street_number' in address_data:
            components.append(str(address_data['street_number']))

        if 'street_name' in address_data:
            components.append(address_data['street_name'])

        if 'suburb' in address_data:
            components.append(address_data['suburb'])

        if 'state' in address_data:
            components.append(address_data['state'])

        if 'postcode' in address_data:
            components.append(str(address_data['postcode']))

        if not components and 'full_address' in address_data:
            return address_data['full_address']

        return ', '.join(components)

    def add_manual_pharmacy(self, name: str, address: str) -> bool:
        """
        Manually add a pharmacy to the database.

        Args:
            name: Pharmacy name
            address: Full address

        Returns:
            True if successfully added
        """
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode address: {address}")
                return False

            latitude, longitude = coords

            pharmacy_data = {
                'name': name,
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'source': 'Manual Entry'
            }

            self.db.insert_pharmacy(pharmacy_data)
            print(f"Added pharmacy: {name} at {address}")
            return True

        except Exception as e:
            print(f"Error adding manual pharmacy: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import pharmacies from a CSV file.

        CSV should have columns: name, address
        Optionally: latitude, longitude

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of pharmacies imported
        """
        import csv

        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    latitude = row.get('latitude')
                    longitude = row.get('longitude')

                    if not address:
                        continue

                    # Geocode if coordinates not provided
                    if not latitude or not longitude:
                        coords = self.geocoder.geocode(address)
                        if coords:
                            latitude, longitude = coords
                        else:
                            print(f"Could not geocode: {address}")
                            continue
                    else:
                        latitude = float(latitude)
                        longitude = float(longitude)

                    pharmacy_data = {
                        'name': name or 'Pharmacy',
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'source': 'CSV Import'
                    }

                    self.db.insert_pharmacy(pharmacy_data)
                    count += 1

            print(f"Imported {count} pharmacies from {csv_path}")

        except Exception as e:
            print(f"Error importing from CSV: {e}")

        return count


# Example usage and testing
if __name__ == '__main__':
    from utils.database import Database
    from utils.geocoding import Geocoder
    import config

    db = Database()
    db.connect()

    geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, db)
    scraper = PharmacyScraper(db, geocoder)

    # Test with manual entry
    scraper.add_manual_pharmacy(
        "Test Pharmacy",
        "123 George Street, Sydney, NSW 2000"
    )

    db.close()
