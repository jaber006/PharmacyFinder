"""
Scraper for supermarket locations (Woolworths, Coles, ALDI, IGA).
"""
import requests
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


class SupermarketScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'NSW') -> int:
        """
        Scrape all supermarket locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of supermarkets scraped
        """
        count = 0

        print("Scraping Woolworths stores...")
        count += self.scrape_woolworths(region)

        print("Scraping Coles stores...")
        count += self.scrape_coles(region)

        print("Scraping ALDI stores...")
        count += self.scrape_aldi(region)

        print(f"Total supermarkets scraped: {count}")
        return count

    def scrape_woolworths(self, region: str = 'NSW') -> int:
        """
        Scrape Woolworths store locations.

        Args:
            region: State/territory to search

        Returns:
            Number of stores scraped
        """
        count = 0

        try:
            # Woolworths store locator
            # Note: This is a placeholder. The actual implementation would depend on
            # whether Woolworths provides an API or requires web scraping

            if config.WOOLWORTHS_API_KEY:
                # Use API if available
                print("Using Woolworths API...")
                # API implementation would go here
            else:
                # Web scraping approach
                print("Note: Woolworths store data requires manual collection or API access.")
                print("Please visit: https://www.woolworths.com.au/shop/storelocator")

        except Exception as e:
            print(f"Error scraping Woolworths: {e}")

        return count

    def scrape_coles(self, region: str = 'NSW') -> int:
        """
        Scrape Coles store locations.

        Args:
            region: State/territory to search

        Returns:
            Number of stores scraped
        """
        count = 0

        try:
            # Coles store locator
            print("Note: Coles store data requires manual collection or API access.")
            print("Please visit: https://www.coles.com.au/store-finder")

        except Exception as e:
            print(f"Error scraping Coles: {e}")

        return count

    def scrape_aldi(self, region: str = 'NSW') -> int:
        """
        Scrape ALDI store locations.

        Args:
            region: State/territory to search

        Returns:
            Number of stores scraped
        """
        count = 0

        try:
            # ALDI store locator
            print("Note: ALDI store data requires manual collection.")
            print("Please visit: https://www.aldi.com.au/en/shopping-at-aldi/store-locations/")

        except Exception as e:
            print(f"Error scraping ALDI: {e}")

        return count

    def add_manual_supermarket(
        self,
        name: str,
        address: str,
        floor_area_sqm: Optional[float] = None
    ) -> bool:
        """
        Manually add a supermarket to the database.

        Args:
            name: Supermarket name (e.g., "Woolworths", "Coles")
            address: Full address
            floor_area_sqm: Floor area in square meters (optional)

        Returns:
            True if successfully added
        """
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode address: {address}")
                return False

            latitude, longitude = coords

            supermarket_data = {
                'name': name,
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'floor_area_sqm': floor_area_sqm
            }

            self.db.insert_supermarket(supermarket_data)
            print(f"Added supermarket: {name} at {address}")
            return True

        except Exception as e:
            print(f"Error adding manual supermarket: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import supermarkets from a CSV file.

        CSV should have columns: name, address
        Optionally: latitude, longitude, floor_area_sqm

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of supermarkets imported
        """
        import csv

        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    floor_area_sqm = row.get('floor_area_sqm')
                    latitude = row.get('latitude')
                    longitude = row.get('longitude')

                    if not address:
                        continue

                    # Parse floor area
                    if floor_area_sqm:
                        try:
                            floor_area_sqm = float(floor_area_sqm)
                        except (ValueError, TypeError):
                            floor_area_sqm = None

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

                    supermarket_data = {
                        'name': name or 'Supermarket',
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'floor_area_sqm': floor_area_sqm
                    }

                    self.db.insert_supermarket(supermarket_data)
                    count += 1

            print(f"Imported {count} supermarkets from {csv_path}")

        except Exception as e:
            print(f"Error importing from CSV: {e}")

        return count

    def is_major_supermarket(self, name: str) -> bool:
        """
        Check if a supermarket is a major chain.

        Args:
            name: Supermarket name

        Returns:
            True if it's a major supermarket
        """
        name_lower = name.lower()
        return any(chain in name_lower for chain in config.MAJOR_SUPERMARKETS)

    def meets_size_requirement(self, floor_area_sqm: Optional[float]) -> bool:
        """
        Check if supermarket meets minimum size requirement (1,000 sqm).

        Args:
            floor_area_sqm: Floor area in square meters

        Returns:
            True if meets requirement
        """
        if floor_area_sqm is None:
            # If size unknown, assume major supermarkets meet requirement
            return True

        return floor_area_sqm >= config.FLOOR_AREA_THRESHOLDS['supermarket']


# Example usage and testing
if __name__ == '__main__':
    from utils.database import Database
    from utils.geocoding import Geocoder
    import config

    db = Database()
    db.connect()

    geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, db)
    scraper = SupermarketScraper(db, geocoder)

    # Test with manual entry
    scraper.add_manual_supermarket(
        "Woolworths Metro",
        "789 George Street, Sydney, NSW 2000",
        floor_area_sqm=1200.0
    )

    db.close()
