"""
Scraper for commercial real estate listings (retail/medical tenancies for lease).
"""
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


class CommercialREScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'NSW', limit: int = 100) -> int:
        """
        Scrape commercial property listings from all sources.

        Args:
            region: Australian state/territory code
            limit: Maximum number of properties to scrape per source

        Returns:
            Number of properties scraped
        """
        count = 0

        print("Scraping commercial real estate listings...")
        print("Note: This requires manual implementation based on website structure.")
        print("The following provides a template for scraping.")

        # Clear existing properties for fresh scrape
        # self.db.clear_properties()

        return count

    def scrape_commercial_real_estate_com_au(self, region: str, limit: int = 100) -> int:
        """
        Scrape listings from commercialrealestate.com.au

        Note: This is a template implementation. Actual website structure
        may require different selectors and logic.

        Args:
            region: State to search
            limit: Max properties to scrape

        Returns:
            Number of properties scraped
        """
        count = 0

        print("Note: commercialrealestate.com.au scraping requires")
        print("website-specific selectors and may need Selenium for dynamic content.")

        # Template URL structure
        base_url = config.COMMERCIAL_RE_SITES['commercial_real_estate']['url']
        # Example: https://www.commercialrealestate.com.au/for-lease/nsw/

        return count

    def scrape_with_selenium(self, url: str) -> List[Dict]:
        """
        Scrape a page using Selenium for dynamic content.

        Args:
            url: URL to scrape

        Returns:
            List of property data dicts
        """
        properties = []

        try:
            # Set up Chrome options
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument(f'user-agent={config.SCRAPER_CONFIG["user_agent"]}')

            # Initialize driver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)

            try:
                driver.get(url)

                # Wait for content to load
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # Parse the page
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                # Extract property listings
                # This is a placeholder - actual selectors depend on website
                listings = soup.find_all('div', class_='property-listing')

                for listing in listings:
                    property_data = self._parse_listing(listing)
                    if property_data:
                        properties.append(property_data)

            finally:
                driver.quit()

        except Exception as e:
            print(f"Error with Selenium scraping: {e}")

        return properties

    def _parse_listing(self, listing_element) -> Optional[Dict]:
        """
        Parse a property listing element.

        Note: This is a template. Actual parsing depends on website structure.

        Args:
            listing_element: BeautifulSoup element

        Returns:
            Property data dict or None
        """
        try:
            # Extract address
            address_elem = listing_element.find('span', class_='address')
            if not address_elem:
                return None

            address = address_elem.get_text(strip=True)

            # Extract listing URL
            link_elem = listing_element.find('a', href=True)
            listing_url = link_elem['href'] if link_elem else None

            # Extract property type
            type_elem = listing_element.find('span', class_='property-type')
            property_type = type_elem.get_text(strip=True) if type_elem else None

            # Extract size
            size_elem = listing_element.find('span', class_='size')
            size_text = size_elem.get_text(strip=True) if size_elem else None
            size_sqm = self._parse_size(size_text) if size_text else None

            # Extract agent info
            agent_name = None
            agent_phone = None
            agent_email = None

            agent_elem = listing_element.find('div', class_='agent-details')
            if agent_elem:
                name_elem = agent_elem.find('span', class_='agent-name')
                agent_name = name_elem.get_text(strip=True) if name_elem else None

                phone_elem = agent_elem.find('a', class_='phone')
                agent_phone = phone_elem.get_text(strip=True) if phone_elem else None

                email_elem = agent_elem.find('a', class_='email')
                agent_email = email_elem.get_text(strip=True) if email_elem else None

            # Geocode address
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode: {address}")
                return None

            latitude, longitude = coords

            return {
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'listing_url': listing_url,
                'property_type': property_type,
                'size_sqm': size_sqm,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
                'agent_email': agent_email
            }

        except Exception as e:
            print(f"Error parsing listing: {e}")
            return None

    def _parse_size(self, size_text: str) -> Optional[float]:
        """
        Parse size text to extract square meters.

        Args:
            size_text: Size string (e.g., "150 sqm", "1,500 m2")

        Returns:
            Size in square meters or None
        """
        import re

        try:
            # Remove commas
            size_text = size_text.replace(',', '')

            # Extract number
            match = re.search(r'(\d+\.?\d*)', size_text)
            if match:
                return float(match.group(1))

        except Exception:
            pass

        return None

    def add_manual_property(
        self,
        address: str,
        listing_url: Optional[str] = None,
        property_type: Optional[str] = None,
        size_sqm: Optional[float] = None,
        agent_name: Optional[str] = None,
        agent_phone: Optional[str] = None,
        agent_email: Optional[str] = None
    ) -> bool:
        """
        Manually add a commercial property to the database.

        Args:
            address: Property address
            listing_url: URL to listing
            property_type: Type of property
            size_sqm: Size in square meters
            agent_name: Agent name
            agent_phone: Agent phone
            agent_email: Agent email

        Returns:
            True if successfully added
        """
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode address: {address}")
                return False

            latitude, longitude = coords

            property_data = {
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'listing_url': listing_url,
                'property_type': property_type,
                'size_sqm': size_sqm,
                'agent_name': agent_name,
                'agent_phone': agent_phone,
                'agent_email': agent_email
            }

            property_id = self.db.insert_property(property_data)
            print(f"Added property: {address} (ID: {property_id})")
            return True

        except Exception as e:
            print(f"Error adding manual property: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import commercial properties from a CSV file.

        CSV should have columns: address
        Optionally: listing_url, property_type, size_sqm, agent_name, agent_phone, agent_email

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of properties imported
        """
        import csv

        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    address = row.get('address', '')

                    if not address:
                        continue

                    # Parse size
                    size_sqm = row.get('size_sqm')
                    if size_sqm:
                        try:
                            size_sqm = float(size_sqm)
                        except (ValueError, TypeError):
                            size_sqm = None

                    # Geocode address
                    coords = self.geocoder.geocode(address)
                    if not coords:
                        print(f"Could not geocode: {address}")
                        continue

                    latitude, longitude = coords

                    property_data = {
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'listing_url': row.get('listing_url'),
                        'property_type': row.get('property_type'),
                        'size_sqm': size_sqm,
                        'agent_name': row.get('agent_name'),
                        'agent_phone': row.get('agent_phone'),
                        'agent_email': row.get('agent_email')
                    }

                    self.db.insert_property(property_data)
                    count += 1

            print(f"Imported {count} properties from {csv_path}")

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
    scraper = CommercialREScraper(db, geocoder)

    # Test with manual entry
    scraper.add_manual_property(
        address="123 Pitt Street, Sydney, NSW 2000",
        listing_url="https://example.com/listing/123",
        property_type="Retail",
        size_sqm=80.0,
        agent_name="John Smith",
        agent_phone="02 9999 9999",
        agent_email="john@agency.com.au"
    )

    db.close()
