"""
Scraper for GP (General Practitioner) practice locations with FTE calculations.
"""
import requests
import time
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
from utils.distance import calculate_fte_from_hours
import config


class GPScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'NSW') -> int:
        """
        Scrape all GP practice locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of GP practices scraped
        """
        count = 0

        print("Scraping Healthdirect API for GPs...")
        count += self.scrape_healthdirect(region)

        print(f"Total GP practices scraped: {count}")
        return count

    def scrape_healthdirect(self, region: str = 'NSW') -> int:
        """
        Scrape GP practice locations using Healthdirect API.

        Args:
            region: State/territory to search

        Returns:
            Number of GP practices scraped
        """
        count = 0

        if not config.HEALTHDIRECT_API_KEY:
            print("WARNING: HEALTHDIRECT_API_KEY not configured. Skipping Healthdirect scraping.")
            return 0

        try:
            base_url = f"{config.API_ENDPOINTS['healthdirect']}/service-finder"

            params = {
                'api_key': config.HEALTHDIRECT_API_KEY,
                'service_type': 'gp',
                'state': region,
                'limit': 100,
            }

            offset = 0
            while True:
                params['offset'] = offset

                response = self.session.get(
                    base_url,
                    params=params,
                    timeout=config.SCRAPER_CONFIG['timeout']
                )

                if response.status_code != 200:
                    print(f"Error fetching GPs: HTTP {response.status_code}")
                    break

                data = response.json()
                results = data.get('results', [])

                if not results:
                    break

                for gp_practice in results:
                    gp_data = self._parse_healthdirect_gp(gp_practice)
                    if gp_data:
                        self.db.insert_gp(gp_data)
                        count += 1

                # Check if there are more results
                if len(results) < params['limit']:
                    break

                offset += params['limit']
                time.sleep(config.SCRAPER_CONFIG['rate_limit_delay'])

        except Exception as e:
            print(f"Error scraping Healthdirect for GPs: {e}")

        return count

    def _parse_healthdirect_gp(self, data: Dict) -> Optional[Dict]:
        """
        Parse GP practice data from Healthdirect API response.

        Args:
            data: Raw API response data

        Returns:
            Parsed GP practice data or None
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

            # Extract hours information
            hours_data = data.get('hours', {})
            hours_per_week = self._calculate_hours_per_week(hours_data)

            # Calculate FTE
            fte = calculate_fte_from_hours(hours_per_week)

            # Extract number of GPs if available
            num_gps = data.get('gp_count', 1)  # Default to 1 if not specified

            # If the practice has multiple GPs, we might need to create separate entries
            # or adjust the FTE calculation. For simplicity, we'll store the total FTE.

            return {
                'name': data.get('name', 'GP Practice'),
                'address': full_address,
                'latitude': latitude,
                'longitude': longitude,
                'hours_per_week': hours_per_week,
                'fte': fte
            }

        except Exception as e:
            print(f"Error parsing GP data: {e}")
            return None

    def _calculate_hours_per_week(self, hours_data: Dict) -> float:
        """
        Calculate total hours per week from hours data.

        Args:
            hours_data: Hours information from API

        Returns:
            Total hours per week
        """
        # This is a simplified calculation
        # The actual API might provide hours in different formats

        total_hours = 0.0

        # Example: hours_data might have fields like:
        # { 'monday': '9am-5pm', 'tuesday': '9am-5pm', ... }
        # or { 'hours_per_week': 38 }

        if 'hours_per_week' in hours_data:
            total_hours = float(hours_data['hours_per_week'])
        else:
            # Default assumption: if practice operates, assume full-time hours
            # This is a rough estimate and should be refined based on actual data
            total_hours = config.HOURS_PER_WEEK_FULL_TIME

        return total_hours

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

    def add_manual_gp(
        self,
        name: str,
        address: str,
        hours_per_week: float
    ) -> bool:
        """
        Manually add a GP practice to the database.

        Args:
            name: Practice name
            address: Full address
            hours_per_week: Hours operated per week

        Returns:
            True if successfully added
        """
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode address: {address}")
                return False

            latitude, longitude = coords
            fte = calculate_fte_from_hours(hours_per_week)

            gp_data = {
                'name': name,
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'hours_per_week': hours_per_week,
                'fte': fte
            }

            self.db.insert_gp(gp_data)
            print(f"Added GP practice: {name} at {address} ({fte:.2f} FTE)")
            return True

        except Exception as e:
            print(f"Error adding manual GP: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import GP practices from a CSV file.

        CSV should have columns: name, address, hours_per_week
        Optionally: latitude, longitude, fte

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of GP practices imported
        """
        import csv

        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    hours_per_week = row.get('hours_per_week', config.HOURS_PER_WEEK_FULL_TIME)
                    latitude = row.get('latitude')
                    longitude = row.get('longitude')
                    fte = row.get('fte')

                    if not address:
                        continue

                    try:
                        hours_per_week = float(hours_per_week)
                    except (ValueError, TypeError):
                        hours_per_week = config.HOURS_PER_WEEK_FULL_TIME

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

                    # Calculate FTE if not provided
                    if not fte:
                        fte = calculate_fte_from_hours(hours_per_week)
                    else:
                        fte = float(fte)

                    gp_data = {
                        'name': name or 'GP Practice',
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'hours_per_week': hours_per_week,
                        'fte': fte
                    }

                    self.db.insert_gp(gp_data)
                    count += 1

            print(f"Imported {count} GP practices from {csv_path}")

        except Exception as e:
            print(f"Error importing from CSV: {e}")

        return count

    def calculate_gp_coverage(self, lat: float, lng: float, radius_km: float = 1.5) -> float:
        """
        Calculate total FTE of GPs within a radius of a location.

        Args:
            lat, lng: Center coordinates
            radius_km: Search radius in kilometers

        Returns:
            Total FTE within radius
        """
        from utils.distance import find_within_radius

        gps = self.db.get_all_gps()
        nearby_gps = find_within_radius(lat, lng, gps, radius_km)

        total_fte = sum(gp[0]['fte'] for gp in nearby_gps if gp[0].get('fte'))

        return total_fte


# Example usage and testing
if __name__ == '__main__':
    from utils.database import Database
    from utils.geocoding import Geocoder
    import config

    db = Database()
    db.connect()

    geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, db)
    scraper = GPScraper(db, geocoder)

    # Test with manual entry
    scraper.add_manual_gp(
        "Test Medical Centre",
        "456 Pitt Street, Sydney, NSW 2000",
        hours_per_week=38.0
    )

    # Test FTE calculation
    coverage = scraper.calculate_gp_coverage(-33.8688, 151.2093, radius_km=1.5)
    print(f"GP coverage at test location: {coverage:.2f} FTE")

    db.close()
