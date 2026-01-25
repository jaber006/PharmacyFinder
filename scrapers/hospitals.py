"""
Scraper for hospital locations with bed count data.
"""
import requests
import time
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


class HospitalScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'NSW') -> int:
        """
        Scrape all hospital locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of hospitals scraped
        """
        count = 0

        print("Scraping MyHospitals data...")
        count += self.scrape_myhospitals(region)

        print(f"Total hospitals scraped: {count}")
        return count

    def scrape_myhospitals(self, region: str = 'NSW') -> int:
        """
        Scrape hospital data from MyHospitals API.

        Args:
            region: State/territory to search

        Returns:
            Number of hospitals scraped
        """
        count = 0

        if not config.MYHOSPITALS_API_KEY:
            print("WARNING: MYHOSPITALS_API_KEY not configured.")
            print("Note: Hospital data requires manual collection or API access.")
            print("Please visit: https://www.myhospitals.gov.au/")
            return 0

        try:
            base_url = f"{config.API_ENDPOINTS['myhospitals']}/hospitals"

            params = {
                'api_key': config.MYHOSPITALS_API_KEY,
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
                    print(f"Error fetching hospitals: HTTP {response.status_code}")
                    break

                data = response.json()
                results = data.get('results', [])

                if not results:
                    break

                for hospital in results:
                    hospital_data = self._parse_hospital(hospital)
                    if hospital_data:
                        # Only include hospitals with 100+ beds
                        if hospital_data.get('bed_count', 0) >= config.HOSPITAL_BED_COUNT:
                            self.db.insert_hospital(hospital_data)
                            count += 1

                # Check if there are more results
                if len(results) < params['limit']:
                    break

                offset += params['limit']
                time.sleep(config.SCRAPER_CONFIG['rate_limit_delay'])

        except Exception as e:
            print(f"Error scraping MyHospitals: {e}")

        return count

    def _parse_hospital(self, data: Dict) -> Optional[Dict]:
        """
        Parse hospital data from API response.

        Args:
            data: Raw API response data

        Returns:
            Parsed hospital data or None
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

            # Extract bed count
            bed_count = data.get('bed_count') or data.get('beds', 0)
            if isinstance(bed_count, str):
                try:
                    bed_count = int(bed_count)
                except (ValueError, TypeError):
                    bed_count = 0

            # Extract hospital type
            hospital_type = data.get('hospital_type', 'public')

            # Only include public hospitals
            if hospital_type.lower() != 'public':
                return None

            return {
                'name': data.get('name', 'Hospital'),
                'address': full_address,
                'latitude': latitude,
                'longitude': longitude,
                'bed_count': bed_count,
                'hospital_type': hospital_type
            }

        except Exception as e:
            print(f"Error parsing hospital data: {e}")
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

    def add_manual_hospital(
        self,
        name: str,
        address: str,
        bed_count: int,
        hospital_type: str = 'public'
    ) -> bool:
        """
        Manually add a hospital to the database.

        Args:
            name: Hospital name
            address: Full address
            bed_count: Number of beds
            hospital_type: Type of hospital (default: 'public')

        Returns:
            True if successfully added
        """
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode address: {address}")
                return False

            latitude, longitude = coords

            hospital_data = {
                'name': name,
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'bed_count': bed_count,
                'hospital_type': hospital_type
            }

            self.db.insert_hospital(hospital_data)
            print(f"Added hospital: {name} at {address} ({bed_count} beds)")
            return True

        except Exception as e:
            print(f"Error adding manual hospital: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """
        Import hospitals from a CSV file.

        CSV should have columns: name, address, bed_count
        Optionally: latitude, longitude, hospital_type

        Args:
            csv_path: Path to CSV file

        Returns:
            Number of hospitals imported
        """
        import csv

        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    bed_count = row.get('bed_count', 0)
                    hospital_type = row.get('hospital_type', 'public')
                    latitude = row.get('latitude')
                    longitude = row.get('longitude')

                    if not address:
                        continue

                    # Parse bed count
                    try:
                        bed_count = int(bed_count)
                    except (ValueError, TypeError):
                        bed_count = 0

                    # Skip if below minimum bed count
                    if bed_count < config.HOSPITAL_BED_COUNT:
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

                    hospital_data = {
                        'name': name or 'Hospital',
                        'address': address,
                        'latitude': latitude,
                        'longitude': longitude,
                        'bed_count': bed_count,
                        'hospital_type': hospital_type
                    }

                    self.db.insert_hospital(hospital_data)
                    count += 1

            print(f"Imported {count} hospitals from {csv_path}")

        except Exception as e:
            print(f"Error importing from CSV: {e}")

        return count

    def meets_bed_requirement(self, bed_count: int) -> bool:
        """
        Check if hospital meets minimum bed count requirement.

        Args:
            bed_count: Number of beds

        Returns:
            True if meets requirement (>= 100 beds)
        """
        return bed_count >= config.HOSPITAL_BED_COUNT


# Example usage and testing
if __name__ == '__main__':
    from utils.database import Database
    from utils.geocoding import Geocoder
    import config

    db = Database()
    db.connect()

    geocoder = Geocoder(config.GOOGLE_MAPS_API_KEY, db)
    scraper = HospitalScraper(db, geocoder)

    # Test with manual entry
    scraper.add_manual_hospital(
        "Royal Prince Alfred Hospital",
        "Missenden Road, Camperdown, NSW 2050",
        bed_count=600,
        hospital_type='public'
    )

    db.close()
