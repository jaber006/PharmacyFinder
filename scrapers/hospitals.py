"""
Scraper for hospital locations with bed count data.

Sources:
1. OpenStreetMap Overpass API (free, has coordinates)
2. AIHW MyHospitals public data
3. Manual entry/CSV import
"""
import requests
import time
import json
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


# Known major hospitals by state with bed counts (from AIHW public data)
# This is a curated list of hospitals with 100+ beds
KNOWN_HOSPITALS = {
    'TAS': [
        {'name': 'Royal Hobart Hospital', 'address': '48 Liverpool Street, Hobart TAS 7000',
         'latitude': -42.8821, 'longitude': 147.3272, 'bed_count': 550, 'hospital_type': 'public'},
        {'name': 'Launceston General Hospital', 'address': '274-280 Charles Street, Launceston TAS 7250',
         'latitude': -41.4437, 'longitude': 147.1353, 'bed_count': 300, 'hospital_type': 'public'},
        {'name': 'North West Regional Hospital', 'address': 'Parker Street, Burnie TAS 7320',
         'latitude': -41.0546, 'longitude': 145.9117, 'bed_count': 160, 'hospital_type': 'public'},
        {'name': 'Mersey Community Hospital', 'address': 'Bass Highway, Latrobe TAS 7307',
         'latitude': -41.2372, 'longitude': 146.4130, 'bed_count': 115, 'hospital_type': 'public'},
    ],
    'NSW': [
        {'name': 'Royal Prince Alfred Hospital', 'address': 'Missenden Road, Camperdown NSW 2050',
         'latitude': -33.8891, 'longitude': 151.1826, 'bed_count': 900, 'hospital_type': 'public'},
        {'name': 'Westmead Hospital', 'address': 'Hawkesbury Road, Westmead NSW 2145',
         'latitude': -33.8049, 'longitude': 150.9876, 'bed_count': 975, 'hospital_type': 'public'},
        {'name': 'Liverpool Hospital', 'address': 'Elizabeth Street, Liverpool NSW 2170',
         'latitude': -33.9262, 'longitude': 150.9235, 'bed_count': 877, 'hospital_type': 'public'},
    ],
    'VIC': [
        {'name': 'The Royal Melbourne Hospital', 'address': '300 Grattan Street, Parkville VIC 3052',
         'latitude': -37.7990, 'longitude': 144.9560, 'bed_count': 800, 'hospital_type': 'public'},
        {'name': 'Monash Medical Centre', 'address': '246 Clayton Road, Clayton VIC 3168',
         'latitude': -37.9191, 'longitude': 145.1218, 'bed_count': 640, 'hospital_type': 'public'},
    ],
    'QLD': [
        {'name': 'Royal Brisbane and Women\'s Hospital', 'address': 'Butterfield Street, Herston QLD 4006',
         'latitude': -27.4490, 'longitude': 153.0265, 'bed_count': 929, 'hospital_type': 'public'},
    ],
    'SA': [
        {'name': 'Royal Adelaide Hospital', 'address': 'Port Road, Adelaide SA 5000',
         'latitude': -34.9207, 'longitude': 138.5870, 'bed_count': 800, 'hospital_type': 'public'},
    ],
    'WA': [
        {'name': 'Royal Perth Hospital', 'address': 'Wellington Street, Perth WA 6000',
         'latitude': -31.9530, 'longitude': 115.8690, 'bed_count': 450, 'hospital_type': 'public'},
    ],
    'NT': [
        {'name': 'Royal Darwin Hospital', 'address': 'Rocklands Drive, Tiwi NT 0810',
         'latitude': -12.3977, 'longitude': 130.8731, 'bed_count': 363, 'hospital_type': 'public'},
    ],
    'ACT': [
        {'name': 'Canberra Hospital', 'address': 'Yamba Drive, Garran ACT 2605',
         'latitude': -35.3461, 'longitude': 149.1006, 'bed_count': 600, 'hospital_type': 'public'},
    ],
}


class HospitalScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all hospital locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of hospitals scraped
        """
        total = 0

        # Primary: Load known hospitals from curated data
        print(f"  [1/2] Loading known hospitals for {region}...")
        known_count = self._load_known_hospitals(region)
        total += known_count
        print(f"        Loaded {known_count} known hospitals")

        # Secondary: OSM Overpass for additional hospitals
        print(f"  [2/2] Scraping OpenStreetMap for hospitals in {region}...")
        osm_count = self.scrape_osm_overpass(region)
        total += osm_count
        print(f"        Found {osm_count} additional hospitals from OSM")

        print(f"  Total hospitals: {total}")
        return total

    def _load_known_hospitals(self, region: str) -> int:
        """Load known hospitals from curated data."""
        count = 0
        hospitals = KNOWN_HOSPITALS.get(region, [])

        for hospital in hospitals:
            if hospital.get('bed_count', 0) >= config.HOSPITAL_BED_COUNT:
                self.db.insert_hospital(hospital)
                count += 1

        return count

    def scrape_osm_overpass(self, region: str = 'TAS') -> int:
        """Scrape hospital locations from OpenStreetMap."""
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        overpass_url = "https://overpass-api.de/api/interpreter"
        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["amenity"="hospital"](area.state);
          way["amenity"="hospital"](area.state);
          relation["amenity"="hospital"](area.state);
        );
        out center;
        """

        try:
            response = self.session.post(
                overpass_url,
                data={'data': query},
                timeout=120
            )

            if response.status_code != 200:
                return 0

            data = response.json()
            elements = data.get('elements', [])

            for element in elements:
                hospital_data = self._parse_osm_hospital(element, region)
                if hospital_data:
                    # Only add if not already in known list
                    name = hospital_data['name'].lower()
                    existing = self.db.get_all_hospitals()
                    already_exists = any(
                        h['name'].lower() in name or name in h['name'].lower()
                        for h in existing
                    )
                    if not already_exists:
                        self.db.insert_hospital(hospital_data)
                        count += 1

        except Exception as e:
            print(f"        Error with Overpass: {e}")

        return count

    def _parse_osm_hospital(self, element: Dict, region: str) -> Optional[Dict]:
        """Parse OSM element into hospital data."""
        try:
            tags = element.get('tags', {})

            if element.get('type') == 'node':
                lat = element.get('lat')
                lon = element.get('lon')
            else:
                center = element.get('center', {})
                lat = center.get('lat', element.get('lat'))
                lon = center.get('lon', element.get('lon'))

            if not lat or not lon:
                return None

            name = tags.get('name', '')
            if not name:
                return None

            # Build address
            addr_parts = []
            if tags.get('addr:housenumber'):
                addr_parts.append(tags['addr:housenumber'])
            if tags.get('addr:street'):
                addr_parts.append(tags['addr:street'])
            if tags.get('addr:suburb') or tags.get('addr:city'):
                addr_parts.append(tags.get('addr:suburb', tags.get('addr:city', '')))
            addr_parts.append(region)

            address = ', '.join(p for p in addr_parts if p)
            if not address or address == region:
                address = f"{name}, {region}, Australia"

            # OSM doesn't reliably have bed counts - estimate from hospital type
            bed_count = int(tags.get('beds', 0))
            hospital_type = tags.get('operator:type', 'unknown')

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'bed_count': bed_count,
                'hospital_type': hospital_type,
            }

        except Exception:
            return None

    def add_manual_hospital(self, name: str, address: str, bed_count: int,
                            hospital_type: str = 'public') -> bool:
        """Manually add a hospital."""
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode: {address}")
                return False

            lat, lon = coords

            self.db.insert_hospital({
                'name': name,
                'address': address,
                'latitude': lat,
                'longitude': lon,
                'bed_count': bed_count,
                'hospital_type': hospital_type,
            })
            print(f"  Added hospital: {name} ({bed_count} beds)")
            return True

        except Exception as e:
            print(f"Error adding hospital: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """Import hospitals from CSV."""
        import csv
        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    bed_count = int(row.get('bed_count', 0))
                    if not address or bed_count < config.HOSPITAL_BED_COUNT:
                        continue

                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if not lat or not lon:
                        coords = self.geocoder.geocode(address)
                        if coords:
                            lat, lon = coords
                        else:
                            continue
                    else:
                        lat, lon = float(lat), float(lon)

                    self.db.insert_hospital({
                        'name': name or 'Hospital',
                        'address': address,
                        'latitude': lat,
                        'longitude': lon,
                        'bed_count': bed_count,
                        'hospital_type': row.get('hospital_type', 'public'),
                    })
                    count += 1

        except Exception as e:
            print(f"Error importing CSV: {e}")

        return count
