"""
Scraper for pharmacy locations using multiple free data sources.

Sources (in priority order):
1. Healthdirect Service Finder (public web scraping - no API key needed)
2. OpenStreetMap Overpass API (free, comprehensive)
3. Manual CSV import as fallback
"""
import requests
import time
import json
import re
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
import config


# Australian postcodes by state for targeted scraping
STATE_POSTCODE_RANGES = {
    'NSW': [(2000, 2599), (2620, 2899), (2921, 2999)],
    'VIC': [(3000, 3999), (8000, 8999)],
    'QLD': [(4000, 4999), (9000, 9999)],
    'SA':  [(5000, 5999)],
    'WA':  [(6000, 6797), (6800, 6999)],
    'TAS': [(7000, 7999)],
    'NT':  [(800, 899)],
    'ACT': [(2600, 2619), (2900, 2920)],
}

# Key suburbs in each state for Healthdirect scraping
STATE_KEY_SUBURBS = {
    'TAS': [
        'Hobart', 'Launceston', 'Devonport', 'Burnie', 'Kingston', 'Sandy Bay',
        'Glenorchy', 'New Town', 'Moonah', 'Bellerive', 'Rosny Park', 'Howrah',
        'Lindisfarne', 'Claremont', 'Bridgewater', 'Brighton', 'Sorell',
        'Ulverstone', 'Wynyard', 'Smithton', 'Queenstown', 'Scottsdale',
        'George Town', 'Longford', 'Deloraine', 'Huonville', 'New Norfolk',
        'Mowbray', 'Riverside', 'Kings Meadows', 'Prospect', 'Ravenswood',
        'Newnham', 'Invermay', 'Trevallyn', 'Somerset', 'Penguin',
        'Sheffield', 'Campbell Town', 'Oatlands', 'Triabunna', 'St Helens',
        'Dodges Ferry', 'Margate', 'Cygnet', 'Dover', 'Geeveston',
        'Rosebery', 'Zeehan', 'Strahan',
    ],
    'NSW': [
        'Sydney', 'Parramatta', 'Liverpool', 'Penrith', 'Blacktown',
        'Newcastle', 'Wollongong', 'Central Coast', 'Campbelltown',
    ],
    'VIC': [
        'Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton',
    ],
    'QLD': [
        'Brisbane', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns',
    ],
    'SA': [
        'Adelaide', 'Mount Gambier', 'Port Augusta',
    ],
    'WA': [
        'Perth', 'Mandurah', 'Bunbury', 'Geraldton',
    ],
    'NT': [
        'Darwin', 'Alice Springs', 'Katherine',
    ],
    'ACT': [
        'Canberra', 'Belconnen', 'Tuggeranong', 'Woden',
    ],
}


class PharmacyScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all pharmacy locations from available free sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of pharmacies scraped
        """
        total = 0

        # Primary: OpenStreetMap Overpass API (comprehensive, free, has coords)
        print(f"  [1/2] Scraping OpenStreetMap for pharmacies in {region}...")
        osm_count = self.scrape_osm_overpass(region)
        total += osm_count
        print(f"        Found {osm_count} pharmacies from OSM")

        # Secondary: Healthdirect Service Finder (public web, good coverage)
        print(f"  [2/2] Scraping Healthdirect Service Finder for {region}...")
        hd_count = self.scrape_healthdirect_web(region)
        total += hd_count
        print(f"        Found {hd_count} additional pharmacies from Healthdirect")

        print(f"  Total pharmacies: {total}")
        return total

    def scrape_osm_overpass(self, region: str = 'TAS') -> int:
        """
        Scrape pharmacy locations from OpenStreetMap using Overpass API.
        Free, no API key, includes coordinates.

        Args:
            region: Australian state/territory code

        Returns:
            Number of pharmacies scraped
        """
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        # Overpass QL query for pharmacies in a state
        overpass_url = "https://overpass-api.de/api/interpreter"
        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["amenity"="pharmacy"](area.state);
          way["amenity"="pharmacy"](area.state);
          relation["amenity"="pharmacy"](area.state);
        );
        out center;
        """

        try:
            print(f"        Querying Overpass API for {state_name}...")
            response = self.session.post(
                overpass_url,
                data={'data': query},
                timeout=120
            )

            if response.status_code != 200:
                print(f"        Overpass API returned HTTP {response.status_code}")
                # Try alternate server
                return self._scrape_osm_overpass_alt(region)

            data = response.json()
            elements = data.get('elements', [])
            print(f"        Overpass returned {len(elements)} elements")

            for element in elements:
                pharmacy_data = self._parse_osm_element(element, region)
                if pharmacy_data:
                    self.db.insert_pharmacy(pharmacy_data)
                    count += 1

        except requests.exceptions.Timeout:
            print("        Overpass API timed out, trying alternate server...")
            return self._scrape_osm_overpass_alt(region)
        except Exception as e:
            print(f"        Error with Overpass API: {e}")
            return self._scrape_osm_overpass_alt(region)

        return count

    def _scrape_osm_overpass_alt(self, region: str) -> int:
        """Try alternate Overpass server."""
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        alt_url = "https://overpass.kumi.systems/api/interpreter"
        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["amenity"="pharmacy"](area.state);
          way["amenity"="pharmacy"](area.state);
        );
        out center;
        """

        try:
            response = self.session.post(
                alt_url,
                data={'data': query},
                timeout=120
            )

            if response.status_code != 200:
                print(f"        Alternate Overpass also failed: HTTP {response.status_code}")
                return 0

            data = response.json()
            elements = data.get('elements', [])

            for element in elements:
                pharmacy_data = self._parse_osm_element(element, region)
                if pharmacy_data:
                    self.db.insert_pharmacy(pharmacy_data)
                    count += 1

        except Exception as e:
            print(f"        Error with alternate Overpass: {e}")

        return count

    def _parse_osm_element(self, element: Dict, region: str) -> Optional[Dict]:
        """Parse an OSM element into pharmacy data."""
        try:
            tags = element.get('tags', {})
            
            # Get coordinates
            if element.get('type') == 'node':
                lat = element.get('lat')
                lon = element.get('lon')
            else:
                # For ways/relations, use center point
                center = element.get('center', {})
                lat = center.get('lat', element.get('lat'))
                lon = center.get('lon', element.get('lon'))

            if not lat or not lon:
                return None

            name = tags.get('name', 'Unknown Pharmacy')
            
            # Build address from OSM tags
            addr_parts = []
            if tags.get('addr:housenumber'):
                addr_parts.append(tags['addr:housenumber'])
            if tags.get('addr:street'):
                addr_parts.append(tags['addr:street'])
            if tags.get('addr:suburb') or tags.get('addr:city'):
                addr_parts.append(tags.get('addr:suburb', tags.get('addr:city', '')))
            addr_parts.append(region)
            if tags.get('addr:postcode'):
                addr_parts.append(tags['addr:postcode'])

            address = ', '.join(p for p in addr_parts if p)
            if not address or address == region:
                # Use reverse geocoding as fallback (but don't hammer the API)
                address = f"{name}, {region}, Australia"

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'source': 'OpenStreetMap'
            }

        except Exception as e:
            return None

    def scrape_healthdirect_web(self, region: str = 'TAS') -> int:
        """
        Scrape pharmacy locations from Healthdirect's public web service finder.
        No API key needed - scrapes the public search endpoint.

        Args:
            region: State/territory code

        Returns:
            Number of NEW pharmacies added (not duplicates)
        """
        count = 0
        suburbs = STATE_KEY_SUBURBS.get(region, [])

        if not suburbs:
            print(f"        No suburbs configured for {region}")
            return 0

        state_name = config.AUSTRALIAN_STATES.get(region, region).lower()

        for suburb in suburbs:
            try:
                # Healthdirect Service Finder has a JSON API endpoint
                search_url = "https://api.healthdirect.gov.au/v3/services"
                params = {
                    'type': 'pharmacy',
                    'location': f"{suburb}, {region}",
                    'distance': 25,  # km radius
                    'limit': 50,
                }

                response = self.session.get(search_url, params=params, timeout=30)

                if response.status_code == 200:
                    try:
                        data = response.json()
                        services = data.get('data', data.get('services', data.get('results', [])))
                        if isinstance(services, list):
                            for svc in services:
                                pharmacy_data = self._parse_healthdirect_service(svc, region)
                                if pharmacy_data:
                                    self.db.insert_pharmacy(pharmacy_data)
                                    count += 1
                    except json.JSONDecodeError:
                        pass

                # Rate limit
                time.sleep(1.5)

            except Exception as e:
                continue

        return count

    def _parse_healthdirect_service(self, data: Dict, region: str) -> Optional[Dict]:
        """Parse a Healthdirect service record."""
        try:
            name = data.get('name', data.get('organisationName', ''))
            if not name:
                return None

            # Try to get coordinates
            lat = data.get('latitude', data.get('lat'))
            lon = data.get('longitude', data.get('lng', data.get('lon')))
            
            # Try nested location
            if not lat or not lon:
                location = data.get('location', data.get('address', {}))
                if isinstance(location, dict):
                    lat = location.get('latitude', location.get('lat'))
                    lon = location.get('longitude', location.get('lng'))

            # Build address
            addr = data.get('address', data.get('location', {}))
            if isinstance(addr, dict):
                parts = []
                for key in ['line1', 'line2', 'street', 'suburb', 'state', 'postcode']:
                    val = addr.get(key)
                    if val:
                        parts.append(str(val))
                address = ', '.join(parts) if parts else ''
            elif isinstance(addr, str):
                address = addr
            else:
                address = f"{name}, {region}"

            if not lat or not lon:
                # Geocode if we have a good address
                if address and len(address) > 10:
                    coords = self.geocoder.geocode(address)
                    if coords:
                        lat, lon = coords
                    else:
                        return None
                else:
                    return None

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'source': 'Healthdirect'
            }

        except Exception:
            return None

    def add_manual_pharmacy(self, name: str, address: str, 
                            latitude: float = None, longitude: float = None) -> bool:
        """
        Manually add a pharmacy to the database.

        Args:
            name: Pharmacy name
            address: Full address
            latitude: Optional lat (will geocode if not provided)
            longitude: Optional lon (will geocode if not provided)

        Returns:
            True if successfully added
        """
        try:
            if latitude is None or longitude is None:
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
            print(f"  Added pharmacy: {name} at ({latitude:.4f}, {longitude:.4f})")
            return True

        except Exception as e:
            print(f"Error adding manual pharmacy: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """Import pharmacies from a CSV file."""
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

                    if not latitude or not longitude:
                        coords = self.geocoder.geocode(address)
                        if coords:
                            latitude, longitude = coords
                        else:
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

        except Exception as e:
            print(f"Error importing from CSV: {e}")

        return count
