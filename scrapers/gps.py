"""
Scraper for GP (General Practitioner) practice locations with FTE calculations.

Sources:
1. OpenStreetMap Overpass API (free, has coordinates)
2. Healthdirect web search (public, no API key)
3. Manual CSV import
"""
import requests
import time
import json
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
from utils.distance import calculate_fte_from_hours
from utils.boundaries import in_state
from utils.overpass_cache import cached_overpass_query
import config


# Key suburbs for GP searching per state
STATE_GP_SUBURBS = {
    'TAS': [
        'Hobart', 'Launceston', 'Devonport', 'Burnie', 'Kingston', 'Sandy Bay',
        'Glenorchy', 'New Town', 'Moonah', 'Bellerive', 'Rosny Park', 'Howrah',
        'Lindisfarne', 'Claremont', 'Bridgewater', 'Brighton', 'Sorell',
        'Ulverstone', 'Wynyard', 'Smithton', 'Queenstown', 'Scottsdale',
        'George Town', 'Longford', 'Deloraine', 'Huonville', 'New Norfolk',
        'Mowbray', 'Riverside', 'Kings Meadows', 'Prospect',
        'Newnham', 'Invermay', 'Somerset', 'Penguin',
        'Sheffield', 'Campbell Town', 'St Helens',
        'Dodges Ferry', 'Margate', 'Cygnet', 'Dover', 'Geeveston',
    ],
    'NSW': [
        'Sydney', 'Parramatta', 'Liverpool', 'Penrith', 'Blacktown',
        'Newcastle', 'Wollongong', 'Campbelltown', 'Hornsby',
    ],
    'VIC': [
        'Melbourne', 'Geelong', 'Ballarat', 'Bendigo', 'Shepparton',
    ],
    'QLD': [
        'Brisbane', 'Gold Coast', 'Sunshine Coast', 'Townsville', 'Cairns',
    ],
    'SA':  ['Adelaide', 'Mount Gambier', 'Port Augusta'],
    'WA':  ['Perth', 'Mandurah', 'Bunbury', 'Geraldton'],
    'NT':  ['Darwin', 'Alice Springs', 'Katherine'],
    'ACT': ['Canberra', 'Belconnen', 'Tuggeranong'],
}


class GPScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all GP practice locations from free sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of GP practices scraped
        """
        total = 0

        # Primary: OpenStreetMap (free, has coordinates)
        print(f"  [1/2] Scraping OpenStreetMap for GPs/clinics in {region}...")
        osm_count = self.scrape_osm_overpass(region)
        total += osm_count
        print(f"        Found {osm_count} GP practices from OSM")

        # Secondary: Healthdirect web (public)
        print(f"  [2/2] Scraping Healthdirect for GPs in {region}...")
        hd_count = self.scrape_healthdirect_web(region)
        total += hd_count
        print(f"        Found {hd_count} additional GP practices from Healthdirect")

        print(f"  Total GP practices: {total}")
        return total

    def scrape_osm_overpass(self, region: str = 'TAS') -> int:
        """
        Scrape GP/clinic/doctor locations from OpenStreetMap.
        Uses cached_overpass_query for reliable results with automatic
        fallback to cached data on API failure.
        """
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["amenity"="doctors"](area.state);
          way["amenity"="doctors"](area.state);
          node["amenity"="clinic"](area.state);
          way["amenity"="clinic"](area.state);
          node["healthcare"="doctor"](area.state);
          way["healthcare"="doctor"](area.state);
          node["healthcare"="clinic"](area.state);
          way["healthcare"="clinic"](area.state);
        );
        out center;
        """

        data = cached_overpass_query(
            query=query,
            cache_key=f"gps_{region}",
            session=self.session,
        )

        if data is None:
            print(f"        [ERROR] Overpass unavailable and no cache for gps_{region}")
            return 0

        elements = data.get('elements', [])
        print(f"        Overpass returned {len(elements)} elements")

        for element in elements:
            gp_data = self._parse_osm_gp(element, region)
            if gp_data:
                self.db.insert_gp(gp_data)
                count += 1

        return count

    def _parse_osm_gp(self, element: Dict, region: str) -> Optional[Dict]:
        """Parse OSM element into GP data."""
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

            lat, lon = float(lat), float(lon)
            if not in_state(lat, lon, region):
                return None

            name = tags.get('name', 'Medical Practice')

            # Skip veterinary, dentists, etc.
            healthcare = tags.get('healthcare', '').lower()
            speciality = tags.get('healthcare:speciality', '').lower()
            if 'veterinary' in name.lower() or 'vet' == name.lower()[:3]:
                return None
            if 'dentist' in name.lower() or 'dental' in name.lower():
                return None
            if healthcare == 'dentist' or speciality == 'dentistry':
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
            if tags.get('addr:postcode'):
                addr_parts.append(tags['addr:postcode'])

            address = ', '.join(p for p in addr_parts if p)
            if not address or address == region:
                address = f"{name}, {region}, Australia"

            # Default FTE: assume 1.0 per practice (conservative estimate)
            # Real FTE data would come from practice websites or AHPRA
            fte = 1.0
            hours_per_week = 38.0

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'fte': fte,
                'hours_per_week': hours_per_week,
            }

        except Exception:
            return None

    def scrape_healthdirect_web(self, region: str = 'TAS') -> int:
        """
        Scrape GP practices from Healthdirect's public web interface.
        """
        count = 0
        suburbs = STATE_GP_SUBURBS.get(region, [])

        if not suburbs:
            return 0

        for suburb in suburbs:
            try:
                search_url = "https://api.healthdirect.gov.au/v3/services"
                params = {
                    'type': 'gp',
                    'location': f"{suburb}, {region}",
                    'distance': 15,
                    'limit': 50,
                }

                response = self.session.get(search_url, params=params, timeout=30)

                if response.status_code == 200:
                    try:
                        data = response.json()
                        services = data.get('data', data.get('services', data.get('results', [])))
                        if isinstance(services, list):
                            for svc in services:
                                gp_data = self._parse_healthdirect_gp(svc, region)
                                if gp_data:
                                    self.db.insert_gp(gp_data)
                                    count += 1
                    except json.JSONDecodeError:
                        pass

                time.sleep(1.5)

            except Exception:
                continue

        return count

    def _parse_healthdirect_gp(self, data: Dict, region: str) -> Optional[Dict]:
        """Parse Healthdirect service data into GP record."""
        try:
            name = data.get('name', data.get('organisationName', ''))
            if not name:
                return None

            lat = data.get('latitude', data.get('lat'))
            lon = data.get('longitude', data.get('lng', data.get('lon')))

            if not lat or not lon:
                location = data.get('location', data.get('address', {}))
                if isinstance(location, dict):
                    lat = location.get('latitude', location.get('lat'))
                    lon = location.get('longitude', location.get('lng'))

            addr = data.get('address', data.get('location', {}))
            if isinstance(addr, dict):
                parts = []
                for key in ['line1', 'line2', 'street', 'suburb', 'state', 'postcode']:
                    val = addr.get(key)
                    if val:
                        parts.append(str(val))
                address = ', '.join(parts) if parts else f"{name}, {region}"
            elif isinstance(addr, str):
                address = addr
            else:
                address = f"{name}, {region}"

            if not lat or not lon:
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
                'fte': 1.0,
                'hours_per_week': 38.0,
            }

        except Exception:
            return None

    def add_manual_gp(self, name: str, address: str, hours_per_week: float = 38.0,
                      latitude: float = None, longitude: float = None) -> bool:
        """Manually add a GP practice."""
        try:
            if latitude is None or longitude is None:
                coords = self.geocoder.geocode(address)
                if not coords:
                    print(f"Could not geocode: {address}")
                    return False
                latitude, longitude = coords

            fte = calculate_fte_from_hours(hours_per_week)

            gp_data = {
                'name': name,
                'address': address,
                'latitude': latitude,
                'longitude': longitude,
                'hours_per_week': hours_per_week,
                'fte': fte,
            }

            self.db.insert_gp(gp_data)
            print(f"  Added GP: {name} ({fte:.1f} FTE)")
            return True

        except Exception as e:
            print(f"Error adding GP: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """Import GP practices from CSV."""
        import csv
        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    if not address:
                        continue

                    hours = float(row.get('hours_per_week', 38.0))
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

                    fte = float(row.get('fte', 0)) or calculate_fte_from_hours(hours)

                    self.db.insert_gp({
                        'name': name or 'GP Practice',
                        'address': address,
                        'latitude': lat,
                        'longitude': lon,
                        'hours_per_week': hours,
                        'fte': fte,
                    })
                    count += 1

        except Exception as e:
            print(f"Error importing CSV: {e}")

        return count
