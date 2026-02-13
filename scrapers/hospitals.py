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
from utils.boundaries import in_state
import config


# Known major hospitals by state with bed counts
# Source: AIHW MyHospitals public data, state health department reports
# Covers hospitals with 100+ beds — the threshold for Item 135
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
        {'name': 'Calvary Lenah Valley Hospital', 'address': '49 Augusta Road, Lenah Valley TAS 7008',
         'latitude': -42.8654, 'longitude': 147.2996, 'bed_count': 105, 'hospital_type': 'private'},
        {'name': 'Hobart Private Hospital', 'address': '478 Macquarie Street, South Hobart TAS 7004',
         'latitude': -42.8944, 'longitude': 147.3212, 'bed_count': 118, 'hospital_type': 'private'},
        {'name': 'St Lukes Private Hospital', 'address': '24 Lyttleton Street, Launceston TAS 7250',
         'latitude': -41.4389, 'longitude': 147.1411, 'bed_count': 112, 'hospital_type': 'private'},
    ],
    'NSW': [
        {'name': 'Royal Prince Alfred Hospital', 'address': 'Missenden Road, Camperdown NSW 2050',
         'latitude': -33.8891, 'longitude': 151.1826, 'bed_count': 900, 'hospital_type': 'public'},
        {'name': 'Westmead Hospital', 'address': 'Hawkesbury Road, Westmead NSW 2145',
         'latitude': -33.8049, 'longitude': 150.9876, 'bed_count': 975, 'hospital_type': 'public'},
        {'name': 'Liverpool Hospital', 'address': 'Elizabeth Street, Liverpool NSW 2170',
         'latitude': -33.9262, 'longitude': 150.9235, 'bed_count': 877, 'hospital_type': 'public'},
        {'name': 'John Hunter Hospital', 'address': 'Lookout Road, New Lambton Heights NSW 2305',
         'latitude': -32.9231, 'longitude': 151.6838, 'bed_count': 700, 'hospital_type': 'public'},
        {'name': 'Royal North Shore Hospital', 'address': 'Reserve Road, St Leonards NSW 2065',
         'latitude': -33.8220, 'longitude': 151.1934, 'bed_count': 650, 'hospital_type': 'public'},
        {'name': 'St Vincent\'s Hospital Sydney', 'address': '390 Victoria Street, Darlinghurst NSW 2010',
         'latitude': -33.8780, 'longitude': 151.2210, 'bed_count': 550, 'hospital_type': 'public'},
        {'name': 'Prince of Wales Hospital', 'address': 'Barker Street, Randwick NSW 2031',
         'latitude': -33.9078, 'longitude': 151.2400, 'bed_count': 450, 'hospital_type': 'public'},
        {'name': 'Nepean Hospital', 'address': 'Derby Street, Kingswood NSW 2747',
         'latitude': -33.7618, 'longitude': 150.6770, 'bed_count': 520, 'hospital_type': 'public'},
        {'name': 'Gosford Hospital', 'address': 'Holden Street, Gosford NSW 2250',
         'latitude': -33.4306, 'longitude': 151.3427, 'bed_count': 400, 'hospital_type': 'public'},
        {'name': 'Wollongong Hospital', 'address': 'Crown Street, Wollongong NSW 2500',
         'latitude': -34.4253, 'longitude': 150.8930, 'bed_count': 500, 'hospital_type': 'public'},
        {'name': 'Campbelltown Hospital', 'address': 'Therry Road, Campbelltown NSW 2560',
         'latitude': -34.0606, 'longitude': 150.8139, 'bed_count': 350, 'hospital_type': 'public'},
        {'name': 'Blacktown Hospital', 'address': 'Marcel Crescent, Blacktown NSW 2148',
         'latitude': -33.7700, 'longitude': 150.9100, 'bed_count': 400, 'hospital_type': 'public'},
    ],
    'VIC': [
        {'name': 'The Royal Melbourne Hospital', 'address': '300 Grattan Street, Parkville VIC 3052',
         'latitude': -37.7990, 'longitude': 144.9560, 'bed_count': 800, 'hospital_type': 'public'},
        {'name': 'Monash Medical Centre', 'address': '246 Clayton Road, Clayton VIC 3168',
         'latitude': -37.9191, 'longitude': 145.1218, 'bed_count': 640, 'hospital_type': 'public'},
        {'name': 'The Alfred Hospital', 'address': '55 Commercial Road, Melbourne VIC 3004',
         'latitude': -37.8463, 'longitude': 144.9811, 'bed_count': 600, 'hospital_type': 'public'},
        {'name': 'St Vincent\'s Hospital Melbourne', 'address': '41 Victoria Parade, Fitzroy VIC 3065',
         'latitude': -37.8093, 'longitude': 144.9749, 'bed_count': 400, 'hospital_type': 'public'},
        {'name': 'Austin Hospital', 'address': '145 Studley Road, Heidelberg VIC 3084',
         'latitude': -37.7488, 'longitude': 145.0612, 'bed_count': 400, 'hospital_type': 'public'},
        {'name': 'Frankston Hospital', 'address': 'Hastings Road, Frankston VIC 3199',
         'latitude': -38.1450, 'longitude': 145.1245, 'bed_count': 340, 'hospital_type': 'public'},
        {'name': 'Box Hill Hospital', 'address': 'Nelson Road, Box Hill VIC 3128',
         'latitude': -37.8161, 'longitude': 145.1181, 'bed_count': 400, 'hospital_type': 'public'},
        {'name': 'Dandenong Hospital', 'address': 'David Street, Dandenong VIC 3175',
         'latitude': -37.9869, 'longitude': 145.2165, 'bed_count': 350, 'hospital_type': 'public'},
        {'name': 'Geelong Hospital', 'address': 'Ryrie Street, Geelong VIC 3220',
         'latitude': -38.1515, 'longitude': 144.3578, 'bed_count': 380, 'hospital_type': 'public'},
        {'name': 'Sunshine Hospital', 'address': '176 Furlong Road, St Albans VIC 3021',
         'latitude': -37.7444, 'longitude': 144.8025, 'bed_count': 600, 'hospital_type': 'public'},
        {'name': 'Bendigo Hospital', 'address': '100 Barnard Street, Bendigo VIC 3550',
         'latitude': -36.7531, 'longitude': 144.2695, 'bed_count': 350, 'hospital_type': 'public'},
        {'name': 'Ballarat Base Hospital', 'address': '1 Drummond Street North, Ballarat VIC 3350',
         'latitude': -37.5508, 'longitude': 143.8586, 'bed_count': 300, 'hospital_type': 'public'},
    ],
    'QLD': [
        {'name': 'Royal Brisbane and Women\'s Hospital', 'address': 'Butterfield Street, Herston QLD 4006',
         'latitude': -27.4490, 'longitude': 153.0265, 'bed_count': 929, 'hospital_type': 'public'},
        {'name': 'Princess Alexandra Hospital', 'address': '199 Ipswich Road, Woolloongabba QLD 4102',
         'latitude': -27.4937, 'longitude': 153.0333, 'bed_count': 750, 'hospital_type': 'public'},
        {'name': 'Gold Coast University Hospital', 'address': '1 Hospital Boulevard, Southport QLD 4215',
         'latitude': -27.9607, 'longitude': 153.3823, 'bed_count': 750, 'hospital_type': 'public'},
        {'name': 'Townsville University Hospital', 'address': '100 Angus Smith Drive, Douglas QLD 4814',
         'latitude': -19.3201, 'longitude': 146.7274, 'bed_count': 580, 'hospital_type': 'public'},
        {'name': 'Cairns Hospital', 'address': '165 The Esplanade, Cairns North QLD 4870',
         'latitude': -16.9043, 'longitude': 145.7569, 'bed_count': 531, 'hospital_type': 'public'},
        {'name': 'Logan Hospital', 'address': 'Armstrong Road, Meadowbrook QLD 4131',
         'latitude': -27.6665, 'longitude': 153.1344, 'bed_count': 448, 'hospital_type': 'public'},
        {'name': 'Sunshine Coast University Hospital', 'address': '6 Doherty Street, Birtinya QLD 4575',
         'latitude': -26.7383, 'longitude': 153.1216, 'bed_count': 450, 'hospital_type': 'public'},
        {'name': 'Ipswich Hospital', 'address': 'Chelmsford Avenue, Ipswich QLD 4305',
         'latitude': -27.6144, 'longitude': 152.7640, 'bed_count': 350, 'hospital_type': 'public'},
        {'name': 'Redcliffe Hospital', 'address': 'Anzac Avenue, Kippa-Ring QLD 4021',
         'latitude': -27.2314, 'longitude': 153.0967, 'bed_count': 215, 'hospital_type': 'public'},
    ],
    'SA': [
        {'name': 'Royal Adelaide Hospital', 'address': 'Port Road, Adelaide SA 5000',
         'latitude': -34.9207, 'longitude': 138.5870, 'bed_count': 800, 'hospital_type': 'public'},
        {'name': 'Flinders Medical Centre', 'address': 'Flinders Drive, Bedford Park SA 5042',
         'latitude': -35.0227, 'longitude': 138.5688, 'bed_count': 580, 'hospital_type': 'public'},
        {'name': 'Lyell McEwin Hospital', 'address': 'Haydown Road, Elizabeth Vale SA 5112',
         'latitude': -34.7188, 'longitude': 138.6620, 'bed_count': 351, 'hospital_type': 'public'},
        {'name': 'The Queen Elizabeth Hospital', 'address': '28 Woodville Road, Woodville South SA 5011',
         'latitude': -34.8833, 'longitude': 138.5501, 'bed_count': 330, 'hospital_type': 'public'},
        {'name': 'Modbury Hospital', 'address': 'Smart Road, Modbury SA 5092',
         'latitude': -34.8343, 'longitude': 138.6905, 'bed_count': 150, 'hospital_type': 'public'},
    ],
    'WA': [
        {'name': 'Royal Perth Hospital', 'address': 'Wellington Street, Perth WA 6000',
         'latitude': -31.9530, 'longitude': 115.8690, 'bed_count': 450, 'hospital_type': 'public'},
        {'name': 'Fiona Stanley Hospital', 'address': '11 Robin Warren Drive, Murdoch WA 6150',
         'latitude': -32.0720, 'longitude': 115.8380, 'bed_count': 783, 'hospital_type': 'public'},
        {'name': 'Sir Charles Gairdner Hospital', 'address': 'Hospital Avenue, Nedlands WA 6009',
         'latitude': -31.9450, 'longitude': 115.7990, 'bed_count': 600, 'hospital_type': 'public'},
        {'name': 'Joondalup Health Campus', 'address': 'Grand Boulevard, Joondalup WA 6027',
         'latitude': -31.7458, 'longitude': 115.7603, 'bed_count': 489, 'hospital_type': 'public'},
        {'name': 'Bunbury Hospital', 'address': 'Bussell Highway, Bunbury WA 6230',
         'latitude': -33.3326, 'longitude': 115.6378, 'bed_count': 156, 'hospital_type': 'public'},
        {'name': 'Rockingham General Hospital', 'address': 'Elanora Drive, Cooloongup WA 6168',
         'latitude': -32.3164, 'longitude': 115.7762, 'bed_count': 200, 'hospital_type': 'public'},
    ],
    'NT': [
        {'name': 'Royal Darwin Hospital', 'address': 'Rocklands Drive, Tiwi NT 0810',
         'latitude': -12.3977, 'longitude': 130.8731, 'bed_count': 363, 'hospital_type': 'public'},
        {'name': 'Alice Springs Hospital', 'address': 'Gap Road, Alice Springs NT 0870',
         'latitude': -23.7104, 'longitude': 133.8764, 'bed_count': 186, 'hospital_type': 'public'},
    ],
    'ACT': [
        {'name': 'Canberra Hospital', 'address': 'Yamba Drive, Garran ACT 2605',
         'latitude': -35.3461, 'longitude': 149.1006, 'bed_count': 600, 'hospital_type': 'public'},
        {'name': 'Calvary Public Hospital Bruce', 'address': 'Mary Potter Circuit, Bruce ACT 2617',
         'latitude': -35.2437, 'longitude': 149.0847, 'bed_count': 250, 'hospital_type': 'public'},
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

            lat, lon = float(lat), float(lon)
            if not in_state(lat, lon, region):
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
